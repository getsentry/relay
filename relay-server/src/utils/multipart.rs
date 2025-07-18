use std::convert::Infallible;
use std::io;
use std::task::Poll;

use axum::RequestExt;
use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use multer::{Field, Multipart};
use relay_config::Config;
use relay_quotas::DataCategory;
use relay_system::Addr;
use serde::{Deserialize, Serialize};

use crate::envelope::{AttachmentType, ContentType, Item, ItemType, Items};
use crate::extractors::{BadEventMeta, PartialDsn, Remote, RequestMeta};
use crate::service::ServiceState;
use crate::services::outcome::{
    DiscardAttachmentType, DiscardItemType, DiscardReason, Outcome, TrackOutcome,
};
use crate::utils::ApiErrorResponse;

/// Type used for encoding string lengths.
type Len = u32;

/// Serializes a Pascal-style string with a 4 byte little-endian length prefix.
fn write_string<W>(mut writer: W, string: &str) -> io::Result<()>
where
    W: io::Write,
{
    writer.write_all(&(string.len() as Len).to_le_bytes())?;
    writer.write_all(string.as_bytes())?;

    Ok(())
}

/// Safely consumes a slice of the given length.
fn split_front<'a>(data: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
    if data.len() < len {
        *data = &[];
        return None;
    }

    let (slice, rest) = data.split_at(len);
    *data = rest;
    Some(slice)
}

/// Consumes the 4-byte length prefix of a string.
fn consume_len(data: &mut &[u8]) -> Option<usize> {
    let len = std::mem::size_of::<Len>();
    let slice = split_front(data, len)?;
    let bytes = slice.try_into().ok();
    bytes.map(|b| Len::from_le_bytes(b) as usize)
}

/// Consumes a Pascal-style string with a 4 byte little-endian length prefix.
fn consume_string<'a>(data: &mut &'a [u8]) -> Option<&'a str> {
    let len = consume_len(data)?;
    let bytes = split_front(data, len)?;
    std::str::from_utf8(bytes).ok()
}

/// An entry in a serialized form data item.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FormDataEntry<'a>(&'a str, &'a str);

impl<'a> FormDataEntry<'a> {
    pub fn new(key: &'a str, value: &'a str) -> Self {
        Self(key, value)
    }

    pub fn key(&self) -> &'a str {
        self.0
    }

    pub fn value(&self) -> &'a str {
        self.1
    }

    fn to_writer<W: io::Write>(&self, mut writer: W) {
        write_string(&mut writer, self.key()).ok();
        write_string(&mut writer, self.value()).ok();
    }

    fn read(data: &mut &'a [u8]) -> Option<Self> {
        let key = consume_string(data)?;
        let value = consume_string(data)?;
        Some(Self::new(key, value))
    }
}

/// A writer for serialized form data.
///
/// This writer is used to serialize multiple plain fields from a multipart form data request into a
/// single envelope item. Use `FormDataIter` to iterate all entries.
struct FormDataWriter {
    data: Vec<u8>,
}

impl FormDataWriter {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn append(&mut self, key: &str, value: &str) {
        let entry = FormDataEntry::new(key, value);
        entry.to_writer(&mut self.data);
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.data
    }
}

/// Iterates through serialized form data written with `FormDataWriter`.
pub struct FormDataIter<'a> {
    data: &'a [u8],
}

impl<'a> FormDataIter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> Iterator for FormDataIter<'a> {
    type Item = FormDataEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.data.is_empty() {
            match FormDataEntry::read(&mut self.data) {
                Some(entry) => return Some(entry),
                None => relay_log::error!("form data deserialization failed"),
            }
        }

        None
    }
}

/// Looks for a multipart boundary at the beginning of the data
/// and returns it as a `&str` if it is found
///
/// A multipart boundary starts at the beginning of the data (possibly
/// after some blank lines) and it is prefixed by '--' (two dashes)
///
/// ```ignore
/// let boundary = get_multipart_boundary(b"--The boundary\r\n next line");
/// assert_eq!(Some("The boundary"), boundary);
///
/// let invalid_boundary = get_multipart_boundary(b"The boundary\r\n next line");
/// assert_eq!(None, invalid_boundary);
/// ```
pub fn get_multipart_boundary(data: &[u8]) -> Option<&str> {
    data.split(|&byte| byte == b'\r' || byte == b'\n')
        // Get the first non-empty line
        .find(|slice| !slice.is_empty())
        // Check for the form boundary indicator
        .filter(|slice| slice.len() > 2 && slice.starts_with(b"--"))
        // Form boundaries must be valid UTF-8 strings
        .and_then(|slice| std::str::from_utf8(&slice[2..]).ok())
}

#[derive(Debug, thiserror::Error)]
pub enum BadMultipart {
    #[error("event metadata error: {0}")]
    EventMeta(#[from] BadEventMeta),
    #[error("multipart error: {0}")]
    Multipart(#[from] multer::Error),
}

impl From<Infallible> for BadMultipart {
    fn from(infallible: Infallible) -> Self {
        match infallible {}
    }
}

impl IntoResponse for BadMultipart {
    fn into_response(self) -> Response {
        let status_code = match self {
            BadMultipart::Multipart(
                multer::Error::FieldSizeExceeded { .. } | multer::Error::StreamSizeExceeded { .. },
            ) => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::BAD_REQUEST,
        };

        (status_code, ApiErrorResponse::from_error(&self)).into_response()
    }
}

async fn multipart_items<F, G>(
    mut multipart: Multipart<'_>,
    mut infer_type: F,
    mut emit_outcome: G,
    config: &Config,
    ignore_large_fields: bool,
) -> Result<Items, multer::Error>
where
    F: FnMut(Option<&str>, &str) -> AttachmentType,
    G: FnMut(Outcome, u32),
{
    let mut items = Items::new();
    let mut form_data = FormDataWriter::new();
    let mut attachments_size = 0;

    while let Some(field) = multipart.next_field().await? {
        if let Some(file_name) = field.file_name() {
            let mut item = Item::new(ItemType::Attachment);
            item.set_attachment_type(infer_type(field.name(), file_name));
            item.set_filename(file_name);

            let content_type = field.content_type().cloned();
            let field = LimitedField::new(field, config.max_attachment_size());
            match field.bytes().await {
                Err(multer::Error::FieldSizeExceeded { limit, .. }) if ignore_large_fields => {
                    emit_outcome(
                        Outcome::Invalid(DiscardReason::TooLarge(DiscardItemType::Attachment(
                            DiscardAttachmentType::Attachment,
                        ))),
                        u32::try_from(limit).unwrap_or(u32::MAX),
                    );
                    continue;
                }
                Err(err) => return Err(err),
                Ok(bytes) => {
                    attachments_size += bytes.len();

                    if attachments_size > config.max_attachments_size() {
                        return Err(multer::Error::StreamSizeExceeded {
                            limit: config.max_attachments_size() as u64,
                        });
                    }

                    if let Some(content_type) = content_type {
                        item.set_payload(content_type.as_ref().into(), bytes);
                    } else {
                        item.set_payload_without_content_type(bytes);
                    }
                }
            }

            items.push(item);
        } else if let Some(field_name) = field.name().map(str::to_owned) {
            // Ensure to decode this SAFELY to match Django's POST data behavior. This allows us to
            // process sentry event payloads even if they contain invalid encoding.
            let string = field.text().await?;
            form_data.append(&field_name, &string);
        } else {
            relay_log::trace!("multipart content without name or file_name");
        }
    }

    let form_data = form_data.into_inner();
    if !form_data.is_empty() {
        let mut item = Item::new(ItemType::FormData);
        // Content type is `Text` (since it is not a json object but multiple
        // json arrays serialized one after the other).
        item.set_payload(ContentType::Text, form_data);
        items.push(item);
    }

    Ok(items)
}

/// Wrapper around `multer::Field` which consumes the entire underlying stream even when the
/// size limit is exceeded.
///
/// The idea being that you can process fields in a multi-part form even if one fields is too large.
struct LimitedField<'a> {
    field: Field<'a>,
    consumed_size: usize,
    size_limit: usize,
    inner_finished: bool,
}

impl<'a> LimitedField<'a> {
    fn new(field: Field<'a>, limit: usize) -> Self {
        LimitedField {
            field,
            consumed_size: 0,
            size_limit: limit,
            inner_finished: false,
        }
    }

    async fn bytes(self) -> Result<Bytes, multer::Error> {
        self.try_fold(BytesMut::new(), |mut acc, x| async move {
            acc.extend_from_slice(&x);
            Ok(acc)
        })
        .await
        .map(|x| x.freeze())
    }
}

impl futures::Stream for LimitedField<'_> {
    type Item = Result<Bytes, multer::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.inner_finished {
            return Poll::Ready(None);
        }

        match self.field.poll_next_unpin(cx) {
            err @ Poll::Ready(Some(Err(_))) => err,
            Poll::Ready(Some(Ok(t))) => {
                self.consumed_size += t.len();
                match self.consumed_size <= self.size_limit {
                    true => Poll::Ready(Some(Ok(t))),
                    false => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
            Poll::Ready(None) if self.consumed_size > self.size_limit => {
                self.inner_finished = true;
                Poll::Ready(Some(Err(multer::Error::FieldSizeExceeded {
                    limit: self.consumed_size as u64,
                    field_name: self.field.name().map(Into::into),
                })))
            }
            Poll::Ready(None) => {
                self.inner_finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Wrapper around [`multer::Multipart`] that checks each field is smaller than
/// `max_attachment_size` and that the combined size of all fields is smaller than
/// 'max_attachments_size'.
pub struct ConstrainedMultipart(pub Multipart<'static>);

impl FromRequest<ServiceState> for ConstrainedMultipart {
    type Rejection = Remote<multer::Error>;

    async fn from_request(request: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        // Still want to enforce multer limits here so that we avoid parsing large fields.
        let limits =
            multer::SizeLimit::new().whole_stream(state.config().max_attachments_size() as u64);

        multipart_from_request(request, multer::Constraints::new().size_limit(limits))
            .map(Self)
            .map_err(Remote)
    }
}

impl ConstrainedMultipart {
    pub async fn items<F>(self, infer_type: F, config: &Config) -> Result<Items, multer::Error>
    where
        F: FnMut(Option<&str>, &str) -> AttachmentType,
    {
        // The emit outcome closure here does nothing since in this code branch we don't want to
        // emit outcomes as we already return an error to the request.
        multipart_items(self.0, infer_type, |_, _| (), config, false).await
    }
}

/// Wrapper around [`multer::Multipart`] that skips over fields which are larger than
/// `max_attachment_size`. These fields are also not taken into account when checking that the
/// combined size of all fields is smaller than `max_attachments_size`.
#[allow(dead_code)]
pub struct UnconstrainedMultipart {
    multipart: Multipart<'static>,
    outcome_aggregator: Addr<TrackOutcome>,
    request_meta: RequestMeta,
}

impl FromRequest<ServiceState> for UnconstrainedMultipart {
    type Rejection = BadMultipart;

    async fn from_request(
        mut request: Request,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let mut parts = request.extract_parts().await?;
        let request_meta = RequestMeta::<PartialDsn>::from_request_parts(&mut parts, state).await?;

        let multipart = multipart_from_request(request, multer::Constraints::new())?;
        Ok(UnconstrainedMultipart {
            multipart,
            outcome_aggregator: state.outcome_aggregator().clone(),
            request_meta,
        })
    }
}

#[cfg_attr(not(any(test, sentry)), expect(dead_code))]
impl UnconstrainedMultipart {
    pub async fn items<F>(self, infer_type: F, config: &Config) -> Result<Items, multer::Error>
    where
        F: FnMut(Option<&str>, &str) -> AttachmentType,
    {
        let UnconstrainedMultipart {
            multipart,
            outcome_aggregator,
            request_meta,
        } = self;

        multipart_items(
            multipart,
            infer_type,
            |outcome, quantity| {
                outcome_aggregator.send(TrackOutcome {
                    timestamp: request_meta.received_at(),
                    scoping: request_meta.get_partial_scoping(),
                    outcome,
                    event_id: None,
                    remote_addr: request_meta.remote_addr(),
                    category: DataCategory::Attachment,
                    quantity,
                })
            },
            config,
            true,
        )
        .await
    }
}

pub fn multipart_from_request(
    request: Request,
    constraints: multer::Constraints,
) -> Result<Multipart<'static>, multer::Error> {
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let boundary = multer::parse_boundary(content_type)?;

    Ok(Multipart::with_constraints(
        request.into_body().into_data_stream(),
        boundary,
        constraints,
    ))
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    #[test]
    fn test_get_boundary() {
        let examples: &[(&[u8], Option<&str>)] = &[
            (b"--some_val", Some("some_val")),
            (b"--\nsecond line", None),
            (b"\n\r--some_val", Some("some_val")),
            (b"\n\r--some_val\nadfa", Some("some_val")),
            (b"\n\r--some_val\rfasdf", Some("some_val")),
            (b"\n\r--some_val\r\nfasdf", Some("some_val")),
            (b"\n\rsome_val", None),
            (b"", None),
            (b"--", None),
        ];

        for (input, expected) in examples {
            let boundary = get_multipart_boundary(input);
            assert_eq!(*expected, boundary);
        }
    }

    #[test]
    fn test_formdata() {
        let mut writer = FormDataWriter::new();
        writer.append("foo", "foo");
        writer.append("bar", "");
        writer.append("blub", "blub");

        let payload = writer.into_inner();
        let iter = FormDataIter::new(&payload);
        let entries: Vec<_> = iter.collect();

        assert_eq!(
            entries,
            vec![
                FormDataEntry::new("foo", "foo"),
                FormDataEntry::new("bar", ""),
                FormDataEntry::new("blub", "blub"),
            ]
        );
    }

    #[test]
    fn test_empty_formdata() {
        let writer = FormDataWriter::new();
        let payload = writer.into_inner();

        let iter = FormDataIter::new(&payload);
        let entries: Vec<_> = iter.collect();

        assert_eq!(entries, vec![]);
    }

    /// Regression test for multipart payloads without a trailing newline.
    #[tokio::test]
    async fn missing_trailing_newline() -> anyhow::Result<()> {
        let data = "--X-BOUNDARY\r\nContent-Disposition: form-data; \
        name=\"my_text_field\"\r\n\r\nabcd\r\n--X-BOUNDARY--"; // No trailing newline

        let stream = futures::stream::once(async { Ok::<_, Infallible>(data) });
        let mut multipart = Multipart::new(stream, "X-BOUNDARY");

        assert!(multipart.next_field().await?.is_some());
        assert!(multipart.next_field().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_individual_size_limit_exceeded() -> anyhow::Result<()> {
        let data = "--X-BOUNDARY\r\n\
              Content-Disposition: form-data; name=\"file\"; filename=\"large.txt\"\r\n\
              Content-Type: text/plain\r\n\
              \r\n\
              content too large for limit\r\n\
              --X-BOUNDARY\r\n\
              Content-Disposition: form-data; name=\"small_file\"; filename=\"small.txt\"\r\n\
              Content-Type: text/plain\r\n\
              \r\n\
              ok\r\n\
              --X-BOUNDARY--\r\n";

        let stream = futures::stream::once(async move { Ok::<_, Infallible>(data) });
        let multipart = Multipart::new(stream, "X-BOUNDARY");

        let config = Config::from_json_value(serde_json::json!({
            "limits": {
                "max_attachment_size": 5
            }
        }))?;

        let mut mock_outcomes = vec![];
        let items = multipart_items(
            multipart,
            |_, _| AttachmentType::Attachment,
            |_, x| (mock_outcomes.push(x)),
            &config,
            true,
        )
        .await?;

        // The large field is skipped so only the small one should make it through.
        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item.filename(), Some("small.txt"));
        assert_eq!(item.payload(), Bytes::from("ok"));
        assert_eq!(mock_outcomes, vec![27]);

        Ok(())
    }

    #[tokio::test]
    async fn test_collective_size_limit_exceeded() -> anyhow::Result<()> {
        let data = "--X-BOUNDARY\r\n\
              Content-Disposition: form-data; name=\"file\"; filename=\"large.txt\"\r\n\
              Content-Type: text/plain\r\n\
              \r\n\
              content too large for limit\r\n\
              --X-BOUNDARY\r\n\
              Content-Disposition: form-data; name=\"small_file\"; filename=\"small.txt\"\r\n\
              Content-Type: text/plain\r\n\
              \r\n\
              ok\r\n\
              --X-BOUNDARY--\r\n";

        let stream = futures::stream::once(async move { Ok::<_, Infallible>(data) });

        let config = Config::from_json_value(serde_json::json!({
            "limits": {
                "max_attachments_size": 5
            }
        }))?;

        let multipart = Multipart::new(stream, "X-BOUNDARY");

        let result = UnconstrainedMultipart {
            multipart,
            outcome_aggregator: Addr::dummy(),
            request_meta: RequestMeta::new(
                "https://a94ae32be2584e0bbd7a4cbb95971fee:@sentry.io/42"
                    .parse()
                    .unwrap(),
            ),
        }
        .items(|_, _| AttachmentType::Attachment, &config)
        .await;

        // Should be warned if the overall stream limit is being breached.
        assert!(result.is_err_and(|x| matches!(x, multer::Error::StreamSizeExceeded { limit: _ })));

        Ok(())
    }
}
