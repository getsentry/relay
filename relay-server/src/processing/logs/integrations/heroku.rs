//! Heroku Logplex log expansion.

use relay_event_schema::protocol::OurLog;

use crate::envelope::Item;
use crate::processing::logs::{Error, Result, Settings};
use crate::services::outcome::DiscardReason;

/// The maximum amount of messages accepted in a single Logplex payload.
///
/// Logplex sends batches of around 100KB with at most 1024 buffered messages, payloads with
/// more messages are not sent by Logplex and are rejected as a whole.
const MAX_MESSAGES: usize = 10_000;

/// Expands Heroku Logplex logs into the [`OurLog`] format.
///
/// The `item` carries the Logplex request headers, see [`Item::logplex_headers`].
pub fn expand<F>(item: &Item, payload: &[u8], mut produce: F) -> Result<Settings>
where
    F: FnMut(OurLog),
{
    // Count first, without allocating, to reject oversized payloads before producing any logs.
    if relay_ourlogs::parse_logplex_frames(payload)
        .take(MAX_MESSAGES + 1)
        .count()
        > MAX_MESSAGES
    {
        relay_log::debug!("Heroku Logplex payload exceeds the maximum amount of messages");
        return Err(Error::TooLarge);
    }

    let logplex = item.logplex_headers().unwrap_or_default();
    let drain_token = logplex.drain_token.as_deref();
    let mut count: usize = 0;

    for frame in relay_ourlogs::parse_logplex_frames(payload) {
        let frame = match frame {
            Ok(frame) => frame,
            Err(err) => {
                relay_log::debug!(
                    error = &err as &dyn std::error::Error,
                    "Failed to parse Logplex frame, skipping the rest of the payload"
                );
                break;
            }
        };

        let frame = String::from_utf8_lossy(frame);
        match relay_ourlogs::parse_logplex_message(&frame) {
            Ok(message) => {
                count += 1;
                produce(relay_ourlogs::logplex_message_to_sentry_log(
                    message,
                    drain_token,
                ));
            }
            Err(err) => {
                relay_log::debug!(
                    error = &err as &dyn std::error::Error,
                    "Failed to parse Logplex message, skipping"
                );
            }
        }
    }

    if let Some(expected) = logplex.msg_count
        && usize::try_from(expected).ok() != Some(count)
    {
        relay_log::debug!(
            expected,
            count,
            "Parsed message count differs from Logplex-Msg-Count"
        );
    }

    if count == 0 {
        relay_log::debug!("Failed to parse any logs from Heroku Logplex payload");
        return Err(Error::Invalid(DiscardReason::InvalidLog));
    }

    Ok(Settings::default())
}

#[cfg(test)]
mod tests {
    use relay_protocol::Value;

    use super::*;
    use crate::envelope::ItemType;
    use crate::extractors::LogplexHeaders;

    fn expand_payload(item: &Item, payload: &str) -> Result<Vec<OurLog>> {
        let mut logs = Vec::new();
        expand(item, payload.as_bytes(), |log| logs.push(log))?;
        Ok(logs)
    }

    fn frame(msg: &str) -> String {
        format!("{} {msg}", msg.len())
    }

    #[test]
    fn test_expand() {
        let mut item = Item::new(ItemType::Integration);
        item.set_logplex_headers(LogplexHeaders {
            drain_token: Some("d.123".to_owned()),
            ..Default::default()
        });

        let payload = [
            frame("<190>1 2024-04-26T16:58:32+00:00 host app web.1 - one\n"),
            frame("not a syslog message\n"),
            frame("<45>1 2024-04-26T16:58:33+00:00 host heroku web.1 - Error R14 (Memory quota exceeded)\n"),
        ]
        .concat();

        let logs = expand_payload(&item, &payload).unwrap();
        let bodies: Vec<_> = logs.iter().map(|log| log.body.value().unwrap()).collect();
        assert_eq!(bodies, vec!["one", "Error R14 (Memory quota exceeded)"]);

        let attributes = logs[1].attributes.value().unwrap();
        assert_eq!(
            attributes
                .get_value("heroku.logplex.drain_token")
                .and_then(Value::as_str),
            Some("d.123")
        );
        assert_eq!(
            attributes
                .get_value("heroku.error.code")
                .and_then(Value::as_str),
            Some("R14")
        );
    }

    #[test]
    fn test_expand_invalid_utf8() {
        let item = Item::new(ItemType::Integration);
        let mut payload = frame("<190>1 - host app web.1 - caf\u{e9}").into_bytes();
        // Replace the `é` with an invalid continuation sequence of the same length.
        let len = payload.len();
        payload[len - 2..].copy_from_slice(b"\xff\xfe");

        let mut logs = Vec::new();
        expand(&item, &payload, |log| logs.push(log)).unwrap();
        assert_eq!(logs[0].body.value().unwrap(), "caf\u{fffd}\u{fffd}");
    }

    #[test]
    fn test_expand_stops_at_invalid_frame() {
        let item = Item::new(ItemType::Integration);
        let payload = frame("<190>1 - host app web.1 - one")
            + "garbage "
            + &frame("<190>1 - host app web.1 - two");

        let logs = expand_payload(&item, &payload).unwrap();
        assert_eq!(logs.len(), 1);
    }

    #[test]
    fn test_expand_nothing_valid() {
        let item = Item::new(ItemType::Integration);
        assert!(matches!(
            expand_payload(&item, ""),
            Err(Error::Invalid(DiscardReason::InvalidLog))
        ));
        assert!(matches!(
            expand_payload(&item, "{\"not\": \"logplex\"}"),
            Err(Error::Invalid(DiscardReason::InvalidLog))
        ));
        assert!(matches!(
            expand_payload(&item, &frame("hello")),
            Err(Error::Invalid(DiscardReason::InvalidLog))
        ));
    }

    #[test]
    fn test_expand_too_many_messages() {
        let item = Item::new(ItemType::Integration);
        let line = frame("<190>1 - - - - -");

        let payload = line.repeat(MAX_MESSAGES);
        assert_eq!(expand_payload(&item, &payload).unwrap().len(), MAX_MESSAGES);

        let payload = line.repeat(MAX_MESSAGES + 1);
        assert!(matches!(
            expand_payload(&item, &payload),
            Err(Error::TooLarge)
        ));
    }
}
