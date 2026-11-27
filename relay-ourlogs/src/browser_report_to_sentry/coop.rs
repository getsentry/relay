use relay_browser_reporting::SourceFile;
use relay_browser_reporting::coop::{Body, Disposition};
use relay_conventions::{attributes::*, interpolate};
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel};

use crate::browser_report_to_sentry::attributes::*;

fn create_message(body: &Body) -> String {
    let policy = &body.effective_policy;
    let enforced = body.disposition == Disposition::Enforce;

    match body.ty.as_str() {
        "navigation-to-response" if enforced => format!(
            "COOP '{policy}' moved the page to a new browsing context group when navigating to it"
        ),
        "navigation-to-response" => format!(
            "COOP '{policy}' would move the page to a new browsing context group when navigating to it, but does not due to report only configuration"
        ),
        "navigation-from-response" if enforced => format!(
            "COOP '{policy}' moved the next page to a new browsing context group when navigating away"
        ),
        "navigation-from-response" => format!(
            "COOP '{policy}' would move the next page to a new browsing context group when navigating away, but does not due to report only configuration"
        ),
        ty => match &body.property {
            Some(property) if enforced => {
                format!("COOP '{policy}' blocked access to '{property}' ({ty})")
            }
            Some(property) => format!("COOP '{policy}' would block access to '{property}' ({ty})"),
            None => format!("COOP '{policy}' violation ({ty})"),
        },
    }
}

pub fn create_log_from_coop_body(body: Body, mut attributes: Attributes) -> OurLog {
    let message = create_message(&body);

    let Body {
        ty,
        disposition,
        effective_policy,
        property,
        previous_response_url,
        next_response_url,
        opener_url,
        openee_url,
        initial_popup_url,
        other_document_url,
        referrer,
        source_file,
    } = body;

    add_attribute!(
        attributes,
        BROWSER__REPORT__REPORT_ONLY,
        disposition == Disposition::Reporting
    );
    add_string_attribute!(attributes, BROWSER__REPORT__COOP__TYPE, ty);
    add_string_attribute!(
        attributes,
        BROWSER__REPORT__COOP__EFFECTIVE_POLICY,
        effective_policy
    );

    for (key, value) in [
        (BROWSER__REPORT__COOP__PROPERTY, property),
        (
            BROWSER__REPORT__COOP__PREVIOUS_RESPONSE_URL,
            previous_response_url,
        ),
        (BROWSER__REPORT__COOP__NEXT_RESPONSE_URL, next_response_url),
        (BROWSER__REPORT__COOP__OPENER_URL, opener_url),
        (BROWSER__REPORT__COOP__OPENEE_URL, openee_url),
        (BROWSER__REPORT__COOP__INITIAL_POPUP_URL, initial_popup_url),
        (
            BROWSER__REPORT__COOP__OTHER_DOCUMENT_URL,
            other_document_url,
        ),
    ] {
        if let Some(value) = value {
            add_string_attribute!(attributes, key, value);
        }
    }

    if let Some(SourceFile {
        source_file,
        column_number,
        line_number,
    }) = source_file
    {
        add_string_attribute!(attributes, CODE__FILE__PATH, source_file);
        if let Some(column_number) = column_number {
            add_attribute!(attributes, CODE__COLUMN__NUMBER, column_number as i64);
        }
        if let Some(line_number) = line_number {
            add_attribute!(attributes, CODE__LINE__NUMBER, line_number as i64);
        }
    }

    if let Some(referrer) = referrer
        && !referrer.is_empty()
    {
        let referrer_key = interpolate::http__request__header__key("referer");
        add_attribute!(attributes, referrer_key, vec![referrer]);
    }

    OurLog {
        body: message.into(),
        level: OurLogLevel::Warn.into(),
        attributes: attributes.into(),
        ..Default::default()
    }
}
