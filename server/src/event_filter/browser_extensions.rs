//! Implements filtering for events caused by problematic browsers extensions
//!
use crate::actors::project::FilterConfig;
use regex::Regex;
use semaphore_general::protocol::{Event, Exception, Frame, JsonLenientString, Values};

use lazy_static::lazy_static;
use semaphore_general::types::{Annotated, Array};

/// Filters events originating from known problematic browser extensions.
pub fn should_filter(event: &Event, config: &FilterConfig) -> Result<(), String> {
    if !config.is_enabled {
        return Ok(());
    }

    if let Some(ex_val) = get_exception_value(event) {
        if EXTENSION_EXC_VALUES.is_match(ex_val) {
            return Err("filter browser extension value".to_string());
        }
    }
    if let Some(ex_source) = get_exception_source(event) {
        if EXTENSION_EXC_SOURCES.is_match(ex_source) {
            return Err("filter browser extension source".to_string());
        }
    }

    return Ok(());
}

fn get_first_exception(event: &Event) -> Option<&Exception> {
    return event
        .exceptions
        .value()
        .and_then(|values: &Values<Exception>| values.values.value())
        .and_then(|exceptions: &Array<Exception>| exceptions.first())
        .and_then(|a_exc: &Annotated<Exception>| a_exc.value());
}

fn get_exception_value(event: &Event) -> Option<&str> {
    return get_first_exception(event)
        .and_then(|exc: &Exception| -> Option<&JsonLenientString> { exc.value.value() })
        .map(|json| json.as_str());
}

fn get_exception_source(event: &Event) -> Option<&str> {
    return get_first_exception(event)
        .and_then(|exc| exc.stacktrace.value())
        .and_then(|stack_trace| stack_trace.frames.value())
        .and_then(|frames: &Vec<Annotated<Frame>>| frames.last())
        .and_then(|a_frame: &Annotated<Frame>| a_frame.value())
        .and_then(|frame: &Frame| frame.abs_path.value())
        .map(|abs_path: &String| abs_path.as_str());
}

lazy_static! {

    static ref EXTENSION_EXC_VALUES: Regex = Regex::new(r#"(?ix)
        top\.GLOBALS|                           # Random plugins/extensions
        # See: http://blog.errorception.com/2012/03/tale-of-unfindable-js-error.html
        originalCreateNotification|
        canvas.contentDocument|
        MyApp_RemoveAllHighlights|
        http://tt\.epicplay\.com|
        Can't\sfind\svariable:\sZiteReader|
        jigsaw\sis\snot\sdefined|
        ComboSearch\sis\snot\sdefined|
        http://loading\.retry\.widdit\.com/|
        atomicFindClose|
        fb_xd_fragment|                         # Facebook borked
        # ISP "optimizing" proxy - `Cache-Control: no-transform` seems to
        # reduce this. (thanks @acdha)
        bmi_SafeAddOnload|                      # See http://stackoverflow.com/questions/4113268
        EBCallBackMessageReceived|
         _gCrWeb|                               # See https://groups.google.com/a/chromium.org/forum/#!topic/chromium-discuss/7VU0_VvC7mE
        conduitPage|                            # See http://toolbar.conduit.com/Debveloper/HtmlAndGadget/Methods/JSInjection.aspx
        # Google Search app (iOS)
        null\sis\snot\san\sobject\s\(evaluating\s'elt.parentNode'\)|  # See: https://github.com/getsentry/raven-js/issues/756
        # Dragon Web Extension from Nuance Communications
        plugin\.setSuspendState\sis\snot\sa\sfunction               # See: https://forum.sentry.io/t/error-in-raven-js-plugin-setsuspendstate/481/
    "#).expect("Invalid browser extensions filter (Exec Vals) Regex");

    static ref EXTENSION_EXC_SOURCES: Regex = Regex::new(r#"(?ix)
        graph\.facebook\.com|                           # Facebook flakiness
        connect\.facebook\.net|                         # Facebook blocked
        eatdifferent\.com\.woopra-ns\.com|              # Woopra flakiness
        static\.woopra\.com/js/woopra\.js|
        ^chrome(-extension)?://|                        # Chrome extensions
        127\.0\.0\.1:4001/isrunning|                    # Cacaoweb
        webappstoolbarba\.texthelp\.com/|               # Other
        metrics\.itunes\.apple\.com\.edgesuite\.net/|
        kaspersky-labs\.com                             # Kaspersky Protection browser extension
    "#).expect("Invalid browser extensions filter (Exec Sources) Regex");

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_filter::util::test_utils::*;
    use semaphore_general::protocol::{RawStacktrace, Stacktrace};

    /// Returns an event with the specified exception on the last position in the stack
    fn get_event_with_exception(e: Exception) -> Event {
        Event {
            exceptions: Annotated::from(Values::<Exception> {
                values: Annotated::from(vec![
                    Annotated::from(e), // our exception
                    // some dummy exception in the stack
                    Annotated::from(Exception {
                        ..Default::default()
                    }),
                    // another dummy exception
                    Annotated::from(Exception {
                        ..Default::default()
                    }),
                ]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn get_event_with_exception_source(src: &str) -> Event {
        let ex = Exception {
            stacktrace: Annotated::from(Stacktrace(RawStacktrace {
                frames: Annotated::new(vec![Annotated::new(Frame {
                    abs_path: Annotated::new(src.to_string()),
                    ..Default::default()
                })]),
                ..Default::default()
            })),
            ..Default::default()
        };
        return get_event_with_exception(ex);
    }

    fn get_event_with_exception_value(val: &str) -> Event {
        let ex = Exception {
            value: Annotated::from(JsonLenientString::from(val.to_string())),
            ..Default::default()
        };

        return get_event_with_exception(ex);
    }

    #[test]
    fn it_should_not_filter_events_when_disabled() {
        for ref event_ref in [
            get_event_with_exception_source("https://fscr.kaspersky-labs.com/B-9B72-7B7/main.js"),
            get_event_with_exception_value("fb_xd_fragment"),
        ]
        .iter()
        {
            let filter_result = should_filter(event_ref, &get_f_config(false));
            assert_eq!(
                filter_result,
                Ok(()),
                "Event filtered although filter should have been disabled"
            )
        }
    }

    #[test]
    fn it_should_filter_events_with_known_browser_extension_source() {
        for source_name in [
            "https://graph.facebook.com/",
            "https://connect.facebook.net/en_US/sdk.js",
            "https://eatdifferent.com.woopra-ns.com/main.js",
            "https://static.woopra.com/js/woopra.js",
            "chrome-extension://my-extension/or/something",
            "chrome://my-extension/or/something",
            "127.0.0.1:4001/isrunning",
            "webappstoolbarba.texthelp.com/",
            "http://metrics.itunes.apple.com.edgesuite.net/itunespreview/itunes/browser:firefo",
            "https://fscr.kaspersky-labs.com/B-9B72-7B7/main.js",
        ]
        .iter()
        {
            let event = get_event_with_exception_source(source_name);
            let filter_result = should_filter(&event, &get_f_config(true));

            assert_ne!(
                filter_result,
                Ok(()),
                "Event filter not recognizing events with known source {}",
                source_name
            )
        }
    }

    #[test]
    fn it_should_filter_events_with_known_browser_extension_values() {
        for exc_value in [
            "what does conduitPage even do",
            "null is not an object (evaluating 'elt.parentNode')",
            "plugin.setSuspendState is not a function",
            "some error on top.GLOBALS",
            "biiig problem on originalCreateNotification",
            "canvas.contentDocument",
            "MyApp_RemoveAllHighlights",
            "http://tt.epicplay.com/not/very/good",
            "Can't find variable: ZiteReader, I wonder why?",
            "jigsaw is not defined and I'm not happy about it",
            "ComboSearch is not defined",
            "http://loading.retry.widdit.com/some/obscure/error",
            "atomicFindClose has messed up",
            "bad news, we have a fb_xd_fragment",
            "oh no! we have a case of: bmi_SafeAddOnload, again !",
            "watch out ! EBCallBackMessageReceived",
            "error _gCrWeb",
            "conduitPage",
            "null is not an object (evaluating 'elt.parentNode')",
            "plugin.setSuspendState is not a function",
        ]
        .iter()
        {
            let event = get_event_with_exception_value(exc_value);
            let filter_result = should_filter(&event, &get_f_config(true));
            assert_ne!(
                filter_result,
                Ok(()),
                "Event filter not recognizing events with known values {}",
                exc_value
            )
        }
    }

    #[test]
    fn it_should_not_filter_events_with_unkown_browser_extenstion_source_or_value() {
        for ref event_ref in [
            get_event_with_exception_source("https://some/resonable/source.js"),
            get_event_with_exception_value("some perfectly reasonable value"),
        ]
        .iter()
        {
            let filter_result = should_filter(event_ref, &get_f_config(true));
            assert_eq!(
                filter_result,
                Ok(()),
                "Event filter although the source or value are ok "
            )
        }
    }
}
