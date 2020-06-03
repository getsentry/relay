//! Implements filtering for events caused by problematic browsers extensions.

use lazy_static::lazy_static;
use regex::Regex;

use relay_general::protocol::{Event, Exception};

use crate::{FilterConfig, FilterStatKey};

/// Filters events originating from known problematic browser extensions.
pub fn should_filter(event: &Event, config: &FilterConfig) -> Result<(), FilterStatKey> {
    if !config.is_enabled {
        return Ok(());
    }

    if let Some(ex_val) = get_exception_value(event) {
        if EXTENSION_EXC_VALUES.is_match(ex_val) {
            return Err(FilterStatKey::BrowserExtensions);
        }
    }
    if let Some(ex_source) = get_exception_source(event) {
        if EXTENSION_EXC_SOURCES.is_match(ex_source) {
            return Err(FilterStatKey::BrowserExtensions);
        }
    }

    Ok(())
}

fn get_first_exception(event: &Event) -> Option<&Exception> {
    let values = event.exceptions.value()?;
    let exceptions = values.values.value()?;
    exceptions.first()?.value()
}

fn get_exception_value(event: &Event) -> Option<&str> {
    let exception = get_first_exception(event)?;
    Some(exception.value.value()?.as_str())
}

fn get_exception_source(event: &Event) -> Option<&str> {
    let exception = get_first_exception(event)?;
    let frames = exception.stacktrace.value()?.frames.value()?;
    let last_frame = frames.last()?.value()?;
    Some(last_frame.abs_path.value()?.as_str())
}

lazy_static! {
    static ref EXTENSION_EXC_VALUES: Regex = Regex::new(
        r#"(?ix)
        # Random plugins/extensions
        top\.GLOBALS|
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
        # Facebook borked
        fb_xd_fragment|
        # ISP "optimizing" proxy - `Cache-Control: no-transform` seems to
        # reduce this. (thanks @acdha)
        # See http://stackoverflow.com/questions/4113268
        bmi_SafeAddOnload|
        EBCallBackMessageReceived|
        # See https://groups.google.com/a/chromium.org/forum/#!topic/chromium-discuss/7VU0_VvC7mE
         _gCrWeb|
         # See http://toolbar.conduit.com/Debveloper/HtmlAndGadget/Methods/JSInjection.aspx
        conduitPage|
        # Google Search app (iOS)
        # See: https://github.com/getsentry/raven-js/issues/756
        null\sis\snot\san\sobject\s\(evaluating\s'elt.parentNode'\)|
        # Dragon Web Extension from Nuance Communications
        # See: https://forum.sentry.io/t/error-in-raven-js-plugin-setsuspendstate/481/
        plugin\.setSuspendState\sis\snot\sa\sfunction|
        # Chrome extension message passing failure
        Extension\scontext\sinvalidated
    "#
    )
    .expect("Invalid browser extensions filter (Exec Vals) Regex");
    static ref EXTENSION_EXC_SOURCES: Regex = Regex::new(
        r#"(?ix)
        graph\.facebook\.com|                           # Facebook flakiness
        connect\.facebook\.net|                         # Facebook blocked
        eatdifferent\.com\.woopra-ns\.com|              # Woopra flakiness
        static\.woopra\.com/js/woopra\.js|
        ^chrome(-extension)?://|                        # Chrome extensions
        ^moz-extension://|                              # Firefox extensions
        ^safari-extension://|                           # Safari extensions
        127\.0\.0\.1:4001/isrunning|                    # Cacaoweb
        webappstoolbarba\.texthelp\.com/|               # Other
        metrics\.itunes\.apple\.com\.edgesuite\.net/|
        kaspersky-labs\.com                             # Kaspersky Protection browser extension
    "#
    )
    .expect("Invalid browser extensions filter (Exec Sources) Regex");
}

#[cfg(test)]
mod tests {
    use super::*;

    use relay_general::protocol::{Frame, JsonLenientString, RawStacktrace, Stacktrace, Values};
    use relay_general::types::Annotated;

    /// Returns an event with the specified exception on the last position in the stack.
    fn get_event_with_exception(e: Exception) -> Event {
        Event {
            exceptions: Annotated::from(Values::<Exception> {
                values: Annotated::from(vec![
                    Annotated::from(e), // our exception
                    // some dummy exception in the stack
                    Annotated::from(Exception::default()),
                    // another dummy exception
                    Annotated::from(Exception::default()),
                ]),
                ..Values::default()
            }),
            ..Event::default()
        }
    }

    fn get_event_with_exception_source(src: &str) -> Event {
        let ex = Exception {
            stacktrace: Annotated::from(Stacktrace(RawStacktrace {
                frames: Annotated::new(vec![Annotated::new(Frame {
                    abs_path: Annotated::new(src.into()),
                    ..Frame::default()
                })]),
                ..RawStacktrace::default()
            })),
            ..Exception::default()
        };
        get_event_with_exception(ex)
    }

    fn get_event_with_exception_value(val: &str) -> Event {
        let ex = Exception {
            value: Annotated::from(JsonLenientString::from(val)),
            ..Exception::default()
        };

        get_event_with_exception(ex)
    }

    #[test]
    fn test_dont_filter_when_disabled() {
        let events = [
            get_event_with_exception_source("https://fscr.kaspersky-labs.com/B-9B72-7B7/main.js"),
            get_event_with_exception_value("fb_xd_fragment"),
        ];

        for event in &events {
            let filter_result = should_filter(event, &FilterConfig { is_enabled: false });
            assert_eq!(
                filter_result,
                Ok(()),
                "Event filtered although filter should have been disabled"
            )
        }
    }

    #[test]
    fn test_filter_known_browser_extension_source() {
        let sources = [
            "https://graph.facebook.com/",
            "https://connect.facebook.net/en_US/sdk.js",
            "https://eatdifferent.com.woopra-ns.com/main.js",
            "https://static.woopra.com/js/woopra.js",
            "chrome-extension://my-extension/or/something",
            "chrome://my-extension/or/something",
            "moz-extension://my-extension/or/something",
            "safari-extension://my-extension/or/something",
            "127.0.0.1:4001/isrunning",
            "webappstoolbarba.texthelp.com/",
            "http://metrics.itunes.apple.com.edgesuite.net/itunespreview/itunes/browser:firefo",
            "https://fscr.kaspersky-labs.com/B-9B72-7B7/main.js",
        ];

        for source_name in &sources {
            let event = get_event_with_exception_source(source_name);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });

            assert_ne!(
                filter_result,
                Ok(()),
                "Event filter not recognizing events with known source {}",
                source_name
            )
        }
    }

    #[test]
    fn test_filter_known_browser_extension_values() {
        let exceptions = [
            "what does conduitPage even do",
            "null is not an object (evaluating 'elt.parentNode')",
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
            "Extension context invalidated",
        ];

        for exc_value in &exceptions {
            let event = get_event_with_exception_value(exc_value);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_ne!(
                filter_result,
                Ok(()),
                "Event filter not recognizing events with known values {}",
                exc_value
            )
        }
    }

    #[test]
    fn test_dont_filter_unkown_browser_extension() {
        let events = [
            get_event_with_exception_source("https://some/resonable/source.js"),
            get_event_with_exception_value("some perfectly reasonable value"),
        ];

        for event in &events {
            let filter_result = should_filter(event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Ok(()),
                "Event filter although the source or value are ok "
            )
        }
    }
}
