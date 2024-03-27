//! Implements filtering for events caused by problematic browsers extensions.

use once_cell::sync::Lazy;
use regex::Regex;
use relay_event_schema::protocol::{Event, Exception};

use crate::{FilterConfig, FilterStatKey};

static EXTENSION_EXC_VALUES: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
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
        Extension\scontext\sinvalidated|
        webkit-masked-url:|
        # Firefox message when an extension tries to modify a no-longer-existing DOM node
        # See https://blog.mozilla.org/addons/2012/09/12/what-does-cant-access-dead-object-mean/
        can't\saccess\sdead\sobject|
        # Cryptocurrency related extension errors solana|ethereum
        # Googletag is also very similar, caused by adblockers
        Cannot\sredefine\sproperty:\s(solana|ethereum|googletag)|
        # Translation service errors in Chrome on iOS
        undefined\sis\snot\san\sobject\s\(evaluating\s'a.L'\)
    "#,
    )
    .expect("Invalid browser extensions filter (Exec Vals) Regex")
});

static EXTENSION_EXC_SOURCES: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)
        graph\.facebook\.com|                           # Facebook flakiness
        connect\.facebook\.net|                         # Facebook blocked
        eatdifferent\.com\.woopra-ns\.com|              # Woopra flakiness
        static\.woopra\.com/js/woopra\.js|
        ^chrome(-extension)?://|                        # Chrome extensions
        ^moz-extension://|                              # Firefox extensions
        ^safari(-web)?-extension://|                    # Safari extensions
        webkit-masked-url|                              # Safari extensions
        127\.0\.0\.1:4001/isrunning|                    # Cacaoweb
        webappstoolbarba\.texthelp\.com/|               # Other
        metrics\.itunes\.apple\.com\.edgesuite\.net/|
        kaspersky-labs\.com                             # Kaspersky Protection browser extension
    ",
    )
    .expect("Invalid browser extensions filter (Exec Sources) Regex")
});

/// Frames which do not have defined function, method or type name. Or frames which come from the
/// native V8 code.
///
/// These frames do not give us any information about the exception source and can be ignored.
const ANONYMOUS_FRAMES: [&str; 2] = ["<anonymous>", "[native code]"];

/// Check if the event originates from known problematic browser extensions.
pub fn matches(event: &Event) -> bool {
    if let Some(ex_val) = get_exception_value(event) {
        if EXTENSION_EXC_VALUES.is_match(ex_val) {
            return true;
        }
    }
    if let Some(ex_source) = get_exception_source(event) {
        if EXTENSION_EXC_SOURCES.is_match(ex_source) {
            return true;
        }
    }
    false
}

/// Filters events originating from known problematic browser extensions.
pub fn should_filter(event: &Event, config: &FilterConfig) -> Result<(), FilterStatKey> {
    if !config.is_enabled {
        return Ok(());
    }

    if matches(event) {
        Err(FilterStatKey::BrowserExtensions)
    } else {
        Ok(())
    }
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
    // Iterate from the tail and get the first frame which is not anonymous.
    for f in frames.iter().rev() {
        let abs_path = f.value()?.abs_path.value()?;
        let path = abs_path.as_str();
        if !ANONYMOUS_FRAMES.contains(&path) {
            return Some(path);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{
        Frame, JsonLenientString, RawStacktrace, Stacktrace, Values,
    };
    use relay_protocol::Annotated;

    use super::*;

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
                frames: Annotated::new(vec![
                    Annotated::new(Frame {
                        abs_path: Annotated::new(src.into()),
                        ..Frame::default()
                    }),
                    Annotated::new(Frame {
                        abs_path: Annotated::new("<anonymous>".into()),
                        ..Frame::default()
                    }),
                    Annotated::new(Frame {
                        abs_path: Annotated::new("<anonymous>".into()),
                        ..Frame::default()
                    }),
                ]),
                ..RawStacktrace::default()
            })),
            ..Exception::default()
        };
        get_event_with_exception(ex)
    }

    fn get_event_with_exception_value(val: &str) -> Event {
        let ex = Exception {
            value: Annotated::from(JsonLenientString::from(val.to_string())),
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
            "safari-web-extension://my-extension/or/something",
            "127.0.0.1:4001/isrunning",
            "webappstoolbarba.texthelp.com/",
            "http://metrics.itunes.apple.com.edgesuite.net/itunespreview/itunes/browser:firefo",
            "https://fscr.kaspersky-labs.com/B-9B72-7B7/main.js",
            "webkit-masked-url:",
        ];

        for source_name in &sources {
            let event = get_event_with_exception_source(source_name);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });

            assert_ne!(
                filter_result,
                Ok(()),
                "Event filter not recognizing events with known source {source_name}"
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
            "useless error webkit-masked-url: please filter",
            "TypeError: can't access dead object because dead stuff smells bad",
            "Cannot redefine property: solana",
            "Cannot redefine property: ethereum",
            "Cannot redefine property: googletag",
            "undefined is not an object (evaluating 'a.L')",
        ];

        for exc_value in &exceptions {
            let event = get_event_with_exception_value(exc_value);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_ne!(
                filter_result,
                Ok(()),
                "Event filter not recognizing events with known value '{exc_value}'"
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
