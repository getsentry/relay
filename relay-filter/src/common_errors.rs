//! Implements filtering for events with commonly occurring error messages.
//!
//! This filter targets error messages that are typically noise and not actionable,
//! such as network errors, browser quirks, and third-party script failures.
//! These patterns are derived from the most commonly configured error message filters
//! across Sentry customers.

use std::borrow::Cow;
use std::sync::LazyLock;

use regex::Regex;

use crate::{FilterConfig, FilterStatKey, Filterable};

/// Regex patterns for common error messages that should be filtered.
///
/// These patterns match exception values (error messages) that are typically
/// noise and not actionable for developers.
static COMMON_ERROR_VALUES: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        # Network and fetch errors - these are typically caused by user network issues
        Failed\sto\sfetch|
        Network\s[Ee]rror|
        Load\sfailed|
        Network\srequest\sfailed|
        NetworkError(\swhen\sattempting\sto\sfetch\sresource)?|
        The\snetwork\sconnection\swas\slost|
        The\sInternet\sconnection\sappears\sto\sbe\soffline|
        A\snetwork\serror\soccurred|
        The\srequest\stimed\sout|
        Request\saborted|

        # AbortError - user or browser cancelled the request
        AbortError|

        # ResizeObserver errors - browser quirk, not actionable
        # See: https://developer.mozilla.org/en-US/docs/Web/API/ResizeObserver#observation_errors
        ResizeObserver\sloop\s(limit\sexceeded|completed\swith\sundelivered\snotifications)|

        # Non-Error promise rejections - often not useful for debugging
        Non-Error\spromise\srejection\scaptured|
        Non-Error\sexception\scaptured|
        Object\scaptured\sas\s(promise\srejection|exception)|

        # Quota exceeded - user's browser storage is full
        QuotaExceededError|

        # Generic timeout errors
        timeout(\sof\s\d+ms)?\sexceeded|
        TimeoutError|

        # Maximum call stack / recursion - usually infinite recursion, but message alone isn't helpful
        Maximum\scall\sstack\ssize\sexceeded|
        too\smuch\srecursion|

        # JSON parsing errors from malformed responses
        Unexpected\stoken|
        Unexpected\send\sof\s(JSON\sinput|input|script)|
        JSON\sParse\serror|
        is\snot\svalid\sJSON|

        # HTTP status code errors - too generic without context
        Request\sfailed\swith\sstatus\scode|

        # Mobile WebView and bridge errors
        Java\sobject\sis\sgone|
        Java\sbridge\smethod\sinvocation\serror|
        Java\sexception\swas\sraised\sduring\smethod\sinvocation|
        _AutofillCallbackHandler|
        instantSearchSDKJSBridgeClearHighlight|
        ceCurrentVideo\.currentTime|
        _pcmBridgeCallbackHandler|
        setIOSParameters|
        # webkit messageHandlers errors from iOS
        window\.webkit\.messageHandlers|
        # Symantec browser extension
        SymBrowser_|
        # Vietnamese Zalo app WebView
        zaloJSV2|

        # CustomEvent promise rejections
        Event\s`(CustomEvent|Event|ErrorEvent|ProgressEvent)`.*captured\sas\s(promise\srejection|exception)|

        # Frame blocking errors - browser security, not actionable
        Blocked\sa\sframe\swith\sorigin|
        SecurityError.*Blocked\sa\sframe|

        # Application not responding - mobile ANR
        ApplicationNotResponding|
        App\sHanging|

        # Permission/Security errors from browser APIs
        NotAllowedError|
        SecurityError:\sThe\soperation\sis\sinsecure|
        The\soperation\sis\sinsecure|
        InvalidStateError|
        NotFoundError:\sThe\sobject\scan\snot\sbe\sfound|

        # Illegal invocation - typically calling DOM methods incorrectly
        Illegal\sinvocation|

        # jQuery not loaded errors
        \$\sis\snot\sdefined|
        jQuery\sis\snot\sdefined|

        # Empty or unknown error messages
        No\serror\smessage|
        <unknown>|
        ^undefined$|

        # Cancelled requests (various languages)
        ^cancelled$|
        ^annul(é|ato|eret)$|      # French/Italian/Danish
        ^cancelado$|               # Spanish/Portuguese
        ^Abgebrochen$|             # German
        ^anulowane$|               # Polish
        ^avbruten$|                # Swedish
        ^キャンセルしました$|       # Japanese
        ^取소됨$|                   # Korean
        ^已取消$|                   # Chinese

        # Module import failures - deployment/network related
        Importing\sa\smodule\sscript\sfailed|
        Failed\sto\sfetch\sdynamically\simported\smodule|
        error\sloading\sdynamically\simported\smodule|
        Unable\sto\spreload\sCSS|

        # Third-party script/SDK errors that aren't actionable
        UET\sis\snot\sdefined|             # Microsoft UET
        fbq\sis\snot\sdefined|             # Facebook pixel
        gtag\sis\snot\sdefined|            # Google Analytics
        _hsq\sis\snot\sdefined|            # HubSpot
        pintrk\sis\snot\sdefined|          # Pinterest
        ttd_dom_ready\sis\snot\sdefined|   # The Trade Desk
        otBannerSdk|                        # OneTrust cookie banner
        mraid|                              # Mobile ads SDK
        googletag|                          # Google Publisher Tag (ad blocker)

        # WKWebView errors
        WKWebView\sAPI\sclient\sdid\snot\srespond\sto\sthis\spostMessage|

        # Out of memory errors
        Out\sof\smemory|

        # Script error with no details
        Script\serror\.?$|

        # Database errors from browser
        The\sdatabase\sconnection\sis\sclosing|
        Database\sdeleted\sby\srequest\sof\sthe\suser|

        # Media playback errors - user interaction required or media removed
        The\splay\(\)\srequest\swas\sinterrupted|

        # Context/port errors from extensions
        Attempting\sto\suse\sa\sdisconnected\sport\sobject|

        # Browser-specific non-actionable errors
        can't\sredefine\snon-configurable\sproperty|
        webkitExitFullScreen|
        msDiscoverChatAvailable|

        # Hydration errors - typically deployment/caching issues, not bugs
        Hydration\sfailed|
        There\swas\san\serror\swhile\shydrating|
        Text\scontent\sdoes\snot\smatch\sserver-rendered\sHTML|

        # Service worker errors - typically transient
        Failed\sto\sregister\sa\sServiceWorker|
        Failed\sto\supdate\sa\sServiceWorker|

        # Cancel rendering - Next.js navigation
        Cancel\srendering\sroute|

        # Connection errors (various)
        ERR_INTERNET_DISCONNECTED|
        ERR_NETWORK_CHANGED|
        ERR_CONNECTION_RESET|
        ERR_NAME_NOT_RESOLVED|
        ECONNREFUSED|
        ETIMEDOUT|
        ENOTFOUND|
        socket\shang\sup
    "#,
    )
    .expect("Invalid common errors filter Regex")
});

/// Check if the event has a commonly occurring error message.
fn matches<F: Filterable>(item: &F) -> bool {
    // Check exception values
    if let Some(exception_values) = item.exceptions()
        && let Some(exceptions) = exception_values.values.value()
    {
        for exception in exceptions {
            if let Some(exception) = exception.value() {
                // Check the exception value (message)
                if let Some(value) = exception.value.value()
                    && COMMON_ERROR_VALUES.is_match(value.as_str())
                {
                    return true;
                }

                // Check type + value combination (e.g., "TypeError: Failed to fetch")
                let ty = exception.ty.as_str().unwrap_or_default();
                let value = exception.value.as_str().unwrap_or_default();
                let message = match (ty, value) {
                    ("", value) => Cow::Borrowed(value),
                    (ty, "") => Cow::Borrowed(ty),
                    (ty, value) => Cow::Owned(format!("{ty}: {value}")),
                };
                if !message.is_empty() && COMMON_ERROR_VALUES.is_match(message.as_ref()) {
                    return true;
                }
            }
        }
    }

    // Check log entry message
    if let Some(logentry) = item.logentry() {
        if let Some(message) = logentry.formatted.value() {
            if COMMON_ERROR_VALUES.is_match(message.as_ref()) {
                return true;
            }
        } else if let Some(message) = logentry.message.value()
            && COMMON_ERROR_VALUES.is_match(message.as_ref())
        {
            return true;
        }
    }

    false
}

/// Filters events with commonly occurring error messages.
///
/// This filter is designed to reduce noise from error messages that are typically
/// not actionable, such as network errors, browser quirks, and third-party script failures.
pub fn should_filter<F: Filterable>(item: &F, config: &FilterConfig) -> Result<(), FilterStatKey> {
    if !config.is_enabled {
        return Ok(());
    }

    if matches(item) {
        Err(FilterStatKey::CommonErrors)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{Event, Exception, JsonLenientString, LogEntry, Values};
    use relay_protocol::Annotated;

    use super::*;

    fn get_event_with_exception(e: Exception) -> Event {
        Event {
            exceptions: Annotated::from(Values::<Exception> {
                values: Annotated::from(vec![Annotated::from(e)]),
                ..Values::default()
            }),
            ..Event::default()
        }
    }

    fn get_event_with_exception_value(val: &str) -> Event {
        let ex = Exception {
            value: Annotated::from(JsonLenientString::from(val.to_owned())),
            ..Exception::default()
        };
        get_event_with_exception(ex)
    }

    fn get_event_with_exception_type_and_value(ty: &str, val: &str) -> Event {
        let ex = Exception {
            ty: Annotated::from(ty.to_owned()),
            value: Annotated::from(JsonLenientString::from(val.to_owned())),
            ..Exception::default()
        };
        get_event_with_exception(ex)
    }

    fn get_event_with_logentry(message: &str) -> Event {
        Event {
            logentry: Annotated::new(LogEntry {
                formatted: Annotated::new(message.to_owned().into()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_dont_filter_when_disabled() {
        let events = [
            get_event_with_exception_value("Failed to fetch"),
            get_event_with_exception_value("Network Error"),
            get_event_with_exception_value("ResizeObserver loop limit exceeded"),
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
    fn test_filter_network_errors() {
        let errors = [
            "Failed to fetch",
            "TypeError: Failed to fetch",
            "Network Error",
            "Error: Network Error",
            "Load failed",
            "TypeError: Load failed",
            "Network request failed",
            "NetworkError when attempting to fetch resource",
            "NetworkError when attempting to fetch resource.",
            "The network connection was lost",
            "The network connection was lost.",
            "The Internet connection appears to be offline",
            "The Internet connection appears to be offline.",
            "A network error occurred",
            "A network error occurred.",
            "The request timed out",
            "The request timed out.",
            "Request aborted",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_abort_errors() {
        let errors = [
            "AbortError",
            "AbortError: The operation was aborted",
            "AbortError: The user aborted a request",
            "AbortError: The play() request was interrupted by a call to pause()",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_resize_observer_errors() {
        let errors = [
            "ResizeObserver loop limit exceeded",
            "ResizeObserver loop completed with undelivered notifications",
            "ResizeObserver loop completed with undelivered notifications.",
            "Error: ResizeObserver loop limit exceeded",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_recursion_errors() {
        let errors = ["too much recursion", "InternalError: too much recursion"];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_hydration_errors() {
        let errors = [
            "Hydration failed because the initial UI does not match what was rendered on the server",
            "There was an error while hydrating",
            "Text content does not match server-rendered HTML",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_connection_errors() {
        let errors = [
            "ERR_INTERNET_DISCONNECTED",
            "net::ERR_INTERNET_DISCONNECTED",
            "ERR_NETWORK_CHANGED",
            "ERR_CONNECTION_RESET",
            "ERR_NAME_NOT_RESOLVED",
            "ECONNREFUSED",
            "ETIMEDOUT",
            "ENOTFOUND",
            "socket hang up",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_third_party_sdk_errors() {
        let errors = [
            "gtag is not defined",
            "ReferenceError: gtag is not defined",
            "_hsq is not defined",
            "pintrk is not defined",
            "ttd_dom_ready is not defined",
            "otBannerSdk is not defined",
            "mraid is not defined",
            "jQuery is not defined",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_cancelled_in_other_languages() {
        let errors = [
            "annulé",             // French
            "cancelado",          // Spanish
            "Abgebrochen",        // German
            "anulowane",          // Polish
            "キャンセルしました", // Japanese
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_service_worker_errors() {
        let errors = [
            "Failed to register a ServiceWorker",
            "Failed to register a ServiceWorker for scope",
            "Failed to update a ServiceWorker",
            "Failed to update a ServiceWorker for scope",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_security_errors() {
        let errors = [
            "SecurityError: The operation is insecure",
            "The operation is insecure",
            "InvalidStateError",
            "InvalidStateError: The object is in an invalid state",
            "NotFoundError: The object can not be found here",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_database_errors() {
        let errors = [
            "The database connection is closing",
            "Database deleted by request of the user",
            "UnknownError: Database deleted by request of the user",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_media_playback_errors() {
        let errors = [
            "The play() request was interrupted by a call to pause()",
            "The play() request was interrupted because the media was removed from the document",
            "AbortError: The play() request was interrupted by a call to pause()",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_non_error_promise_rejections() {
        let errors = [
            "Non-Error promise rejection captured with value: undefined",
            "Non-Error promise rejection captured with value: Timeout",
            "Non-Error promise rejection captured with keys: currentTarget, detail, isTrusted, target",
            "Non-Error exception captured with keys: message, name",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_quota_exceeded() {
        let errors = [
            "QuotaExceededError",
            "QuotaExceededError: The quota has been exceeded",
            "QuotaExceededError: QuotaExceededError",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_timeout_errors() {
        let errors = [
            "timeout exceeded",
            "timeout of 0ms exceeded",
            "timeout of 5000ms exceeded",
            "timeout of 30000ms exceeded",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_call_stack_errors() {
        let errors = [
            "Maximum call stack size exceeded",
            "Maximum call stack size exceeded.",
            "RangeError: Maximum call stack size exceeded",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_json_parsing_errors() {
        let errors = [
            "Unexpected token",
            "Unexpected token '<'",
            "Unexpected token 'else'",
            "Unexpected end of JSON input",
            "Unexpected end of input",
            "Unexpected end of script",
            "SyntaxError: Unexpected token '<'",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_http_status_errors() {
        let errors = [
            "Request failed with status code 400",
            "Request failed with status code 401",
            "Request failed with status code 403",
            "Request failed with status code 404",
            "Request failed with status code 500",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_mobile_webview_errors() {
        let errors = [
            "Java object is gone",
            "Java bridge method invocation error",
            "Java exception was raised during method invocation",
            "Can't find variable: _AutofillCallbackHandler",
            "ReferenceError: Can't find variable: _AutofillCallbackHandler",
            "instantSearchSDKJSBridgeClearHighlight is not defined",
            "ceCurrentVideo.currentTime is not defined",
            "undefined is not an object (evaluating 'window.webkit.messageHandlers')",
            "WKWebView API client did not respond to this postMessage",
            "_pcmBridgeCallbackHandler is not defined",
            "Can't find variable: setIOSParameters",
            "SymBrowser_ModifyWindowOpenWithTarget is not defined",
            "zaloJSV2 is not defined",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_application_not_responding() {
        let errors = [
            "ApplicationNotResponding",
            "ApplicationNotResponding: ANR for at least 5000ms",
            "App Hanging",
            "App Hanging: App hanging for at least 2000 ms",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_frame_blocking_errors() {
        let errors = [
            "Blocked a frame with origin",
            "Blocked a frame with origin \"https://example.com\"",
            "SecurityError: Blocked a frame with origin \"https://example.com\" from accessing a cross-origin frame",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_misc_errors() {
        let errors = [
            "NotAllowedError",
            "NotAllowedError: The request is not allowed by the user agent",
            "Illegal invocation",
            "TypeError: Illegal invocation",
            "$ is not defined",
            "ReferenceError: $ is not defined",
            "No error message",
            "Error: No error message",
            "<unknown>",
            "cancelled",
            "Importing a module script failed",
            "Importing a module script failed.",
            "Failed to fetch dynamically imported module",
            "error loading dynamically imported module",
            "Unable to preload CSS",
            "UET is not defined",
            "fbq is not defined",
            "Out of memory",
            "Script error",
            "Script error.",
            "undefined",
            "TimeoutError",
            "TimeoutError: The operation timed out",
            "JSON Parse error: Unexpected identifier",
            "is not valid JSON",
            "Attempting to use a disconnected port object",
            "can't redefine non-configurable property \"userAgent\"",
            "webkitExitFullScreen",
            "msDiscoverChatAvailable",
            "Cancel rendering route",
            "Object captured as promise rejection with keys: message, status",
            "Object captured as exception with keys: message",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Event not filtered for error: '{error}'"
            );
        }
    }

    #[test]
    fn test_filter_custom_event_promise_rejection() {
        let error = "Event `CustomEvent` (type=unhandledrejection) captured as promise rejection";
        let event = get_event_with_exception_value(error);
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
        assert_eq!(
            filter_result,
            Err(FilterStatKey::CommonErrors),
            "Event not filtered for CustomEvent promise rejection"
        );
    }

    #[test]
    fn test_filter_exception_with_type_and_value() {
        let event = get_event_with_exception_type_and_value("TypeError", "Failed to fetch");
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
        assert_eq!(
            filter_result,
            Err(FilterStatKey::CommonErrors),
            "Event not filtered for typed exception"
        );
    }

    #[test]
    fn test_filter_logentry_message() {
        let event = get_event_with_logentry("Failed to fetch resource from server");
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
        assert_eq!(
            filter_result,
            Err(FilterStatKey::CommonErrors),
            "Event not filtered for logentry message"
        );
    }

    #[test]
    fn test_dont_filter_legitimate_errors() {
        let errors = [
            "TypeError: Cannot read property 'foo' of undefined",
            "ReferenceError: myVariable is not defined",
            "Error: Something went wrong in my application",
            "ValidationError: Email is invalid",
            "DatabaseError: Connection failed",
            "AuthenticationError: Invalid credentials",
            "PaymentError: Card declined",
            "Custom application error",
            "User not found",
            "Permission denied for this resource",
            "Invalid input provided",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Ok(()),
                "Event incorrectly filtered for legitimate error: '{error}'"
            );
        }
    }

    #[test]
    fn test_dont_filter_empty_exception() {
        let event = get_event_with_exception(Exception::default());
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
        assert_eq!(
            filter_result,
            Ok(()),
            "Empty exception should not be filtered"
        );
    }

    #[test]
    fn test_case_insensitive_matching() {
        let errors = [
            "FAILED TO FETCH",
            "failed to fetch",
            "Failed To Fetch",
            "NETWORK ERROR",
            "network error",
            "RESIZEOBSERVER LOOP LIMIT EXCEEDED",
        ];

        for error in errors {
            let event = get_event_with_exception_value(error);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Err(FilterStatKey::CommonErrors),
                "Case insensitive matching failed for: '{error}'"
            );
        }
    }
}
