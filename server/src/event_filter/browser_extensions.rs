use crate::actors::project::{FilterConfig, FiltersConfig};
use regex::{Regex, RegexBuilder};
use semaphore_general::protocol::{Event, IpAddr, User};

use lazy_static::lazy_static;

pub fn browser_extensions_filter(event: &Event, config: &FilterConfig) -> Result<(), String> {
    if !config.is_enabled {
        return Ok(());
    }

    return Ok(());
}

lazy_static::lazy_static! {
    static ref EXTENSION_EXC_VALUES_STR: String = [
        // Random plugins/extensions
        r"top.GLOBALS",
        // See: http://blog.errorception.com/2012/03/tale-of-unfindable-js-error.html
        r"originalCreateNotification",
        r"canvas.contentDocument",
        r"MyApp_RemoveAllHighlights",
        r"http://tt.epicplay.com",
        r"Can't find variable: ZiteReader",
        r"jigsaw is not defined",
        r"ComboSearch is not defined",
        r"http://loading.retry.widdit.com/",
        r"atomicFindClose",
        // Facebook borked
        r"fb_xd_fragment",
        // ISP "optimizing" proxy - `Cache-Control: no-transform` seems to
        // reduce this. (thanks @acdha)
        // See http://stackoverflow.com/questions/4113268
        r"bmi_SafeAddOnload",
        r"EBCallBackMessageReceived",
        // See https://groups.google.com/a/chromium.org/forum/#!topic/chromium-discuss/7VU0_VvC7mE
        r"_gCrWeb",
        // See http://toolbar.conduit.com/Debveloper/HtmlAndGadget/Methods/JSInjection.aspx
        r"conduitPage",
        // Google Search app (iOS)
        // See: https://github.com/getsentry/raven-js/issues/756
        r"null is not an object \(evaluating 'elt.parentNode'\)",
        // Dragon Web Extension from Nuance Communications
        // See: https://forum.sentry.io/t/error-in-raven-js-plugin-setsuspendstate/481/
        r"plugin.setSuspendState is not a function"
        ].join("|");



    static ref EXTENSION_EXC_VALUES: Regex =
    RegexBuilder::new(EXTENSION_EXC_VALUES_STR.as_str())
        .case_insensitive(true)
        .build()
        .expect("Invalid browser extensions filter (Exec Vals) Regex");


    static ref EXTENSION_EXC_SOURCES_STR: String = [
        // Facebook flakiness
        r"graph\.facebook\.com",
        // Facebook blocked
        r"connect\.facebook\.net",
        // Woopra flakiness
        r"eatdifferent\.com\.woopra-ns\.com",
        r"static\.woopra\.com\/js\/woopra\.js",
        // Chrome extensions
        r"^chrome(?:-extension)?:\/\/",
        // Cacaoweb
        r"127\.0\.0\.1:4001\/isrunning",
        // Other
        r"webappstoolbarba\.texthelp\.com\/",
        r"metrics\.itunes\.apple\.com\.edgesuite\.net\/",
        // Kaspersky Protection browser extension
        r"kaspersky-labs\.com",
    ].join("|");



    static ref EXTENSION_EXC_SOURCES: Regex =
    RegexBuilder::new(EXTENSION_EXC_SOURCES_STR.as_str())
        .case_insensitive(true)
        .build()
        .expect("Invalid browser extensions filter (Exec Sources) Regex");
}
