/// A list well known clients.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ClientName<'a> {
    Relay,
    Ruby,
    CocoaFlutter,
    CocoaReactNative,
    Cocoa,
    Dotnet,
    AndroidReactNative,
    AndroidJava,
    SpringBoot,
    JavascriptBrowser,
    Electron,
    NestJs,
    NextJs,
    Node,
    React,
    Vue,
    Native,
    Laravel,
    Symfony,
    Php,
    Python,
    Other(&'a str),
}

impl<'a> ClientName<'a> {
    /// Returns the client name as a `str` with a static lifetime.
    ///
    /// Returns `None` if the client name is not a well known client.
    pub fn as_static_str(&self) -> Option<&'static str> {
        Some(match self {
            Self::Relay => "sentry.relay",
            Self::Ruby => "sentry-ruby",
            Self::CocoaFlutter => "sentry.cocoa.flutter",
            Self::CocoaReactNative => "sentry.cocoa.react-native",
            Self::Cocoa => "sentry.cocoa",
            Self::Dotnet => "sentry.dotnet",
            Self::AndroidReactNative => "sentry.java.android.react-native",
            Self::AndroidJava => "sentry.java.android",
            Self::SpringBoot => "sentry.java.spring-boot.jakarta",
            Self::JavascriptBrowser => "sentry.javascript.browser",
            Self::Electron => "sentry.javascript.electron",
            Self::NestJs => "sentry.javascript.nestjs",
            Self::NextJs => "sentry.javascript.nextjs",
            Self::Node => "sentry.javascript.node",
            Self::React => "sentry.javascript.react",
            Self::Vue => "sentry.javascript.vue",
            Self::Native => "sentry.native",
            Self::Laravel => "sentry.php.laravel",
            Self::Symfony => "sentry.php.symfony",
            Self::Php => "sentry.php",
            Self::Python => "sentry.python",
            Self::Other(_) => return None,
        })
    }
}

impl<'a> From<&'a str> for ClientName<'a> {
    fn from(value: &'a str) -> Self {
        match value {
            "sentry.relay" => Self::Relay,
            "sentry-ruby" => Self::Ruby,
            "sentry.cocoa.flutter" => Self::CocoaFlutter,
            "sentry.cocoa.react-native" => Self::CocoaReactNative,
            "sentry.cocoa" => Self::Cocoa,
            "sentry.dotnet" => Self::Dotnet,
            "sentry.java.android.react-native" => Self::AndroidReactNative,
            "sentry.java.android" => Self::AndroidJava,
            "sentry.java.spring-boot.jakarta" => Self::SpringBoot,
            "sentry.javascript.browser" => Self::JavascriptBrowser,
            "sentry.javascript.electron" => Self::Electron,
            "sentry.javascript.nestjs" => Self::NestJs,
            "sentry.javascript.nextjs" => Self::NextJs,
            "sentry.javascript.node" => Self::Node,
            "sentry.javascript.react" => Self::React,
            "sentry.javascript.vue" => Self::Vue,
            "sentry.native" => Self::Native,
            "sentry.php.laravel" => Self::Laravel,
            "sentry.php.symfony" => Self::Symfony,
            "sentry.php" => Self::Php,
            "sentry.python" => Self::Python,
            other => Self::Other(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relay_client_name() {
        let name = crate::constants::CLIENT.split_once('/').unwrap().0;

        assert_eq!(ClientName::from(name), ClientName::Relay);
        assert_eq!(ClientName::Relay.as_static_str(), Some(name));
    }
}
