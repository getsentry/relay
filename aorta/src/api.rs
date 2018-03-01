use std::borrow::Cow;

use hyper::Method;
use serde::ser::Serialize;
use serde::de::DeserializeOwned;


/// For well known types this can automatically define the API request to issue.
pub trait AortaApiRequest: Serialize {
    /// The response type.
    type Response: DeserializeOwned + 'static;

    /// Returns the target URL and method.
    fn get_aorta_request_target<'a>(&'a self) -> (Method, Cow<'a, str>);
}
