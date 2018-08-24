use std::fmt;

use actix::dev::{MessageResponse, ResponseChannel};
use actix::prelude::*;
use failure::Fail;
use futures::prelude::*;

pub enum Response<T, E> {
    Reply(Result<T, E>),
    Async(Box<Future<Item = T, Error = E>>),
}

impl<T, E> Response<T, E> {
    pub fn ok(value: T) -> Self {
        Response::Reply(Ok(value))
    }

    pub fn err(error: E) -> Self {
        Response::Reply(Err(error))
    }

    pub fn reply(result: Result<T, E>) -> Self {
        Response::Reply(result)
    }

    pub fn async<F>(future: F) -> Self
    where
        F: IntoFuture<Item = T, Error = E>,
        F::Future: 'static,
    {
        Response::Async(Box::new(future.into_future()))
    }
}

impl<T: 'static, E: 'static> Response<T, E> {
    pub fn map<U, F: 'static>(self, f: F) -> Response<U, E>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Response::Reply(result) => Response::reply(result.map(f)),
            Response::Async(future) => Response::async(future.map(f)),
        }
    }

    pub fn and_then<U: 'static, F: 'static>(self, f: F) -> Response<U, E>
    where
        F: FnOnce(T) -> Response<U, E>,
    {
        match self {
            Response::Reply(Ok(t)) => f(t),
            Response::Reply(Err(e)) => Response::Reply(Err(e)),
            Response::Async(future) => Response::async(future.and_then(|t| match f(t) {
                Response::Reply(res) => Box::new(res.into_future()),
                Response::Async(fut) => fut,
            })),
        }
    }
}

impl<A, M, T: 'static, E: 'static> MessageResponse<A, M> for Response<T, E>
where
    A: Actor,
    M: Message<Result = Result<T, E>>,
    A::Context: AsyncContext<A>,
{
    fn handle<R: ResponseChannel<M>>(self, _context: &mut A::Context, tx: Option<R>) {
        match self {
            Response::Async(fut) => {
                Arbiter::spawn(fut.then(move |res| {
                    if let Some(tx) = tx {
                        tx.send(res);
                    }
                    Ok(())
                }));
            }
            Response::Reply(res) => {
                if let Some(tx) = tx {
                    tx.send(res);
                }
            }
        }
    }
}

/// An error response from an api.
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct ApiErrorResponse {
    detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    causes: Option<Vec<String>>,
}

impl ApiErrorResponse {
    /// Creates an error response with a detail message
    pub fn with_detail<S: AsRef<str>>(s: S) -> ApiErrorResponse {
        ApiErrorResponse {
            detail: Some(s.as_ref().to_string()),
            causes: None,
        }
    }

    /// Creates an error response from a fail.
    pub fn from_fail<F: Fail>(fail: &F) -> ApiErrorResponse {
        let mut messages = vec![];

        for cause in Fail::iter_chain(fail) {
            let msg = cause.to_string();
            if !messages.contains(&msg) {
                messages.push(msg);
            }
        }

        ApiErrorResponse {
            detail: Some(messages.remove(0)),
            causes: if messages.is_empty() {
                None
            } else {
                Some(messages)
            },
        }
    }
}

impl fmt::Display for ApiErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref detail) = self.detail {
            write!(f, "{}", detail)
        } else {
            write!(f, "no error details")
        }
    }
}
