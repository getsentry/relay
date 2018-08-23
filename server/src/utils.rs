use actix::dev::{MessageResponse, ResponseChannel};
use actix::prelude::*;
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
