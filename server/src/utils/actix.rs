use actix::dev::{MessageResponse, ResponseChannel};
use actix::prelude::*;
use futures::prelude::*;

#[derive(Debug, Default)]
pub struct One<T>(pub T);

impl<T> One<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> From<T> for One<T> {
    fn from(value: T) -> Self {
        One(value)
    }
}

impl<A, M, T: 'static> MessageResponse<A, M> for One<T>
where
    A: Actor,
    M: Message<Result = One<T>>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self);
        }
    }
}

pub enum Response<T, E> {
    Reply(Result<T, E>),
    Async(ResponseFuture<T, E>),
}

impl<T, E> Response<T, E> {
    pub fn ok(value: T) -> Self {
        Response::Reply(Ok(value))
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
