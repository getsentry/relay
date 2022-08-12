use ::actix::dev::{MessageResponse, ResponseChannel};
use ::actix::prelude::*;
use futures01::prelude::*;

pub enum Response<T, E> {
    Reply(Result<T, E>),
    Future(ResponseFuture<T, E>),
}

impl<T, E> Response<T, E> {
    pub fn ok(value: T) -> Self {
        Response::Reply(Ok(value))
    }

    pub fn reply(result: Result<T, E>) -> Self {
        Response::Reply(result)
    }

    pub fn future<F>(future: F) -> Self
    where
        F: IntoFuture<Item = T, Error = E>,
        F::Future: 'static,
    {
        Response::Future(Box::new(future.into_future()))
    }
}

impl<T: 'static, E: 'static> Response<T, E> {
    pub fn map<U, F: 'static>(self, f: F) -> Response<U, E>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Response::Reply(result) => Response::reply(result.map(f)),
            Response::Future(future) => Response::future(future.map(f)),
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
            Response::Future(fut) => {
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
