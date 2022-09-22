use ::actix::dev::{MessageResponse, ResponseChannel};
use ::actix::prelude::*;
use futures01::prelude::*;
use tokio::runtime::Runtime;

use relay_common::clone;

pub enum Response<T, E> {
    Reply(Result<T, E>),
    Future(ResponseFuture<T, E>),
}

impl<T, E> Response<T, E> {
    pub fn ok(value: T) -> Self {
        Response::Reply(Ok(value))
    }

    pub fn future<F>(future: F) -> Self
    where
        F: IntoFuture<Item = T, Error = E>,
        F::Future: 'static,
    {
        Response::Future(Box::new(future.into_future()))
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

/// Constructs a single threaded tokio [`Runtime`] containing a clone of the actix [`System`].
///
/// This is required if you need to send messages from the tokio runtime to actix
/// actors.
///
/// # Panics
///
/// The calling thread must have the actix system enabled, panics if this is invoked
/// in a thread where actix is not enabled.
pub fn tokio_runtime_with_actix() -> Runtime {
    let system = System::current();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .on_thread_start(clone!(system, || System::set_current(system.clone())))
        .build()
        .unwrap()
}
