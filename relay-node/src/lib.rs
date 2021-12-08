use actix::SystemService;
use crossbeam_channel::{bounded, unbounded, Sender};
use futures::prelude::*;
use napi::{
    bindgen_prelude::*,
    threadsafe_function::{ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode},
};
use napi_derive::napi;
use relay_config::{Config, OverridableConfig};
use relay_server::{
    Controller, EnvelopeManager, Server, SetCapturedEnvelopeHook, ShutdownExternal,
};
use std::thread::JoinHandle;

#[derive(Debug)]
struct Inner {
    exit: Sender<()>,
    handle: JoinHandle<()>,
}

#[napi]
#[derive(Debug, Default)]
/// Node Relay test server
pub struct RelayNodeServer {
    inner: Option<Inner>,
}

#[napi]
impl RelayNodeServer {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self { inner: None }
    }

    #[napi]
    /// Start the server
    pub fn start(&mut self, callback: JsFunction) -> Result<()> {
        // Create an napi-rs thread-safe function
        let func: ThreadsafeFunction<String> = callback
            .create_threadsafe_function(0, |ctx: ThreadSafeCallContext<String>| {
                ctx.env.to_js_value(&ctx.value).map(|v| vec![v])
            })?;

        let (tx_envelope, rx_envelope) = unbounded::<std::result::Result<String, String>>();
        let (exit, rx_exit) = bounded::<()>(1);

        let handle = std::thread::spawn(|| {
            let mut config: Config = Default::default();
            let over_ride = OverridableConfig {
                mode: Some("capture".to_owned()),
                ..Default::default()
            };
            config.apply_override(over_ride).unwrap();

            Controller::run(|| {
                let result = Server::start(config);
                let envelope_manager = EnvelopeManager::from_registry();

                std::thread::spawn(move || {
                    envelope_manager
                        .send(SetCapturedEnvelopeHook { tx: tx_envelope })
                        .wait()
                        .unwrap();
                });

                let controller = Controller::from_registry();

                std::thread::spawn(move || {
                    for _ in rx_exit.iter() {
                        controller.send(ShutdownExternal {}).wait().unwrap();
                    }
                });

                result
            })
            .unwrap();
        });

        std::thread::spawn(move || {
            for result in rx_envelope.iter() {
                match result {
                    Ok(env) => {
                        func.call(Ok(env), ThreadsafeFunctionCallMode::NonBlocking);
                    }
                    Err(_) => todo!(),
                }
            }
        });

        self.inner = Some(Inner { handle, exit });

        Ok(())
    }

    #[napi]
    /// Stops the server
    pub fn stop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let _ = inner.exit.send(());
            let _ = inner.handle.join();
        }
    }
}

impl Drop for RelayNodeServer {
    fn drop(&mut self) {
        self.stop();
    }
}
