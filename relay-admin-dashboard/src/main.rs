use std::ops::Deref;

use futures::{SinkExt, StreamExt};
use gloo_net::websocket::{futures::WebSocket, Message};
use tokio::sync::watch;
use yew::prelude::*;

const RELAY_URL: &str = "localhost:3001"; // TODO: make configurable

#[function_component(App)]
fn app() -> Html {
    html! {
        <>
            <h1>{ "Relay Admin Dashboard" }</h1>
            <Logs/>
        </>
    }
}

fn main() {
    yew::Renderer::<App>::new().render();
}

#[function_component(Logs)]
fn logs() -> Html {
    let raw_content = use_state(|| "Loading...".to_owned());
    {
        let raw_content = raw_content.clone();
        let ws = WebSocket::open(&format!("ws://{RELAY_URL}/api/relay/logs/")).unwrap();
        let (_, mut read) = ws.split();
        use_effect_with_deps(
            move |_| {
                let raw_content = raw_content.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    if let Some(Ok(Message::Text(message))) = read.next().await {
                        raw_content.set(message);
                    }
                });
            },
            (), // TODO: monitor web socket updates to reload.
        );
    }

    html! {
        <>
            <h2>{ "Logs" }</h2>
            <pre>{raw_content.deref()}</pre>
        </>
    }
}
