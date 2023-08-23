use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use futures::StreamExt;
use gloo_net::websocket::{futures::WebSocket, Message};
use yew::prelude::*;

const RELAY_URL: &str = "localhost:3001"; // TODO: make configurable
const MAX_LOG_SIZE: usize = 1000;

#[function_component(App)]
fn app() -> Html {
    html! {
        <div class="container">
            <h1>{ "Relay Admin Dashboard" }</h1>
            <Logs/>
            <Stats/>
        </div>
    }
}

fn main() {
    yew::Renderer::<App>::new().render();
}

fn on_next_message(socket: Rc<RefCell<Option<WebSocket>>>, f: impl Fn(Message) + 'static) {
    wasm_bindgen_futures::spawn_local(async move {
        // Take the socket so it will not be polled concurrently:
        let inner_socket = (*socket).borrow_mut().take();
        if let Some(mut inner_socket) = inner_socket {
            if let Some(Ok(message)) = inner_socket.next().await {
                f(message);
            }
            // Put the socket back
            (*socket).borrow_mut().replace(inner_socket);
        }
    });
}

#[function_component(Logs)]
fn logs() -> Html {
    let update_trigger = use_force_update();
    let log_entries = use_mut_ref(VecDeque::new);

    let socket = use_mut_ref(|| {
        Some(WebSocket::open(&format!("ws://{RELAY_URL}/api/relay/logs/")).unwrap())
    });
    {
        let log_entries = log_entries.clone();
        let socket = socket.clone();
        use_effect(move || {
            on_next_message(socket.clone(), move |message| {
                if let Message::Text(message) = message {
                    let mut log_entries = (*log_entries).borrow_mut();
                    while log_entries.len() >= MAX_LOG_SIZE {
                        log_entries.pop_front();
                    }
                    log_entries.push_back(message);
                    update_trigger.force_update();
                }
            })
        });
    }

    html! {
        <div class="logs-container">
            <h2>{ "Logs" }</h2>
            <div class="logs">{ (*log_entries).borrow().iter().collect::<Html>() }</div>
        </div>
    }
}

#[function_component(Stats)]
fn stats() -> Html {
    let log_entries = use_state(Vec::new);
    let socket = use_mut_ref(|| {
        Some(WebSocket::open(&format!("ws://{RELAY_URL}/api/relay/stats/")).unwrap())
    });
    {
        let log_entries = log_entries.clone();
        let socket = socket.clone();
        use_effect(move || {
            let mut socket = socket.clone();
            wasm_bindgen_futures::spawn_local(async move {
                // Take the socket so it will not be polled concurrently:
                let inner_socket = socket.borrow_mut().take();
                if let Some(mut inner_socket) = inner_socket {
                    if let Some(Ok(Message::Bytes(message))) = inner_socket.next().await {
                        if let Ok(s) = String::from_utf8(message) {
                            let index = log_entries.len().saturating_sub(10);
                            let mut new_vec = log_entries[index..].to_vec();
                            new_vec.push(format!("{s}\n"));
                            log_entries.set(new_vec);
                        }
                    }
                    // Put the socket back
                    socket.borrow_mut().replace(Some(inner_socket));
                }
            });
        });
    }

    html! {
        <>
            <h2>{ "Stats" }</h2>
            <pre>{ log_entries.iter().collect::<Html>() }</pre>
        </>
    }
}
