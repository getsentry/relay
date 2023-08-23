use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use futures::StreamExt;
use gloo_net::websocket::{futures::WebSocket, Message};
use yew::prelude::*;

mod stats;

const RELAY_URL: &str = "localhost:3001"; // TODO: make configurable
const MAX_LOG_SIZE: usize = 1000;

#[function_component(App)]
fn app() -> Html {
    html! {
        <>
            <MenuBar />

            <div class="stats-container">
                <Logs/>
                <stats::Stats/>
            </div>
        </>
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

#[function_component(MenuBar)]
fn menu_bar() -> Html {
    html! {
        <nav class="deep-purple darken-1 grey-text text-darken-3 z-depth-3">
            <div class="nav-wrapper">
                <img class="logo" src="img/relay-logo.png"/>
                <ul id="nav-mobile" class="right hide-on-med-and-down">
                    <li><a href="#">{ "Stats" }</a></li>
                    <li><a href="#">{ "Tools" }</a></li>
                </ul>
            </div>
        </nav>
    }
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
        <div class="row">
            <h3>{ "Logs" }</h3>
            <div class="card logs blue-grey darken-3 z-depth-3">
                <div class="card-content white-text">
                    <p>
                          {
                            (*log_entries).borrow().iter().filter_map(|entry| {
                                ansi_to_html::convert_escaped(entry).ok()
                            }).map(|e| Html::from_html_unchecked(AttrValue::from(format!("<span>{}</span>",e)))).collect::<Html>()
                          }
                    </p>
                </div>
            </div>
        </div>
    }
}
