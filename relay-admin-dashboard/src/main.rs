use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use futures::StreamExt;
use gloo_console::log;
use gloo_net::websocket::State;
use gloo_net::websocket::{futures::WebSocket, Message};
use yew::platform::time::sleep;
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

struct Socket {
    url: String,
    socket: Result<WebSocket, ()>,
}

impl Socket {
    fn open(url: String) -> Self {
        let socket = WebSocket::open(&url).map_err(|_| ());
        Self { url, socket }
    }

    async fn reconnect(&mut self) {
        log!("Attempting reconnect...");
        sleep(Duration::from_millis(1000)).await;
        if !matches!(self.socket.as_ref().map(|s| s.state()), Ok(State::Open)) {
            self.socket = WebSocket::open(&self.url).map_err(|_| ());
        }
    }

    async fn try_next(&mut self) -> Result<String, ()> {
        if let Ok(socket) = self.socket.as_mut() {
            if !matches!(socket.state(), State::Closed) {
                if let Some(Ok(Message::Text(message))) = socket.next().await {
                    return Ok(message);
                }
            }
        }
        self.reconnect().await;
        Err(())
    }
}

fn on_next_message(
    socket: Rc<RefCell<Option<Socket>>>,
    update_trigger: UseForceUpdateHandle,
    f: impl Fn(String) + 'static,
) {
    let inner_socket = (*socket).borrow_mut().take();
    if let Some(mut inner_socket) = inner_socket {
        wasm_bindgen_futures::spawn_local(async move {
            if let Ok(message) = inner_socket.try_next().await {
                f(message);
            }
            // Put the socket back
            (*socket).borrow_mut().replace(inner_socket);

            // Update component
            update_trigger.force_update();
        });
    }
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

    let socket = use_mut_ref(|| Some(Socket::open(format!("ws://{RELAY_URL}/api/relay/logs/"))));
    {
        let log_entries = log_entries.clone();
        use_effect(move || {
            on_next_message(socket.clone(), update_trigger, move |message| {
                let mut log_entries = (*log_entries).borrow_mut();
                while log_entries.len() >= MAX_LOG_SIZE {
                    log_entries.pop_front();
                }
                log_entries.push_back(message);
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
