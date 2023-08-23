use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use futures::StreamExt;
use gloo_net::websocket::{futures::WebSocket, Message};
use yew::prelude::*;

const RELAY_URL: &str = "localhost:3001"; // TODO: make configurable
const MAX_LOG_SIZE: usize = 1000;

#[function_component(App)]
fn app() -> Html {
    html! {
        <>
            <MenuBar />

            <div class="stats-container">
                <Logs/>
                <Stats/>
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
        <nav class="deep-purple darken-1 grey-text text-darken-3">
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
            <div class="card logs blue-grey darken-3">
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

#[function_component(Stats)]
fn stats() -> Html {
    // name -> (tags -> values)
    let time_series = use_mut_ref(BTreeMap::<String, BTreeMap<String, Vec<f32>>>::new);
    let update_trigger = use_force_update();

    let socket = use_mut_ref(|| {
        Some(WebSocket::open(&format!("ws://{RELAY_URL}/api/relay/stats/")).unwrap())
    });
    {
        let time_series = time_series.clone();
        use_effect(move || {
            on_next_message(socket.clone(), move |message| {
                if let Message::Bytes(message) = message {
                    let message = String::from_utf8_lossy(&message);
                    let (name, tags, value) = parse_metric(&message);
                    let mut time_series = (*time_series).borrow_mut();
                    time_series
                        .entry(name.to_owned())
                        .or_default()
                        .entry(tags.to_owned())
                        .or_default()
                        .push(value);
                    update_trigger.force_update();
                }
            })
        });
    }

    html! {
        <>
            <h2>{ "Stats" }</h2>
            <ul>{
                (*time_series).borrow_mut().iter().map(|(name, time_series)| {
                    html! {
                        <li key={name.clone()}><h3>{name}</h3>{
                            time_series.iter().map(|(tags, values)| {
                                html!{
                                    <>
                                        <b>{tags}</b>
                                        <br/>
                                        {values.iter().collect::<Html>()}
                                        <br/>
                                    </>
                                }
                            }).collect::<Html>()
                        }</li>
                    }
                }).collect::<Html>() }
            </ul>
        </>
    }
}

fn parse_metric(metric: &str) -> (&str, &str, f32) {
    let mut parts = metric.split('|');
    let name_and_value = parts.next().unwrap_or("");
    let _type = parts.next();
    let tags = parts.next().unwrap_or("");

    let (name, value) = name_and_value.split_once(':').unwrap_or(("", ""));
    let value = value.parse::<f32>().unwrap_or_default();
    (name, tags, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_metric() {
        let metric = "service.back_pressure:0|g|#service:relay_server::actors::processor::EnvelopeProcessorService";
        assert_eq!(
            parse_metric(metric),
            (
                "service.back_pressure",
                "#service:relay_server::actors::processor::EnvelopeProcessorService",
                0.0
            )
        )
    }
}
