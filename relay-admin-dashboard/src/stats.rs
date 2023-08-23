#![allow(non_camel_case_types)]

use gloo_net::websocket::{futures::WebSocket, Message};
use yew::prelude::*;

use crate::{on_next_message, RELAY_URL};

#[function_component(Stats)]
pub(crate) fn stats() -> Html {
    let update_trigger = use_force_update();

    let socket = use_mut_ref(|| {
        Some(WebSocket::open(&format!("ws://{RELAY_URL}/api/relay/stats/")).unwrap())
    });
    {
        use_effect(move || {
            on_next_message(socket.clone(), move |message| {
                if let Message::Bytes(message) = message {
                    let message = String::from_utf8_lossy(&message);
                    let (name, tags, value) = parse_metric(&message);
                    // I gave up on the hole reactive framework here and decided
                    // to just pass the data to javascript.
                    js::update_chart(name, tags, value);
                    update_trigger.force_update();
                }
            });
        });
    }

    html! {
        <>
            <h3>{ "Stats" }</h3>
            <div id="charts"></div>
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

mod js {

    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen]
        pub fn update_chart(metric: &str, tags: &str, value: f32); // TODO: can borrow?
    }
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
