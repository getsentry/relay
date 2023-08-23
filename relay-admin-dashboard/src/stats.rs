use std::collections::BTreeMap;

use gloo_net::websocket::{futures::WebSocket, Message};
use wasm_bindgen::convert::FromWasmAbi;
use wasm_bindgen::JsValue;
use yew::prelude::*;

use crate::{on_next_message, RELAY_URL};

#[function_component(Stats)]
pub(crate) fn stats() -> Html {
    // name -> (tags -> values)
    // TODO: Use timestamps
    let time_series = use_mut_ref(BTreeMap::<String, BTreeMap<String, Vec<f32>>>::new);
    let update_trigger = use_force_update();

    let socket = use_mut_ref(|| {
        Some(WebSocket::open(&format!("ws://{RELAY_URL}/api/relay/stats/")).unwrap())
    });
    {
        let time_series = time_series.clone();
        use_effect(move || {
            let time_series = time_series.clone();
            on_next_message(socket.clone(), move |message| {
                if let Message::Bytes(message) = message {
                    let message = String::from_utf8_lossy(&message);
                    let (name, tags, value) = parse_metric(&message);
                    let mut time_series = (*time_series).borrow_mut();
                    let series = time_series
                        .entry(name.to_owned())
                        .or_default()
                        .entry(tags.to_owned())
                        .or_default();

                    series.push(value);
                    // TODO: limit number of entries

                    js::show_chart(name, series.clone());
                }
            });
        });
    }

    html! {
        <>
            <h2>{ "Stats" }</h2>
            <canvas id="the-canvas"/>
            <div></div>
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

#[derive(Properties, PartialEq)]
struct GraphProps {
    name: String,
    time_series: BTreeMap<String, Vec<f32>>,
}

#[function_component(Graph)]
fn graph(props: &GraphProps) -> Html {
    html! {
        <div class="graph-container" key={props.name.clone()}>
            <h3>{&props.name}</h3>
            <canvas width="200" height="200" id={format!("chart-{}", props.name)}/>
            // <ul>{
            //     props.time_series.iter().map(|(tags, values)| {
            //         html!{
            //             <li>
            //                 <h4>{tags}</h4>
            //                 <br/>
            //                 {values.iter().collect::<Html>()}
            //                 <br/>
            //             </li>
            //         }
            //     }).collect::<Html>()
            // }</ul>
        </div>
    }
}

mod js {
    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen]
        pub fn show_chart(id: String, data: Vec<f32>); // TODO: can borrow?
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
