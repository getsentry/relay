#![allow(non_camel_case_types)]

use instant::Duration;
use yew::prelude::*;

use crate::utils::{buffering_socket, window_location};

#[function_component(Stats)]
pub(crate) fn stats() -> Html {
    use_effect_with_deps(
        move |_| {
            let relay_address = window_location();
            let url = format!("ws://{relay_address}/api/relay/stats/");
            let interval = Duration::from_millis(100);
            buffering_socket(url, interval, move |messages| {
                for message in messages {
                    let Metric {
                        ty,
                        name,
                        tags,
                        value,
                    } = parse_metric(&message);
                    // I gave up on the hole reactive framework here and decided
                    // to just pass the data to javascript.
                    js::update_chart(ty, name, tags, value);
                }
            });
        },
        (),
    );

    html! {
        <>
            <h3>{ "Stats" }</h3>
            <div id="charts"></div>
        </>
    }
}

#[derive(Debug, PartialEq)]
struct Metric<'a> {
    ty: &'a str,
    name: &'a str,
    tags: &'a str,
    value: f32,
}

fn parse_metric(metric: &str) -> Metric {
    let mut parts = metric.split('|');
    let name_and_value = parts.next().unwrap_or("");
    let ty = parts.next().unwrap_or("");
    let tags = parts.next().unwrap_or("");

    let (name, value) = name_and_value.split_once(':').unwrap_or(("", ""));
    let value = value.parse::<f32>().unwrap_or_default();
    Metric {
        ty,
        name,
        tags,
        value,
    }
}

mod js {

    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen]
        pub fn update_chart(ty: &str, metric: &str, tags: &str, value: f32); // TODO: can borrow?
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
            Metric {
                ty: "g",
                name: "service.back_pressure",
                tags: "#service:relay_server::actors::processor::EnvelopeProcessorService",
                value: 0.0
            }
        )
    }
}
