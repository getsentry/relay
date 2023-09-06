use std::collections::VecDeque;
use std::time::Duration;

use yew::prelude::*;
use yew_router::prelude::*;

use crate::utils::{buffering_socket, window_location};

mod stats;
mod utils;

const MAX_LOG_SIZE: usize = 1000;

#[derive(Clone, Routable, PartialEq)]
enum Route {
    #[at("/")]
    Stats,
    #[at("/tools")]
    Tools,
    #[not_found]
    #[at("/404")]
    NotFound,
}

fn switch(routes: Route) -> Html {
    match routes {
        Route::Stats => html! { <Stats /> },
        Route::Tools => html! {
            <Tools />
        },
        Route::NotFound => html! { <h1>{ "404" }</h1> },
    }
}

#[function_component(MenuBar)]
fn menu_bar() -> Html {
    html! {
        <nav class="deep-purple darken-1 grey-text text-darken-3 z-depth-3">
            <div class="nav-wrapper">
                <img class="logo" src="img/relay-logo.png"/>
                <ul id="nav-mobile" class="right hide-on-med-and-down">
                    <li> <Link<Route> to={ Route::Stats }> { "Stats" } </Link<Route>> </li>
                    <li> <Link<Route> to={ Route::Tools }> { "Tools" } </Link<Route>> </li>
                </ul>
            </div>
        </nav>
    }
}

#[function_component(Tools)]
fn tools() -> Html {
    html! {
        <div class="padding">
            <h1>{ "TOOLS" }</h1>
        </div>
    }
}

#[function_component(Main)]
fn app() -> Html {
    html! {
        <>
            <BrowserRouter>
            <MenuBar />
                <Switch<Route> render={switch} /> // <- must be child of <BrowserRouter>
            </BrowserRouter>
        </>
    }
}

#[function_component(Stats)]
fn stats() -> Html {
    html! {
        <div class="padding">
            <Logs/>
            <stats::Stats/>
        </div>
    }
}

fn main() {
    yew::Renderer::<Main>::new().render();
}

#[function_component(Logs)]
fn logs() -> Html {
    let update_trigger = use_force_update();
    let log_entries = use_mut_ref(VecDeque::new);

    {
        let log_entries = log_entries.clone();
        use_effect_with_deps(
            move |_| {
                let relay_address = window_location();
                let url = format!("ws://{relay_address}/api/relay/logs/");
                let interval = Duration::from_millis(100);
                buffering_socket(url, interval, move |messages| {
                    let shrink_to = MAX_LOG_SIZE.saturating_sub(messages.len());
                    let mut log_entries = (*log_entries).borrow_mut();
                    while log_entries.len() > shrink_to {
                        log_entries.pop_front();
                    }
                    log_entries.extend(
                        messages
                            .into_iter()
                            .filter_map(|entry| ansi_to_html::convert_escaped(&entry).ok()),
                    );
                    update_trigger.force_update();
                });
            },
            (),
        );
    }

    html! {
        <div class="row">
            <h3>{ "Logs" }</h3>
            <div class="card logs blue-grey darken-3 z-depth-3">
                <div class="card-content white-text">
                    <p>
                          {
                            (*log_entries).borrow().iter().map(|e|
                                Html::from_html_unchecked(AttrValue::from(format!("<span>{}</span>",e)))
                            ).collect::<Html>()
                          }
                    </p>
                </div>
            </div>
        </div>
    }
}
