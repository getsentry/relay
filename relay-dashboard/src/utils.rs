use std::time::Duration;

use futures::StreamExt;
use gloo_console::log;
use gloo_net::websocket::State;
use gloo_net::websocket::{futures::WebSocket, Message};
use yew::platform::time::sleep;

static RELAY_URL: once_cell::sync::OnceCell<String> = once_cell::sync::OnceCell::new();

/// Returns the origin location from the browser window.
///
/// If the window cannot be found, return the default: "localhost:3001".
/// Since this app is always should be opened in the broweser, we can safely assume that the window
/// will be present when the location is requested.
pub fn window_location() -> &'static String {
    RELAY_URL.get_or_init(|| {
        web_sys::window()
            .and_then(|w| w.location().host().ok())
            .unwrap_or("localhost:3001".to_string())
    })
}

/// Buffers socket messages and flushes them in batches.
pub fn buffering_socket(
    url: String,
    flush_interval: Duration,
    flush: impl Fn(Vec<String>) + 'static,
) {
    wasm_bindgen_futures::spawn_local(async move {
        let mut buffer = Vec::new();
        let mut last_flush = instant::Instant::now();
        let mut socket = AutoSocket::open(url);
        loop {
            let message = socket.next().await;
            buffer.push(message);
            if last_flush.elapsed() >= flush_interval {
                flush(std::mem::take(&mut buffer));
                last_flush = instant::Instant::now();
            }
        }
    });
}

/// A socket that reconnects on error.
pub struct AutoSocket {
    url: String,
    socket: Option<WebSocket>,
}

impl AutoSocket {
    /// Opens a new socket connection.
    pub fn open(url: String) -> Self {
        let socket = WebSocket::open(&url).ok();
        Self { url, socket }
    }

    /// Waits for the next message to be message received.
    ///
    /// Waits indefinitely when no connection can be established.
    pub async fn next(&mut self) -> String {
        loop {
            if let Some(socket) = self.socket.as_mut() {
                // WebSocket::next hangs when the socket is closed, so reconnect instead.
                if !matches!(socket.state(), State::Closed) {
                    if let Some(Ok(Message::Text(message))) = socket.next().await {
                        return message;
                    }
                }
            }
            self.reconnect().await;
        }
    }

    async fn reconnect(&mut self) {
        log!("Attempting reconnect...");
        sleep(Duration::from_millis(1000)).await;
        if !matches!(self.socket.as_ref().map(|s| s.state()), Some(State::Open)) {
            self.socket = WebSocket::open(&self.url).ok();
        }
    }
}
