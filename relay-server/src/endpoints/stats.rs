use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::Response,
};
use tokio::task::spawn_blocking;

pub async fn handle(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    if let Some(rx) = relay_statsd::with_client(|client| client.rx.clone()) {
        loop {
            let rx = rx.clone();
            if let Ok(Ok(bytes)) = spawn_blocking(move || rx.recv()).await {
                socket.send(bytes.into()).await.ok();
            }
        }
    }
}
