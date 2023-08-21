use std::time::Duration;

use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::Response,
};

pub async fn handle(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    let mut counter = 0;
    loop {
        let res = socket.send(format!("dummy message {counter}").into()).await;
        if res.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        counter += 1;
    }
}
