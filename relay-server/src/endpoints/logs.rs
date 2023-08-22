use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::Response,
};

async fn handle_socket(mut socket: WebSocket) {
    let mut logs = relay_log::LOGS.1.resubscribe();

    while let Ok(entry) = logs.recv().await {
        let message = String::from_utf8_lossy(&entry).to_string();

        let res = socket.send(message.into()).await;
        if res.is_err() {
            break;
        }
    }
}

pub async fn handle(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}
