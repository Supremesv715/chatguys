#![allow(unused)]
use futures::{channel, stream};

use futures::{SinkExt, stream::StreamExt};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tungstenite::http::request::Parts;

use tokio_tungstenite::{accept_async, tungstenite::Message};

type Sender = mpsc::UnboundedSender<Message>;
struct ChannelManager {
    channels: Arc<Mutex<std::collections::HashMap<String, Vec<Sender>>>>,
}

impl ChannelManager {
    fn new() -> Self {
        ChannelManager {
            channels: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }
    async fn add_sender_to_channel(&self, channel_name: String, sender: Sender) {
        let mut channels = self.channels.lock().await;
        if let Some(senders) = channels.get_mut(&channel_name) {
            senders.push(sender);
        }
    }
    async fn get_or_create_channel(&self, channel_name: String) {
        let mut channels = self.channels.lock().await;
        if !channels.contains_key(&channel_name) {
            channels.insert(channel_name.clone(), Vec::new());
        }
    }
    async fn remove_sender_from_channel(&self, channel_name: String, sender: &Sender) {
        let mut channels = self.channels.lock().await;
        if let Some(senders) = channels.get_mut(&channel_name) {
            senders.retain(|s| !std::ptr::eq(s, sender));
        }
    }
    async fn broadcast(&self, channel_name: String, message: Message) {
        let channels = self.channels.lock().await;
        if let Some(senders) = channels.get(&channel_name) {
            for sender in senders.iter() {
                let _ = sender.send(message.clone());
            }
        }
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    channel_manager: Arc<ChannelManager>,
) {
    let ws_stream = accept_async(stream)
        .await
        .expect("Error during Websocket handshake");
    println!("New WebSocket connection: {}", addr);
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let channel_manager_clone = channel_manager.clone();
    
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write.send(msg).await.is_err() {
                break;
            }
        }
    });
    
    let mut current_channel = String::new();
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if text.starts_with("CREATE_ROOM:") {
                    let room_name = &text[12..];
                    channel_manager_clone
                        .get_or_create_channel(room_name.to_string())
                        .await;
                    channel_manager_clone
                        .add_sender_to_channel(room_name.to_string(), tx.clone())
                        .await;
                    current_channel = room_name.to_string();
                    println!("Room created/joined: {}", room_name);
                } else if text.starts_with("join_room:") {
                    let room_name = &text[10..];
                    channel_manager_clone
                        .get_or_create_channel(room_name.to_string())
                        .await;
                    channel_manager_clone
                        .add_sender_to_channel(room_name.to_string(), tx.clone())
                        .await;
                    current_channel = room_name.to_string();
                    println!("Room joined: {}", room_name);
                } else if text.starts_with("leave_room:") {
                    let room_name = &text[11..].trim();
                    channel_manager_clone
                        .remove_sender_from_channel(room_name.to_string(), &tx)
                        .await;
                    println!("Left room: {}", room_name);
                    current_channel.clear();
                } else if text.starts_with("room_msg:") {
                    let parts: Vec<&str> = text[9..].splitn(3, ':').collect();
                    if parts.len() == 3 {
                        let room_name = parts[0].trim();
                        let username = parts[1].trim();
                        let message = parts[2].trim();
                        let formatted_msg = format!("{}: {}", username, message);
                        println!("Broadcasting to {}: {}", room_name, formatted_msg);
                        channel_manager_clone
                            .broadcast(room_name.to_string(), Message::Text(formatted_msg))
                            .await;
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
        }
    }
    
    // Clean up when connection closes
    if !current_channel.is_empty() {
        channel_manager_clone
            .remove_sender_from_channel(current_channel, &tx)
            .await;
    }
    
    println!("WebSocket connection closed: {}", addr);
}

pub async fn start_server() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("WebSocket server started at ws://127.0.0.1:8080");
    let channel_manager = Arc::new(ChannelManager::new());
    
    while let Ok((stream, addr)) = listener.accept().await {
        let channel_manager = channel_manager.clone();
        tokio::spawn(handle_connection(stream, addr, channel_manager));
    }
    Ok(())
}