#![allow(unused)]
use futures::{SinkExt, stream::StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};

use tokio_tungstenite::{accept_async, tungstenite::Message};

type Sender = mpsc::UnboundedSender<Message>;

struct RoomState {
    members: Vec<(Sender, String)>,
}

struct ChannelManager {
    channels: Arc<Mutex<HashMap<String, RoomState>>>,
}

impl ChannelManager {
    fn new() -> Self {
        ChannelManager {
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn add_sender_to_channel(&self, channel_name: String, sender: Sender) {
        let mut channels = self.channels.lock().await;
        channels.entry(channel_name).or_insert_with(|| RoomState {
            members: Vec::new(),
        });

        let _ = sender;
    }

    async fn add_member_to_channel(&self, channel_name: String, sender: Sender, username: String) {
        let mut channels = self.channels.lock().await;
        let room = channels.entry(channel_name).or_insert_with(|| RoomState {
            members: Vec::new(),
        });

        if !room.members.iter().any(|(existing_sender, _)| existing_sender.same_channel(&sender)) {
            room.members.push((sender, username));
        }
    }

    async fn remove_sender_from_channel(&self, channel_name: String, sender: &Sender) {
        let mut channels = self.channels.lock().await;
        if let Some(room) = channels.get_mut(&channel_name) {
            room.members.retain(|(existing_sender, _)| !existing_sender.same_channel(sender));
        }
    }

    async fn get_or_create_channel(&self, channel_name: String) {
        let mut channels = self.channels.lock().await;
        channels.entry(channel_name).or_insert_with(|| RoomState {
            members: Vec::new(),
        });
    }

    async fn list_rooms(&self) -> Vec<String> {
        let channels = self.channels.lock().await;
        let mut rooms: Vec<String> = channels.keys().cloned().collect();
        rooms.sort();
        rooms
    }

    async fn room_users(&self, channel_name: &str) -> Vec<String> {
        let channels = self.channels.lock().await;
        channels
            .get(channel_name)
            .map(|room| room.members.iter().map(|(_, username)| username.clone()).collect())
            .unwrap_or_default()
    }

    async fn broadcast(&self, channel_name: String, message: Message) {
        let channels = self.channels.lock().await;
        if let Some(room) = channels.get(&channel_name) {
            for (sender, _) in room.members.iter() {
                let _ = sender.send(message.clone());
            }
        }
    }
}

async fn send_room_users(tx: &Sender, channel_manager: &Arc<ChannelManager>, room_name: &str) {
    let users = channel_manager.room_users(room_name).await;
    let response = if users.is_empty() {
        format!("ROOM_USERS:{}:", room_name)
    } else {
        format!("ROOM_USERS:{}:{}", room_name, users.join(","))
    };
    let _ = tx.send(Message::Text(response));
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
    let mut current_username = String::new();
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if text.starts_with("CREATE_ROOM:") {
                    let parts: Vec<&str> = text[12..].splitn(2, ':').collect();
                    if parts.len() != 2 {
                        continue;
                    }
                    let room_name = parts[0].trim().to_string();
                    let username = parts[1].trim().to_string();
                    channel_manager_clone
                        .get_or_create_channel(room_name.clone())
                        .await;
                    channel_manager_clone
                        .add_member_to_channel(room_name.clone(), tx.clone(), username.clone())
                        .await;
                    current_channel = room_name.clone();
                    current_username = username;
                    println!("Room created/joined: {}", room_name);
                    let _ = tx.send(Message::Text(format!("JOINED:{}", room_name)));
                    send_room_users(&tx, &channel_manager_clone, &room_name).await;
                } else if text.starts_with("JOIN_ROOM:") || text.starts_with("join_room:") {
                    let parts: Vec<&str> = text[10..].splitn(2, ':').collect();
                    if parts.len() != 2 {
                        continue;
                    }
                    let room_name = parts[0].trim().to_string();
                    let username = parts[1].trim().to_string();
                    channel_manager_clone
                        .get_or_create_channel(room_name.clone())
                        .await;
                    channel_manager_clone
                        .add_member_to_channel(room_name.clone(), tx.clone(), username.clone())
                        .await;
                    current_channel = room_name.clone();
                    current_username = username;
                    println!("Room joined: {}", room_name);
                    let _ = tx.send(Message::Text(format!("JOINED:{}", room_name)));
                    send_room_users(&tx, &channel_manager_clone, &room_name).await;
                } else if text.starts_with("leave_room:") {
                    let room_name = &text[11..].trim();
                    channel_manager_clone
                        .remove_sender_from_channel(room_name.to_string(), &tx)
                        .await;
                    println!("Left room: {}", room_name);
                    current_channel.clear();
                    current_username.clear();
                    let _ = tx.send(Message::Text(format!("LEFT_ROOM:{}", room_name)));
                } else if text == "LIST_ROOMS" {
                    let rooms = channel_manager_clone.list_rooms().await;
                    let response = if rooms.is_empty() {
                        "ROOM_LIST:".to_string()
                    } else {
                        format!("ROOM_LIST:{}", rooms.join(","))
                    };
                    let _ = tx.send(Message::Text(response));
                } else if text.starts_with("room_msg:") {
                    let parts: Vec<&str> = text[9..].splitn(3, ':').collect();
                    if parts.len() == 3 {
                        let room_name = parts[0].trim();
                        let username = parts[1].trim();
                        let message = parts[2].trim();
                        let formatted_msg = format!("{}: {}", username, message);
                        println!("Broadcasting to {}: {}", room_name, formatted_msg);
                        channel_manager_clone
                            .broadcast(room_name.to_string(), Message::Text(formatted_msg.clone()))
                            .await;
                    }
                } else if text == "ROOM_USERS" {
                    if !current_channel.is_empty() {
                        send_room_users(&tx, &channel_manager_clone, &current_channel).await;
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