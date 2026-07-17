#![allow(unused)]
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use std::io::stdin;
use std::io::stdout;
use std::io::Write;

use futures::{stream::StreamExt, SinkExt};

use std::{error::Error, sync::Arc};

pub async fn start_client() -> Result<(), Box<dyn Error>> {
    let Ok((ws_stream, _)) = connect_async("ws://127.0.0.1:8080").await else {
        eprintln!("Failed to connect to WebSocket server");
        return Err("Connection failed".into());
    };
    println!("Connected to WebSocket server");
    
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    
    tokio::spawn({
        async move {
            while let Some(msg) = rx.recv().await {
                if write.send(msg).await.is_err() {
                    break;
                }
            }
        }
    });

    tokio::spawn({
        async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Some(rest) = text.strip_prefix("ROOM_LIST:") {
                            if rest.trim().is_empty() {
                                println!("Available rooms: none");
                            } else {
                                println!("Available rooms: {}", rest.replace(',', ", "));
                            }
                        } else if let Some(rest) = text.strip_prefix("ROOM_USERS:") {
                            let parts: Vec<&str> = rest.splitn(2, ':').collect();
                            if parts.len() == 2 {
                                let room_name = parts[0];
                                let users = parts[1];
                                if users.trim().is_empty() {
                                    println!("Users in {}: none", room_name);
                                } else {
                                    println!("Users in {}: {}", room_name, users.replace(',', ", "));
                                }
                            } else {
                                println!("{}", text);
                            }
                        } else if let Some(room) = text.strip_prefix("JOINED:") {
                            println!("You joined room: {}", room);
                        } else if let Some(room) = text.strip_prefix("LEFT_ROOM:") {
                            println!("Server confirmed you left room: {}", room);
                        } else if !text.starts_with("Room_MSG:") {
                            println!("{}", text);
                        }
                    }
                    Ok(Message::Binary(_)) => {}
                    Err(e) => {
                        eprintln!("Error receiving message: {}", e);
                        break;
                    }
                    _ => {
                        println!("Received an unknown message type");
                    }
                }
            }
        }
    });
    
    print!("Enter your username: ");
    stdout().flush()?;
    let mut username = String::new();
    stdin().read_line(&mut username)?;
    let username = username.trim().to_string();
    
    println!("Commands: /help, /rooms, /leave");
    println!("Use CREATE <ROOM_NAME> or JOIN <ROOM_NAME> to enter a room");
    
    let mut current_room = String::new();
    loop {
        print!("> ");
        stdout().flush()?;
        let mut input = String::new();
        stdin().read_line(&mut input)?;
        let input = input.trim();
        
        if input.starts_with("CREATE ") {
            current_room = input[7..].trim().to_string();
            tx.send(Message::Text(format!("CREATE_ROOM:{}:{}", current_room, username)))
                .expect("Failed to send message");
            break;
        } else if input.starts_with("JOIN ") {
            current_room = input[5..].trim().to_string();
            tx.send(Message::Text(format!("JOIN_ROOM:{}:{}", current_room, username)))
                .expect("Failed to send message");
            break;
        } else if input == "/help" {
            println!("/help   show commands");
            println!("/rooms  list available rooms");
            println!("/leave  leave the current room");
        } else if input == "/rooms" {
            tx.send(Message::Text("LIST_ROOMS".to_string()))
                .expect("Failed to send message");
        } else if input == "/leave" {
            println!("Join or create a room first, then /leave works inside the room.");
        } else {
            println!("Invalid command. Use CREATE, JOIN, /help, or /rooms.");
        }
    }
    
    println!("You can chat in room: {}", current_room);
    tx.send(Message::Text("ROOM_USERS".to_string()))
        .expect("Failed to send message");
    loop {
        print!("{} > ", username);
        stdout().flush()?;
        let mut message = String::new();
        stdin().read_line(&mut message)?;
        let message = message.trim();
        
        if message == "/help" {
            println!("/help   show commands");
            println!("/rooms  list available rooms");
            println!("/leave  leave the current room");
            continue;
        }

        if message == "/rooms" {
            tx.send(Message::Text("LIST_ROOMS".to_string()))
                .expect("Failed to send message");
            continue;
        }

        if message == "/leave" {
            tx.send(Message::Text(format!("leave_room: {}", current_room)))
                .expect("Failed to send message");
            println!("You left the room");
            break;
        }
        
        tx.send(Message::Text(format!(
            "room_msg:{}:{}:{}",
            current_room, username, message
        )))
        .expect("Failed to send message");
    }
    
    Ok(())
}