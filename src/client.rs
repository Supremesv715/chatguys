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
    let message = Arc::new(Mutex::new(Vec::new()));
    
    tokio::spawn({
        async move {
            while let Some(msg) = rx.recv().await {
                if write.send(msg).await.is_err() {
                    break;
                }
            }
        }
    });

    let value = message.clone();
    tokio::spawn({
        let message = message.clone();
        async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        let mut msgs = message.lock().await;
                        if !text.starts_with("Room_MSG:") {
                            msgs.push(text.clone());
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
    
    println!("Do you want to create a room or join a room?");
    println!("Type 'CREATE <ROOM_NAME>' to create a room");
    println!("Type 'JOIN <ROOM_NAME>' to join a room");
    
    let mut current_room = String::new();
    loop {
        print!("> ");
        stdout().flush()?;
        let mut input = String::new();
        stdin().read_line(&mut input)?;
        let input = input.trim();
        
        if input.starts_with("CREATE ") {
            current_room = input[7..].to_string();
            tx.send(Message::Text(format!("CREATE_ROOM:{}", current_room)))
                .expect("Failed to send message");
            println!("You created and joined room: {}", current_room);
            break;
        } else if input.starts_with("JOIN ") {
            current_room = input[5..].to_string();
            tx.send(Message::Text(format!("CREATE_ROOM:{}", current_room)))
                .expect("Failed to send message");
            println!("You joined room: {}", current_room);
            
            let msgs = value.lock().await;
            println!("------ Previous Messages ------");
            for msg in msgs.iter() {
                println!("{}", msg);
            }
            println!("-------------------------------");
            break;
        } else {
            println!("Invalid command. Please type 'CREATE <ROOM_NAME>' or 'JOIN <ROOM_NAME>'.");
        }
    }
    
    println!("You can chat in room: {}", current_room);
    loop {
        print!("{} > ", username);
        stdout().flush()?;
        let mut message = String::new();
        stdin().read_line(&mut message)?;
        let message = message.trim();
        
        if message == "leave" {
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