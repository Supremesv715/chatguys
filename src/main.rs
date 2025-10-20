use tokio::task;
use std::env;

mod server;
mod client;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: cargo run -- <server|client|both>");
        return;
    }

    match args[1].as_str() {
        "server" => {
            if let Err(e) = server::start_server().await {
                eprintln!("Error starting server: {}", e);
            }
        }

        "client" => {
            if let Err(e) = client::start_client().await{
                eprintln!("Error starting client: {}", e);
            }
        }

        "both" => {
            let server_task = task::spawn(async {
                if let Err(e) = server::start_server().await{
                    eprintln!("Error running server: {}", e);
                }
            });

            let client_task = task::spawn(async {
                // delay client startup to let server initialize
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                if let Err(e) = client::start_client().await{
                    eprintln!("Error running client: {}", e);
                }
            });

            let _ = tokio::join!(server_task, client_task);
        }

        _ => {
            eprintln!("Invalid argument. Use 'server', 'client', or 'both'.");
        }
    }
}
