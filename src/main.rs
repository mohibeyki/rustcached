use scc::HashIndex;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;

pub enum Command {
    Get { key: String },
    Set { key: String, value: String },
    None,
}

pub fn parse_command(cmd: String) -> Command {
    match cmd.split_once(' ') {
        Some(("get", key)) => Command::Get {
            key: key.to_string(),
        },
        Some(("set", rest)) => {
            rest.split_once(' ')
                .map_or(Command::None, |(key, value)| Command::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                })
        }
        _ => Command::None,
    }
}

async fn handle_client(
    socket: tokio::net::TcpStream,
    db: Arc<HashIndex<String, Vec<u8>>>,
) -> Result<(), Box<dyn Error>> {
    tracing::info!("got a new connection.");
    socket.readable().await?;

    let mut buf_stream = tokio::io::BufReader::new(socket);

    loop {
        let mut line = String::new();
        match buf_stream.read_line(&mut line).await {
            Ok(0) => {
                tracing::info!("connection closed.");
                break;
            }
            Ok(n) => {
                tracing::trace!("received [{}] bytes.", n)
            }
            Err(e) => {
                tracing::error!("error reading from socket: [{}].", e);
                break;
            }
        }

        tracing::trace!("received [{}]", line.trim());
        match parse_command(line.trim().to_string()) {
            Command::Get { key } => {
                tracing::trace!("command: get [{}].", key);

                if let Some(value) = db.get(&key) {
                    tracing::trace!("key [{}]: [{}].", key, String::from_utf8_lossy(value.get()));
                } else {
                    tracing::trace!("key [{}] not found.", key);
                }
            }
            Command::Set { key, value } => {
                tracing::trace!("command: set [{}]: [{}]).", key, value);

                if db.contains(&key) {
                    db.remove_async(&key).await;
                }

                match db.insert_async(key.clone(), value.into_bytes()).await {
                    Ok(_) => {
                        tracing::trace!("key [{}] inserted.", key);
                    }
                    Err((k, v)) => {
                        tracing::error!(
                            "key [{}] already exists with value [{:?}]. This should never occur!",
                            k,
                            v
                        );
                    }
                }
            }
            Command::None => {
                tracing::error!("unrecognized command.");
            }
        }
    }
    Ok(())
}

async fn run(db: Arc<HashIndex<String, Vec<u8>>>) {
    tracing::info!("starting up.");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:11211")
        .await
        .unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        let db = db.clone();

        tokio::spawn(async move {
            match handle_client(socket, db.clone()).await {
                Ok(_) => {
                    tracing::info!("connection handled successfully.");
                }
                Err(e) => {
                    tracing::error!("error occured during handling connection: [{}].", e);
                }
            }
        });
    }
}

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();

    let db: HashIndex<String, Vec<u8>> = scc::HashIndex::default();
    run(Arc::new(db)).await;
}
