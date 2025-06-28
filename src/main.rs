use anyhow::{Context, anyhow};
use clap::{Args, Parser, Subcommand, arg};
use dirs::config_dir;
use lapin::options::{BasicAckOptions, BasicGetOptions, BasicPublishOptions, BasicRejectOptions};
use lapin::uri::{AMQPAuthority, AMQPScheme, AMQPUri, AMQPUserInfo};
use lapin::{Connection, ConnectionProperties};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

/// A CLI tool for interacting with RabbitMQ queues.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Read one or more messages from the queue (this removes the message)
    Read(ReadArgs),
    /// Peek at the head of the queue, leaving the head in place
    Peek(PeekArgs),
    /// Move all messages from the source queue to a destination queue
    Shovel(ShovelArgs),
}

#[derive(Args, Debug)]
struct ReadArgs {
    /// A connection name defined in the application config (other options will be ignored)
    #[arg(short, long, global = true)]
    connection: Option<String>,

    /// The name of the queue to read from.
    #[arg()]
    queue_name: String,

    /// The maximum number of messages to read.
    #[arg(long, short)]
    limit: Option<u32>,

    #[arg(long, short)]
    output: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct PeekArgs {
    /// A connection name defined in the application config (other options will be ignored)
    #[arg(short, long, global = true)]
    connection: Option<String>,

    /// The name of the queue to read from.
    #[arg()]
    queue_name: String,
}
#[derive(Args, Debug)]
struct ShovelArgs {
    #[arg(short, long)]
    source_connection: Option<String>,

    #[arg(short, long)]
    destination_connection: Option<String>,

    #[arg()]
    source_queue_name: String,

    #[arg()]
    destination_queue_name: String,
}

#[derive(Deserialize, Debug, Clone)]
struct Config {
    username: String,
    password: String,
    port: u16,
    host: String,
    secure: bool,
    vhost: String,
}

impl Config {
    pub fn ensure_file_exists() -> anyhow::Result<PathBuf> {
        let mut config_file = config_dir().context("cannot obtain config dir")?;
        (config_file).push("amqp-tools");
        if !config_file.exists() {
            std::fs::create_dir_all(&config_file).context("failed to create config directory")?;
        }

        config_file.push("config.toml");

        if !config_file.exists() {
            std::fs::File::create(&config_file).context("Failed to create config file")?;
        }

        Ok(config_file)
    }

    pub fn from_file(path: &PathBuf) -> anyhow::Result<HashMap<String, Self>> {
        Ok(toml::from_str(
            &std::fs::read_to_string(&path).context("cannot read config file")?,
        )?)
    }
}

impl Into<AMQPUri> for &Config {
    fn into(self) -> AMQPUri {
        AMQPUri {
            scheme: if self.secure {
                AMQPScheme::AMQPS
            } else {
                AMQPScheme::AMQP
            },
            authority: AMQPAuthority {
                userinfo: AMQPUserInfo {
                    username: self.username.to_string(),
                    password: self.password.to_string(),
                },
                host: self.host.to_string(),
                port: self.port,
            },
            vhost: self.vhost.to_string(),
            query: Default::default(),
        }
    }
}

fn open_output_file<D: Display>(path: &PathBuf, offset: D) -> std::io::Result<File> {
    if path.is_dir() || path.to_string_lossy().ends_with("/") {
        std::fs::create_dir_all(path)?;
        let file_path = path.join(format!("message_{offset}"));
        File::create(file_path)
    } else if path.is_file() {
        File::open(path)
    } else {
        File::create(path)
    }
}

fn get_uri_from_config(name: &str) -> anyhow::Result<AMQPUri> {
    let config_map = Config::from_file(&Config::ensure_file_exists()?)?;
    config_map
        .get(name)
        .ok_or(anyhow!("connection \"{name}\" does not exist in config"))
        .map(Into::into)
}

async fn create_connection_by_name(name: Option<&str>) -> anyhow::Result<Connection> {
    let uri = name
        .map(get_uri_from_config)
        .transpose()?
        .unwrap_or_default();

    Ok(Connection::connect_uri(uri, ConnectionProperties::default()).await?)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Read(args) => {
            let connection = create_connection_by_name(args.connection.as_deref()).await?;
            let channel = connection
                .create_channel()
                .await
                .context("failed to create channel")?;

            let mut read_count = 0;
            while let Some(message) = channel
                .basic_get(&args.queue_name, BasicGetOptions::default())
                .await
                .context("Failed to read message")?
            {
                let mut output: Box<dyn Write> = if let Some(file_name) = &args.output {
                    Box::new(open_output_file(file_name, read_count)?)
                } else {
                    Box::new(std::io::stdout())
                };

                read_count += 1;

                output
                    .write_all(&message.data)
                    .context("Failed to write message stdout")?;
                output.write(b"\n")?;
                output.flush()?;

                channel
                    .basic_ack(message.delivery_tag, BasicAckOptions { multiple: false })
                    .await
                    .context("failed to ack message")?;

                if read_count >= args.limit.unwrap_or(u32::MAX) {
                    break;
                }
            }

            eprintln!("Read {read_count} messages from {}", args.queue_name);
        }
        Commands::Peek(args) => {
            let connection = create_connection_by_name(args.connection.as_deref()).await?;
            let channel = connection.create_channel().await?;

            let message = channel
                .basic_get(&args.queue_name, BasicGetOptions::default())
                .await?;

            if let Some(message) = message {
                std::io::stdout().write_all(&message.data)?;

                channel
                    .basic_reject(message.delivery_tag, BasicRejectOptions { requeue: true })
                    .await?;
            } else {
                println!("the queue is empty");
            }
        }
        Commands::Shovel(args) => {
            // TODO: should we try to figure out that source and destination might be the same server?
            let source = create_connection_by_name(args.source_connection.as_deref()).await?;
            let source_channel = source.create_channel().await?;

            let destination = create_connection_by_name(args.source_connection.as_deref()).await?;
            let destination_channel = destination.create_channel().await?;

            while let Some(msg) = source_channel
                .basic_get(&args.source_queue_name, BasicGetOptions::default())
                .await?
            {
                destination_channel
                    .basic_publish(
                        "",
                        &args.destination_queue_name,
                        BasicPublishOptions::default(),
                        &msg.data,
                        msg.properties.clone(),
                    )
                    .await?;

                source_channel
                    .basic_ack(msg.delivery_tag, BasicAckOptions::default())
                    .await?;
            }
        }
    }

    Ok(())
}
