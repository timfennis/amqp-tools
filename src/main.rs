use anyhow::Context;
use clap::{Args, Parser, Subcommand, arg};
use dirs::config_dir;
use lapin::options::{BasicGetOptions, BasicRejectOptions};
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
    Read(ReadArgs),
}

#[derive(Args, Debug)]
struct ReadArgs {
    /// A connection name defined in the application config (other options will be ignored)
    #[arg(short, long, global = true)]
    connection: Option<String>,

    /// The name of the queue to read from.
    #[arg()]
    queue_name: String,

    /// Requeue the message after reading instead of acknowledging it.
    #[arg(long)]
    requeue: bool,

    /// The maximum number of messages to read.
    #[arg(long, short, default_value_t = 1)]
    limit: u32,

    #[arg(long, short)]
    output: Option<PathBuf>,
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
    if path.is_dir() {
        let file_path = path.join(format!("message_{offset}"));
        File::create(file_path)
    } else {
        if std::fs::exists(path)? {
            File::open(path)
        } else {
            File::create(path)
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Read(args) => {
            let uri = if let Some(connection_name) = args.connection {
                let config_map = Config::from_file(&Config::ensure_file_exists()?)?;
                config_map.get(&connection_name).unwrap().into()
            } else {
                todo!("you must specify -c for now");
            };

            let connection = Connection::connect_uri(uri, ConnectionProperties::default()).await?;
            // Match on the subcommand to execute the correct logic.
            let channel = connection
                .create_channel()
                .await
                .context("Failed to create channel")?;

            let mut read_count = 0;
            'main: for id in 0..args.limit {
                let message = channel
                    .basic_get(&args.queue_name, BasicGetOptions::default())
                    .await
                    .context("Failed to read message")?;

                if let Some(message) = message {
                    let mut output: Box<dyn Write> = if let Some(file_name) = &args.output {
                        Box::new(open_output_file(file_name, id)?)
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
                        .basic_reject(
                            message.delivery_tag,
                            BasicRejectOptions {
                                requeue: args.requeue,
                            },
                        )
                        .await
                        .context("failed to nack message")?;
                } else {
                    break 'main;
                }
            }

            eprintln!("Read {read_count} messages from {}", args.queue_name);
        }
    }

    Ok(())
}
