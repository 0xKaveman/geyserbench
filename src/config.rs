use crate::proto::geyser::CommitmentLevel;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigToml {
    pub config: Config,
    pub endpoint: Vec<Endpoint>,
    #[serde(default)]
    pub backend: BackendSettings,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub transactions: i32,
    pub account: String,
    pub commitment: ArgsCommitment,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Endpoint {
    pub name: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_addr: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_follow: bool,
    pub kind: EndpointKind,
}

fn default_true() -> bool {
    true
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct BackendSettings {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EndpointKind {
    Yellowstone,
    Arpc,
    Thor,
    Shredstream,
    #[serde(rename = "shredstream_raw")]
    ShredstreamRaw,
    #[serde(rename = "raw_shred")]
    RawShred,
    Node1,
    #[serde(
        rename = "tx_stream",
        alias = "subscribe_tx_stream",
        alias = "subscribe-tx-stream"
    )]
    TxStream,
    #[serde(rename = "xw_tx")]
    XwTx,
    Shreder,
    #[serde(rename = "shreder_binary")]
    ShrederBinary,
    Jetstream,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

impl ArgsCommitment {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArgsCommitment::Processed => "processed",
            ArgsCommitment::Confirmed => "confirmed",
            ArgsCommitment::Finalized => "finalized",
        }
    }
}

impl EndpointKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EndpointKind::Yellowstone => "yellowstone",
            EndpointKind::Arpc => "arpc",
            EndpointKind::Thor => "thor",
            EndpointKind::Shredstream => "shredstream",
            EndpointKind::ShredstreamRaw => "shredstream_raw",
            EndpointKind::RawShred => "raw_shred",
            EndpointKind::Node1 => "node1",
            EndpointKind::TxStream => "tx_stream",
            EndpointKind::XwTx => "xw_tx",
            EndpointKind::Shreder => "shreder",
            EndpointKind::ShrederBinary => "shreder_binary",
            EndpointKind::Jetstream => "jetstream",
        }
    }
}

impl ConfigToml {
    pub fn load(path: &str) -> Result<Self> {
        let content =
            fs::read_to_string(path).with_context(|| format!("Failed to read config {}", path))?;
        let config = toml::from_str(&content).map_err(|err| anyhow!(err))?;
        Ok(config)
    }

    pub fn create_default(path: &str) -> Result<Self> {
        let default_config = ConfigToml {
            config: Config {
                transactions: 1000,
                account: "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string(),
                commitment: ArgsCommitment::Processed,
            },
            endpoint: vec![
                Endpoint {
                    name: "grpc".to_string(),
                    url: "http://fra.corvus-labs.io:10101".to_string(),
                    x_token: None,
                    control_addr: None,
                    is_follow: false,
                    kind: EndpointKind::Yellowstone,
                },
                Endpoint {
                    name: "arpc".to_string(),
                    url: "http://fra.corvus-labs.io:20202".to_string(),
                    x_token: None,
                    control_addr: None,
                    is_follow: false,
                    kind: EndpointKind::Arpc,
                },
            ],
            backend: BackendSettings::default(),
        };

        let toml_string = toml::to_string_pretty(&default_config)
            .context("Failed to serialize default config")?;
        fs::write(path, toml_string)
            .with_context(|| format!("Failed to write default config {}", path))?;

        Ok(default_config)
    }

    pub fn load_or_create(path: &str) -> Result<Self> {
        if Path::new(path).exists() {
            Self::load(path)
        } else {
            Self::create_default(path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigToml, EndpointKind};

    #[test]
    fn parses_node1_endpoint_kind() {
        let parsed: ConfigToml = toml::from_str(
            r#"
[config]
transactions = 1
account = "*"
commitment = "processed"

[[endpoint]]
name = "node1"
url = "udp://0.0.0.0:9999"
kind = "node1"
"#,
        )
        .unwrap();

        assert_eq!(parsed.endpoint[0].kind, EndpointKind::Node1);
    }

    #[test]
    fn parses_tx_stream_endpoint_kind_with_control_addr() {
        let parsed: ConfigToml = toml::from_str(
            r#"
[config]
transactions = 1
account = "11111111111111111111111111111111"
commitment = "processed"

[[endpoint]]
name = "tx-stream"
url = "udp://0.0.0.0:3032"
control_addr = "127.0.0.1:3031"
is_follow = true
kind = "tx_stream"
"#,
        )
        .unwrap();

        assert_eq!(parsed.endpoint[0].kind, EndpointKind::TxStream);
        assert_eq!(
            parsed.endpoint[0].control_addr.as_deref(),
            Some("127.0.0.1:3031")
        );
        assert!(parsed.endpoint[0].is_follow);
    }

    #[test]
    fn parses_raw_shred_endpoint_kind() {
        let parsed: ConfigToml = toml::from_str(
            r#"
[config]
transactions = 1
account = "*"
commitment = "processed"

[[endpoint]]
name = "raw"
url = "udp://0.0.0.0:12000"
kind = "raw_shred"
"#,
        )
        .unwrap();

        assert_eq!(parsed.endpoint[0].kind, EndpointKind::RawShred);
    }

    #[test]
    fn parses_shreder_binary_endpoint_kind() {
        let parsed: ConfigToml = toml::from_str(
            r#"
[config]
transactions = 1
account = "*"
commitment = "processed"

[[endpoint]]
name = "shreder-binary"
url = "http://ny.binary.shreder.xyz:9991"
kind = "shreder_binary"
"#,
        )
        .unwrap();

        assert_eq!(parsed.endpoint[0].kind, EndpointKind::ShrederBinary);
    }
}
