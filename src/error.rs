use crate::mqtt_client;
use std::io;
use thiserror::Error;

pub type OidbsResult<T> = Result<T, OidbsError>;

#[derive(Error, Debug)]
pub enum OidbsError {
    #[error("Unimplemented Model {0}")]
    UnimplementedModel(String),
    #[error("Generic errors: {0}")]
    Generic(&'static str),
    #[error("missing parameter {0}")]
    MissingArgs(String),
    #[error("Invalid {0} parameter")]
    InvalidArgs(String),
    #[error("IO Error {0}")]
    IOError(#[from] io::Error),
    #[error("MQTT Error {0}")]
    MQTTError(#[from] mqtt_client::Error),
    #[error("PG Error {0}")]
    PgError(#[from] tokio_postgres::Error),
    #[error(transparent)]
    WalkdirError(#[from] walkdir::Error),
    #[error(transparent)]
    CsvError(#[from] csv::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    VarError(#[from] std::env::VarError),
    #[error(transparent)]
    LibpqError(#[from] libpq::errors::Error),
}
