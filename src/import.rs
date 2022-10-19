use crate::{
    error::{OidbsError, OidbsResult},
    model::{Model, TargetKind},
    mqtt_client::{client::Client, MqttOptions, QoS},
};
use clap::Args;
use log::*;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    str::FromStr,
    thread,
};
use tokio::time::Instant;
use tokio_postgres::{connect, tls};

#[derive(Args, Debug)]
pub struct Import {
    /// Input directory, which contains the dataset with supported formats(csv or json)
    input_dir: String,

    /// JoinBase server url part for MQTT endpoint
    #[clap(short='m', long, default_value_t = String::from("abc:abc@127.0.0.1:1883"))]
    ib_srv_part_mqtt: String,

    /// JoinBase server url part for pg wire protocol endpoint
    #[clap(short, long, default_value_t = String::from("abc:abc@127.0.0.1:5433"))]
    ib_srv_part_pg: String,

    /// Postgresql server url part for any timescale/postgresql wire protocol endpoints
    #[clap(short, long, default_value_t = String::from("postgres:postgres@127.0.0.1:5432"))]
    pg_srv_part: String,

    /// target database/databases to import, which allows users to import to a specified server. Options included of joinbase, timescale, all, default is joinbase
    ///
    /// !!!Important Note!!!
    ///
    /// 1. Only importing data to IoTbase is done by MQTT client writing one message by one message. Except IoTbase, all other databases are done in its batch me. Because if data/messages importing via one by one, no meaningful importing can be done in a meaningful time limitation.
    ///
    /// 2. To import to the TimescaleDB, we use the official `timescaledb-parallel-copy` tool. Because it is found that the common postgresql way to import a relative big dataset is very slow. So, make sure you have put the `timescaledb-parallel-copy`(https://github.com/timescale/timescaledb-parallel-copy) tool in your system path to before the TimescaleDB importing.
    #[clap(short, long, default_value_t = String::from("joinbase"))]
    target_kind: String,

    /// the model name to import, which allows users to import to a specified OIDBS data model. Options included of pstations, nyct
    #[clap(short='n', long, default_value_t = String::from("pstations"))]
    model_name: String,

    /// to only import data, it is the users's responsibility of to preparing all schemas previously
    #[clap(short = 'd', long)]
    import_data_only: bool,

    /// the number of workers for importing data into TimescaleDB via timescaledb-parallel-copy
    #[clap(short = 'w', long, default_value_t = 1)]
    num_workers_timescale: i32,

    /// the number of rows in one batch for importing data into JoinBase
    #[clap(short = 'b', long, default_value_t = 1)]
    num_rows_in_batch: i32,
}

#[derive(Debug, Clone)]
struct IBBrokerUrl {
    ib_broker_host: String,
    ib_broker_port: String,
    ib_broker_username: String,
    ib_broker_password: String,
}

impl IBBrokerUrl {
    fn parse_from(url_part: &str) -> OidbsResult<IBBrokerUrl> {
        let idx0 = url_part.find(':').unwrap();
        let idx1 = url_part.rfind('@').unwrap();
        let ib_broker_username = url_part[..idx0].to_string();
        let ib_broker_password = url_part[idx0 + 1..idx1].to_string();
        let ipp: Vec<&str> = url_part[idx1 + 1..].split(":").collect();
        let ib_broker_host = ipp[0].to_string();
        let ib_broker_port = ipp[1].to_string();
        Ok(Self {
            ib_broker_host,
            ib_broker_port,
            ib_broker_username,
            ib_broker_password,
        })
    }
}

pub struct Importer {
    ib_pg_uri: url::Url,
    ib_broker_uri: IBBrokerUrl,
    pg_uri: url::Url,
    // database: String,
    // table: String,
    // topic: String,
    data_dir: String,
    target: TargetKind,
    model: Model,
    import_data_only: bool,
    num_workers_timescale: i32,
    num_rows_in_batch: i32,
}

impl Importer {
    pub fn new(import: Import, models: Vec<Model>) -> Result<Self, OidbsError> {
        let target: TargetKind = TargetKind::from_str(import.target_kind.as_str())?;

        let ib_broker_uri = IBBrokerUrl::parse_from(&import.ib_srv_part_mqtt)
            .map_err(|_| OidbsError::InvalidArgs("broker".into()))?;
        // debug!("models")
        let model = if let Some(model) = models.iter().find(|m| m.name == import.model_name) {
            model.clone()
        } else {
            return Err(OidbsError::InvalidArgs(format!(
                "can not find a validate model for {}",
                import.model_name
            )));
        };
        //NOTE pg driver requires the database name to connect?!
        //NOTE all databases use same database name
        let ib_pg_uri: url::Url =
            ("postgres://".to_owned() + &import.ib_srv_part_pg + "/benchmark")
                .parse()
                .map_err(|_| OidbsError::InvalidArgs("broker".into()))?;
        let pg_uri: url::Url = ("postgres://".to_owned() + &import.pg_srv_part + "/benchmark")
            .parse()
            .map_err(|_| OidbsError::InvalidArgs("broker".into()))?;

        Ok(Self {
            ib_pg_uri,
            ib_broker_uri,
            pg_uri,
            // database: "benchmark".to_string(),
            // table: "puppet".to_string(),
            // topic: "".to_string(),
            data_dir: import.input_dir,
            target,
            model,
            import_data_only: import.import_data_only,
            num_workers_timescale: import.num_workers_timescale,
            num_rows_in_batch: import.num_rows_in_batch,
        })
    }

    pub async fn run(self) -> Result<(), OidbsError> {
        match self.target {
            TargetKind::JoinBase => {
                if !self.import_data_only {
                    self.setup_schemas(self.target.to_str(), self.ib_pg_uri.as_str())
                        .await
                        .unwrap();
                }
                let t = Instant::now();
                self.import_csv_to_ib()?;
                println!("importing done in {:#?}", t.elapsed());
            }
            TargetKind::TimeScale => {
                if !self.import_data_only {
                    self.setup_schemas(self.target.to_str(), self.pg_uri.as_str())
                        .await?;
                }
                let t = Instant::now();
                self.import_csv_to_tsdb()?;
                println!("importing done in {:#?}", t.elapsed());
            }
            // TargetKind::All => {
            //     self.import_csv_to_ib().await?;
            //     self.import_csv_to_pg()?;
            // }
            _ => todo!("."),
        }
        println!("imported data completed.");

        Ok(())
    }

    fn import_csv_to_ib(self) -> Result<(), OidbsError> {
        let broker_uri = self.ib_broker_uri.clone();
        println!("-> import to: {:?}", broker_uri);
        let mut options = MqttOptions::new(
            "oidbs",
            broker_uri.ib_broker_host,
            broker_uri
                .ib_broker_port
                .parse::<u16>()
                .map_err(|_| OidbsError::InvalidArgs("broker_uri".into()))?,
        );
        options.set_credentials(broker_uri.ib_broker_username, broker_uri.ib_broker_password);
        let model_dir = self.data_dir + "/" + &self.model.name;
        debug!("model_dir: {}", model_dir);
        thread::scope(|s| {
            for e in fs::read_dir(model_dir).unwrap() {
                let file_path = e.unwrap().path();
                println!("-> to import: {:?}", file_path.as_path());
                let schema = self
                    .model
                    .target_infos
                    .get("joinbase")
                    .expect("can not find a schema?");
                let topic = format!("/{}/{}", schema.database, schema.table);
                let opts = options.clone();
                s.spawn(move || {
                    let mut client = Client::new(opts).unwrap();
                    client.handshake().unwrap();
                    let file = File::open(file_path).unwrap();
                    let reader = BufReader::new(file);

                    // for res_line in reader.lines() {
                    //     // println!("{}", line);
                    //     let bs = res_line.unwrap().into();
                    //     if let Err(e) = client.publish_bytes(topic.clone(), QoS::AtMostOnce, bs) {
                    //         error!("publish failed, {}", e);
                    //     }
                    // }

                    let batch = self.num_rows_in_batch as usize;
                    let lines = reader.lines();
                    use itertools::Itertools;
                    for chunk in &lines.chunks(batch) {
                        let text = chunk.into_iter().map(|c| c.unwrap()).join("\n");
                        if let Err(e) = client.publish_bytes(topic.clone(), QoS::AtMostOnce, text.into()) {
                            error!("publish failed, {}", e);
                        }
                    }
                });
            }
        });

        Ok(())
    }

    fn import_csv_to_tsdb(&self) -> Result<(), OidbsError> {
        let model = &self.model;
        let model_dir = self.data_dir.to_string() + "/" + &model.name;
        debug!("model_dir: {}", model_dir);
        //NOTE other pg/tsdb/... uses the same database/table to ib
        let schema = model
            .target_infos
            .get("joinbase")
            .expect("can not find joinbase/pg/tsdb.. schema");
        for e in fs::read_dir(model_dir).unwrap() {
            let file_path = e.unwrap().path();
            let file = file_path.as_path().to_str().unwrap();
            println!("-> to import: {:?}", file);

            //timescaledb-parallel-copy --db-name nyc_data --table rides --file ./nyc_data_rides.csv --workers 4 --reporting-period 10s

            // println!(
            //     "command agrgs: {}, {}, {} ",
            //     &schema.database, &schema.table, file
            // );
            let ip_addr = match self.pg_uri.host().unwrap() {
                url::Host::Domain(d) => d.to_string(),
                url::Host::Ipv4(addr) => addr.to_string(),
                url::Host::Ipv6(_) => todo!(),
            };
            let port = self.pg_uri.port().unwrap_or(5432);
            let con_str = format!(
                "host={} port={} user=postgres password=postgres  dbname=benchmark sslmode=disable",
                ip_addr, port
            );
            debug!("con_str: {}", con_str);
            Command::new("timescaledb-parallel-copy")
                .arg("--connection")
                .arg(&con_str)
                .arg("--db-name")
                .arg(&schema.database)
                .arg("--table")
                .arg(&schema.table)
                .arg("--file")
                .arg(file)
                .arg("--workers")
                .arg(self.num_workers_timescale.to_string())
                .arg("--reporting-period")
                .arg("10s")
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .output()
                .expect("failed to execute timescaledb-parallel-copy");
        }

        Ok(())
    }

    pub async fn setup_schemas(&self, target: &str, uri: &str) -> Result<(), OidbsError> {
        println!("[setup_schemas]to connect to {} server: {}", target, uri);
        let (pg_client, pg_connection) = connect(uri, tls::NoTls).await.unwrap();
        debug!("to prepare connection...");
        tokio::spawn(async move {
            if let Err(e) = pg_connection.await {
                error!("!!!Err: {:?}", e);
            }
        });

        debug!("to setup schemas for {}...", target);
        match self.model.target_infos.get(target) {
            Some(v) => {
                run_simple_query(&pg_client, &v.schema).await?;
            }
            None => {
                log::debug!("no {} schema found for target: {}", self.model.name, target)
            }
        }
        debug!("schemas setup done!");

        Ok(())
    }
}

async fn run_simple_query(client: &tokio_postgres::Client, sql: &str) -> Result<(), OidbsError> {
    debug!("to run query: {}", sql);
    let _res = client.simple_query(sql).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use postgres::tls;
    use tokio_postgres::connect;
    use url::{EncodingOverride, Url};
    use urlencoding::decode;

    use crate::{error::OidbsResult, import::IBBrokerUrl};

    use super::run_simple_query;

    #[tokio::test]
    async fn test_some() -> OidbsResult<()> {
        let ib_pg_uri = "postgres://abc:abc@127.0.0.1:5433/benchmark";
        let (ib_pg_client, ib_pg_connection) = connect(ib_pg_uri, tls::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = ib_pg_connection.await {
                panic!("Err: {:?}", e);
            }
        });

        let schema = r#"drop table if exists benchmark.MyCoinMiner;
        create table benchmark.MyCoinMiner
        (
            id Int64,
            name String,
            firmware_version String,
            location String,
            pox UInt32,
            pox_status String,
            ts DateTime,
            coins UInt32,
            cpu_usage UInt8,
            mem_usage UInt16
        );"#;
        run_simple_query(&ib_pg_client, schema).await?;

        Ok(())
    }

    #[test]
    fn test_url_parse() {
        let mut ib_broker_uri: url::Url = ("mqtt://demo1:<CUR$O:Q@3.212.220.171:1883")
            .parse()
            .unwrap();
        // let ib_broker_uri: url::Url = url
        //     .parse("mqtt://demo1:<CUR$O:Q@3.212.220.171:1883")
        //     .unwrap();
        let decoded = decode(&ib_broker_uri.password().unwrap())
            .unwrap()
            .as_ref()
            .to_string();
        ib_broker_uri.set_password(Some("\x26"));
        println!("{:#?}, decoded: {}", ib_broker_uri, decoded);

        let url_part = "demo1:<CUR$O:Q@3.212.220.171:1883";
        let ib_broker_uri = IBBrokerUrl::parse_from(url_part).unwrap();
        println!("ib_broker_uri: {:#?}", ib_broker_uri);
    }
}
