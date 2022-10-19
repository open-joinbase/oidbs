use crate::{
    error::OidbsError,
    model::{Model, TargetKind},
};
use clap::Args;
use comfy_table::{Cell, Row, Table};
use log::*;
// use postgres::SimpleQueryMessage;
use std::{
    io::Write,
    path::Path,
    str::FromStr,
    thread,
    time::{Duration, Instant},
};
// use tokio::time::{sleep, Instant};
// use tokio_postgres::{connect, tls};
use libpq::Status::*;

#[derive(Args, Debug)]
pub struct Bench {
    // /// Input directory, which contains formated files with queries
    // input_dir: String,
    /// JoinBase server url part for pg wire protocol endpoint
    #[clap(short, long, default_value_t = String::from("abc:abc@127.0.0.1:5433"))]
    ib_srv_part_pg: String,

    /// Postgresql server url part for any timescale/postgresql wire protocol endpoints
    #[clap(short, long, default_value_t = String::from("postgres:postgres@127.0.0.1:5432"))]
    pg_srv_part: String,

    /// target database/databases to query, which allows users to query to a specified server. Options included of joinbase, timescale, all, default is joinbase
    #[clap(short, long, default_value_t = String::from("joinbase"))]
    target_kind: String,

    /// the model name to query, which allows users to query to a specified OIDBS data model. Options included of pstations, nyct
    #[clap(short='n', long, default_value_t = String::from("pstations"))]
    model_name: String,

    /// the round times to formally run queries for the final performance measurement
    #[clap(short = 'r', long, default_value_t = 3)]
    run_times: u32,

    /// to control the measurement mode: `latency` is for measuring single query latency, and `concurrency` is for measuring the query throughput, a.k.a., QPS(Queries Per Second)
    #[clap(short = 'c', long, default_value_t = String::from("latency"))]
    measurement_mode: String,

    /// the times to run warm-up round for concurrency measurement mode, the run time in this round will not contributed to the final performance measurement
    #[clap(short = 'w', long, default_value_t = 10)]
    warmup_times: u32,

    /// the number of concurrent running threads, this option is only valid for the `concurrency` measurement mode
    #[clap(short = 'm', long, default_value_t = 24)]
    num_concurrent_threads: usize,

    /// the number of concurrent running threads, this option is only valid for the `concurrency` measurement mode
    #[clap(short = 'g', parse(try_from_str = true_or_false), default_value_t)]
    gen_to_results_csv: bool,
}

fn true_or_false(s: &str) -> Result<bool, &'static str> {
    match s {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err("expected `true` or `false`"),
    }
}

fn uppercase_first_letter(s: &str) -> String {
    if s == "joinbase" {
        "JoinBase".to_string()
    } else {
        s[0..1].to_uppercase() + &s[1..]
    }
}

// #[derive(Debug, Clone)]
// struct IBBrokerUrl {
//     ib_broker_host: String,
//     ib_broker_port: String,
//     ib_broker_username: String,
//     ib_broker_password: String,
// }

// impl IBBrokerUrl {
//     fn parse_from(url_part: &str) -> OidbsResult<IBBrokerUrl> {
//         let idx0 = url_part.find(':').unwrap();
//         let idx1 = url_part.rfind('@').unwrap();
//         let ib_broker_username = url_part[..idx0].to_string();
//         let ib_broker_password = url_part[idx0 + 1..idx1].to_string();
//         let ipp: Vec<&str> = url_part[idx1 + 1..].split(":").collect();
//         let ib_broker_host = ipp[0].to_string();
//         let ib_broker_port = ipp[1].to_string();
//         Ok(Self {
//             ib_broker_host,
//             ib_broker_port,
//             ib_broker_username,
//             ib_broker_password,
//         })
//     }
// }
#[derive(Debug, Clone, Copy)]
pub enum MeasurementMode {
    Latency,
    Concurrency,
}

impl MeasurementMode {
    pub fn to_str(&self) -> &'static str {
        match self {
            MeasurementMode::Latency => "latency",
            MeasurementMode::Concurrency => "concurrency",
        }
    }
}

impl FromStr for MeasurementMode {
    type Err = OidbsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latency" => Ok(MeasurementMode::Latency),
            "concurrency" => Ok(MeasurementMode::Concurrency),
            _ => Err(OidbsError::InvalidArgs(s.into())),
        }
    }
}

#[derive(Debug)]
pub struct QueryRequestor {
    ib_pg_uri: url::Url,
    pg_uri: url::Url,
    // data_dir: String,
    target: TargetKind,
    model: Model,
    warmup_times: u32,
    run_times: u32,
    measurement_mode: MeasurementMode,
    num_concurrent_threads: usize,
    gen_to_results_csv: bool,
}

struct QueryEntry {
    sql: String,
    desc: String,
    result: Option<libpq::Result>,
    meas_time: Duration,
}

impl QueryEntry {
    fn new(sql: &str, desc: &str) -> Self {
        Self {
            sql: sql.into(),
            desc: desc.into(),
            result: None,
            meas_time: Duration::from_secs(u64::MAX),
        }
    }
}

impl QueryRequestor {
    pub fn new(query: Bench, models: Vec<Model>) -> Result<Self, OidbsError> {
        let target: TargetKind = TargetKind::from_str(query.target_kind.as_str())?;
        // debug!("models")
        let model = if let Some(model) = models.iter().find(|m| m.name == query.model_name) {
            model.clone()
        } else {
            return Err(OidbsError::InvalidArgs(format!(
                "can not find a validate model for {}",
                query.model_name
            )));
        };
        //NOTE pg driver requires the database name to connect?!
        //NOTE all databases use same database name
        let ib_pg_uri: url::Url = ("postgres://".to_owned() + &query.ib_srv_part_pg + "/benchmark")
            .parse()
            .map_err(|_| OidbsError::InvalidArgs("broker".into()))?;
        let pg_uri: url::Url = ("postgres://".to_owned() + &query.pg_srv_part + "/benchmark")
            .parse()
            .map_err(|_| OidbsError::InvalidArgs("broker".into()))?;

        Ok(Self {
            ib_pg_uri,
            pg_uri,
            // data_dir: query.input_dir,
            target,
            model,
            warmup_times: query.warmup_times,
            run_times: query.run_times,
            measurement_mode: MeasurementMode::from_str(&query.measurement_mode)?,
            num_concurrent_threads: query.num_concurrent_threads,
            gen_to_results_csv: query.gen_to_results_csv,
        })
    }

    pub fn run(self) -> Result<(), OidbsError> {
        let mut entries = self.prepare_sqls();
        match self.target {
            TargetKind::JoinBase | TargetKind::TimeScale => {
                // let t = Instant::now();
                match self.measurement_mode {
                    MeasurementMode::Latency => {
                        self.run_latency_mode(&mut entries)?;
                        self.print_report(&entries);
                    }
                    MeasurementMode::Concurrency => {
                        self.run_concurrency_mode()?;
                    }
                }
                // println!("All queries done in {:#?}", t.elapsed());
            }
            // TargetKind::All => {
            //     self.query_csv_to_ib().await?;
            //     self.query_csv_to_pg()?;
            // }
            _ => todo!("..."),
        }
        // println!("all queries completed.");

        Ok(())
    }

    fn run_latency_mode(&self, entries: &mut Vec<QueryEntry>) -> Result<(), OidbsError> {
        let uri = match self.target {
            TargetKind::JoinBase => self.ib_pg_uri.as_str(),
            TargetKind::TimeScale => self.pg_uri.as_str(),
            TargetKind::All => todo!(),
        };
        let target = self.target.to_str();
        self.run_queries(entries, uri, target, self.run_times)?;
        Ok(())
    }

    fn run_concurrency_mode(&self) -> Result<(), OidbsError> {
        let uri = match self.target {
            TargetKind::JoinBase => self.ib_pg_uri.as_str(),
            TargetKind::TimeScale => self.pg_uri.as_str(),
            TargetKind::All => todo!(),
        };
        let target = self.target.to_str();

        {
            self.run_concurrent_queries(uri, target, true, self.warmup_times)?;
        }
        self.run_concurrent_queries(uri, target, false, self.run_times)?;
        Ok(())
    }

    fn prepare_sqls(&self) -> Vec<QueryEntry> {
        let target = self.target.to_str();
        let query = &self.model.target_infos.get(target).unwrap().query;
        let mut sqls = Vec::new();
        let lines = query.split('\n');
        for line in lines {
            if !line.is_empty() {
                let idx = line.find(':').unwrap();
                // println!("line: {:#?}", line);
                let desc = line[..idx].trim();
                let sql = line[idx + 1..].trim();
                sqls.push(QueryEntry::new(sql, desc));
            }
        }
        sqls
    }

    fn run_queries(
        &self,
        entries: &mut Vec<QueryEntry>,
        uri: &str,
        target: &str,
        runt_times: u32,
    ) -> Result<(), OidbsError> {
        // println!("[latency mode] To connect to {} server: {}", target, uri);
        println!("[latency mode][{}] warm up", target);
        //run phase
        println!("[latency mode][{}] run", target);
        let conn = libpq::Connection::new(uri).unwrap();

        for qe in entries.iter_mut() {
            for _ in 0..runt_times {
                let ts = Instant::now();
                let result = conn.exec(&qe.sql);
                match result.status() {
                    BadResponse | FatalError | NonFatalError => {
                        println!("fail to query{}", result.error_message().unwrap().unwrap());
                    }
                    _ => {}
                }
                let time = ts.elapsed();
                println!("{}: time: {:#?}", qe.desc, time);
                qe.result = Some(result);
                qe.meas_time = qe.meas_time.min(time);
            }
            thread::sleep(Duration::from_secs(1));
        }

        debug!("[latency mode] Queries for {} done", target,);

        Ok(())
    }

    fn run_concurrent_queries(
        &self,
        uri: &str,
        target: &str,
        is_warmup: bool,
        n: u32,
    ) -> Result<(), OidbsError> {
        let phase_label: &'static str = if is_warmup {
            "warmup|concurrency mode"
        } else {
            "run|concurrency mode"
        };
        let sql: &'static str = match target {
            "joinbase" => {
                "select count(total_amount) from nyct_lite where parts 2016013112 where total_amount<0"
            }
            "timescale" => {
                "select count(total_amount) from nyct_lite where pickup_datetime>='2016-01-31 12:00:00' and pickup_datetime<'2016-02-01 00:00:00' and total_amount<0"
            }
            _ => unimplemented!(),
        };

        let ts = Instant::now();
        thread::scope(|s| {
            for i in 0..self.num_concurrent_threads {
                let phase_label = phase_label.to_string();
                let uri = uri.to_string();
                s.spawn(move || {
                    // println!("[{}] To connect to {} server: {}", phase_label, target, uri);
                    let conn = libpq::Connection::new(uri.as_str()).unwrap();
                    println!("[{}][#{}]To run queries for  ...", phase_label, i);
                    for _ in 0..n {
                        let result = conn.exec(&sql);
                        use libpq::Status::*;

                        match result.status() {
                            BadResponse | FatalError | NonFatalError => {
                                println!(
                                    "fail to query{}",
                                    result.error_message().unwrap().unwrap()
                                );
                            }
                            _ => {
                                // for r in 0..result.ntuples() {
                                //     let res: String = String::from_utf8(result.value(r, 0).unwrap().to_vec())
                                //         .unwrap()
                                //         .parse()
                                //         .unwrap();
                                //     println!("res: {}", res);
                                // }
                            }
                        }
                    }
                });
            }
        });

        let time = ts.elapsed();
        let num_queries = n as usize * self.num_concurrent_threads;
        let qps = num_queries as f64 / time.as_secs_f64();
        println!(
            "[{}|target={}]\n  Total {} adhoc concurrent queries done in time: {:?}, max QPS: {}",
            phase_label, target, num_queries, time, qps
        );

        if !is_warmup && self.gen_to_results_csv {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open("concurrency_results.csv")
                .unwrap();
            writeln!(&mut file, "{},{}", uppercase_first_letter(target), qps).unwrap();
        }

        Ok(())
    }

    fn print_report(&self, entries: &Vec<QueryEntry>) {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        let header = Row::from(vec![
            "No",
            "Query Description",
            // "Query",
            "Best Query Latency",
        ]);
        table.set_header(header);
        table.set_width(50);

        let mut ct = 1usize;
        let mut stime = Duration::default();
        for e in entries {
            let cells = vec![
                Cell::new(ct),
                Cell::new(&e.desc),
                // Cell::new(&e.sql),
                Cell::new(format!("{:?}", e.meas_time)),
            ];
            table.add_row(cells);
            ct += 1;
            stime += e.meas_time;
        }
        println!("{}", table);
        println!("sum time of all queries(in millis): {}", stime.as_millis());

        if self.gen_to_results_csv {
            let is_results_first_created = !Path::new("latency_results.csv").exists();
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open("latency_results.csv")
                .unwrap();
            let results = entries
                .iter()
                .map(|e| e.meas_time.as_micros().to_string())
                .collect::<Vec<_>>();
            if is_results_first_created {
                let header = (1..=results.len())
                    .map(|i| format!("Q{}", i))
                    .collect::<Vec<_>>()
                    .join(",");
                writeln!(&mut file, "db,{}", header).unwrap();
            }
            writeln!(
                &mut file,
                "{},{}",
                uppercase_first_letter(self.target.to_str()),
                results.join(",")
            )
            .unwrap();
        }
    }
}

// pub fn compare_query_res(results: HashMap<String, Vec<SimpleQueryMessage>>) {
// }

#[cfg(test)]
mod tests {
    // use postgres::tls;
    // use tokio_postgres::connect;
    // use urlencoding::decode;

    // use crate::{error::OidbsResult};

    // #[tokio::test]
    // async fn test_some() -> OidbsResult<()> {
    //     let ib_pg_uri = "postgres://abc:abc@127.0.0.1:5433/benchmark";
    //     let (ib_pg_client, ib_pg_connection) = connect(ib_pg_uri, tls::NoTls).await?;

    //     tokio::spawn(async move {
    //         if let Err(e) = ib_pg_connection.await {
    //             panic!("Err: {:?}", e);
    //         }
    //     });

    //     let schema = r#"drop table if exists benchmark.MyCoinMiner;
    //     create table benchmark.MyCoinMiner
    //     (
    //         id Int64,
    //         name String,
    //         firmware_version String,
    //         location String,
    //         pox UInt32,
    //         pox_status String,
    //         ts DateTime,
    //         coins UInt32,
    //         cpu_usage UInt8,
    //         mem_usage UInt16
    //     );"#;
    //     run_simple_query(&ib_pg_client, schema).await?;

    //     Ok(())
    // }

    // #[test]
    // fn test_url_parse() {
    //     let mut ib_broker_uri: url::Url = ("mqtt://demo1:<CUR$O:Q@3.212.220.171:1883")
    //         .parse()
    //         .unwrap();
    //     // let ib_broker_uri: url::Url = url
    //     //     .parse("mqtt://demo1:<CUR$O:Q@3.212.220.171:1883")
    //     //     .unwrap();
    //     let decoded = decode(&ib_broker_uri.password().unwrap())
    //         .unwrap()
    //         .as_ref()
    //         .to_string();
    //     ib_broker_uri.set_password(Some("\x26"));
    //     println!("{:#?}, decoded: {}", ib_broker_uri, decoded);

    //     let url_part = "demo1:<CUR$O:Q@3.212.220.171:1883";
    //     let ib_broker_uri = IBBrokerUrl::parse_from(url_part).unwrap();
    //     println!("ib_broker_uri: {:#?}", ib_broker_uri);
    // }
}
