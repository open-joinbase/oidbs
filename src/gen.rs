use crate::{
    error::{OidbsError, OidbsResult},
    model::{GenWriter, Model},
};
use chrono::{Duration, NaiveDateTime};
use clap::Args;
use csv::WriterBuilder;
use std::{
    cmp,
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::PathBuf,
    thread,
};

#[derive(Args, Debug)]
pub struct Gen {
    /// output generated data directory
    output_dir: String,

    /// number of workers to start, different workers will generate different data files
    #[clap(short, long, default_value_t = 64)]
    workers: u32,

    /// the start timestamp for generated dataset
    #[clap(short, long, default_value_t = String::from("2021-01-01 00:00:01"))]
    timestamp_start: String,

    /// the timestamp step for generated dataset, in seconds
    #[clap(short, long, default_value_t = 10)]
    step: u32,

    /// the total messages to generate
    #[clap(short = 'm', long, default_value_t = 200_000_000)]
    total_messages: u64,

    /// a multiplier to control the number unique devices identifier
    #[clap(short = 'i', long, default_value_t = 1)]
    id_multiplier: i64,

    /// format of output, options: csv, json
    #[clap(short = 'f', long, default_value_t = String::from("csv"))]
    format: String,
}

#[derive(Debug, Clone)]
pub struct Generator {
    pub num_workers: u32,
    pub msgs_per_worker_per_id: u64,
    pub total_msgs: u64,
    pub path: String,
    pub gen_data_start_ts: NaiveDateTime,
    pub gen_data_interval: u32,
    pub models: Vec<Model>,
    pub id_multiplier: i64,
    pub format: String,
}

pub fn gen_data(mut g: Generator, i: u32) -> OidbsResult<()> {
    log::debug!("worker#{} to start...", i);
    let start_dt = g.gen_data_start_ts;
    let idm = g.id_multiplier;
    let interval_sec = g.gen_data_interval;
    let output_dir = PathBuf::from(&g.path);
    for model in g.models.iter_mut() {
        if model.has_completed {
            // log::debug!("to gen data for model: {:#?}...", &model);
            let ext_name = format!(".{}", g.format);
            let gen_file_path = model.get_gen_file_path(output_dir.clone(), i, ext_name.as_str());
            log::debug!("gen_file_path: {:#?}...", gen_file_path.as_path());
            let gen_file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .append(true)
                .open(gen_file_path.clone())?;
            let buf1 = BufWriter::with_capacity(128 * 1024, gen_file);
            let mut wtr = WriterBuilder::new().has_headers(false).from_writer(buf1);
            //FIXME buf2 is ugly hacking
            let gen_file = OpenOptions::new()
                .create_new(false)
                .write(true)
                .append(true)
                .open(gen_file_path)?;
            let mut buf2 = BufWriter::with_capacity(128 * 1024, gen_file);

            for j in 0..g.msgs_per_worker_per_id {
                let ts = start_dt + Duration::seconds(interval_sec as i64 * j as i64);
                for id in (i as i64 * idm)..(i as i64 + 1) * idm {
                    match g.format.as_str() {
                        "csv" => {
                            model.gen(id, ts, GenWriter::Csv(&mut wtr))?;
                        }
                        _ => {
                            model.gen(id, ts, GenWriter::Json(&mut buf2))?;
                            buf2.write(&[b'\n'])?;
                        }
                    }
                }
            }
        }
    }

    log::debug!("worker#{} ended", i);
    Ok(())
}

impl Generator {
    pub fn new(gen: Gen, models: Vec<Model>) -> Result<Self, OidbsError> {
        let num_workers = gen.workers as u32;
        let id_multiplier = gen.id_multiplier;
        let msgs_per_worker_per_id = cmp::max(
            gen.total_messages / num_workers as u64 / id_multiplier as u64,
            1,
        );
        if msgs_per_worker_per_id * num_workers as u64 * id_multiplier as u64 != gen.total_messages
        {
            return Err(OidbsError::InvalidArgs(
                "For simplicity， the number of total messages should be divisible by workers."
                    .to_string(),
            ));
        }

        Ok(Generator {
            format: gen.format,
            path: gen.output_dir,
            total_msgs: gen.total_messages,
            gen_data_start_ts: NaiveDateTime::parse_from_str(
                &gen.timestamp_start,
                "%Y-%m-%d %H:%M:%S",
            )
            .map_err(|_| OidbsError::InvalidArgs("timestamp_start".into()))?,
            gen_data_interval: gen.step,
            num_workers,
            msgs_per_worker_per_id,
            models,
            id_multiplier,
        })
    }

    pub fn run(self) -> Result<(), OidbsError> {
        for model in self.models.iter() {
            model.ensure_gen_dir_clean(self.path.as_str())?;
        }
        thread::scope(|s| {
            for i in 0..self.num_workers {
                let g = self.clone();
                s.spawn(move || {
                    if let Err(e) = gen_data(g, i) {
                        panic!("error: {}", e.to_string())
                    }
                });
            }
        });

        log::debug!("Generator run done!");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveDateTime};
    // use rand::{prelude::SmallRng, SeedableRng};

    // use crate::{error::OidbsResult, model::Model};

    // use super::{Gen, Generator};

    #[test]
    fn test_datetime() {
        let ts = NaiveDateTime::parse_from_str("2022-02-02 22:22:22", "%Y-%m-%d %H:%M:%S").unwrap();
        println!("ts: {}", ts + Duration::seconds(10i64));
        assert_eq!(
            NaiveDateTime::parse_from_str("2022-02-02 22:22:32", "%Y-%m-%d %H:%M:%S").unwrap(),
            ts + Duration::seconds(10i64)
        )
    }

    macro_rules! hashmap{
        ( $($key:tt : $val:expr),* $(,)? ) =>{{
            #[allow(unused_mut)]
            let mut map = ::std::collections::HashMap::with_capacity(hashmap!(@count $($key),* ));
            $(
                #[allow(unused_parens)]
                let _ = map.insert($key, $val);
            )*
            map
        }};
        (@replace $_t:tt $e:expr ) => { $e };
        (@count $($t:tt)*) => { <[()]>::len(&[$( hashmap!(@replace $t ()) ),*]) }
    }

    // #[test]
    // fn test_generator() -> OidbsResult<()> {
    //     let models = vec![Model {
    //         name: "m4".into(),
    //         target_infos: hashmap!(("joinbase".to_string()) : Schema { schema: "".into(), table: "".into(), database: "".into() }),
    //         rng: SmallRng::seed_from_u64(123),
    //         has_completed: true,
    //     }];
    //     let gen = Gen {
    //         output_dir: "/data/n4/oidbs_data".into(),
    //         workers: 1,
    //         timestamp_start: "2022-02-02 22:22:22".into(),
    //         step: 1,
    //         total_messages: 10,
    //         id_multiplier: 1,
    //         format: "csv".into(),
    //     };
    //     let g = Generator::new(gen, models)?;
    //     g.run()
    // }
}
