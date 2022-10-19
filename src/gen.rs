use crate::{
    error::{OidbsError, OidbsResult},
    model::{GenRecords, Model, PStations},
};
use chrono::{Duration, NaiveDateTime};
use clap::Args;
use rand::{rngs::SmallRng, SeedableRng};
use serde_json::{Map, Value};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
};

#[derive(Args, Debug)]
pub struct Gen {
    /// output generated data directory
    output_dir: String,

    /// number of workers to start, different workers will generate different data files
    #[clap(short, long, default_value_t = 1)]
    workers: u32,

    /// the start timestamp for generated dataset
    #[clap(short, long, default_value_t = String::from("2021-01-01 00:00:01"))]
    timestamp_start: String,

    /// interval per worker to gen, in seconds
    #[clap(short, long, default_value_t = 1)]
    interval_per_worker_sec: u32,

    /// the timestamp step for all dataset to gen, in seconds
    #[clap(short, long, default_value_t = 1)]
    step_sec: u32,

    /// format of output, options: csv, json
    #[clap(short = 'f', long, default_value_t = String::from("csv"))]
    format: String,

    /// make output out of order in bool, true if have
    #[clap(short, long)]
    out_of_order: bool,

    /// model parameters, in the model specific json string format
    #[clap(short, long, default_value_t = String::from("{}"))]
    model_parameters: String,
}

#[derive(Debug, Clone)]
pub struct Generator {
    pub num_workers: u32,
    pub path: String,
    pub gen_start_ts: NaiveDateTime,
    pub gen_interval_per_worker_sec: u32,
    pub gen_step_sec: u32,
    pub models: Vec<Model>,
    pub format: String,
    pub out_of_order: bool,
    pub model_parameters: Map<String, Value>,
}

pub fn gen_data(
    mut g: Generator,
    i: u32,
    model_paras: Map<String, Value>,
    gen_stats: Arc<Mutex<HashMap<String, u64>>>,
) -> OidbsResult<()> {
    log::debug!("worker#{} to start...", i);
    let sdt = g.gen_start_ts;
    let interval_per_worker_sec = g.gen_interval_per_worker_sec as i64;
    let step_sec = g.gen_step_sec as usize;
    let out_of_order = g.out_of_order;
    log::debug!("out_of_order: {}", out_of_order);
    let output_dir = PathBuf::from(&g.path);
    let mut rng: SmallRng = SmallRng::seed_from_u64(666666);
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
                .open(gen_file_path)?;
            let mut buf = BufWriter::with_capacity(1024 * 1024, gen_file);
            let model_name = model.name.as_str();
            let ts0 = sdt + Duration::seconds(interval_per_worker_sec as i64 * i as i64);
            let mut num_all_lines = 0u64;
            let mut ooo_buf = Vec::<String>::with_capacity(1024 * 1024);
            const CT_OOO: u32 = 5;//TODO configurable
            let mut ooo_ct = CT_OOO - 1;
            for tsp in (0..interval_per_worker_sec).step_by(step_sec) {
                let ts = ts0 + Duration::seconds(tsp);
                match g.format.as_str() {
                    "csv" => match model_name {
                        "pstations" => {
                            let lines = PStations::gen_csv_records(ts, &mut rng, &model_paras)?;
                            ooo_buf.extend_from_slice(&lines);
                            num_all_lines += lines.len() as u64;
                            if ooo_ct == 0 {
                                if out_of_order {
                                    fastrand::shuffle(&mut ooo_buf);
                                }
                                // buf.write_all(&)?;
                                for s in &ooo_buf {
                                    buf.write_all(s.as_bytes()).unwrap();
                                    buf.write(&[b'\n']);
                                }
                                ooo_buf.clear();
                                ooo_ct = CT_OOO - 1;
                            } else {
                                ooo_ct -= 1;
                            }
                        }
                        _ => unimplemented!(),
                    },
                    "json" => match model_name {
                        "pstations" => {
                            PStations::gen_json_records(ts, &mut rng, &model_paras)?;
                        }
                        _ => unimplemented!(),
                    },
                    format_str @ _ => {
                        unimplemented!("format: {} does not supported", format_str)
                    }
                }
            }
            if ooo_buf.len() > 0 {
                for s in &ooo_buf {
                    buf.write_all(s.as_bytes()).unwrap();
                    buf.write(&[b'\n']).unwrap();
                }
                ooo_buf.clear();
            }
            buf.flush().unwrap();

            gen_stats
                .lock()
                .unwrap()
                .entry(model_name.to_string())
                .and_modify(|e| *e += num_all_lines)
                .or_insert(num_all_lines);
        }
    }

    log::debug!("worker#{} ended", i);
    Ok(())
}

impl Generator {
    pub fn new(gen: Gen, models: Vec<Model>) -> Result<Self, OidbsError> {
        let num_workers = gen.workers as u32;
        let parsed: Value = serde_json::from_str(&gen.model_parameters)?;
        let model_parameters = parsed.as_object().unwrap().clone();
        Ok(Generator {
            format: gen.format,
            path: gen.output_dir,
            gen_start_ts: NaiveDateTime::parse_from_str(&gen.timestamp_start, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| OidbsError::InvalidArgs("timestamp_start".into()))?,
            gen_interval_per_worker_sec: gen.interval_per_worker_sec,
            num_workers,
            models,
            gen_step_sec: gen.step_sec,
            model_parameters,
            out_of_order: gen.out_of_order,
        })
    }

    pub fn run(self) -> Result<(), OidbsError> {
        for model in self.models.iter() {
            model.ensure_gen_dir_clean(self.path.as_str())?;
        }
        let gen_stats = Arc::new(Mutex::new(HashMap::new()));
        thread::scope(|s| {
            for i in 0..self.num_workers {
                let g = self.clone();
                let gs = gen_stats.clone();
                let mp = self.model_parameters.clone();
                s.spawn(move || {
                    if let Err(e) = gen_data(g, i, mp, gs) {
                        panic!("error: {}", e.to_string())
                    }
                });
            }
        });

        log::debug!("Generator run done!");
        let gs = gen_stats.lock().unwrap();
        for stat in &*gs {
            println!("model {} gen, total lines: {}", stat.0, stat.1);
        }

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
    //         name: "pstations".into(),
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
