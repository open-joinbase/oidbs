use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::format;
use std::fs::File;
use std::str::FromStr;
use std::{fs, path::PathBuf, vec};

use crate::error::{OidbsError, OidbsResult};
use chrono::NaiveDateTime;
use csv::{Writer, WriterBuilder};
use rand::prelude::SmallRng;
use rand::Rng;
use serde_derive::Serialize;
use serde_json::{Map, Value};
use std::io::BufWriter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Model {
    pub name: String,
    pub target_infos: HashMap<String, TargetInfo>,
    pub has_completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetInfo {
    pub schema: String,
    pub database: String,
    pub table: String,
    pub query: String,
}

#[inline]
fn get_file_name(path: &PathBuf) -> String {
    path.file_name().unwrap().to_str().unwrap().to_string()
}

#[inline]
fn get_base_name(path: &PathBuf) -> String {
    basename(path.to_str().unwrap(), '/').to_string()
}

fn basename<'a>(path: &'a str, sep: char) -> Cow<'a, str> {
    let mut pieces = path.rsplit(sep);
    match pieces.next() {
        Some(p) => p.into(),
        None => path.into(),
    }
}

fn extract_db_tab(s: &str) -> Option<(String, String)> {
    if let Some(idx) = s.to_lowercase().find("create table") {
        let st = idx + "create table".len();
        let sp = s[st..].find('(').unwrap();
        // println!("st: {}, ed: {}", st, sp);
        let t = s[st..st + sp].trim().to_string();
        let ss: Vec<&str> = t.split('.').collect();
        if ss.len() == 2 {
            Some((ss[0].trim().to_string(), ss[1].trim().to_string()))
        } else {
            None
        }
    } else {
        None
    }
}

pub fn read_from_path(root_models: String) -> Vec<Model> {
    let mut rt = vec![];
    let r = PathBuf::from(root_models);
    // r.push("schemas");
    for e in fs::read_dir(r).unwrap() {
        let path = e.unwrap().path();
        if path.is_dir() {
            let name = get_base_name(&path);
            // log::debug!("{}, name: {:#?}", path.display(), name);

            const COMPLETED_MODELS: &'static [&'static str] = &["pstations"];
            let has_completed = COMPLETED_MODELS.contains(&name.as_str());
            let mut model = Model {
                name: name.clone(),
                target_infos: Default::default(),
                has_completed,
            };

            let mut schema_infos = HashMap::new();
            let mut query_infos = HashMap::new();

            let mut sp = path.clone();
            sp.push("schemas");
            // log::debug!("sp: {:#?}", sp.as_path());
            assert!(sp.is_dir());

            for f in fs::read_dir(&sp).unwrap() {
                let path = f.unwrap().path();
                assert!(path.is_file());
                let target = get_file_name(&path);
                // println!("path: {}, target: {:#?}", path.display(), target);
                let schema = fs::read_to_string(path).unwrap();
                schema_infos.insert(target, schema);
            }

            let mut qp = path.clone();
            // log::debug!("qp: {:#?}", qp.as_path());
            qp.push("queries");
            assert!(qp.is_dir() || !qp.exists());
            if qp.exists() {
                for f in fs::read_dir(&qp).unwrap() {
                    let path = f.unwrap().path();
                    assert!(path.is_file());
                    let target = get_file_name(&path);
                    // println!("path: {}, target: {:#?}", path.display(), target);
                    let query = fs::read_to_string(path).unwrap();
                    query_infos.insert(target, query);
                }
            }

            let mut keys = HashSet::new();
            keys.extend(schema_infos.keys());
            keys.extend(query_infos.keys());

            for target in keys {
                let s = schema_infos.get(target);
                let q = query_infos.get(target);
                model.target_infos.insert(
                    target.clone(),
                    match (s, q) {
                        (None, None) => unreachable!(),
                        (None, Some(query)) => TargetInfo {
                            schema: String::new(),
                            database: String::new(),
                            table: String::new(),
                            query: query.into(),
                        },
                        (Some(schema), None) => {
                            let (database, table) = extract_db_tab(schema).unwrap_or_default();
                            TargetInfo {
                                schema: schema.into(),
                                database,
                                table,
                                query: String::new(),
                            }
                        }
                        (Some(schema), Some(query)) => {
                            let (database, table) = extract_db_tab(schema).unwrap_or_default();
                            TargetInfo {
                                schema: schema.into(),
                                database,
                                table,
                                query: query.into(),
                            }
                        }
                    },
                );

                // if let Some((database, table)) = extract_db_tab(&schema) {
                //     model.target_infos.insert(
                //         target,
                //         TargetInfo {
                //             schema,
                //             database,
                //             table,
                //         },
                //     );
                // } else {
                //     //default to benchmark.$(model_name)
                //     model.target_infos.insert(
                //         target,
                //         TargetInfo {
                //             schema,
                //             database: "benchmark".into(),
                //             table: name.clone(),
                //         },
                //     );
                // }
            }

            rt.push(model);
        }
    }
    rt
}

pub enum GenWriter<'a> {
    Csv(&'a mut Writer<BufWriter<File>>),
    Json(&'a mut BufWriter<File>),
}

/*
NOTE
a continuous system available 24h a day, 7 days a week
millions of inserts per second

default:
5000 stations
200 sensor types
1M sensor points inserts per sec

1hour: 3600*1M = 3.6G
24h: 24*3.6G=86.4G
    18*86.4G=1.512T dump
*/

// station_id UInt32,
// sensor_id UInt8,
// sensor_kind UInt8,
// sensor_value Float32,
// ts DateTime
#[derive(Debug, Serialize)]
pub struct PStations {
    station_id: u32,
    sensor_id: u8,
    sensor_kind: u8,
    sensor_value: f32,
    ts: NaiveDateTime,
}

impl PStations {
    fn gen_records(
        ts: NaiveDateTime,
        rng: &mut SmallRng,
        model_paras: &Map<String, Value>,
    ) -> Vec<PStations> {
        let num_stations = if let Some(v) = model_paras.get("num_stations") {
            log::trace!("v:{}", v);
            let ret = v
                .as_i64()
                .expect("num_stations should be a postivie integer");
            assert!(ret > 0);
            ret as _
        } else {
            5_000u32
        };
        let num_sensors = if let Some(v) = model_paras.get("num_sensors") {
            log::trace!("v:{}", v);
            let ret = v
                .as_i64()
                .expect("num_stations should be a postivie integer");
            assert!(ret > 0);
            ret as _
        } else {
            200u8
        };
        let mut rt = Vec::with_capacity(num_stations as usize * num_sensors as usize);
        for station_id in 0..num_stations {
            for sensor_id in 0..num_sensors {
                let sensor_kind = sensor_id % 20;
                let sensor_value: f32 = rng.gen_range(
                    10.0 * 2i32.pow(sensor_kind as u32) as f32
                        ..50.0 * 2i32.pow(sensor_kind as u32) as f32,
                );
                rt.push(Self {
                    station_id,
                    sensor_id,
                    sensor_kind,
                    sensor_value,
                    ts,
                })
            }
        }
        rt
    }
}

type Records = (Vec<u8>, u64);

pub trait GenRecords {
    fn gen_csv_records(
        ts: NaiveDateTime,
        rng: &mut SmallRng,
        model_paras: &Map<String, Value>,
    ) -> OidbsResult<Vec<String>>;
    fn gen_json_records(
        ts: NaiveDateTime,
        rng: &mut SmallRng,
        model_paras: &Map<String, Value>,
    ) -> OidbsResult<Records>;
}

impl GenRecords for PStations {
    fn gen_csv_records(
        ts: NaiveDateTime,
        rng: &mut SmallRng,
        model_paras: &Map<String, Value>,
    ) -> OidbsResult<Vec<String>> {
        log::trace!("model_paras: {:?}", model_paras);
        let pss = PStations::gen_records(ts, rng, model_paras);
        let rt = pss
            .iter()
            .map(|ps| {
                format!(
                    "{},{},{},{},{}",
                    ps.station_id, ps.sensor_id, ps.sensor_kind, ps.sensor_value, ps.ts,
                )
            })
            .collect();
        Ok(rt)
    }

    fn gen_json_records(
        ts: NaiveDateTime,
        rng: &mut SmallRng,
        model_paras: &Map<String, Value>,
    ) -> OidbsResult<Records> {
        let pss = PStations::gen_records(ts, rng, model_paras);
        let nlines = pss.len() as u64;
        let mut wtr = vec![];
        for ps in pss {
            serde_json::to_writer(&mut wtr, &ps)?;
            wtr.push(b'\n');
        }
        Ok((wtr, nlines))
    }
}

// Create small, cheap to initialize and fast RNG with a random seed.
// The randomness is supplied by the operating system.
impl Model {
    //TODO it is better to have declarative gen method
    // pub fn gen_csv(&mut self, ts: NaiveDateTime) -> OidbsResult<Vec<u8>> {
    //     let model_name = self.name.as_str();
    //     match model_name {
    //         "pstations" => {
    //             let pss = PStations::gen_records(ts, &mut self.rng);
    //             let mut wtr = Writer::from_writer(vec![]);
    //             match gen_wrt {
    //                 GenWriter::Csv(wtr) => {
    //                     for ps in pss {
    //                         wtr.serialize(ps)?;
    //                     }
    //                 }
    //                 GenWriter::Json(wtr) => {
    //                     for ps in pss {
    //                         serde_json::to_writer(wtr, &ps)?;
    //                     }
    //                 }
    //             }
    //             // let bs = wtr.into_inner().unwrap();
    //             // println!("bs: {}", String::from_utf8(bs).unwrap());
    //             // buffer.write(&bs).unwrap();
    //         }
    //         _ => unimplemented!("{}", model_name),
    //     }
    //     Ok(())
    // }

    pub fn ensure_gen_dir_clean(&self, path: &str) -> OidbsResult<()> {
        let mut output = PathBuf::from(path);
        output.push(&self.name);
        if output.exists() {
            fs::remove_dir_all(output.as_path())?;
            fs::create_dir(output)?;
        } else {
            fs::create_dir(output)?;
        }

        Ok(())
    }

    pub fn get_gen_file_path(&self, mut output: PathBuf, path_seq: u32, ext_name: &str) -> PathBuf {
        output.push(&self.name);
        output.push(format!("{:06}{}", path_seq, ext_name));
        // log::debug!("gen_file_path: {:#?}...", output.as_path());
        output
    }

    // pub fn get_topic(&self, target: &str) -> Option<&str> {
    //     self.schemas.get(target).map(|s| s.topic.as_ref())
    // }
}

#[derive(Debug, Clone, Copy)]
pub enum TargetKind {
    JoinBase,
    TimeScale,
    All,
}

impl TargetKind {
    pub fn to_str(&self) -> &'static str {
        match self {
            TargetKind::JoinBase => "joinbase",
            TargetKind::TimeScale => "timescale",
            TargetKind::All => "all",
        }
    }
}

impl FromStr for TargetKind {
    type Err = OidbsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "joinbase" => Ok(TargetKind::JoinBase),
            "timescale" => Ok(TargetKind::TimeScale),
            "all" => Ok(TargetKind::All),
            _ => Err(OidbsError::InvalidArgs(s.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::{BufWriter, Write},
        path::PathBuf,
    };

    use chrono::NaiveDateTime;
    use rand::{prelude::SmallRng, SeedableRng};
    use serde_json::Value;

    use crate::model::{GenRecords, PStations};

    use super::{read_from_path, Model};

    #[test]
    fn test_read_from_path_and_gen() {
        let mut rng: SmallRng = SmallRng::seed_from_u64(666666);
        let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        root.push("models");
        let models = read_from_path(root.display().to_string());
        // println!("{:#?}", models);

        let output_path = "/tmp/test";
        for m in models {
            if m.name == "pstations" {
                println!("to gen for {:?}", m);
                let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(output_path)
                    .unwrap();
                // let buf = BufWriter::with_capacity(128 * 1024, f);F
                // let mut wtr = WriterBuilder::new().has_headers(false).from_writer(buf);
                let mut buf = BufWriter::with_capacity(128 * 1024, f);
                let ts = NaiveDateTime::parse_from_str("2022-02-02 11:11:11", "%Y-%m-%d %H:%M:%S")
                    .unwrap();
                let parsed: Value = serde_json::from_str("{}").unwrap();
                let model_paras = parsed.as_object().unwrap().clone();
                let rs = PStations::gen_csv_records(ts, &mut rng, &model_paras).unwrap();
                for s in rs {
                    buf.write_all(s.as_bytes()).unwrap();
                    buf.write(&[b'\n']);
                }
            }
        }
    }

    #[test]
    fn test_gen_2() {
        let mut rng: SmallRng = SmallRng::seed_from_u64(666666);
        // let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        // root.push("models");
        // let models = read_from_path(root.display().to_string());
        let m = Model {
            name: "pstations".into(),
            target_infos: Default::default(),
            has_completed: Default::default(),
        };
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/test")
            .unwrap();
        let mut buf = BufWriter::with_capacity(128 * 1024, f);
        let ts = NaiveDateTime::parse_from_str("2022-02-02 11:11:11", "%Y-%m-%d %H:%M:%S").unwrap();
        let parsed: Value = serde_json::from_str("{}").unwrap();
        let model_paras = parsed.as_object().unwrap().clone();
        let (bs, _nlines) = PStations::gen_json_records(ts, &mut rng, &model_paras).unwrap();
        buf.write_all(&bs).unwrap();
    }

    #[test]
    fn test_basename() {
        let bn = crate::model::basename("/a/b/c_d", '/');
        println!("bn: {}", bn);
        assert_eq!(bn, std::borrow::Cow::from("c_d"));
    }

    #[test]
    fn test_extract_db_tab() {
        let db_tab = crate::model::extract_db_tab("create table a123.b456\n");
        println!("db_tab: {:#?}", db_tab);
        assert_eq!(db_tab, Some(("a123".to_string(), "b456".to_string())));
    }
}
