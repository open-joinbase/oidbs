use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::str::FromStr;
use std::{fs, path::PathBuf, vec};

use crate::error::{OidbsResult, OidbsError};
use chrono::NaiveDateTime;
use csv::Writer;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use serde_derive::Serialize;
use std::io::BufWriter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Model {
    pub name: String,
    pub target_infos: HashMap<String, TargetInfo>,
    pub rng: SmallRng,
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

            const COMPLETED_MODELS: &'static [&'static str] = &["m4"];
            let has_completed = COMPLETED_MODELS.contains(&name.as_str());
            let mut model = Model {
                name: name.clone(),
                target_infos: Default::default(),
                rng: SmallRng::seed_from_u64(123),
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

#[derive(Debug, Serialize)]
struct M4 {
    id: i64,
    name: String,
    firmware_version: String,
    location: String,
    pox: u32,
    pox_status: String,
    ts: NaiveDateTime,
    coins: u32,
    cpu_usage: u8,
    mem_usage: u16,
}

impl M4 {
    fn new(id: i64, ts: NaiveDateTime, rng: &mut SmallRng) -> Self {
        const VERS: &'static [&'static str] =
            &["1.0", "1.1", "1.2", "1.3", "2.0", "2.1", "3.0", "3.1"];
        const STATUS: &'static [&'static str] = &["new", "accept", "core"];
        let pox: u32 = rng.gen_range(0..10_000_000);
        let coins: u32 = rng.gen_range(0..10_000_000);
        let cpu_usage: u8 = rng.gen_range(0..101);
        let mem_usage: u16 = rng.gen_range(500..64_000);
        Self {
            id,
            name: format!("MyCoin Miner {}", id),
            firmware_version: VERS[id as usize % VERS.len()].to_string(),
            location: format!("room#{}", id / 128), //FIXME configurable
            pox,
            pox_status: STATUS[id as usize % STATUS.len()].to_string(),
            ts,
            coins,
            cpu_usage,
            mem_usage,
        }
    }
}

// Create small, cheap to initialize and fast RNG with a random seed.
// The randomness is supplied by the operating system.
impl Model {
    //TODO it is better to have declarative gen method
    pub fn gen(
        &mut self,
        id: i64,
        ts: NaiveDateTime,
        // buffer: &mut impl Write,
        gen_wrt: GenWriter,
    ) -> OidbsResult<()> {
        match gen_wrt {
            GenWriter::Csv(wtr) => {
                match self.name.as_str() {
                    "m4" => wtr.serialize(M4::new(id, ts, &mut self.rng))?,
                    _ => todo!(),
                }

                // let bs = wtr.into_inner().unwrap();
                // println!("bs: {}", String::from_utf8(bs).unwrap());
                // buffer.write(&bs).unwrap();
            }
            GenWriter::Json(wtr) => {
                serde_json::to_writer(wtr, &M4::new(id, ts, &mut self.rng))?;
            }
        }
        Ok(())
    }

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
    use std::{fs::OpenOptions, io::BufWriter, path::PathBuf};

    use chrono::NaiveDateTime;
    use csv::WriterBuilder;
    use rand::{prelude::SmallRng, SeedableRng};

    use crate::model::GenWriter;

    use super::{read_from_path, Model};

    #[test]
    fn test_read_from_path_and_gen() {
        let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        root.push("models");
        let models = read_from_path(root.display().to_string());
        println!("{:#?}", models);

        for mut m in models {
            if m.name == "m4" {
                let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open("/tmp/test")
                    .unwrap();
                let buf = BufWriter::with_capacity(128 * 1024, f);
                let mut wtr = WriterBuilder::new().has_headers(false).from_writer(buf);
                let ts = NaiveDateTime::parse_from_str("2022-02-02 11:11:11", "%Y-%m-%d %H:%M:%S")
                    .unwrap();
                m.gen(123, ts, GenWriter::Csv(&mut wtr)).unwrap();
            }
        }
    }

    #[test]
    fn test_gen_2() {
        // let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        // root.push("models");
        // let models = read_from_path(root.display().to_string());
        let mut m = Model {
            name: "m4".into(),
            target_infos: Default::default(),
            rng: SmallRng::seed_from_u64(123),
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
        m.gen(123, ts, GenWriter::Json(&mut buf)).unwrap();
        m.gen(456, ts, GenWriter::Json(&mut buf)).unwrap();
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
