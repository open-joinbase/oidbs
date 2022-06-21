use std::{env, path::PathBuf};

use clap::{Parser, Subcommand};
use oidbs::{
    bench::{Bench, QueryRequestor},
    error::OidbsResult,
    gen::{Gen, Generator},
    import::{Import, Importer},
    model::read_from_path,
};
use tokio::runtime::Builder;

#[derive(Parser)]
#[clap(name = "Open IoT Database Benchmark Suite Tools")]
#[clap(author = "JoinBase team and all contributors")]
#[clap(version = "2022.06")]
#[clap(about = "Tools for Open IoT Database Benchmark Suite(OIDBS)", long_about = None)]
#[clap(propagate_version = true)]
struct Oidbs {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Generate kinds of benchmark datasets based on kinds of data models
    Gen(Gen),
    /// Import the benchmark dataset to JoinBase and other databases(TimescaleDB/PostgreSQL)
    Import(Import),
    /// Bench the performance of JoinBase and other databases(TimescaleDB/PostgreSQL) via kinds of queries and modes
    Bench(Bench),
}

fn main() -> OidbsResult<()> {
    env_logger::init();

    let oidbs = Oidbs::parse();

    let models = load_models()?;
    log::trace!("{:#?}", models);

    match oidbs.command {
        Commands::Gen(gen) => {
            log::trace!("gen: {:#?}", gen);
            let g = Generator::new(gen, models)?;
            g.run()?
        }
        Commands::Import(import) => {
            log::trace!("import: {:#?}", import);
            let i = Importer::new(import, models)?;
            let runtime = Builder::new_multi_thread()
                .worker_threads(2)
                .enable_io()
                .build()?;
            runtime.block_on(async { i.run().await.unwrap() });
        }
        Commands::Bench(query) => {
            log::trace!("query: {:#?}", query);
            let q = QueryRequestor::new(query, models)?;
            log::trace!("QueryRequestor: {:#?}", q);
            q.run()?;
        }
    }

    Ok(())
}

fn load_models() -> OidbsResult<Vec<oidbs::model::Model>> {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.push("models");
    let p = root.as_path();
    let models = if p.exists() {
        read_from_path(p.display().to_string())
    } else {
        let mut root = PathBuf::from(env::var("MODELS_ROOT")?);
        root.push("models");
        let p = root.as_path();
        if p.exists() {
            read_from_path(p.display().to_string())
        } else {
            panic!("Can not find a valid root dir of models. Consider specify the root dir of models via MODELS_ROOT enviroment variable!")
        }
    };
    Ok(models)
}
