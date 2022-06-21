use clickhouse_rs::Pool;
use futures_util::StreamExt;
use std::{env, error::Error, thread};
use tokio::task;

///NOTE this is a pure query concurrency bench client,
///    all schemas and data should be prepared previously

async fn execute(database_url: String) -> Result<(), Box<dyn Error>> {
    // env::set_var("RUST_LOG", "clickhouse_rs=debug");
    // env_logger::init();
    let pool = Pool::new(database_url);

    println!("to start query loop...");
    let num_queries = 1000;
    // let client_sync = Arc::new(client);

    let ts = tokio::time::Instant::now();
    for _ in 0..num_queries {
        // let mut client = client.clone();
        let mut client = pool.get_handle().await.unwrap();
        tokio::spawn(async move {
        // Force the `Rc` to stay in a scope with no `.await`
        {
            let mut stream = client.query("select count(total_amount) from nyct_lite where pickup_datetime>='2016-01-31 12:00:00' and pickup_datetime<'2016-02-01 00:00:00' and total_amount<0").stream();
            while let Some(row) = stream.next().await {
                let row = row.unwrap();
                assert_eq!(row.len(), 1);
                let ct_res: u64 = row.get(0).unwrap();
                assert_eq!(ct_res, 70);
                // println!("ct_res: {:}", ct_res);
            }
        }
        task::yield_now().await;
    }).await.unwrap();
    }

    let time = ts.elapsed();

    let qps = num_queries as f64 / time.as_secs_f64();
    println!(
        "[target=ClickHouse]\n  Total {} adhoc concurrent queries done in time: {:?}, max QPS: {}",
        num_queries, time, qps
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let count = thread::available_parallelism()?.get();
    assert!(count >= 1);

    let args: Vec<String> = env::args().collect();
    assert!(
        args.len() > 1,
        "Clickhouse TCP server ip address should be provided"
    );
    let ip_addr = &args[1];

    let database_url = format!(
        "tcp://{}:9000?compression=lz4&pool_max={}&pool_min={}",
        ip_addr, count, count
    );
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| database_url.into());
    execute(database_url).await
}
