use anyhow::Result;
use scylla::transport::session::Session;
use scylla::cql_to_rust::FromRow;
use scylla::macros::{FromRow};
use std::env;
use std::sync::Arc;

use tokio::sync::Semaphore;

#[derive(FromRow)]
struct MyTypes { 
    pub my_text: String,
    pub my_int: i32,
    pub my_bigint: i64,
    pub my_boolean: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = Arc::new(Session::connect(uri, None).await?);

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session.query(r#"
        CREATE TABLE ks.my_types (
            my_text text,
            my_int int,
            my_bigint bigint,
            my_boolean bool,
            PRIMARY KEY (my_text)
        )
    "#, &[]).await?;

    #[allow(overflowing_literals)]
    let data =  MyTypes {
        my_text: "hello world!".to_string(),
        my_int: 0xDEADC0DE,
        my_bigint: 0xDEADBEEFC0DEDEAD,
        my_boolean: false,
    };

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t2 (a int, b int, c text, primary key (a, b))",
            &data,
        )
        .await?;

    let prepared = Arc::new(
        session
            .prepare("INSERT INTO ks.t2 (a, b, c) VALUES (?, ?, 'abc')")
            .await?,
    );

    let parallelism = 256;
    let sem = Arc::new(Semaphore::new(parallelism));

    for i in 0..100_000usize {
        if i % 1000 == 0 {
            println!("{}", i);
        }
        let session = session.clone();
        let prepared = prepared.clone();
        let permit = sem.clone().acquire_owned().await;
        tokio::task::spawn(async move {
            let i = i;
            session
                .execute(&prepared, (i as i32, 2 * i as i32))
                .await
                .unwrap();

            let _permit = permit;
        });
    }

    // Wait for all in-flight requests to finish
    for _ in 0..parallelism {
        sem.acquire().await.forget();
    }

    println!("Ok.");

    Ok(())
}
