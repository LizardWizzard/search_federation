use opensearch::indices::{IndicesCreateParts, IndicesDeleteParts};
use opensearch::{IndexParts, OpenSearch};
use serde_json::json;

const PG_CONNSTR: &str = "postgres://postgres:pgpass@127.0.0.1:5432";

async fn make_pg_conn() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(PG_CONNSTR, tokio_postgres::NoTls)
        .await
        .expect("cant connect to postgres");

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
}

fn data() -> &'static [(i32, &'static str, &'static str)] {
    &[
        (1, "news", "no matches for anything"),
        (2, "publication", "Postgres is great"),
        (3, "news", "Postgres and Oracle are ok to use"),
        (
            4,
            "news",
            "Postgres is rather good and is quite far from Oracle",
        ),
    ]
}

async fn seed_pg() {
    let conn = make_pg_conn().await;

    let queries = [
        "DROP TABLE IF EXISTS documents",
        "CREATE TABLE documents (id int primary key, type text, content text)",
    ];
    for query in queries {
        conn.execute(query, &[])
            .await
            .expect(&format!("failed to execute '{query}'"));
    }

    for (id, kind, content) in data() {
        conn.execute(
            "INSERT INTO documents VALUES ($1, $2, $3)",
            &[id, kind, content],
        )
        .await
        .expect(&format!(
            "failed to insert with '{id}' '{kind}' '{content}'"
        ));
    }
}

async fn seed_opensearch() {
    let client = OpenSearch::default();

    // Delete the index
    let mut response = client
        .indices()
        .delete(IndicesDeleteParts::Index(&["documents"]))
        .send()
        .await
        .unwrap();

    let successful = response.status_code().is_success();

    if successful {
        println!("Successfully deleted the index");
    } else {
        println!(
            "Could not delete the index: {}",
            &response.text().await.unwrap()
        );
    }

    // Create an index
    response = client
        .indices()
        .create(IndicesCreateParts::Index("documents"))
        .body(json!({
            "mappings" : {
                "properties" : {
                    "content" : { "type" : "text" }
                }
            }
        }))
        .send()
        .await
        .unwrap();

    let mut successful = response.status_code().is_success();

    if successful {
        println!("Successfully created an index");
    } else {
        println!("Could not create an index");
    }

    for (id, kind, content) in data() {
        response = client
            .index(IndexParts::Index("documents"))
            .body(json!({
                "id": id,
                "kind": kind,
                "content": content,
            }))
            .send()
            .await
            .unwrap();

        successful = response.status_code().is_success();

        if successful {
            println!("Successfully indexed a document");
        } else {
            println!("Could not index document");
        }
    }
}

pub async fn seed() {
    seed_pg().await;
    seed_opensearch().await;
}
