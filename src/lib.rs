use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, sql::db_connection_pool::postgrespool::PostgresConnectionPool,
};
use opensearch::register_opensearch;
use secrecy::SecretString;

mod opensearch;
pub mod repl;
pub mod rules;
pub mod seed;
mod udf;

fn secret(s: &'static str) -> SecretString {
    SecretString::from(s.to_owned())
}

/// See https://github.com/datafusion-contrib/datafusion-table-providers/blob/main/examples/postgres.rs
async fn register_postgres(ctx: &SessionContext) {
    // Create PostgreSQL connection parameters
    let postgres_params: HashMap<String, secrecy::SecretString> = HashMap::from([
        ("host".to_string(), secret("127.0.0.1")),
        ("user".to_string(), secret("postgres")),
        ("db".to_string(), secret("postgres")),
        ("pass".to_string(), secret("pgpass")),
        ("port".to_string(), secret("5432")),
        ("sslmode".to_string(), secret("disable")),
    ]);

    // Create PostgreSQL connection pool
    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(postgres_params)
            .await
            .expect("unable to create PostgreSQL connection pool"),
    );

    // Create database catalog provider
    // This allows us to access tables through catalog structure (catalog.schema.table)
    let catalog = DatabaseCatalogProvider::try_new(postgres_pool)
        .await
        .unwrap();

    // Register PostgreSQL catalog, making it accessible via the "postgres" name
    ctx.register_catalog("postgres", Arc::new(catalog));

    // // Demonstrate direct table provider registration
    // // This method registers the table in the default catalog
    // // Here we register the PostgreSQL "companies" table as "companies_v2"
    // ctx.register_table(
    //     "companies_v2",
    //     table_factory
    //         .table_provider(TableReference::bare("companies"))
    //         .await
    //         .expect("failed to register table provider"),
    // )
    // .expect("failed to register table");
}

pub async fn make_context() -> SessionContext {
    let state = datafusion_federation::default_session_state();
    let ctx = SessionContext::new_with_state(state);

    register_postgres(&ctx).await;
    register_opensearch(&ctx);

    ctx
}
