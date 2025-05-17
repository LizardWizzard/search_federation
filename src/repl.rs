use std::slice::Iter;
use std::sync::Arc;
use std::{any::Any, collections::HashMap};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Builder, RecordBatch};
use datafusion::catalog::Session;
use datafusion::common::project_schema;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError as DfError;
use datafusion::error::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion::scalar::ScalarValue;
use datafusion::{
    arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef},
    catalog::TableProvider,
    prelude::*,
    sql::TableReference,
};
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, sql::db_connection_pool::postgrespool::PostgresConnectionPool,
};
use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use opensearch::http::Url;
use opensearch::{OpenSearch, SearchParts};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use secrecy::SecretString;
use serde_json::{json, Value};

use crate::udf::{self, OPENSEARCH_INTERVALS_MARKER_UDF_NAME};

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

#[derive(Debug)]
struct OpenSearchExec {
    properties: PlanProperties,
    table_name: String,
    projected_schema: SchemaRef,
    filters: OpenSearchFilters,
    client: OpenSearch,
}

impl OpenSearchExec {
    fn new(
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        filters: OpenSearchFilters,
        schema: &SchemaRef,
        table_name: String,
        client: OpenSearch,
    ) -> Self {
        let projected_schema = project_schema(&schema, projection).expect("cant project schema");

        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        use datafusion::physical_expr;
        use datafusion::physical_plan;
        Self {
            properties: PlanProperties::new(
                eq_properties,
                physical_expr::Partitioning::UnknownPartitioning(1),
                physical_plan::ExecutionMode::Bounded,
            ),
            projected_schema,
            filters,
            table_name,
            client,
        }
    }

    fn query_for_filters(&self) -> serde_json::Value {
        match &self.filters {
            OpenSearchFilters::Intervals {
                field,
                terms,
                max_gap,
            } => {
                json!({
                    "query": {
                        "intervals": {
                            field: {
                                "match": {
                                    "query": terms,
                                    "max_gaps": max_gap,
                                }
                            }
                        }
                    }
                })
            }
            OpenSearchFilters::Wildcard {
                field,
                pattern,
                case_insensitive,
            } => json!({
                "query": {
                    "wildcard": {
                        field: {
                            "value": pattern,
                            "case_insensitive": case_insensitive,
                        }
                    }
                }
            }),
        }
    }
}

impl ExecutionPlan for OpenSearchExec {
    fn name(&self) -> &str {
        "OpenSearchExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let projected_schema = self.projected_schema.clone();
        let client = self.client.clone();
        let query = self.query_for_filters();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            // TODO: Stream this to work on bigger result sets
            futures::stream::once(async move {
                assert_eq!(
                    projected_schema.fields().len(),
                    1,
                    "can retrieve only id: {projected_schema:?}"
                );

                let mut cols: Vec<Arc<dyn Array>> =
                    Vec::with_capacity(projected_schema.fields().len());

                let response = client
                    .search(SearchParts::Index(&[&table_name]))
                    .from(0)
                    .size(10)
                    .body(query)
                    .send()
                    .await
                    .unwrap();

                let mut id_col_builder = Int64Builder::new();

                let response_body = response.json::<Value>().await.unwrap();
                for hit in response_body["hits"]["hits"].as_array().unwrap() {
                    id_col_builder.append_value(hit["_source"]["id"].as_i64().unwrap());
                    // println!("{}", serde_json::to_string_pretty(&hit).unwrap());
                }
                cols.push(Arc::new(id_col_builder.finish()));

                RecordBatch::try_new(projected_schema, cols)
                    .map_err(|e| DfError::ArrowError(e, None))
            }),
        )))
    }
}

impl DisplayAs for OpenSearchExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match &self.filters {
            OpenSearchFilters::Intervals {
                field,
                terms,
                max_gap,
            } => f.write_fmt(format_args!(
                "OpenSearchExec filters=[intervals(field={}, terms='{}' max_gap={})]",
                field, terms, max_gap
            )),
            OpenSearchFilters::Wildcard {
                field,
                pattern,
                case_insensitive,
            } => f.write_fmt(format_args!(
                "OpenSearchExec filters=[wildcard(field={}, pattern='{}' case_insensitive={})]",
                field, pattern, case_insensitive
            )),
        }
    }
}

fn take_column<'a>(iter: &mut Iter<'a, Expr>, arg_name: &'static str) -> DfResult<&'a Column> {
    let column_expr = iter.next().unwrap();
    let Expr::Column(col) = column_expr else {
        return Err(DfError::Execution(format!(
            "Expected columnt reference for `{arg_name}` argument, got: {}",
            column_expr
        )));
    };
    Ok(col)
}

fn take_utf8_literal<'a>(iter: &mut Iter<'a, Expr>, arg_name: &'static str) -> DfResult<&'a str> {
    let literal_expr = iter.next().unwrap();
    let Expr::Literal(ScalarValue::Utf8(Some(literal))) = literal_expr else {
        return Err(DfError::Execution(format!(
            "Expected non null scalar value for `{arg_name}` argument, got: {}",
            literal_expr
        )));
    };
    Ok(literal)
}

fn take_bool_literal<'a>(iter: &mut Iter<'a, Expr>, arg_name: &'static str) -> DfResult<bool> {
    let literal_expr = iter.next().unwrap();
    let Expr::Literal(ScalarValue::Boolean(Some(literal))) = literal_expr else {
        return Err(DfError::Execution(format!(
            "Expected non null scalar value for `{arg_name}` argument, got: {}",
            literal_expr
        )));
    };
    Ok(*literal)
}

fn take_uint64_literal<'a>(iter: &mut Iter<'a, Expr>, arg_name: &'static str) -> DfResult<i64> {
    let literal_expr = iter.next().unwrap();
    let Expr::Literal(ScalarValue::Int64(Some(literal))) = literal_expr else {
        return Err(DfError::Execution(format!(
            "Expected non null scalar value for `{arg_name}` argument, got: {}",
            literal_expr
        )));
    };
    Ok(*literal)
}

#[derive(Debug)]
enum OpenSearchFilters {
    Intervals {
        field: String,
        terms: String,
        max_gap: i64,
    },
    Wildcard {
        field: String,
        pattern: String,
        case_insensitive: bool,
    },
}

impl OpenSearchFilters {
    fn from_scalar_function(f: &ScalarFunction) -> DfResult<OpenSearchFilters> {
        match f.name() {
            "opensearch_intervals" => Self::from_intervals_function(f),
            "opensearch_wildcard" => Self::from_wildcard_function(f),
            unknown_name @ _ => Err(DfError::Execution(format!(
                "unknown open search function: {unknown_name}"
            ))),
        }
    }

    fn from_intervals_function(f: &ScalarFunction) -> DfResult<OpenSearchFilters> {
        if f.args.len() != 3 {
            return Err(DfError::Internal(format!(
                "This is a bug. Somehow got incorrect number of arguments to marker udf {:?}",
                f.args
            )));
        }

        let mut iter = f.args.iter();
        let col = take_column(&mut iter, "field")?;
        let terms = take_utf8_literal(&mut iter, "terms")?;
        let max_gap = take_uint64_literal(&mut iter, "max_gaps")?;

        Ok(OpenSearchFilters::Intervals {
            field: col.name.to_owned(),
            terms: terms.to_owned(),
            max_gap,
        })
    }

    fn from_wildcard_function(f: &ScalarFunction) -> DfResult<OpenSearchFilters> {
        if f.args.len() != 3 {
            return Err(DfError::Internal(format!(
                "This is a bug. Somehow got incorrect number of arguments to marker udf {:?}",
                f.args
            )));
        }

        let mut iter = f.args.iter();

        let col = take_column(&mut iter, "field")?;
        let pattern = take_utf8_literal(&mut iter, "pattern")?;
        let case_insensitive = take_bool_literal(&mut iter, "case_insensitive")?;

        Ok(OpenSearchFilters::Wildcard {
            field: col.name.to_owned(),
            pattern: pattern.to_owned(),
            case_insensitive,
        })
    }
}

#[derive(Debug)]
struct OpenSearchTableProvider {
    table_name: String,
    schema: SchemaRef,
    client: OpenSearch,
}

impl OpenSearchTableProvider {
    fn new(client: OpenSearch) -> Self {
        // For now only document id, should add score, content maybe
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.push(Field::new("id", DataType::Int64, false));
        schema_builder.push(Field::new("content", DataType::Utf8, false));
        Self {
            schema: Arc::new(schema_builder.finish()),
            table_name: "documents".to_owned(),
            client,
        }
    }

    fn filters_to_opensearch(&self, filters: &[Expr]) -> DfResult<OpenSearchFilters> {
        if filters.len() > 1 {
            // TODO: support by issueing several queries
            return Err(DfError::NotImplemented(
                "cant have more than one filter passed to opensearch".to_string(),
            ));
        }

        // https://opensearch.org/docs/latest/api-reference/scroll/
        let Some(filter) = filters.first() else {
            return Err(DfError::NotImplemented(
                "Opensearch scan without filters is not supported".to_string(),
            ));
        };

        let Expr::ScalarFunction(scalar_function) = filter else {
            return Err(DfError::Internal(
                "This is a bug. Somehow got unsupported filter in opensearch TableProvider. Expected marker udf, got: {filter}".to_string(),
            ));
        };

        OpenSearchFilters::from_scalar_function(scalar_function)
    }
}

#[async_trait]
impl TableProvider for OpenSearchTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if limit.is_some() {
            return Err(DfError::NotImplemented(
                "limit is not implemented for opensearch".to_owned(),
            ));
        }

        let filters = self.filters_to_opensearch(filters)?;

        Ok(Arc::new(OpenSearchExec::new(
            projection,
            filters,
            &self.schema,
            self.table_name.clone(),
            self.client.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        let mut pushdown = Vec::with_capacity(filters.len());
        for filter in filters {
            let Expr::ScalarFunction(func) = filter else {
                pushdown.push(TableProviderFilterPushDown::Unsupported);
                continue;
            };

            if !udf::SUPPORTED_UDFS.contains(&func.name()) {
                pushdown.push(TableProviderFilterPushDown::Unsupported);
            }

            // we suport exact filter pushdown for marker udf
            // it gets passed to us in scan filters argument
            pushdown.push(TableProviderFilterPushDown::Exact);
        }

        Ok(pushdown)
    }
}

pub fn register_opensearch(ctx: &SessionContext) {
    let url = Url::parse("http://127.0.0.1:9200").unwrap();
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool)
        .disable_proxy()
        .build()
        .unwrap();

    let client = OpenSearch::new(transport);

    ctx.register_table(
        TableReference::full("datafusion", "public", "opensearch_documents"),
        Arc::new(OpenSearchTableProvider::new(client)),
    )
    .expect("cant register opensearch");

    ctx.register_udf(udf::intervals());
    ctx.register_udf(udf::wildcard());
}

pub async fn make_context() -> SessionContext {
    let state = datafusion_federation::default_session_state();
    let ctx = SessionContext::new_with_state(state);

    register_postgres(&ctx).await;
    register_opensearch(&ctx);

    ctx
}

pub async fn repl() {
    let ctx = make_context().await;

    let mut rl = DefaultEditor::new().unwrap();
    if rl.load_history(".repl_history").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline("sql> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();
                let df = match ctx.sql(&line).await {
                    Ok(df) => df,
                    Err(e) => {
                        eprintln!("Error: {e}");
                        continue;
                    }
                };

                match df.show().await {
                    Ok(df) => df,
                    Err(e) => {
                        eprintln!("Error: {e}");
                        continue;
                    }
                };
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(".repl_history").unwrap();
}
