use datafusion::{
    arrow::{array::RecordBatch, util::pretty::pretty_format_batches},
    prelude::SessionContext,
};
use search_federation::make_context;

async fn execute(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

#[tokio::test]
async fn test_query_intervals() {
    let ctx = make_context().await;

    let sql = r#"
SELECT
    opensearch_documents.id,
    pg_doc.content,
    pg_doc.type
FROM opensearch_documents
JOIN postgres.public.documents pg_doc ON opensearch_documents.id = pg_doc.id
WHERE
    opensearch_intervals(opensearch_documents.content, 'postgres oracle', 2)
    AND pg_doc.type = 'news';
    "#;

    let result = execute(&ctx, sql).await;

    let actual = format!("{}", pretty_format_batches(&result).unwrap());
    let expected = r#"
+----+-----------------------------------+------+
| id | content                           | type |
+----+-----------------------------------+------+
| 3  | Postgres and Oracle are ok to use | news |
+----+-----------------------------------+------+
        "#
    .trim();
    assert_eq!(actual, expected);

    // empty result
    let sql_with_impossible_filter = r#"
SELECT opensearch_documents.id
FROM opensearch_documents
WHERE
    opensearch_intervals(opensearch_documents.content, 'postgres oracle', 0)
    "#;

    let result = execute(&ctx, sql_with_impossible_filter).await;

    let actual = format!("{}", pretty_format_batches(&result).unwrap());
    let expected = r#"
+----+
| id |
+----+
+----+
        "#
    .trim();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_query_wildcard() {
    let ctx = make_context().await;

    let sql = r#"
SELECT
    opensearch_documents.id,
    pg_doc.content
FROM opensearch_documents
JOIN postgres.public.documents pg_doc ON opensearch_documents.id = pg_doc.id
WHERE
    opensearch_wildcard(opensearch_documents.content, 'p?stgres', true)
ORDER BY opensearch_documents.id
    "#;

    let result = execute(&ctx, sql).await;

    let actual = format!("{}", pretty_format_batches(&result).unwrap());
    let expected = r#"
+----+------------------------------------------------------+
| id | content                                              |
+----+------------------------------------------------------+
| 2  | Postgres is great                                    |
| 3  | Postgres and Oracle are ok to use                    |
| 4  | Postgres is rather good and is quite far from Oracle |
+----+------------------------------------------------------+
        "#
    .trim();
    assert_eq!(actual, expected);

    let sql = r#"
SELECT
    opensearch_documents.id,
    pg_doc.content
FROM opensearch_documents
JOIN postgres.public.documents pg_doc ON opensearch_documents.id = pg_doc.id
WHERE
    opensearch_wildcard(opensearch_documents.content, 'O*le', true)
ORDER BY opensearch_documents.id
    "#;

    let result = execute(&ctx, sql).await;

    let actual = format!("{}", pretty_format_batches(&result).unwrap());
    let expected = r#"
+----+------------------------------------------------------+
| id | content                                              |
+----+------------------------------------------------------+
| 3  | Postgres and Oracle are ok to use                    |
| 4  | Postgres is rather good and is quite far from Oracle |
+----+------------------------------------------------------+
            "#
    .trim();
    assert_eq!(actual, expected);
}
