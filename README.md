### Search federation

Basic demo representing how Apache DataFusion can be used to implement federative search engine.

The demo consists of postgresql as a primary datasource and opensearch instance serving full text queries.

Postgresql is hooked into datafusion using datafusion-table-providers crate among with federated components from datafusion-federation.

Custom table provider was implemented for opensearch. It is pretty basic, and works only in specific situations. Main part of it is support for pushdown of specifically added UDFs (user defined functions). The function itself doesnt do any work and serves as a "custom operator" to be only used by the opensearch table provider to construct full-text query. For now the only function is `opensearch_intervals`. It provides a way to execute [intervals](https://opensearch.org/docs/latest/query-dsl/full-text/intervals/) query. Arguments are as follows: the field to search, query (terms space separated), max gap between terms. Other search primitives can be added in similar way.

#### Running the demo

To run the demo working installation of fresh enough rust toolchain and docker are needed.

First, set up containers by `docker compose up -d`

Then `cargo run -- seed` to seed both opensearch and postgres with documents.

Then invoke `cargo run -- repl` to get access to sql repl to execute queries.

Example query:

```sql
SELECT
    opensearch_documents.id,
    pg_doc.content,
    pg_doc.type
FROM opensearch_documents
JOIN postgres.public.documents pg_doc ON opensearch_documents.id = pg_doc.id
WHERE
    opensearch_intervals(opensearch_documents.content, 'postgres oracle', 2)
    AND pg_doc.type = 'news';
```

Note: it is not required that front-end uses sql. It can be something else that gets compiled to the same internal representation (DataFusion logical plans)

### Future work

pg_statistics can be used to optimize the plan. I e to prioritize most cardinality reducing predicates. For example if there are just a few documents matching some non fulltext part of the query we could filter them first and then execute opensearch part on these documents only (querying them by id) or even evaluating predicates on the side of federated engine using tantivy for example.
