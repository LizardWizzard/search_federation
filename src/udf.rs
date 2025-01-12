use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    logical_expr::{ColumnarValue, ScalarUDF, Volatility},
    prelude::create_udf,
};

pub const OPENSEARCH_INTERVALS_MARKER_UDF_NAME: &str = "opensearch_intervals";

pub fn intervals() -> ScalarUDF {
    // https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/simple_udf.rs
    // https://opensearch.org/docs/latest/query-dsl/full-text/intervals/
    let intervals_udf = Arc::new(|_args: &[ColumnarValue]| {
        panic!("called directly, should be eliminated by push-down at the planning stage")
    });

    create_udf(
        OPENSEARCH_INTERVALS_MARKER_UDF_NAME,
        // expects terms and max_gap
        vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
        // returns f64
        DataType::Boolean,
        Volatility::Immutable,
        intervals_udf,
    )
}
