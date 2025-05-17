//! Placeholder udfs for opensearch pushdown
//! derived from example here: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/simple_udf.rs

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    logical_expr::{ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Volatility},
    prelude::create_udf,
};

pub const SUPPORTED_UDFS: &[&str] = &[
    OPENSEARCH_INTERVALS_MARKER_UDF_NAME,
    OPENSEARCH_WILDCARD_MARKER_UDF_NAME,
    OPENSEARCH_FUZZY_MARKER_UDF_NAME,
];

fn panicking_body() -> ScalarFunctionImplementation {
    Arc::new(|_args: &[ColumnarValue]| {
        panic!("called directly, should be eliminated by push-down at the planning stage")
    })
}

pub const OPENSEARCH_INTERVALS_MARKER_UDF_NAME: &str = "opensearch_intervals";

/// https://opensearch.org/docs/latest/query-dsl/full-text/intervals/
pub fn intervals() -> ScalarUDF {
    create_udf(
        OPENSEARCH_INTERVALS_MARKER_UDF_NAME,
        // expects field, terms and max_gap
        vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
        DataType::Boolean,
        Volatility::Immutable,
        panicking_body(),
    )
}

pub const OPENSEARCH_WILDCARD_MARKER_UDF_NAME: &str = "opensearch_wildcard";

/// https://docs.opensearch.org/docs/latest/query-dsl/term/wildcard/
pub fn wildcard() -> ScalarUDF {
    create_udf(
        OPENSEARCH_WILDCARD_MARKER_UDF_NAME,
        // expects field, pattern and case insensitivity flag, insensitive by default
        vec![DataType::Utf8, DataType::Utf8, DataType::Boolean],
        DataType::Boolean,
        Volatility::Immutable,
        panicking_body(),
    )
}

pub const OPENSEARCH_FUZZY_MARKER_UDF_NAME: &str = "opensearch_fuzzy";

/// https://docs.opensearch.org/docs/latest/query-dsl/term/fuzzy/
pub fn fuzzy() -> ScalarUDF {
    create_udf(
        OPENSEARCH_FUZZY_MARKER_UDF_NAME,
        // expects field, value
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        panicking_body(),
    )
}
