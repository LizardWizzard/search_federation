// Optimizer Rule turned out to be not needed. Push down is handled by existing optimizer rule.
// Table provider just needed to be modified to report available push down possibilities to the optimizer.
// Keeping code just as a cheat sheet just in case.
mod optimizer {
    use std::{mem, sync::Arc};

    use datafusion::{
        common::tree_node::Transformed,
        error::DataFusionError,
        logical_expr::{LogicalPlan, Projection},
        optimizer::{OptimizerConfig, OptimizerRule},
        prelude::Expr,
        scalar::ScalarValue,
    };

    use crate::udf::OPENSEARCH_INTERVALS_MARKER_UDF_NAME;

    // https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/optimizer_rule.rs
    // https://github.com/uwheel/datafusion-uwheel/blob/main/datafusion-uwheel/src/lib.rs
    #[derive(Debug)]
    pub struct OpenSearchUdfPushDownRule;

    impl OpenSearchUdfPushDownRule {
        fn take_marker_udf_expr(projection: &mut Projection) -> Option<Expr> {
            for expr in &mut projection.expr {
                let Expr::ScalarFunction(func) = expr else {
                    continue;
                };

                // TODO: validate parameters (must be literals)
                if func.name() != OPENSEARCH_INTERVALS_MARKER_UDF_NAME {
                    continue;
                }

                // replace expr with scalar true value
                let new_expr = Expr::Literal(ScalarValue::Boolean(Some(true)))
                    .alias(expr.name_for_alias().unwrap());
                return Some(mem::replace(expr, new_expr));
            }

            None
        }

        fn try_rewrite(&self, mut plan: LogicalPlan) -> Result<LogicalPlan, LogicalPlan> {
            // get owned plan, first get Option<(mut Expr of marker fun, table provider)>
            // then replace expr with constant true
            // add this expr to filters that get to `TableProvider::scan` (can we actually tweak provider?
            // or provider is global and we need to somehow pass options to it?)

            let LogicalPlan::Projection(projection) = &mut plan else {
                return Err(plan);
            };

            let LogicalPlan::TableScan(scan) = projection.input.as_ref() else {
                return Err(plan);
            };

            // Replace input with new table scan since we cant mutate Arc.
            // TODO: This can be a problem if the plan has multiple references to the table scan.
            let mut new_scan = scan.clone();

            // Replace udf projection with table scan as a source with
            // new table scan with filter containing the udf. Then `TableProvider::scan`
            // can take expr from `filters` arg and build the query
            let Some(marker_udf_expr) = Self::take_marker_udf_expr(projection) else {
                return Err(plan);
            };

            new_scan.filters.push(marker_udf_expr);

            projection.input = Arc::new(LogicalPlan::TableScan(new_scan));

            Ok(plan)
        }
    }

    impl OptimizerRule for OpenSearchUdfPushDownRule {
        fn name(&self) -> &str {
            "OpenSearchUdfPushDownRule"
        }

        fn supports_rewrite(&self) -> bool {
            true
        }

        fn rewrite(
            &self,
            plan: LogicalPlan,
            _config: &dyn OptimizerConfig,
        ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
            match self.try_rewrite(plan) {
                Ok(plan) => Ok(Transformed::yes(plan)),
                Err(plan) => Ok(Transformed::no(plan)),
            }
        }
    }
}

pub use optimizer::OpenSearchUdfPushDownRule;

// Analyzer rule is used to validate semantics of arguments used for the udf
mod analyzer {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        config::ConfigOptions,
        error::Result,
        logical_expr::LogicalPlan,
        optimizer::AnalyzerRule,
        prelude::Expr,
    };

    use crate::udf::OPENSEARCH_INTERVALS_MARKER_UDF_NAME;

    #[derive(Debug)]
    pub struct OpenSearchUdfValidatorRule;

    impl AnalyzerRule for OpenSearchUdfValidatorRule {
        fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
            plan.apply(|n| {
                n.apply_expressions(|expr| {
                    let ret = Ok(TreeNodeRecursion::Continue);
                    let Expr::ScalarFunction(func) = expr else {
                        return ret;
                    };

                    // TODO: validate parameters (must be literals)
                    if func.name() != OPENSEARCH_INTERVALS_MARKER_UDF_NAME {
                        return ret;
                    }
                    todo!();

                    // func.args.first()

                    // ret
                })
            })?;

            Ok(plan)
        }

        fn name(&self) -> &str {
            "OpenSearchUdfValidatorRule"
        }
    }
}

pub use analyzer::OpenSearchUdfValidatorRule;
