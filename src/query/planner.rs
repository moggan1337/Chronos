//! Query planner
//! 
//! Creates an optimized execution plan from a parsed query.

use crate::query::{Query, QueryError};
use crate::storage::Engine;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("Invalid plan: {0}")]
    InvalidPlan(String),
    #[error("Optimization failed: {0}")]
    OptimizationFailed(String),
}

/// Physical execution plan node
#[derive(Debug, Clone)]
pub enum PlanNode {
    /// Scan a table
    TableScan {
        table: String,
        projection: Vec<String>,
        filters: Vec<FilterSpec>,
        time_range: Option<TimeRangeSpec>,
    },
    /// Filter rows
    Filter {
        input: Box<PlanNode>,
        condition: FilterSpec,
    },
    /// Project columns
    Project {
        input: Box<PlanNode>,
        columns: Vec<String>,
    },
    /// Aggregate
    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<String>,
        aggregations: Vec<AggregateSpec>,
    },
    /// Sort
    Sort {
        input: Box<PlanNode>,
        order_by: Vec<OrderBySpec>,
    },
    /// Limit
    Limit {
        input: Box<PlanNode>,
        limit: usize,
    },
    /// Union
    Union {
        inputs: Vec<PlanNode>,
    },
    /// Join
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: JoinCondition,
    },
}

#[derive(Debug, Clone)]
pub struct FilterSpec {
    pub column: String,
    pub operator: String,
    pub value: FilterValueSpec,
}

#[derive(Debug, Clone)]
pub enum FilterValueSpec {
    Scalar(f64),
    Column(String),
    Expression(String),
}

#[derive(Debug, Clone)]
pub struct TimeRangeSpec {
    pub start: i64,
    pub end: i64,
    pub window: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub function: String,
    pub column: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderBySpec {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub enum JoinCondition {
    Equal(String, String),
    None,
}

/// Query planner
pub struct QueryPlanner {
    storage: Arc<Engine>,
}

impl QueryPlanner {
    pub fn new(storage: Arc<Engine>) -> Self {
        Self { storage }
    }
    
    /// Create an execution plan from a query
    pub fn plan(&self, query: &Query) -> Result<PlanNode, QueryError> {
        // Start with table scan
        let mut plan = PlanNode::TableScan {
            table: query.table.clone(),
            projection: query.columns.clone(),
            filters: Vec::new(),
            time_range: query.time_range.as_ref().map(|r| TimeRangeSpec {
                start: r.start.timestamp(),
                end: r.end.timestamp(),
                window: r.window.map(|d| d.num_seconds()),
            }),
        };
        
        // Add filters
        for filter in &query.filters {
            plan = PlanNode::Filter {
                input: Box::new(plan),
                condition: FilterSpec {
                    column: filter.column.clone(),
                    operator: format!("{:?}", filter.operator),
                    value: match &filter.value {
                        crate::query::FilterValue::Scalar(s) => FilterValueSpec::Scalar(*s),
                        crate::query::FilterValue::String(s) => FilterValueSpec::Expression(s.clone()),
                        _ => FilterValueSpec::Scalar(0.0),
                    },
                },
            };
        }
        
        // Add aggregations
        if !query.aggregations.is_empty() {
            let aggs = query.aggregations.iter().map(|a| AggregateSpec {
                function: format!("{:?}", a.function),
                column: a.column.clone(),
                alias: a.alias.clone(),
            }).collect();
            
            plan = PlanNode::Aggregate {
                input: Box::new(plan),
                group_by: query.group_by.clone(),
                aggregations: aggs,
            };
        }
        
        // Add sorting
        if !query.order_by.is_empty() {
            let orders = query.order_by.iter().map(|o| OrderBySpec {
                column: o.column.clone(),
                descending: o.descending,
            }).collect();
            
            plan = PlanNode::Sort {
                input: Box::new(plan),
                order_by: orders,
            };
        }
        
        // Add limit
        if let Some(limit) = query.limit {
            plan = PlanNode::Limit {
                input: Box::new(plan),
                limit,
            };
        }
        
        Ok(plan)
    }
    
    /// Estimate the cost of a plan
    pub fn estimate_cost(&self, plan: &PlanNode) -> PlanCost {
        match plan {
            PlanNode::TableScan { table, filters, time_range, .. } => {
                let base_cost = 1000;
                let filter_cost = filters.len() * 100;
                let time_cost = if time_range.is_some() { -500 } else { 0 };
                PlanCost {
                    estimated_rows: 10000, // Simplified
                    estimated_bytes: 1000000,
                    estimated_time_ms: base_cost + filter_cost + time_cost,
                }
            }
            PlanNode::Filter { input, .. } => {
                let input_cost = self.estimate_cost(input);
                PlanCost {
                    estimated_rows: input_cost.estimated_rows / 2,
                    estimated_bytes: input_cost.estimated_bytes / 2,
                    estimated_time_ms: input_cost.estimated_time_ms + 100,
                }
            }
            PlanNode::Aggregate { input, .. } => {
                let input_cost = self.estimate_cost(input);
                PlanCost {
                    estimated_rows: 100,
                    estimated_bytes: input_cost.estimated_bytes / 10,
                    estimated_time_ms: input_cost.estimated_time_ms + 500,
                }
            }
            _ => PlanCost::default(),
        }
    }
}

/// Estimated plan cost
#[derive(Debug, Clone, Default)]
pub struct PlanCost {
    pub estimated_rows: usize,
    pub estimated_bytes: usize,
    pub estimated_time_ms: u64,
}
