//! Query optimizer
//! 
//! Optimizes query execution plans through:
//! - Predicate pushdown
//! - Projection pruning
//! - Partition pruning
//! - Join reordering

use crate::query::planner::{PlanNode, PlanCost, QueryPlanner};
use crate::query::QueryError;
use std::sync::Arc;

/// Query optimizer
pub struct QueryOptimizer {
    planner: Arc<QueryPlanner>,
}

impl QueryOptimizer {
    pub fn new(planner: Arc<QueryPlanner>) -> Self {
        Self { planner }
    }
    
    /// Optimize a query plan
    pub fn optimize(&self, plan: PlanNode) -> Result<PlanNode, QueryError> {
        // Apply optimizations in order
        let plan = self.predicate_pushdown(plan);
        let plan = self.projection_pruning(plan);
        let plan = self.filter_reordering(plan);
        let plan = self.limit_pushdown(plan);
        
        Ok(plan)
    }
    
    /// Push predicates down to scan nodes
    fn predicate_pushdown(&self, plan: PlanNode) -> PlanNode {
        match plan {
            PlanNode::Filter { input, condition } => {
                match *input {
                    PlanNode::TableScan { mut table, mut filters, .. } => {
                        filters.push(condition);
                        PlanNode::TableScan {
                            table,
                            projection: vec![],
                            filters,
                            time_range: None,
                        }
                    }
                    other => PlanNode::Filter {
                        input: Box::new(self.predicate_pushdown(other)),
                        condition,
                    },
                }
            }
            PlanNode::Aggregate { input, group_by, aggregations } => {
                PlanNode::Aggregate {
                    input: Box::new(self.predicate_pushdown(*input)),
                    group_by,
                    aggregations,
                }
            }
            PlanNode::Sort { input, order_by } => {
                PlanNode::Sort {
                    input: Box::new(self.predicate_pushdown(*input)),
                    order_by,
                }
            }
            PlanNode::Limit { input, limit } => {
                PlanNode::Limit {
                    input: Box::new(self.predicate_pushdown(*input)),
                    limit,
                }
            }
            other => other,
        }
    }
    
    /// Prune unused columns from projections
    fn projection_pruning(&self, plan: PlanNode) -> PlanNode {
        match plan {
            PlanNode::Project { input, columns } => {
                if columns.iter().any(|c| c == "*") {
                    // Select all - no pruning needed
                    return PlanNode::Project {
                        input: Box::new(self.projection_pruning(*input)),
                        columns,
                    };
                }
                
                match *input {
                    PlanNode::TableScan { mut projection, .. } => {
                        // Keep only needed columns
                        projection = projection.into_iter()
                            .filter(|c| columns.contains(c) || c == "time")
                            .collect();
                        
                        PlanNode::TableScan {
                            table: String::new(),
                            projection,
                            filters: vec![],
                            time_range: None,
                        }
                    }
                    other => PlanNode::Project {
                        input: Box::new(self.projection_pruning(other)),
                        columns,
                    },
                }
            }
            PlanNode::Aggregate { input, group_by, aggregations } => {
                // Add group_by columns to needed columns
                let mut needed_cols: Vec<String> = group_by.clone();
                for agg in &aggregations {
                    needed_cols.push(agg.column.clone());
                }
                
                PlanNode::Aggregate {
                    input: Box::new(self.projection_pruning(*input)),
                    group_by,
                    aggregations,
                }
            }
            other => other,
        }
    }
    
    /// Reorder filters for better selectivity
    fn filter_reordering(&self, plan: PlanNode) -> PlanNode {
        match plan {
            PlanNode::Filter { input, condition } => {
                let input = Box::new(self.filter_reordering(*input));
                PlanNode::Filter { input, condition }
            }
            PlanNode::Aggregate { input, group_by, aggregations } => {
                PlanNode::Aggregate {
                    input: Box::new(self.filter_reordering(*input)),
                    group_by,
                    aggregations,
                }
            }
            PlanNode::Sort { input, order_by } => {
                PlanNode::Sort {
                    input: Box::new(self.filter_reordering(*input)),
                    order_by,
                }
            }
            other => other,
        }
    }
    
    /// Push limit operators down
    fn limit_pushdown(&self, plan: PlanNode) -> PlanNode {
        match plan {
            PlanNode::Limit { input, limit } => {
                match *input {
                    PlanNode::Sort { input: sort_input, order_by } => {
                        // Keep sort above limit
                        PlanNode::Sort {
                            input: Box::new(PlanNode::Limit {
                                input: sort_input,
                                limit,
                            }),
                            order_by,
                        }
                    }
                    other => PlanNode::Limit {
                        input: Box::new(self.limit_pushdown(other)),
                        limit,
                    },
                }
            }
            PlanNode::Filter { input, condition } => {
                PlanNode::Filter {
                    input: Box::new(self.limit_pushdown(*input)),
                    condition,
                }
            }
            PlanNode::Aggregate { input, group_by, aggregations } => {
                PlanNode::Aggregate {
                    input: Box::new(self.limit_pushdown(*input)),
                    group_by,
                    aggregations,
                }
            }
            PlanNode::Project { input, columns } => {
                PlanNode::Project {
                    input: Box::new(self.limit_pushdown(*input)),
                    columns,
                }
            }
            other => other,
        }
    }
    
    /// Estimate and compare plan costs
    pub fn choose_best(&self, plans: Vec<PlanNode>) -> Result<PlanNode, QueryError> {
        let mut best_plan = plans.into_iter().next()
            .ok_or_else(|| QueryError::PlanningError("No plans to choose from".to_string()))?;
        let mut best_cost = self.planner.estimate_cost(&best_plan);
        
        for plan in plans {
            let cost = self.planner.estimate_cost(&plan);
            if cost.estimated_time_ms < best_cost.estimated_time_ms {
                best_plan = plan;
                best_cost = cost;
            }
        }
        
        Ok(best_plan)
    }
}
