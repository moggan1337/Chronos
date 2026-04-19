//! SQL-like query parser
//! 
//! Parses time-series queries with support for:
//! - SELECT with time-series functions
//! - WHERE with time and tag filters
//! - GROUP BY with time windows
//! - ORDER BY with time ordering

use crate::query::{Query, Filter, FilterOperator, FilterValue, Aggregation, AggFunction, TimeRange, OrderBy};
use chrono::{DateTime, Utc, Duration};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Syntax error at position {pos}: {message}")]
    SyntaxError { pos: usize, message: String },
    #[error("Unexpected token: {token}")]
    UnexpectedToken { token: String },
    #[error("Missing required clause: {clause}")]
    MissingClause { clause: String },
}

/// Parsed query components
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub select_columns: Vec<SelectColumn>,
    pub from_table: String,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<GroupByClause>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<usize>,
    pub time_range: Option<TimeRange>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    All,
    Column(String),
    Function(FunctionCall),
}

#[derive(Debug, Clone)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<FunctionArg>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum FunctionArg {
    Column(String),
    Value(f64),
    Duration(Duration),
    Interval(IntervalValue),
}

#[derive(Debug, Clone)]
pub struct IntervalValue {
    pub value: i64,
    pub unit: String,
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub conditions: Vec<Condition>,
    pub logic: LogicOperator,
}

#[derive(Debug, Clone)]
pub struct Condition {
    pub column: String,
    pub operator: String,
    pub value: ConditionValue,
}

#[derive(Debug, Clone)]
pub enum ConditionValue {
    Number(f64),
    String(String),
    Duration(Duration),
    Timestamp(DateTime<Utc>),
    List(Vec<f64>),
}

#[derive(Debug, Clone)]
pub enum LogicOperator {
    And,
    Or,
}

#[derive(Debug, Clone)]
pub struct GroupByClause {
    pub columns: Vec<String>,
    pub time_window: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct OrderByClause {
    pub column: String,
    pub descending: bool,
}

/// SQL parser for time-series queries
pub struct QueryParser {
    keywords: Vec<String>,
    time_functions: Vec<String>,
}

impl QueryParser {
    pub fn new() -> Self {
        Self {
            keywords: vec![
                "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT",
                "AND", "OR", "NOT", "AS", "ASC", "DESC", "WITH",
            ].into_iter().map(|s| s.to_string()).collect(),
            time_functions: vec![
                "MEAN", "SUM", "MIN", "MAX", "COUNT", "FIRST", "LAST",
                "MEDIAN", "PERCENTILE", "RATE", "DELTA", "STDDEV",
                "BUCKET", "WINDOW", "FILL", "MOVING_AVG", "EXPONENTIAL_MOVING_AVG",
                "TREND", "ANOMALY", "FORECAST",
            ].into_iter().map(|s| s.to_string()).collect(),
        }
    }
    
    /// Parse a SQL-like query string
    pub fn parse(&self, sql: &str) -> Result<Query, crate::query::QueryError> {
        let sql_upper = sql.to_uppercase();
        let tokens = self.tokenize(&sql_upper)?;
        
        let mut query = Query::new(sql);
        
        // Parse SELECT clause
        let (columns, pos) = self.parse_select(&tokens, 0)?;
        query.columns = columns;
        
        // Parse FROM clause
        let (table, pos) = self.parse_from(&tokens, pos)?;
        query.table = table;
        
        // Parse optional WHERE clause
        let (filters, time_range, pos) = self.parse_where(&tokens, pos)?;
        query.filters = filters;
        query.time_range = time_range;
        
        // Parse optional GROUP BY clause
        let (group_by, aggregations, pos) = self.parse_group_by(&tokens, pos, &query.columns)?;
        query.group_by = group_by;
        query.aggregations = aggregations;
        
        // Parse optional ORDER BY clause
        let (order_by, pos) = self.parse_order_by(&tokens, pos)?;
        query.order_by = order_by;
        
        // Parse optional LIMIT clause
        let limit = self.parse_limit(&tokens, pos)?;
        query.limit = limit;
        
        Ok(query)
    }
    
    /// Tokenize the SQL string
    fn tokenize(&self, sql: &str) -> Result<Vec<Token>, crate::query::QueryError> {
        let mut tokens = Vec::new();
        let mut chars: Vec<char> = sql.chars().collect();
        let mut pos = 0;
        
        while pos < chars.len() {
            let c = chars[pos];
            
            // Skip whitespace
            if c.is_whitespace() {
                pos += 1;
                continue;
            }
            
            // Handle string literals
            if c == '\'' || c == '"' {
                let quote = c;
                let start = pos;
                pos += 1;
                while pos < chars.len() && chars[pos] != quote {
                    if chars[pos] == '\\' && pos + 1 < chars.len() {
                        pos += 2;
                    } else {
                        pos += 1;
                    }
                }
                if pos < chars.len() {
                    pos += 1; // Closing quote
                }
                let text = sql[start..pos].trim_matches(quote);
                tokens.push(Token::String(text.to_string()));
                continue;
            }
            
            // Handle numbers
            if c.is_ascii_digit() || (c == '-' && pos + 1 < chars.len() && chars[pos + 1].is_ascii_digit()) {
                let start = pos;
                if chars[pos] == '-' {
                    pos += 1;
                }
                while pos < chars.len() && (chars[pos].is_ascii_digit() || chars[pos] == '.') {
                    pos += 1;
                }
                // Handle duration suffixes
                if pos < chars.len() && !chars[pos].is_whitespace() && !self.is_delimiter(chars[pos]) {
                    let suffix_start = pos;
                    while pos < chars.len() && chars[pos].is_alphabetic() {
                        pos += 1;
                    }
                    tokens.push(Token::Duration(sql[start..pos].to_string()));
                } else {
                    tokens.push(Token::Number(sql[start..pos].parse().unwrap_or(0.0)));
                }
                continue;
            }
            
            // Handle identifiers and keywords
            if c.is_alphabetic() || c == '_' {
                let start = pos;
                while pos < chars.len() && (chars[pos].is_alphanumeric() || chars[pos] == '_') {
                    pos += 1;
                }
                let word = sql[start..pos].to_uppercase();
                let word_lower = sql[start..pos].to_lowercase();
                
                if self.keywords.contains(&word) {
                    tokens.push(Token::Keyword(word));
                } else if self.time_functions.contains(&word) {
                    tokens.push(Token::Function(word));
                } else {
                    tokens.push(Token::Ident(word_lower));
                }
                continue;
            }
            
            // Handle operators and delimiters
            let single_char_tokens = [
                ('*', Token::Star),
                (',', Token::Comma),
                ('(', Token::LParen),
                (')', Token::RParen),
                (';', Token::SemiColon),
                ('+', Token::Plus),
                ('-', Token::Minus),
                ('/', Token::Slash),
                ('%', Token::Percent),
                ('=', Token::Eq),
                ('<', Token::Lt),
                ('>', Token::Gt),
                ('.', Token::Dot),
            ];
            
            for (oc, tok) in single_char_tokens {
                if c == oc {
                    tokens.push(tok);
                    pos += 1;
                    
                    // Handle two-character operators
                    if oc == '<' && pos < chars.len() && chars[pos] == '=' {
                        tokens.push(Token::Lte);
                        pos += 1;
                    } else if oc == '>' && pos < chars.len() && chars[pos] == '=' {
                        tokens.push(Token::Gte);
                        pos += 1;
                    } else if oc == '<' && pos < chars.len() && chars[pos] == '>' {
                        tokens.push(Token::Ne);
                        pos += 1;
                    }
                    break;
                }
            }
            
            pos += 1;
        }
        
        Ok(tokens)
    }
    
    fn is_delimiter(&self, c: char) -> bool {
        matches!(c, ' ' | ',' | ')' | ';' | '+' | '-' | '*' | '/' | '=' | '<' | '>')
    }
    
    /// Parse SELECT clause
    fn parse_select(&self, tokens: &[Token], pos: usize) -> Result<(Vec<String>, usize), crate::query::QueryError> {
        let mut columns = Vec::new();
        let mut current_pos = pos;
        
        if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Keyword(ref k) if k == "SELECT") {
            return Err(crate::query::QueryError::ParseError(
                "Expected SELECT keyword".to_string()
            ));
        }
        current_pos += 1;
        
        // Check for ALL
        if current_pos < tokens.len() && matches!(tokens[current_pos], Token::Star) {
            columns.push("*".to_string());
            current_pos += 1;
        } else {
            // Parse column list
            loop {
                if current_pos >= tokens.len() {
                    break;
                }
                
                match &tokens[current_pos] {
                    Token::Ident(name) => {
                        columns.push(name.clone());
                        current_pos += 1;
                    }
                    Token::Function(name) => {
                        // Parse function call
                        current_pos += 1;
                        if current_pos < tokens.len() && matches!(tokens[current_pos], Token::LParen) {
                            current_pos += 1;
                            // Skip until RParen
                            let mut depth = 1;
                            while current_pos < tokens.len() && depth > 0 {
                                if matches!(tokens[current_pos], Token::LParen) {
                                    depth += 1;
                                } else if matches!(tokens[current_pos], Token::RParen) {
                                    depth -= 1;
                                }
                                current_pos += 1;
                            }
                        }
                        // Add function result column
                        columns.push(name.to_lowercase());
                    }
                    _ => break,
                }
                
                if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Comma) {
                    break;
                }
                current_pos += 1;
            }
        }
        
        Ok((columns, current_pos))
    }
    
    /// Parse FROM clause
    fn parse_from(&self, tokens: &[Token], pos: usize) -> Result<(String, usize), crate::query::QueryError> {
        let mut current_pos = pos;
        
        if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Keyword(ref k) if k == "FROM") {
            return Err(crate::query::QueryError::ParseError(
                "Expected FROM keyword".to_string()
            ));
        }
        current_pos += 1;
        
        if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Ident(_)) {
            return Err(crate::query::QueryError::ParseError(
                "Expected table name".to_string()
            ));
        }
        
        let table_name = match &tokens[current_pos] {
            Token::Ident(name) => name.clone(),
            _ => unreachable!(),
        };
        current_pos += 1;
        
        Ok((table_name, current_pos))
    }
    
    /// Parse WHERE clause
    fn parse_where(&self, tokens: &[Token], pos: usize) -> Result<(Vec<Filter>, Option<TimeRange>, usize), crate::query::QueryError> {
        let mut filters = Vec::new();
        let mut time_range = None;
        let mut current_pos = pos;
        
        if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Keyword(ref k) if k == "WHERE") {
            return Ok((filters, time_range, current_pos));
        }
        current_pos += 1;
        
        // Parse conditions
        while current_pos < tokens.len() {
            if matches!(tokens[current_pos], Token::Keyword(ref k) if k == "GROUP" || k == "ORDER" || k == "LIMIT") {
                break;
            }
            
            // Parse simple condition
            if let Token::Ident(col) = &tokens[current_pos] {
                current_pos += 1;
                
                // Parse operator
                let op = if current_pos < tokens.len() {
                    match &tokens[current_pos] {
                        Token::Eq => FilterOperator::Eq,
                        Token::Ne => FilterOperator::Ne,
                        Token::Lt => FilterOperator::Lt,
                        Token::Lte => FilterOperator::Le,
                        Token::Gt => FilterOperator::Gt,
                        Token::Gte => FilterOperator::Ge,
                        _ => FilterOperator::Eq,
                    }
                } else {
                    FilterOperator::Eq
                };
                
                if matches!(tokens[current_pos], Token::Eq | Token::Ne | Token::Lt | Token::Lte | Token::Gt | Token::Gte) {
                    current_pos += 1;
                }
                
                // Parse value
                let value = if current_pos < tokens.len() {
                    match &tokens[current_pos] {
                        Token::Number(n) => FilterValue::Scalar(*n),
                        Token::String(s) => FilterValue::String(s.clone()),
                        Token::Duration(s) => FilterValue::Scalar(self.parse_duration(s).as_secs() as f64),
                        _ => FilterValue::Scalar(0.0),
                    }
                } else {
                    FilterValue::Scalar(0.0)
                };
                
                if matches!(tokens[current_pos], Token::Number(_) | Token::String(_) | Token::Duration(_)) {
                    current_pos += 1;
                }
                
                // Check for time range keywords
                if col == "time" || col == "timestamp" {
                    time_range = self.parse_time_condition(col, &op, &value);
                }
                
                filters.push(Filter {
                    column: col.clone(),
                    operator: op,
                    value,
                });
            }
            
            // Handle AND/OR
            if current_pos < tokens.len() {
                if matches!(tokens[current_pos], Token::Keyword(ref k) if k == "AND" || k == "OR") {
                    current_pos += 1;
                }
            }
        }
        
        Ok((filters, time_range, current_pos))
    }
    
    /// Parse GROUP BY clause
    fn parse_group_by(&self, tokens: &[Token], pos: usize, columns: &[String]) -> Result<(Vec<String>, Vec<Aggregation>, usize), crate::query::QueryError> {
        let mut group_by = Vec::new();
        let mut aggregations = Vec::new();
        let mut current_pos = pos;
        
        if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Keyword(ref k) if k == "GROUP") {
            return Ok((group_by, aggregations, current_pos));
        }
        current_pos += 1;
        
        if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Keyword(ref k) if k == "BY") {
            return Ok((group_by, aggregations, current_pos));
        }
        current_pos += 1;
        
        // Parse group by columns
        while current_pos < tokens.len() {
            match &tokens[current_pos] {
                Token::Ident(name) => {
                    group_by.push(name.clone());
                    current_pos += 1;
                }
                Token::Keyword(k) if k == "ORDER" || k == "LIMIT" => break,
                _ => {
                    current_pos += 1;
                }
            }
            
            if current_pos < tokens.len() && !matches!(tokens[current_pos], Token::Comma | Token::Ident(_)) {
                break;
            }
            
            if current_pos < tokens.len() && matches!(tokens[current_pos], Token::Comma) {
                current_pos += 1;
            }
        }
        
        Ok((group_by, aggregations, current_pos))
    }
    
    /// Parse ORDER BY clause
    fn parse_order_by(&self, tokens: &[Token], pos: usize) -> Result<(Vec<OrderBy>, usize), crate::query::QueryError> {
        let mut order_by = Vec::new();
        let mut current_pos = pos;
        
        if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Keyword(ref k) if k == "ORDER") {
            return Ok((order_by, current_pos));
        }
        current_pos += 1;
        
        if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Keyword(ref k) if k == "BY") {
            return Ok((order_by, current_pos));
        }
        current_pos += 1;
        
        // Parse order by columns
        while current_pos < tokens.len() {
            if let Token::Ident(name) = &tokens[current_pos] {
                current_pos += 1;
                
                let descending = if current_pos < tokens.len() {
                    if matches!(tokens[current_pos], Token::Keyword(ref k) if k == "DESC") {
                        current_pos += 1;
                        true
                    } else if matches!(tokens[current_pos], Token::Keyword(ref k) if k == "ASC") {
                        current_pos += 1;
                        false
                    } else {
                        false
                    }
                } else {
                    false
                };
                
                order_by.push(OrderBy {
                    column: name.clone(),
                    descending,
                });
            }
            
            if current_pos < tokens.len() && matches!(tokens[current_pos], Token::Comma) {
                current_pos += 1;
            } else {
                break;
            }
        }
        
        Ok((order_by, current_pos))
    }
    
    /// Parse LIMIT clause
    fn parse_limit(&self, tokens: &[Token], pos: usize) -> Result<Option<usize>, crate::query::QueryError> {
        let mut current_pos = pos;
        
        if current_pos >= tokens.len() || !matches!(tokens[current_pos], Token::Keyword(ref k) if k == "LIMIT") {
            return Ok(None);
        }
        current_pos += 1;
        
        if current_pos < tokens.len() {
            if let Token::Number(n) = tokens[current_pos] {
                current_pos += 1;
                return Ok(Some(*n as usize));
            }
        }
        
        Ok(None)
    }
    
    /// Parse duration string
    fn parse_duration(&self, s: &str) -> Duration {
        let s = s.trim().to_lowercase();
        let (value, unit) = if let Some(n) = s.find(|c: char| !c.is_ascii_digit()) {
            let v: f64 = s[..n].parse().unwrap_or(1.0);
            let u = &s[n..];
            (v, u)
        } else {
            return Duration::seconds(1);
        };
        
        let secs = match unit {
            "s" | "sec" | "seconds" => value,
            "m" | "min" | "minutes" => value * 60.0,
            "h" | "hr" | "hours" => value * 3600.0,
            "d" | "day" | "days" => value * 86400.0,
            "w" | "wk" | "weeks" => value * 604800.0,
            _ => value,
        };
        
        Duration::seconds(secs as i64)
    }
    
    /// Parse time condition for time range
    fn parse_time_condition(&self, _col: &str, op: &FilterOperator, value: &FilterValue) -> Option<TimeRange> {
        // Simplified time range parsing
        let duration = match value {
            FilterValue::Scalar(s) => Duration::seconds(*s as i64),
            _ => return None,
        };
        
        match op {
            FilterOperator::Gt | FilterOperator::Gte => {
                Some(TimeRange::new(Utc::now() - duration, Utc::now()))
            }
            FilterOperator::Lt | FilterOperator::Lte => {
                Some(TimeRange::new(Utc::now() - duration * 2, Utc::now() - duration))
            }
            _ => None,
        }
    }
}

impl Default for QueryParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Token types for lexical analysis
#[derive(Debug, Clone)]
pub enum Token {
    Keyword(String),
    Ident(String),
    Number(f64),
    String(String),
    Duration(String),
    Function(String),
    Star,
    Comma,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Lte,
    Gte,
    Ne,
    Eq,
    Lt,
    Gt,
    Plus,
    Minus,
    Slash,
    Percent,
    Dot,
    SemiColon,
}
