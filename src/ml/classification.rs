//! Classification models
//! 
//! Implements classification algorithms for time-series:
//! - Decision Tree
//! - Random Forest
//! - Logistic Regression

use crate::ml::{MLError, ModelType};
use serde::{Serialize, Deserialize};
use rand::Rng;

/// Classification result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationResult {
    pub predictions: Vec<usize>,
    pub probabilities: Vec<Vec<f64>>,
    pub classes: Vec<String>,
    pub model_name: String,
}

/// Classification model type
#[derive(Debug, Clone, Copy)]
pub enum ClassificationModel {
    DecisionTree,
    RandomForest,
    LogisticRegression,
}

/// Decision Tree node
#[derive(Debug, Clone)]
struct TreeNode {
    feature_index: usize,
    threshold: f64,
    left: Option<Box<TreeNode>>,
    right: Option<Box<TreeNode>>,
    class: Option<usize>,
}

/// Decision Tree classifier
#[derive(Debug, Clone)]
pub struct DecisionTreeClassifier {
    max_depth: usize,
    min_samples_split: usize,
    root: Option<TreeNode>,
    num_classes: usize,
    feature_importance: Vec<f64>,
}

impl DecisionTreeClassifier {
    pub fn new(max_depth: usize, min_samples_split: usize) -> Self {
        Self {
            max_depth,
            min_samples_split,
            root: None,
            num_classes: 0,
            feature_importance: Vec::new(),
        }
    }
    
    pub fn train(&mut self, data: &[Vec<f64>], labels: &[usize]) -> Result<(), MLError> {
        if data.is_empty() || data.len() != labels.len() {
            return Err(MLError::InvalidData("Data and labels must have same length".to_string()));
        }
        
        self.num_classes = *labels.iter().max().unwrap_or(&0) + 1;
        self.feature_importance = vec![0.0; data[0].len()];
        
        self.root = Some(self.build_tree(data, labels, 0));
        
        Ok(())
    }
    
    fn build_tree(&self, data: &[Vec<f64>], labels: &[usize], depth: usize) -> TreeNode {
        let num_samples = data.len();
        
        // Check stopping conditions
        if depth >= self.max_depth || num_samples < self.min_samples_split {
            let class = self.majority_class(labels);
            return TreeNode {
                feature_index: 0,
                threshold: 0.0,
                left: None,
                right: None,
                class: Some(class),
            };
        }
        
        // Find best split
        let (best_feature, best_threshold, best_gain) = self.find_best_split(data, labels);
        
        if best_gain < 1e-10 {
            let class = self.majority_class(labels);
            return TreeNode {
                feature_index: best_feature,
                threshold: best_threshold,
                left: None,
                right: None,
                class: Some(class),
            };
        }
        
        // Split data
        let mut left_data = Vec::new();
        let mut left_labels = Vec::new();
        let mut right_data = Vec::new();
        let mut right_labels = Vec::new();
        
        for (row, &label) in data.iter().zip(labels.iter()) {
            if row[best_feature] < best_threshold {
                left_data.push(row.clone());
                left_labels.push(label);
            } else {
                right_data.push(row.clone());
                right_labels.push(label);
            }
        }
        
        TreeNode {
            feature_index: best_feature,
            threshold: best_threshold,
            left: if left_data.is_empty() {
                None
            } else {
                Some(Box::new(self.build_tree(&left_data, &left_labels, depth + 1)))
            },
            right: if right_data.is_empty() {
                None
            } else {
                Some(Box::new(self.build_tree(&right_data, &right_labels, depth + 1)))
            },
            class: None,
        }
    }
    
    fn find_best_split(&self, data: &[Vec<f64>], labels: &[usize]) -> (usize, f64, f64) {
        let num_features = data[0].len();
        let mut best_gain = 0.0;
        let mut best_feature = 0;
        let mut best_threshold = 0.0;
        
        let parent_entropy = self.entropy(labels);
        
        for feature in 0..num_features {
            // Get unique values for this feature
            let mut values: Vec<f64> = data.iter().map(|r| r[feature]).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            
            // Try thresholds between values
            for i in 0..values.len() - 1 {
                let threshold = (values[i] + values[i + 1]) / 2.0;
                
                let (left_labels, right_labels) = self.split_labels(data, labels, feature, threshold);
                
                if left_labels.is_empty() || right_labels.is_empty() {
                    continue;
                }
                
                let child_entropy = (left_labels.len() as f64 * self.entropy(&left_labels) +
                    right_labels.len() as f64 * self.entropy(&right_labels)) / data.len() as f64;
                
                let gain = parent_entropy - child_entropy;
                
                if gain > best_gain {
                    best_gain = gain;
                    best_feature = feature;
                    best_threshold = threshold;
                }
            }
        }
        
        (best_feature, best_threshold, best_gain)
    }
    
    fn split_labels(&self, data: &[Vec<f64>], labels: &[usize], feature: usize, threshold: f64) 
        -> (Vec<usize>, Vec<usize>) 
    {
        let mut left = Vec::new();
        let mut right = Vec::new();
        
        for (row, &label) in data.iter().zip(labels.iter()) {
            if row[feature] < threshold {
                left.push(label);
            } else {
                right.push(label);
            }
        }
        
        (left, right)
    }
    
    fn entropy(&self, labels: &[usize]) -> f64 {
        if labels.is_empty() {
            return 0.0;
        }
        
        let mut counts = vec![0usize; self.num_classes];
        for &label in labels {
            counts[label] += 1;
        }
        
        let mut entropy = 0.0;
        let n = labels.len() as f64;
        
        for &count in &counts {
            if count > 0 {
                let p = count as f64 / n;
                entropy -= p * p.log2();
            }
        }
        
        entropy
    }
    
    fn majority_class(&self, labels: &[usize]) -> usize {
        if labels.is_empty() {
            return 0;
        }
        
        let mut counts = vec![0usize; self.num_classes];
        for &label in labels {
            counts[label] += 1;
        }
        
        counts.iter().enumerate()
            .max_by_key(|(_, &c)| c)
            .map(|(i, _)| i)
            .unwrap_or(0)
    }
    
    pub fn predict(&self, data: &[Vec<f64>]) -> ClassificationResult {
        let predictions: Vec<usize> = data.iter()
            .map(|row| self.predict_single(row))
            .collect();
        
        // Simplified probability estimation
        let probabilities: Vec<Vec<f64>> = predictions.iter()
            .map(|&p| {
                let mut probs = vec![0.0; self.num_classes];
                probs[p] = 1.0;
                probs
            })
            .collect();
        
        ClassificationResult {
            predictions,
            probabilities,
            classes: (0..self.num_classes).map(|i| format!("class_{}", i)).collect(),
            model_name: "DecisionTree".to_string(),
        }
    }
    
    fn predict_single(&self, features: &[f64]) -> usize {
        let mut node = &self.root;
        
        while let Some(n) = node {
            if let Some(class) = n.class {
                return class;
            }
            
            if features[n.feature_index] < n.threshold {
                node = n.left.as_ref();
            } else {
                node = n.right.as_ref();
            }
        }
        
        0
    }
}

/// Random Forest classifier
#[derive(Debug, Clone)]
pub struct RandomForestClassifier {
    trees: Vec<DecisionTreeClassifier>,
    num_trees: usize,
    num_classes: usize,
}

impl RandomForestClassifier {
    pub fn new(num_trees: usize, max_depth: usize) -> Self {
        Self {
            trees: (0..num_trees).map(|_| DecisionTreeClassifier::new(max_depth, 5)).collect(),
            num_trees,
            num_classes: 0,
        }
    }
    
    pub fn train(&mut self, data: &[Vec<f64>], labels: &[usize]) -> Result<(), MLError> {
        if data.is_empty() {
            return Err(MLError::InvalidData("Empty data".to_string()));
        }
        
        self.num_classes = *labels.iter().max().unwrap_or(&0) + 1;
        let mut rng = rand::thread_rng();
        
        // Bootstrap sampling and training
        for tree in &mut self.trees {
            let n = data.len();
            let sample_size = n * 2 / 3; // Typical bootstrap size
            
            let mut indices: Vec<usize> = (0..n).collect();
            indices.shuffle(&mut rng);
            indices.truncate(sample_size);
            
            let sample_data: Vec<Vec<f64>> = indices.iter().map(|&i| data[i].clone()).collect();
            let sample_labels: Vec<usize> = indices.iter().map(|&i| labels[i]).collect();
            
            tree.train(&sample_data, &sample_labels)?;
        }
        
        Ok(())
    }
    
    pub fn predict(&self, data: &[Vec<f64>]) -> ClassificationResult {
        let mut all_predictions: Vec<Vec<usize>> = Vec::new();
        
        for tree in &self.trees {
            let predictions = tree.predict(data);
            all_predictions.push(predictions.predictions);
        }
        
        let num_samples = data.len();
        let mut final_predictions = Vec::with_capacity(num_samples);
        let mut probabilities = Vec::with_capacity(num_samples);
        
        for i in 0..num_samples {
            let mut votes = vec![0usize; self.num_classes];
            for tree_preds in &all_predictions {
                votes[tree_preds[i]] += 1;
            }
            
            let predicted = votes.iter().enumerate()
                .max_by_key(|(_, &v)| v)
                .map(|(i, _)| i)
                .unwrap_or(0);
            
            final_predictions.push(predicted);
            
            // Calculate probabilities
            let total: usize = votes.iter().sum();
            let probs: Vec<f64> = if total > 0 {
                votes.iter().map(|&v| v as f64 / total as f64).collect()
            } else {
                vec![1.0 / self.num_classes as f64; self.num_classes]
            };
            probabilities.push(probs);
        }
        
        ClassificationResult {
            predictions: final_predictions,
            probabilities,
            classes: (0..self.num_classes).map(|i| format!("class_{}", i)).collect(),
            model_name: "RandomForest".to_string(),
        }
    }
}

/// Logistic Regression classifier
#[derive(Debug, Clone)]
pub struct LogisticRegressionClassifier {
    weights: Vec<f64>,
    bias: f64,
    learning_rate: f64,
    iterations: usize,
}

impl LogisticRegressionClassifier {
    pub fn new(learning_rate: f64, iterations: usize) -> Self {
        Self {
            weights: Vec::new(),
            bias: 0.0,
            learning_rate,
            iterations,
        }
    }
    
    pub fn train(&mut self, data: &[Vec<f64>], labels: &[usize]) -> Result<(), MLError> {
        if data.is_empty() {
            return Err(MLError::InvalidData("Empty data".to_string()));
        }
        
        // Initialize weights
        self.weights = vec![0.0; data[0].len()];
        self.bias = 0.0;
        
        // Convert labels to binary (for binary classification)
        // For multi-class, would need softmax
        let binary_labels: Vec<f64> = labels.iter().map(|&l| l as f64).collect();
        
        // Gradient descent
        for _ in 0..self.iterations {
            let mut weight_grad = vec![0.0; self.weights.len()];
            let mut bias_grad = 0.0;
            
            for (row, &label) in data.iter().zip(binary_labels.iter()) {
                let z: f64 = row.iter().zip(self.weights.iter())
                    .map(|(x, w)| x * w)
                    .sum::<f64>() + self.bias;
                
                let prediction = 1.0 / (1.0 + (-z).exp());
                let error = prediction - label;
                
                for (i, &x) in row.iter().enumerate() {
                    weight_grad[i] += error * x;
                }
                bias_grad += error;
            }
            
            // Update weights
            let n = data.len() as f64;
            for i in 0..self.weights.len() {
                self.weights[i] -= self.learning_rate * weight_grad[i] / n;
            }
            self.bias -= self.learning_rate * bias_grad / n;
        }
        
        Ok(())
    }
    
    pub fn predict_proba(&self, data: &[Vec<f64>]) -> Vec<f64> {
        data.iter()
            .map(|row| {
                let z: f64 = row.iter().zip(self.weights.iter())
                    .map(|(x, w)| x * w)
                    .sum::<f64>() + self.bias;
                1.0 / (1.0 + (-z).exp())
            })
            .collect()
    }
    
    pub fn predict(&self, data: &[Vec<f64>]) -> ClassificationResult {
        let probs = self.predict_proba(data);
        let predictions: Vec<usize> = probs.iter()
            .map(|&p| if p > 0.5 { 1 } else { 0 })
            .collect();
        
        let probabilities: Vec<Vec<f64>> = probs.iter()
            .map(|&p| vec![1.0 - p, p])
            .collect();
        
        ClassificationResult {
            predictions,
            probabilities,
            classes: vec!["class_0".to_string(), "class_1".to_string()],
            model_name: "LogisticRegression".to_string(),
        }
    }
}

/// Unified classifier interface
pub struct Classifier {
    model: ClassifierModel,
}

enum ClassifierModel {
    DecisionTree(DecisionTreeClassifier),
    RandomForest(RandomForestClassifier),
    LogisticRegression(LogisticRegressionClassifier),
}

impl Classifier {
    pub fn decision_tree(max_depth: usize) -> Self {
        Self {
            model: ClassifierModel::DecisionTree(DecisionTreeClassifier::new(max_depth, 5)),
        }
    }
    
    pub fn random_forest(num_trees: usize, max_depth: usize) -> Self {
        Self {
            model: ClassifierModel::RandomForest(RandomForestClassifier::new(num_trees, max_depth)),
        }
    }
    
    pub fn logistic_regression(learning_rate: f64, iterations: usize) -> Self {
        Self {
            model: ClassifierModel::LogisticRegression(LogisticRegressionClassifier::new(learning_rate, iterations)),
        }
    }
    
    pub fn train(&mut self, data: &[Vec<f64>], labels: &[usize]) -> Result<(), MLError> {
        match &mut self.model {
            ClassifierModel::DecisionTree(m) => m.train(data, labels),
            ClassifierModel::RandomForest(m) => m.train(data, labels),
            ClassifierModel::LogisticRegression(m) => m.train(data, labels),
        }
    }
    
    pub fn predict(&self, data: &[Vec<f64>]) -> ClassificationResult {
        match &self.model {
            ClassifierModel::DecisionTree(m) => m.predict(data),
            ClassifierModel::RandomForest(m) => m.predict(data),
            ClassifierModel::LogisticRegression(m) => m.predict(data),
        }
    }
}
