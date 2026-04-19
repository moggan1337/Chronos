//! CHIMP compression
//! 
//! Implements the CHIMP (Compound Hierarchical In-Memory Prefix) compression algorithm.
//! Optimized for integer time-series with excellent compression ratios for
//! monotonically increasing values like timestamps.

use crate::compression::CompressionError;

/// Encode integer values using CHIMP
pub fn chimp_encode(values: &[i64]) -> Vec<u8> {
    let mut output = Vec::with_capacity(values.len() * 4);
    
    // Header
    output.extend_from_slice(b"CHMP");
    output.push(1); // Version
    
    if values.is_empty() {
        return output;
    }
    
    let mut prev1: Option<i64> = None;
    let mut prev2: Option<i64> = None;
    let mut previous_values: Vec<i64> = Vec::with_capacity(64);
    
    for &value in values.iter() {
        if let (Some(p1), Some(p2)) = (prev1, prev2) {
            // Calculate optimal predictor
            let pred1 = p1; // Previous value
            let pred2 = 2 * p1 - p2; // Linear extrapolation
            let pred3 = *previous_values.last().unwrap_or(&p1); // Last of previous block
            
            let err1 = (value - pred1).unsigned_abs();
            let err2 = (value - pred2).unsigned_abs();
            let err3 = (value - pred3).unsigned_abs();
            
            // Choose best predictor
            let (err, predictor) = if err1 <= err2 && err1 <= err3 {
                (err1, pred1)
            } else if err2 <= err1 && err2 <= err3 {
                (err2, pred2)
            } else {
                (err3, pred3)
            };
            
            // Encode error
            let error = value - predictor;
            encode_integer(&mut output, error);
            
            // Update previous values for next prediction
            prev2 = prev1;
            prev1 = Some(value);
            previous_values.push(value);
            
            // Reset block periodically
            if previous_values.len() >= 64 {
                previous_values.clear();
                previous_values.push(value);
            }
        } else if prev1.is_some() {
            // Second value
            let error = value - prev1.unwrap();
            encode_integer(&mut output, error);
            prev2 = prev1;
            prev1 = Some(value);
            previous_values.push(value);
        } else {
            // First value - store directly
            output.push(0x80); // Marker for direct storage
            output.extend_from_slice(&value.to_le_bytes());
            prev1 = Some(value);
            previous_values.push(value);
        }
    }
    
    output
}

/// Decode integer values from CHIMP encoding
pub fn chimp_decode(data: &[u8], num_values: usize) -> Result<Vec<i64>, CompressionError> {
    if data.len() < 5 {
        return Err(CompressionError::InvalidData("Data too short".to_string()));
    }
    
    // Check magic
    if &data[0..4] != b"CHMP" {
        return Err(CompressionError::InvalidData("Invalid magic".to_string()));
    }
    
    let version = data[4];
    if version != 1 {
        return Err(CompressionError::InvalidData("Unsupported version".to_string()));
    }
    
    let mut values = Vec::with_capacity(num_values);
    let mut pos = 5;
    let mut prev1: Option<i64> = None;
    let mut prev2: Option<i64> = None;
    let mut previous_values: Vec<i64> = Vec::with_capacity(64);
    
    while values.len() < num_values && pos < data.len() {
        let byte = data[pos];
        
        if byte == 0x80 {
            // Direct storage
            if pos + 8 > data.len() {
                return Err(CompressionError::InvalidData("Data too short".to_string()));
            }
            let value = i64::from_le_bytes(data[pos + 1..pos + 9].try_into().unwrap());
            values.push(value);
            prev2 = prev1;
            prev1 = Some(value);
            previous_values.push(value);
            pos += 9;
        } else {
            // Delta encoded
            let (error, bytes_read) = decode_integer(&data[pos..])?;
            pos += bytes_read;
            
            if let Some(p1) = prev1 {
                let pred1 = p1;
                let pred2 = prev2.map(|p| 2 * p1 - p).unwrap_or(p1);
                let pred3 = *previous_values.last().unwrap_or(&p1);
                
                let err1 = (error as i64 - pred1).unsigned_abs();
                let err2 = (error as i64 - pred2).unsigned_abs();
                let err3 = (error as i64 - pred3).unsigned_abs();
                
                let value = if err1 <= err2 && err1 <= err3 {
                    pred1 + error as i64
                } else if err2 <= err1 && err2 <= err3 {
                    pred2 + error as i64
                } else {
                    pred3 + error as i64
                };
                
                values.push(value);
                prev2 = prev1;
                prev1 = Some(value);
                previous_values.push(value);
                
                if previous_values.len() >= 64 {
                    previous_values.clear();
                    previous_values.push(value);
                }
            } else {
                return Err(CompressionError::InvalidData("Invalid state".to_string()));
            }
        }
    }
    
    Ok(values)
}

/// Encode an integer with variable-length encoding
fn encode_integer(output: &mut Vec<u8>, value: i64) {
    // Use zigzag encoding for signed integers
    let zigzag = (value << 1) ^ (value >> 63);
    
    // Then use variable-length encoding
    let mut val = zigzag as u64;
    loop {
        let byte = (val & 0x7F) as u8;
        val >>= 7;
        if val == 0 {
            output.push(byte);
            break;
        } else {
            output.push(byte | 0x80);
        }
    }
}

/// Decode an integer from variable-length encoding
fn decode_integer(data: &[u8]) -> Result<(u64, usize), CompressionError> {
    let mut result = 0u64;
    let mut bytes_read = 0;
    
    loop {
        if bytes_read >= data.len() {
            return Err(CompressionError::InvalidData("Unexpected end".to_string()));
        }
        
        let byte = data[bytes_read];
        bytes_read += 1;
        
        result |= ((byte & 0x7F) as u64) << (7 * (bytes_read - 1));
        
        if byte & 0x80 == 0 {
            break;
        }
    }
    
    Ok((result, bytes_read))
}

/// CHIMP-Float variant for floating-point values
pub fn chimp_encode_float(values: &[f64]) -> Vec<u8> {
    let mut output = Vec::with_capacity(values.len() * 8);
    
    // Header
    output.extend_from_slice(b"CHFP");
    output.push(1);
    
    if values.is_empty() {
        return output;
    }
    
    let mut prev_bits: Option<u64> = None;
    
    for &value in values.iter() {
        if let Some(prev) = prev_bits {
            let xor = value.to_bits() ^ prev;
            if xor == 0 {
                output.push(0); // Same
            } else {
                let leading = leading_zeros(xor);
                let trailing = trailing_zeros(xor);
                
                if leading >= 10 && trailing >= 10 {
                    // Store raw
                    output.push(0x80);
                    output.extend_from_slice(&value.to_bits().to_le_bytes());
                } else {
                    // Store XOR block
                    let significant = (xor >> trailing) << (leading + trailing);
                    output.push(0x40);
                    output.push(leading as u8);
                    output.push(trailing as u8);
                    write_varint(&mut output, significant);
                }
            }
        } else {
            // First value
            output.push(0xC0);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        prev_bits = Some(value.to_bits());
    }
    
    output
}

/// Decode float values from CHIMP encoding
pub fn chimp_decode_float(data: &[u8], num_values: usize) -> Result<Vec<f64>, CompressionError> {
    if data.len() < 5 {
        return Err(CompressionError::InvalidData("Data too short".to_string()));
    }
    
    if &data[0..4] != b"CHFP" {
        return Err(CompressionError::InvalidData("Invalid magic".to_string()));
    }
    
    let mut values = Vec::with_capacity(num_values);
    let mut pos = 5;
    let mut prev_bits: Option<u64> = None;
    
    while values.len() < num_values && pos < data.len() {
        let marker = data[pos];
        
        match marker {
            0 => {
                // Same as previous
                if let Some(prev) = prev_bits {
                    values.push(f64::from_bits(prev));
                }
                pos += 1;
            }
            0x40 => {
                // XOR block
                if pos + 2 > data.len() {
                    return Err(CompressionError::InvalidData("Data too short".to_string()));
                }
                let leading = data[pos + 1] as usize;
                let trailing = data[pos + 2] as usize;
                pos += 3;
                
                let (significant, bytes_read) = read_varint(&data[pos..])?;
                let xor = significant << trailing;
                
                if let Some(prev) = prev_bits {
                    let value_bits = prev ^ xor;
                    values.push(f64::from_bits(value_bits));
                    prev_bits = Some(value_bits);
                }
                pos += bytes_read;
            }
            0x80 => {
                // Raw value
                if pos + 9 > data.len() {
                    return Err(CompressionError::InvalidData("Data too short".to_string()));
                }
                let bits = u64::from_le_bytes(data[pos + 1..pos + 9].try_into().unwrap());
                values.push(f64::from_bits(bits));
                prev_bits = Some(bits);
                pos += 9;
            }
            0xC0 => {
                // First value
                if pos + 9 > data.len() {
                    return Err(CompressionError::InvalidData("Data too short".to_string()));
                }
                let bits = u64::from_le_bytes(data[pos + 1..pos + 9].try_into().unwrap());
                values.push(f64::from_bits(bits));
                prev_bits = Some(bits);
                pos += 9;
            }
            _ => {
                return Err(CompressionError::InvalidData("Invalid marker".to_string()));
            }
        }
    }
    
    Ok(values)
}

/// Write variable-length integer
fn write_varint(output: &mut Vec<u8>, value: u64) {
    let mut val = value;
    loop {
        let byte = (val & 0x7F) as u8;
        val >>= 7;
        if val == 0 {
            output.push(byte);
            break;
        } else {
            output.push(byte | 0x80);
        }
    }
}

/// Read variable-length integer
fn read_varint(data: &[u8]) -> Result<(u64, usize), CompressionError> {
    let mut result = 0u64;
    let mut bytes_read = 0;
    
    loop {
        if bytes_read >= data.len() {
            return Err(CompressionError::InvalidData("Unexpected end".to_string()));
        }
        
        let byte = data[bytes_read];
        bytes_read += 1;
        
        result |= ((byte & 0x7F) as u64) << (7 * (bytes_read - 1));
        
        if byte & 0x80 == 0 {
            break;
        }
    }
    
    Ok((result, bytes_read))
}

/// Count leading zeros
fn leading_zeros(x: u64) -> usize {
    x.leading_zeros() as usize
}

/// Count trailing zeros
fn trailing_zeros(x: u64) -> usize {
    x.trailing_zeros() as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_chimp_roundtrip() {
        let values: Vec<i64> = (0..1000).map(|i| i * 100).collect();
        let encoded = chimp_encode(&values);
        let decoded = chimp_decode(&encoded, values.len()).unwrap();
        assert_eq!(values, decoded);
    }
    
    #[test]
    fn test_chimp_timestamps() {
        // Simulate timestamps (microseconds)
        let start = 1609459200000000i64; // 2021-01-01
        let values: Vec<i64> = (0..10000).map(|i| start + i * 1000000).collect();
        
        let encoded = chimp_encode(&values);
        let ratio = encoded.len() as f64 / (values.len() * 8) as f64;
        assert!(ratio < 0.3, "Compression ratio should be < 30%");
        
        let decoded = chimp_decode(&encoded, values.len()).unwrap();
        assert_eq!(values, decoded);
    }
}
