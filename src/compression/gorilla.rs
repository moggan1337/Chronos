//! Gorilla compression (TSZ variant)
//! 
//! Implements Facebook's Gorilla compression algorithm optimized for float time-series.
//! Provides excellent compression ratios for monotonically increasing or smoothly changing values.

use crate::storage::column::ColumnData;
use crate::compression::CompressionError;
use std::collections::HashMap;

/// Encode a column using Gorilla compression
pub fn gorilla_encode(data: &ColumnData) -> Vec<u8> {
    match data {
        ColumnData::Float64(values) => gorilla_encode_floats(values),
        ColumnData::Int64(values) => gorilla_encode_ints(values),
        ColumnData::Timestamp(values) => gorilla_encode_ints(values),
        ColumnData::Boolean(values) => gorilla_encode_bools(values),
        ColumnData::String(values) => gorilla_encode_strings(values),
    }
}

/// Decode data from Gorilla compression
pub fn gorilla_decode(data: &[u8], col_type: crate::storage::column::ColumnType) 
    -> Result<ColumnData, CompressionError> 
{
    match col_type {
        crate::storage::column::ColumnType::Float64 => {
            Ok(ColumnData::Float64(gorilla_decode_floats(data)?))
        }
        crate::storage::column::ColumnType::Int64 => {
            Ok(ColumnData::Int64(gorilla_decode_ints(data)?))
        }
        crate::storage::column::ColumnType::Timestamp => {
            Ok(ColumnData::Timestamp(gorilla_decode_ints(data)?))
        }
        _ => Err(CompressionError::InvalidData("Unsupported type".to_string())),
    }
}

/// Gorilla-encoded block
#[derive(Debug, Clone)]
pub struct GorillaBlock {
    pub values: Vec<u8>,
    pub first_value: Option<f64>,
    pub first_timestamp: Option<i64>,
    pub num_values: usize,
}

/// Encode float values using Gorilla
fn gorilla_encode_floats(values: &[Option<f64>]) -> Vec<u8> {
    let mut output = Vec::with_capacity(values.len() * 4); // Estimate
    let mut prev_value: Option<u64> = None;
    let mut prev_timestamp: i64 = 0;
    let mut block_starts: Vec<usize> = Vec::new();
    let mut value_count = 0u32;
    
    // Store header: magic + version
    output.extend_from_slice(b"GORF"); // Gorilla Float
    output.extend_from_slice(&[1u8]); // Version
    
    for (i, &opt_val) in values.iter().enumerate() {
        if opt_val.is_none() {
            // Null marker
            output.push(0xFF);
            continue;
        }
        
        let val = opt_val.unwrap();
        let val_bits = val.to_bits();
        
        // Check for block start (every 4096 values or first)
        let is_block_start = value_count == 0 || value_count % 4096 == 0;
        if is_block_start {
            block_starts.push(output.len());
            
            // Write block header
            output.extend_from_slice(&value_count.to_le_bytes());
            output.extend_from_slice(&val_bits.to_le_bytes());
            output.push(0); // Block continues
        }
        
        if let Some(prev_bits) = prev_value {
            let xor = val_bits ^ prev_bits;
            
            if xor == 0 {
                // Case 1: Same value - write single 0 bit
                output.push(0);
            } else {
                let leading = leading_zeros(xor);
                let trailing = trailing_zeros(xor);
                let significant_bits = 64 - leading - trailing;
                
                if leading >= 10 && trailing >= 10 {
                    // Case 4: Store raw value with prefix 11
                    output.push(0xC0 | ((leading - 10) as u8));
                    output.extend_from_slice(&val_bits.to_le_bytes());
                } else {
                    // Case 3: Store XOR with prefix 10 + block
                    output.push(0x80 | (leading as u8) << 1 | (trailing.min(31) as u8));
                    let significant = (xor >> trailing) & ((1u64 << significant_bits) - 1);
                    write_varint(&mut output, significant);
                }
            }
        } else {
            // First value: store with prefix 111
            output.push(0xE0);
            output.extend_from_slice(&val_bits.to_le_bytes());
        }
        
        prev_value = Some(val_bits);
        prev_timestamp = i as i64;
        value_count += 1;
    }
    
    output
}

/// Decode float values from Gorilla encoding
fn gorilla_decode_floats(data: &[u8]) -> Result<Vec<Option<f64>>, CompressionError> {
    if data.len() < 5 {
        return Err(CompressionError::InvalidData("Data too short".to_string()));
    }
    
    // Check magic
    if &data[0..4] != b"GORF" {
        return Err(CompressionError::InvalidData("Invalid magic".to_string()));
    }
    
    let version = data[4];
    if version != 1 {
        return Err(CompressionError::InvalidData("Unsupported version".to_string()));
    }
    
    let mut values = Vec::new();
    let mut pos = 5;
    let mut prev_value: Option<u64> = None;
    let mut block_remaining = 0;
    
    while pos < data.len() {
        let byte = data[pos];
        
        if block_remaining == 0 {
            // Check for block header
            if byte == 0xFF {
                pos += 1;
                // Null value
                values.push(None);
                continue;
            }
            
            // Try to parse based on prefix
            match byte >> 6 {
                0 => {
                    // Same as previous
                    if let Some(prev) = prev_value {
                        values.push(Some(f64::from_bits(prev)));
                    } else {
                        return Err(CompressionError::InvalidData("No previous value".to_string()));
                    }
                    pos += 1;
                }
                1 => {
                    // XOR block
                    pos += 1;
                    let (value, new_pos) = decode_xor_block(&data[pos..], prev_value)?;
                    prev_value = Some(value);
                    values.push(Some(f64::from_bits(value)));
                    pos += new_pos;
                }
                2 => {
                    // Could be raw value prefix or different value
                    if byte == 0xC0 {
                        // Raw value with extended leading
                        pos += 1;
                        if pos + 8 > data.len() {
                            return Err(CompressionError::InvalidData("Data too short".to_string()));
                        }
                        let val_bits = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                        prev_value = Some(val_bits);
                        values.push(Some(f64::from_bits(val_bits)));
                        pos += 8;
                    } else {
                        // Full raw value with prefix 111
                        pos += 1;
                        if pos + 8 > data.len() {
                            return Err(CompressionError::InvalidData("Data too short".to_string()));
                        }
                        let val_bits = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                        prev_value = Some(val_bits);
                        values.push(Some(f64::from_bits(val_bits)));
                        pos += 8;
                    }
                }
                3 => {
                    // Block header
                    if pos + 5 > data.len() {
                        return Err(CompressionError::InvalidData("Data too short".to_string()));
                    }
                    block_remaining = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    let first_bits = u64::from_le_bytes(data[pos + 4..pos + 12].try_into().unwrap());
                    prev_value = Some(first_bits);
                    values.push(Some(f64::from_bits(first_bits)));
                    pos += 12;
                }
                _ => unreachable!(),
            }
        } else {
            // Inside a block
            block_remaining -= 1;
            // Simplified block reading
            pos += 1;
        }
    }
    
    Ok(values)
}

/// Decode XOR block
fn decode_xor_block(data: &[u8], prev: Option<u64>) -> Result<(u64, usize), CompressionError> {
    if prev.is_none() {
        return Err(CompressionError::InvalidData("No previous value".to_string()));
    }
    
    let prev_bits = prev.unwrap();
    let leading = (data[0] >> 1) as usize;
    let trailing = (data[0] & 0x3F) as usize;
    let significant_bits = 64 - leading - trailing;
    
    // Read significant bits as varint
    let (significant, bytes_read) = read_varint(&data[1..])?;
    
    let xor = significant << trailing;
    let value_bits = prev_bits ^ xor;
    
    Ok((value_bits, 1 + bytes_read))
}

/// Encode integer values using Gorilla variant
fn gorilla_encode_ints(values: &[Option<i64>]) -> Vec<u8> {
    let mut output = Vec::with_capacity(values.len() * 4);
    let mut prev_value: Option<i64> = None;
    
    // Header
    output.extend_from_slice(b"GORI"); // Gorilla Int
    
    for &opt_val in values.iter() {
        if opt_val.is_none() {
            output.push(0xFF);
            continue;
        }
        
        let val = opt_val.unwrap();
        
        if let Some(prev) = prev_value {
            let delta = val - prev;
            if delta == 0 {
                output.push(0); // Same value
            } else {
                // Encode delta
                let zigzag = (delta << 1) ^ (delta >> 63);
                output.push(0x80); // Marker
                write_varint(&mut output, zigzag as u64);
            }
        } else {
            // First value
            output.push(0xC0); // First value marker
            output.extend_from_slice(&val.to_le_bytes());
        }
        
        prev_value = Some(val);
    }
    
    output
}

/// Decode integer values from Gorilla encoding
fn gorilla_decode_ints(data: &[u8]) -> Result<Vec<Option<i64>>, CompressionError> {
    if data.len() < 4 {
        return Err(CompressionError::InvalidData("Data too short".to_string()));
    }
    
    if &data[0..4] != b"GORI" {
        return Err(CompressionError::InvalidData("Invalid magic".to_string()));
    }
    
    let mut values = Vec::new();
    let mut pos = 4;
    let mut prev_value: Option<i64> = None;
    
    while pos < data.len() {
        let byte = data[pos];
        
        if byte == 0xFF {
            values.push(None);
            pos += 1;
        } else if byte == 0 {
            // Same as previous
            if let Some(prev) = prev_value {
                values.push(Some(prev));
            }
            pos += 1;
        } else if byte == 0xC0 {
            // First value
            if pos + 8 > data.len() {
                return Err(CompressionError::InvalidData("Data too short".to_string()));
            }
            let val = i64::from_le_bytes(data[pos + 1..pos + 9].try_into().unwrap());
            values.push(Some(val));
            prev_value = Some(val);
            pos += 9;
        } else if byte == 0x80 {
            // Delta encoded
            pos += 1;
            let (zigzag, bytes_read) = read_varint(&data[pos..])?;
            let delta = unzigzag(zigzag as i64);
            if let Some(prev) = prev_value {
                let val = prev + delta;
                values.push(Some(val));
                prev_value = Some(val);
            }
            pos += bytes_read;
        } else {
            pos += 1;
        }
    }
    
    Ok(values)
}

/// Encode boolean values
fn gorilla_encode_bools(values: &[Option<bool>]) -> Vec<u8> {
    let mut output = Vec::with_capacity((values.len() + 7) / 8);
    output.extend_from_slice(b"GORB"); // Gorilla Bool
    output.extend_from_slice(&0u8); // Version
    
    let mut current_byte = 0u8;
    let mut bit_pos = 0;
    
    for &opt_val in values.iter() {
        if let Some(val) = opt_val {
            if val {
                current_byte |= 1 << bit_pos;
            }
            bit_pos += 1;
            
            if bit_pos == 8 {
                output.push(current_byte);
                current_byte = 0;
                bit_pos = 0;
            }
        } else {
            // Null - skip
            output.push(0xFF);
        }
    }
    
    if bit_pos > 0 {
        output.push(current_byte);
    }
    
    output
}

/// Decode boolean values
fn gorilla_encode_strings(values: &[Option<String>]) -> Vec<u8> {
    // For strings, use simple prefix + length + data encoding
    let mut output = Vec::new();
    output.extend_from_slice(b"GORS"); // Gorilla String
    
    for opt_val in values.iter() {
        if let Some(val) = opt_val {
            let bytes = val.as_bytes();
            let len = bytes.len() as u32;
            output.extend_from_slice(&len.to_le_bytes());
            output.extend_from_slice(bytes);
        } else {
            output.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    
    output
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

/// Zigzag encoding for signed integers
fn unzigzag(n: i64) -> i64 {
    ((n >> 1) ^ -(n & 1))
}

/// Count leading zeros
fn leading_zeros(x: u64) -> usize {
    x.leading_zeros() as usize
}

/// Count trailing zeros
fn trailing_zeros(x: u64) -> usize {
    x.trailing_zeros() as usize
}
