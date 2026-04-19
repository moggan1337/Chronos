//! Zstandard compression
//! 
//! Provides high-performance compression using the Zstandard algorithm.

use crate::compression::CompressionError;

/// Compress data using Zstandard
pub fn zstd_compress(data: &[u8], level: i32) -> Result<Vec<u8>, CompressionError> {
    use std::io::Write;
    
    let mut output = Vec::with_capacity(data.len());
    let mut encoder = zstd::stream::Encoder::new(&mut output, level)
        .map_err(|e| CompressionError::CompressFailed(e.to_string()))?;
    
    encoder.write_all(data)
        .map_err(|e| CompressionError::CompressFailed(e.to_string()))?;
    
    encoder.finish()
        .map_err(|e| CompressionError::CompressFailed(e.to_string()))?;
    
    Ok(output)
}

/// Decompress data using Zstandard
pub fn zstd_decompress(data: &[u8], expected_size: usize) -> Result<Vec<u8>, CompressionError> {
    let mut decoder = zstd::stream::Decoder::new(data)
        .map_err(|e| CompressionError::DecompressFailed(e.to_string()))?;
    
    let mut output = Vec::with_capacity(expected_size);
    std::io::Read::read_to_end(&mut decoder, &mut output)
        .map_err(|e| CompressionError::DecompressFailed(e.to_string()))?;
    
    Ok(output)
}

/// Streaming Zstd compressor for large data
pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    pub fn new(level: i32) -> Self {
        Self { level }
    }
    
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        zstd_compress(data, self.level)
    }
    
    pub fn decompress(&self, data: &[u8], expected_size: usize) -> Result<Vec<u8>, CompressionError> {
        zstd_decompress(data, expected_size)
    }
}

/// Zstandard compression with dictionary
pub struct ZstdDictCompressor {
    dict: zstd::dictionary::Dictionary<'static>,
    level: i32,
}

impl ZstdDictCompressor {
    pub fn train(samples: &[&[u8]]) -> Result<Self, CompressionError> {
        let dict = zstd::dictionary::Dictionary::train(samples)
            .map_err(|e| CompressionError::CompressFailed(e.to_string()))?;
        
        Ok(Self { dict, level: 3 })
    }
    
    pub fn from_dict(dict_data: &[u8]) -> Self {
        let dict = zstd::dictionary::Dictionary::new(dict_data)
            .map_err(|e| CompressionError::CompressFailed(e.to_string()))
            .unwrap();
        
        Self { dict, level: 3 }
    }
    
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let compressed = zstd::encode_all(data, self.level, &self.dict)
            .map_err(|e| CompressionError::CompressFailed(e.to_string()))?;
        Ok(compressed)
    }
    
    pub fn decompress(&self, data: &[u8], expected_size: usize) -> Result<Vec<u8>, CompressionError> {
        let decompressed = zstd::decode_all(data, &self.dict)
            .map_err(|e| CompressionError::DecompressFailed(e.to_string()))?;
        
        if decompressed.len() != expected_size {
            tracing::warn!(
                "Decompressed size {} doesn't match expected {}",
                decompressed.len(),
                expected_size
            );
        }
        
        Ok(decompressed)
    }
}

/// Estimate compressed size
pub fn estimate_compressed_size(original_size: usize, level: i32) -> usize {
    // Zstd worst case expansion is limited
    let worst_case = original_size + (original_size / 128) + 128;
    
    // Apply approximate ratio based on level
    let ratio = match level {
        1 => 0.4,
        3 => 0.3,
        6 => 0.25,
        19 => 0.2,
        _ => 0.35,
    };
    
    (original_size as f64 * ratio) as usize
}
