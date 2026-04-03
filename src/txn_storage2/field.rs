use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::txn_storage2::Schema;

// ============================================================================
// Macros for Field Creation and Testing
// ============================================================================

/// Create common field values with less boilerplate
#[macro_export]
macro_rules! field {
    (Int32 $val:expr) => {
        $crate::txn_storage2::field::Field::Int32(Some($val))
    };
    (Int16 $val:expr) => {
        $crate::txn_storage2::field::Field::Int16(Some($val))
    };
    (Int8 $val:expr) => {
        $crate::txn_storage2::field::Field::Int8(Some($val))
    };
    (String $val:expr) => {
        $crate::txn_storage2::field::Field::String(Some($val.to_string()))
    };
    (Float64 $val:expr) => {
        $crate::txn_storage2::field::Field::Float64(Some($val))
    };
    (Float32 $val:expr) => {
        $crate::txn_storage2::field::Field::Float32(Some($val))
    };
    (Bool $val:expr) => {
        $crate::txn_storage2::field::Field::Bool(Some($val))
    };
    (FixedBytes8 $val:expr) => {
        $crate::txn_storage2::field::Field::FixedBytes8(Some($val))
    };
    (VarBytes $val:expr) => {
        $crate::txn_storage2::field::Field::VarBytes(Some($val))
    };
    (Null Int32) => {
        $crate::txn_storage2::field::Field::Int32(None)
    };
    (Null String) => {
        $crate::txn_storage2::field::Field::String(None)
    };
    (Null Bool) => {
        $crate::txn_storage2::field::Field::Bool(None)
    };
    (Null Pointer) => {
        $crate::txn_storage2::field::Field::Pointer(None)
    };
}

/// Assert that a field matches the expected value
#[macro_export]
macro_rules! assert_field {
    ($field:expr, Int32($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::Int32(Some(v)) => assert_eq!(*v, $expected),
            _ => panic!("Expected Int32({}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, Int16($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::Int16(Some(v)) => assert_eq!(*v, $expected),
            _ => panic!("Expected Int16({}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, Int8($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::Int8(Some(v)) => assert_eq!(*v, $expected),
            _ => panic!("Expected Int8({}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, String($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::String(Some(s)) => assert_eq!(s, $expected),
            _ => panic!("Expected String({}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, Float64($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::Float64(Some(f)) => {
                assert!((*f - $expected).abs() < f64::EPSILON)
            }
            _ => panic!("Expected Float64({}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, Float32($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::Float32(Some(f)) => {
                assert!((*f - $expected).abs() < f32::EPSILON)
            }
            _ => panic!("Expected Float32({}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, Bool($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::Bool(Some(b)) => assert_eq!(*b, $expected),
            _ => panic!("Expected Bool({}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, FixedBytes8($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::FixedBytes8(Some(bytes)) => {
                assert_eq!(*bytes, $expected)
            }
            _ => panic!("Expected FixedBytes8({:?}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, VarBytes($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::VarBytes(Some(bytes)) => {
                assert_eq!(*bytes, $expected)
            }
            _ => panic!("Expected VarBytes({:?}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, Pointer($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::Pointer(Some(ptr)) => assert_eq!(*ptr, $expected),
            _ => panic!("Expected Pointer({:?}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, Date($expected:expr)) => {
        match $field {
            $crate::txn_storage2::field::Field::Date(Some(date)) => assert_eq!(*date, $expected),
            _ => panic!("Expected Date({:?}) but got {:?}", $expected, $field),
        }
    };
    ($field:expr, Null) => {{
        assert!($field.is_null(), "Expected null field but got {:?}", $field)
    }};
}

/// Create a record with the given field values
#[macro_export]
macro_rules! record {
    ($($field:expr),* $(,)?) => {{
        let mut record = $crate::txn_storage2::field::Record::new();
        $(record.push($field);)*
        record
    }};
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float32,
    Float64,
    String,
    FixedBytes8,
    FixedBytes16,
    FixedBytes24,
    VarBytes,
    Bool,
    DateTime,
    Months,
    Days,
    Pointer,
}

impl DataType {
    #[inline]
    pub const fn as_byte(&self) -> u8 {
        match self {
            DataType::Int8 => 0,
            DataType::Int16 => 1,
            DataType::Int32 => 2,
            DataType::Int64 => 3,
            DataType::Uint8 => 4,
            DataType::Uint16 => 5,
            DataType::Uint32 => 6,
            DataType::Uint64 => 7,
            DataType::Float32 => 8,
            DataType::Float64 => 9,
            DataType::String => 10,
            DataType::FixedBytes8 => 11,
            DataType::FixedBytes16 => 12,
            DataType::FixedBytes24 => 13,
            DataType::VarBytes => 14,
            DataType::Bool => 15,
            DataType::DateTime => 16,
            DataType::Months => 17,
            DataType::Days => 18,
            DataType::Pointer => 19,
        }
    }

    #[inline]
    pub fn to_bytes(&self) -> Vec<u8> {
        vec![self.as_byte()]
    }

    #[inline]
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(DataType::Int8),
            1 => Some(DataType::Int16),
            2 => Some(DataType::Int32),
            3 => Some(DataType::Int64),
            4 => Some(DataType::Uint8),
            5 => Some(DataType::Uint16),
            6 => Some(DataType::Uint32),
            7 => Some(DataType::Uint64),
            8 => Some(DataType::Float32),
            9 => Some(DataType::Float64),
            10 => Some(DataType::String),
            11 => Some(DataType::FixedBytes8),
            12 => Some(DataType::FixedBytes16),
            13 => Some(DataType::FixedBytes24),
            14 => Some(DataType::VarBytes),
            15 => Some(DataType::Bool),
            16 => Some(DataType::DateTime),
            17 => Some(DataType::Months),
            18 => Some(DataType::Days),
            19 => Some(DataType::Pointer),
            _ => None,
        }
    }

    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_byte(bytes[0]).expect("Unknown data type byte")
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordPointer {
    pub page_id: u32,
    pub frame_id: u32,
}

impl RecordPointer {
    pub fn new(page_id: u32, frame_id: u32) -> Self {
        Self { page_id, frame_id }
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        let mut bytes = [0u8; 8];
        bytes[0..4].copy_from_slice(&self.page_id.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.frame_id.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), 8);
        Self {
            page_id: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            frame_id: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Field {
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Uint8(Option<u8>),
    Uint16(Option<u16>),
    Uint32(Option<u32>),
    Uint64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    String(Option<String>),
    FixedBytes8(Option<[u8; 8]>),
    FixedBytes16(Option<[u8; 16]>),
    FixedBytes24(Option<[u8; 24]>),
    VarBytes(Option<Vec<u8>>),
    Bool(Option<bool>),
    Date(Option<NaiveDate>),
    Months(Option<i32>),
    Days(Option<i64>),
    Pointer(Option<RecordPointer>),
}

impl Field {
    #[inline]
    pub fn is_null(&self) -> bool {
        match self {
            Field::Int8(v) => v.is_none(),
            Field::Int16(v) => v.is_none(),
            Field::Int32(v) => v.is_none(),
            Field::Int64(v) => v.is_none(),
            Field::Uint8(v) => v.is_none(),
            Field::Uint16(v) => v.is_none(),
            Field::Uint32(v) => v.is_none(),
            Field::Uint64(v) => v.is_none(),
            Field::Float32(v) => v.is_none(),
            Field::Float64(v) => v.is_none(),
            Field::String(v) => v.is_none(),
            Field::FixedBytes8(v) => v.is_none(),
            Field::FixedBytes16(v) => v.is_none(),
            Field::FixedBytes24(v) => v.is_none(),
            Field::VarBytes(v) => v.is_none(),
            Field::Bool(v) => v.is_none(),
            Field::Date(v) => v.is_none(),
            Field::Months(v) => v.is_none(),
            Field::Days(v) => v.is_none(),
            Field::Pointer(v) => v.is_none(),
        }
    }

    #[inline]
    fn serialize_bytes(bytes: &mut Vec<u8>, data: &[u8]) {
        bytes.extend_from_slice(data);
    }

    #[inline]
    fn serialize_var_length(bytes: &mut Vec<u8>, data: &[u8]) {
        bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
        bytes.extend_from_slice(data);
    }

    #[inline]
    pub fn size(&self, is_nullable: bool) -> usize {
        let mut size = 0;

        if is_nullable {
            size += 1; // Null indicator byte
        }

        if self.is_null() {
            return size; // If null, return size with only the null indicator
        }

        match self {
            Field::Int8(_) => size += 1,
            Field::Int16(_) => size += 2,
            Field::Int32(_) => size += 4,
            Field::Int64(_) => size += 8,
            Field::Uint8(_) => size += 1,
            Field::Uint16(_) => size += 2,
            Field::Uint32(_) => size += 4,
            Field::Uint64(_) => size += 8,
            Field::Float32(_) => size += 4,
            Field::Float64(_) => size += 8,
            Field::String(Some(s)) => size += 4 + s.len(), // Length + string bytes
            Field::FixedBytes8(_) => size += 8,
            Field::FixedBytes16(_) => size += 16,
            Field::FixedBytes24(_) => size += 24,
            Field::VarBytes(Some(vec)) => size += 4 + vec.len(), // Length + bytes
            Field::Bool(_) => size += 1,
            Field::Date(_) => size += 4,    // Days from CE
            Field::Months(_) => size += 4,  // Months
            Field::Days(_) => size += 8,    // Days
            Field::Pointer(_) => size += 8, // page_id + frame_id
            _ => {}                         // Null case handled above
        }
        size
    }

    pub fn to_bytes(&self, is_nullable: bool) -> Vec<u8> {
        if is_nullable && self.is_null() {
            return vec![0]; // Null indicator
        }

        // Use exact size instead of rough estimate
        let mut bytes = Vec::with_capacity(self.size(is_nullable));

        if is_nullable {
            bytes.push(1); // Not null indicator
        }

        match self {
            Field::Int8(Some(v)) => Self::serialize_bytes(&mut bytes, &v.to_le_bytes()),
            Field::Int16(Some(v)) => Self::serialize_bytes(&mut bytes, &v.to_le_bytes()),
            Field::Int32(Some(v)) => Self::serialize_bytes(&mut bytes, &v.to_le_bytes()),
            Field::Int64(Some(v)) => Self::serialize_bytes(&mut bytes, &v.to_le_bytes()),
            Field::Uint8(Some(v)) => bytes.push(*v),
            Field::Uint16(Some(v)) => Self::serialize_bytes(&mut bytes, &v.to_le_bytes()),
            Field::Uint32(Some(v)) => Self::serialize_bytes(&mut bytes, &v.to_le_bytes()),
            Field::Uint64(Some(v)) => Self::serialize_bytes(&mut bytes, &v.to_le_bytes()),
            Field::Float32(Some(v)) => Self::serialize_bytes(&mut bytes, &v.to_le_bytes()),
            Field::Float64(Some(v)) => Self::serialize_bytes(&mut bytes, &v.to_le_bytes()),
            Field::String(Some(s)) => Self::serialize_var_length(&mut bytes, s.as_bytes()),
            Field::FixedBytes8(Some(arr)) => Self::serialize_bytes(&mut bytes, arr),
            Field::FixedBytes16(Some(arr)) => Self::serialize_bytes(&mut bytes, arr),
            Field::FixedBytes24(Some(arr)) => Self::serialize_bytes(&mut bytes, arr),
            Field::VarBytes(Some(vec)) => Self::serialize_var_length(&mut bytes, vec),
            Field::Bool(Some(b)) => bytes.push(if *b { 1 } else { 0 }),
            Field::Date(Some(d)) => {
                Self::serialize_bytes(&mut bytes, &d.num_days_from_ce().to_le_bytes())
            }
            Field::Months(Some(m)) => Self::serialize_bytes(&mut bytes, &m.to_le_bytes()),
            Field::Days(Some(d)) => Self::serialize_bytes(&mut bytes, &d.to_le_bytes()),
            Field::Pointer(Some(p)) => Self::serialize_bytes(&mut bytes, &p.to_bytes()),
            _ => unreachable!("Null check should have been handled above"),
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8], is_nullable: bool, data_type: DataType) -> Self {
        let start_offset = if is_nullable {
            // Check for null indicator
            if bytes[0] == 0 {
                return match data_type {
                    DataType::Int8 => Field::Int8(None),
                    DataType::Int16 => Field::Int16(None),
                    DataType::Int32 => Field::Int32(None),
                    DataType::Int64 => Field::Int64(None),
                    DataType::Uint8 => Field::Uint8(None),
                    DataType::Uint16 => Field::Uint16(None),
                    DataType::Uint32 => Field::Uint32(None),
                    DataType::Uint64 => Field::Uint64(None),
                    DataType::Float32 => Field::Float32(None),
                    DataType::Float64 => Field::Float64(None),
                    DataType::String => Field::String(None),
                    DataType::FixedBytes8 => Field::FixedBytes8(None),
                    DataType::FixedBytes16 => Field::FixedBytes16(None),
                    DataType::FixedBytes24 => Field::FixedBytes24(None),
                    DataType::VarBytes => Field::VarBytes(None),
                    DataType::Bool => Field::Bool(None),
                    DataType::DateTime => Field::Date(None),
                    DataType::Months => Field::Months(None),
                    DataType::Days => Field::Days(None),
                    DataType::Pointer => Field::Pointer(None),
                };
            }
            1 // Skip the null indicator byte
        } else {
            0
        };

        let mut offset = start_offset;
        match data_type {
            DataType::Int8 => {
                let value = i8::from_le_bytes([bytes[offset]]);
                Field::Int8(Some(value))
            }
            DataType::Int16 => {
                let value = i16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
                Field::Int16(Some(value))
            }
            DataType::Int32 => {
                let value = i32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]);
                Field::Int32(Some(value))
            }
            DataType::Int64 => {
                let value = i64::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                    bytes[offset + 4],
                    bytes[offset + 5],
                    bytes[offset + 6],
                    bytes[offset + 7],
                ]);
                Field::Int64(Some(value))
            }
            DataType::Uint8 => {
                let value = bytes[offset];
                Field::Uint8(Some(value))
            }
            DataType::Uint16 => {
                let value = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
                Field::Uint16(Some(value))
            }
            DataType::Uint32 => {
                let value = u32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]);
                Field::Uint32(Some(value))
            }
            DataType::Uint64 => {
                if bytes.len() < offset + 8 {
                    panic!(
                        "Insufficient bytes for Uint64: expected at least {} bytes, got {}",
                        offset + 8,
                        bytes.len()
                    );
                }
                let value = u64::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                    bytes[offset + 4],
                    bytes[offset + 5],
                    bytes[offset + 6],
                    bytes[offset + 7],
                ]);
                Field::Uint64(Some(value))
            }
            DataType::Float32 => {
                let value = f32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]);
                Field::Float32(Some(value))
            }
            DataType::Float64 => {
                let value = f64::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                    bytes[offset + 4],
                    bytes[offset + 5],
                    bytes[offset + 6],
                    bytes[offset + 7],
                ]);
                Field::Float64(Some(value))
            }

            DataType::String => {
                let len = u32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]) as usize;
                offset += 4;
                let value = String::from_utf8(bytes[offset..offset + len].to_vec()).unwrap();
                Field::String(Some(value))
            }
            DataType::FixedBytes8 => {
                Field::FixedBytes8(Some(bytes[offset..offset + 8].try_into().unwrap()))
            }
            DataType::FixedBytes16 => {
                Field::FixedBytes16(Some(bytes[offset..offset + 16].try_into().unwrap()))
            }
            DataType::FixedBytes24 => {
                Field::FixedBytes24(Some(bytes[offset..offset + 24].try_into().unwrap()))
            }
            DataType::VarBytes => {
                let len = u32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]) as usize;
                offset += 4;
                let value = bytes[offset..offset + len].to_vec();
                Field::VarBytes(Some(value))
            }
            DataType::Bool => {
                let value = bytes[offset] != 0;
                Field::Bool(Some(value))
            }
            DataType::DateTime => {
                let days = i32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]);
                let date = NaiveDate::from_num_days_from_ce_opt(days).unwrap();
                Field::Date(Some(date))
            }
            DataType::Months => {
                let value = i32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]);
                Field::Months(Some(value))
            }
            DataType::Days => {
                let value = i64::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                    bytes[offset + 4],
                    bytes[offset + 5],
                    bytes[offset + 6],
                    bytes[offset + 7],
                ]);
                Field::Days(Some(value))
            }
            DataType::Pointer => {
                let pointer = RecordPointer::from_bytes(&bytes[offset..offset + 8]);
                Field::Pointer(Some(pointer))
            }
        }
    }
}

fn f64_to_order_preserving_bytes(val: f64) -> [u8; 8] {
    let mut val_bits = val.to_bits();
    let sign = (val_bits >> 63) as u8;
    if sign == 1 {
        // Negative number so flip all the bits including the sign bit
        val_bits = !val_bits;
    } else {
        // Positive number. To distinguish between positive and negative numbers,
        // we flip the sign bit.
        val_bits ^= 1 << 63;
    }
    val_bits.to_be_bytes()
}

fn f32_to_order_preserving_bytes(val: f32) -> [u8; 4] {
    let mut val_bits = val.to_bits();
    let sign = (val_bits >> 31) as u8;
    if sign == 1 {
        // Negative number so flip all the bits including the sign bit
        val_bits = !val_bits;
    } else {
        // Positive number. To distinguish between positive and negative numbers,
        // we flip the sign bit.
        val_bits ^= 1 << 31;
    }
    val_bits.to_be_bytes()
}

#[inline]
fn append_null_key(key: &mut Vec<u8>, null_first: bool) {
    key.push(if null_first { 0 } else { 255 });
}

#[inline]
fn append_bytes_key(key: &mut Vec<u8>, bytes: &[u8], asc: bool, null_first: bool) {
    key.push(if null_first { 255 } else { 0 }); // non-null prefix
    if asc {
        key.extend_from_slice(bytes);
    } else {
        key.extend(bytes.iter().map(|b| !b));
    }
}

/// Calculate the exact capacity needed for a normalized key
#[inline]
fn calculate_normalized_key_capacity(
    fields: &[Field],
    key_indexes: &[(usize, bool, bool)],
) -> usize {
    let mut capacity = 0;
    let mut var_field_count = 0;

    for &(field_idx, _, _) in key_indexes {
        capacity += 1; // null indicator byte

        match &fields[field_idx] {
            Field::String(Some(s)) => {
                capacity += s.len() + 1; // string bytes + terminator
                var_field_count += 1;
            }
            Field::String(None) => {
                var_field_count += 1;
            }
            Field::VarBytes(Some(v)) => {
                capacity += v.len() + 1; // bytes + terminator
                var_field_count += 1;
            }
            Field::VarBytes(None) => {
                var_field_count += 1;
            }
            Field::Bool(_) | Field::Int8(_) | Field::Uint8(_) => capacity += 1,
            Field::Int16(_) | Field::Uint16(_) => capacity += 2,
            Field::Int32(_)
            | Field::Uint32(_)
            | Field::Float32(_)
            | Field::Date(_)
            | Field::Months(_) => capacity += 4,
            Field::Int64(_)
            | Field::Uint64(_)
            | Field::Float64(_)
            | Field::Days(_)
            | Field::FixedBytes8(_)
            | Field::Pointer(_) => capacity += 8,
            Field::FixedBytes16(_) => capacity += 16,
            Field::FixedBytes24(_) => capacity += 24,
        }
    }

    // Add space for separator, variable field info, and count
    capacity += 1; // separator
    capacity += var_field_count * 6; // each var field: 2 bytes idx + 4 bytes len
    capacity += 2; // var field count

    capacity
}

pub fn to_normalized_key(fields: &[Field], key_indexes: &[(usize, bool, bool)]) -> Vec<u8> {
    // Calculate exact capacity to avoid reallocations
    let capacity = calculate_normalized_key_capacity(fields, key_indexes);
    let mut key = Vec::with_capacity(capacity);

    // Pre-calculate variable field count for better allocation
    let var_field_count = key_indexes
        .iter()
        .filter(|(idx, _, _)| matches!(&fields[*idx], Field::String(_) | Field::VarBytes(_)))
        .count();
    let mut var_field_info: Vec<(u16, u32)> = Vec::with_capacity(var_field_count);

    for (key_idx, &(field_idx, asc, null_first)) in key_indexes.iter().enumerate() {
        match &fields[field_idx] {
            Field::String(Some(val)) => {
                // Add null indicator for non-null value
                append_bytes_key(&mut key, val.as_bytes(), asc, null_first);
                key.push(1); // String terminator.
                             // Track variable field info (position after null indicator)
                let field_len = val.len() as u32;
                var_field_info.push((key_idx as u16, field_len));
            }
            Field::String(None) => {
                append_null_key(&mut key, null_first);
                var_field_info.push((key_idx as u16, 0));
            }
            Field::VarBytes(Some(val)) => {
                append_bytes_key(&mut key, val, asc, null_first);
                key.push(1); // VarBytes terminator
                let field_len = val.len() as u32;
                var_field_info.push((key_idx as u16, field_len));
            }
            Field::VarBytes(None) => {
                append_null_key(&mut key, null_first);
                var_field_info.push((key_idx as u16, 0));
            }
            // All other fields use the same logic as before
            Field::Bool(Some(val)) => {
                append_bytes_key(&mut key, &[if *val { 1 } else { 0 }], asc, null_first);
            }
            Field::Int8(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Int16(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Int32(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Int64(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Uint8(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Uint16(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Uint32(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Uint64(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Float32(Some(val)) => {
                append_bytes_key(
                    &mut key,
                    &f32_to_order_preserving_bytes(*val),
                    asc,
                    null_first,
                );
            }
            Field::Float64(Some(val)) => {
                append_bytes_key(
                    &mut key,
                    &f64_to_order_preserving_bytes(*val),
                    asc,
                    null_first,
                );
            }
            Field::FixedBytes8(Some(val)) => {
                append_bytes_key(&mut key, val, asc, null_first);
            }
            Field::FixedBytes16(Some(val)) => {
                append_bytes_key(&mut key, val, asc, null_first);
            }
            Field::FixedBytes24(Some(val)) => {
                append_bytes_key(&mut key, val, asc, null_first);
            }
            Field::Date(Some(val)) => {
                append_bytes_key(
                    &mut key,
                    &val.num_days_from_ce().to_be_bytes(),
                    asc,
                    null_first,
                );
            }
            Field::Months(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Days(Some(val)) => {
                append_bytes_key(&mut key, &val.to_be_bytes(), asc, null_first);
            }
            Field::Pointer(Some(ptr)) => {
                append_bytes_key(&mut key, &ptr.to_bytes(), asc, null_first);
            }
            _ => append_null_key(&mut key, null_first), // Handle all null cases
        }
    }

    if !var_field_info.is_empty() {
        // Separator key
        key.push(0);

        // Append variable field info
        for (idx, len) in &var_field_info {
            key.extend_from_slice(&idx.to_be_bytes());
            key.extend_from_slice(&len.to_be_bytes());
        }
    }

    key
}

/// Helper function to decode bytes that were encoded with asc flag
fn decode_bytes(bytes: &[u8], asc: bool) -> Vec<u8> {
    if asc {
        bytes.to_vec()
    } else {
        bytes.iter().map(|b| !b).collect()
    }
}

/// Helper function to decode a single byte directly
#[inline]
fn decode_byte(byte: u8, asc: bool) -> u8 {
    if asc {
        byte
    } else {
        !byte
    }
}

/// Helper function to decode bytes into a fixed-size array
#[inline]
fn decode_bytes_array<const N: usize>(bytes: &[u8], asc: bool) -> [u8; N] {
    let mut result = [0u8; N];
    if asc {
        result.copy_from_slice(bytes);
    } else {
        for (i, &b) in bytes.iter().enumerate() {
            result[i] = !b;
        }
    }
    result
}

/// Helper function to create a null field of the given type
fn create_null_field(data_type: &DataType) -> Field {
    match data_type {
        DataType::Int8 => Field::Int8(None),
        DataType::Int16 => Field::Int16(None),
        DataType::Int32 => Field::Int32(None),
        DataType::Int64 => Field::Int64(None),
        DataType::Uint8 => Field::Uint8(None),
        DataType::Uint16 => Field::Uint16(None),
        DataType::Uint32 => Field::Uint32(None),
        DataType::Uint64 => Field::Uint64(None),
        DataType::Float32 => Field::Float32(None),
        DataType::Float64 => Field::Float64(None),
        DataType::String => Field::String(None),
        DataType::FixedBytes8 => Field::FixedBytes8(None),
        DataType::FixedBytes16 => Field::FixedBytes16(None),
        DataType::FixedBytes24 => Field::FixedBytes24(None),
        DataType::VarBytes => Field::VarBytes(None),
        DataType::Bool => Field::Bool(None),
        DataType::DateTime => Field::Date(None),
        DataType::Months => Field::Months(None),
        DataType::Days => Field::Days(None),
        DataType::Pointer => Field::Pointer(None),
    }
}

/// Converts a normalized key back to fields using schema information
pub fn from_normalized_key(
    normalized_key: &[u8],
    key_indexes: &[(usize, bool, bool)],
    field_types: &[DataType],
) -> Result<Vec<Field>, String> {
    // Read number of variable fields from the end
    let num_var_fields = field_types
        .iter()
        .filter(|&&t| matches!(t, DataType::String | DataType::VarBytes))
        .count();

    // Parse variable field metadata
    let (var_field_map, data_end) = if num_var_fields == 0 {
        // No variable fields, return an empty map
        (HashMap::new(), normalized_key.len())
    } else {
        let mut var_field_map = HashMap::new();
        let var_info_size = num_var_fields * 6; // Each entry is 2 + 4 bytes

        if normalized_key.len() < var_info_size {
            return Err("Normalized key too short for variable field info".to_string());
        }

        let var_info_start = normalized_key.len() - var_info_size;

        for i in 0..num_var_fields {
            let offset = var_info_start + (i * 6);
            let key_idx = u16::from_be_bytes(
                normalized_key[offset..offset + 2]
                    .try_into()
                    .map_err(|_| "Failed to read key index")?,
            );
            let length = u32::from_be_bytes(
                normalized_key[offset + 2..offset + 6]
                    .try_into()
                    .map_err(|_| "Failed to read field length")?,
            );
            var_field_map.insert(key_idx, length);
        }
        (var_field_map, var_info_start - 1) // Exclude the separator byte
    };

    // Parse fields
    let mut fields = Vec::new();
    let mut pos = 0;

    for (key_idx, &(field_idx, asc, null_first)) in key_indexes.iter().enumerate() {
        if pos >= data_end {
            return Err("Unexpected end of normalized key".to_string());
        }

        // Check null indicator
        let null_byte = normalized_key[pos];
        let is_null = null_byte == (if null_first { 0 } else { 255 });
        pos += 1;

        if is_null {
            fields.push(create_null_field(&field_types[field_idx]));
            continue;
        }

        // Parse non-null field based on type
        let field_type = &field_types[field_idx];
        let field = match field_type {
            DataType::String => {
                let length = var_field_map
                    .get(&(key_idx as u16))
                    .ok_or_else(|| "Missing length for string field".to_string())?;

                if pos + *length as usize > data_end {
                    return Err("String field extends beyond data boundary".to_string());
                }

                let bytes = &normalized_key[pos..pos + *length as usize];
                let string = if asc {
                    // For ascending order, bytes are stored as-is
                    String::from_utf8(bytes.to_vec())
                        .map_err(|e| format!("Invalid UTF-8 in string field: {}", e))?
                } else {
                    // For descending order, need to decode
                    let decoded = decode_bytes(bytes, asc);
                    String::from_utf8(decoded)
                        .map_err(|e| format!("Invalid UTF-8 in string field: {}", e))?
                };
                pos += *length as usize;
                pos += 1; // Skip the string terminator
                Field::String(Some(string))
            }
            DataType::VarBytes => {
                let length = var_field_map
                    .get(&(key_idx as u16))
                    .ok_or_else(|| "Missing length for VarBytes field".to_string())?;

                if pos + *length as usize > data_end {
                    return Err("VarBytes field extends beyond data boundary".to_string());
                }

                let bytes = &normalized_key[pos..pos + *length as usize];
                let result = if asc {
                    // For ascending order, bytes are stored as-is
                    bytes.to_vec()
                } else {
                    // For descending order, need to decode
                    decode_bytes(bytes, asc)
                };
                pos += *length as usize;
                pos += 1; // Skip the VarBytes terminator
                Field::VarBytes(Some(result))
            }
            DataType::Bool => {
                if pos >= data_end {
                    return Err("Unexpected end of data for Bool field".to_string());
                }
                let val = decode_byte(normalized_key[pos], asc);
                pos += 1;
                Field::Bool(Some(val != 0))
            }
            DataType::Int8 => {
                if pos + 1 > data_end {
                    return Err("Unexpected end of data for Int8 field".to_string());
                }
                let byte_array = decode_bytes_array::<1>(&normalized_key[pos..pos + 1], asc);
                let val = i8::from_be_bytes(byte_array);
                pos += 1;
                Field::Int8(Some(val))
            }
            DataType::Int16 => {
                if pos + 2 > data_end {
                    return Err("Unexpected end of data for Int16 field".to_string());
                }
                let byte_array = decode_bytes_array::<2>(&normalized_key[pos..pos + 2], asc);
                let val = i16::from_be_bytes(byte_array);
                pos += 2;
                Field::Int16(Some(val))
            }
            DataType::Int32 => {
                if pos + 4 > data_end {
                    return Err("Unexpected end of data for Int32 field".to_string());
                }
                let byte_array = decode_bytes_array::<4>(&normalized_key[pos..pos + 4], asc);
                let val = i32::from_be_bytes(byte_array);
                pos += 4;
                Field::Int32(Some(val))
            }
            DataType::Int64 => {
                if pos + 8 > data_end {
                    return Err("Unexpected end of data for Int64 field".to_string());
                }
                let byte_array = decode_bytes_array::<8>(&normalized_key[pos..pos + 8], asc);
                let val = i64::from_be_bytes(byte_array);
                pos += 8;
                Field::Int64(Some(val))
            }
            DataType::Uint8 => {
                if pos + 1 > data_end {
                    return Err("Unexpected end of data for Uint8 field".to_string());
                }
                let byte_array = decode_bytes_array::<1>(&normalized_key[pos..pos + 1], asc);
                let val = u8::from_be_bytes(byte_array);
                pos += 1;
                Field::Uint8(Some(val))
            }
            DataType::Uint16 => {
                if pos + 2 > data_end {
                    return Err("Unexpected end of data for Uint16 field".to_string());
                }
                let byte_array = decode_bytes_array::<2>(&normalized_key[pos..pos + 2], asc);
                let val = u16::from_be_bytes(byte_array);
                pos += 2;
                Field::Uint16(Some(val))
            }
            DataType::Uint32 => {
                if pos + 4 > data_end {
                    return Err("Unexpected end of data for Uint32 field".to_string());
                }
                let byte_array = decode_bytes_array::<4>(&normalized_key[pos..pos + 4], asc);
                let val = u32::from_be_bytes(byte_array);
                pos += 4;
                Field::Uint32(Some(val))
            }
            DataType::Uint64 => {
                if pos + 8 > data_end {
                    return Err("Unexpected end of data for Uint64 field".to_string());
                }
                let byte_array = decode_bytes_array::<8>(&normalized_key[pos..pos + 8], asc);
                let val = u64::from_be_bytes(byte_array);
                pos += 8;
                Field::Uint64(Some(val))
            }
            DataType::Float32 => {
                if pos + 4 > data_end {
                    return Err("Unexpected end of data for Float32 field".to_string());
                }
                let order_bytes = decode_bytes_array::<4>(&normalized_key[pos..pos + 4], asc);
                let val = f32_from_order_preserving_bytes(order_bytes);
                pos += 4;
                Field::Float32(Some(val))
            }
            DataType::Float64 => {
                if pos + 8 > data_end {
                    return Err("Unexpected end of data for Float64 field".to_string());
                }
                let order_bytes = decode_bytes_array::<8>(&normalized_key[pos..pos + 8], asc);
                let val = f64_from_order_preserving_bytes(order_bytes);
                pos += 8;
                Field::Float64(Some(val))
            }
            DataType::FixedBytes8 => {
                if pos + 8 > data_end {
                    return Err("Unexpected end of data for FixedBytes8 field".to_string());
                }
                let arr = decode_bytes_array::<8>(&normalized_key[pos..pos + 8], asc);
                pos += 8;
                Field::FixedBytes8(Some(arr))
            }
            DataType::FixedBytes16 => {
                if pos + 16 > data_end {
                    return Err("Unexpected end of data for FixedBytes16 field".to_string());
                }
                let arr = decode_bytes_array::<16>(&normalized_key[pos..pos + 16], asc);
                pos += 16;
                Field::FixedBytes16(Some(arr))
            }
            DataType::FixedBytes24 => {
                if pos + 24 > data_end {
                    return Err("Unexpected end of data for FixedBytes24 field".to_string());
                }
                let arr = decode_bytes_array::<24>(&normalized_key[pos..pos + 24], asc);
                pos += 24;
                Field::FixedBytes24(Some(arr))
            }
            DataType::DateTime => {
                if pos + 4 > data_end {
                    return Err("Unexpected end of data for Date field".to_string());
                }
                let byte_array = decode_bytes_array::<4>(&normalized_key[pos..pos + 4], asc);
                let days = i32::from_be_bytes(byte_array);
                let date = NaiveDate::from_num_days_from_ce_opt(days)
                    .ok_or_else(|| "Invalid date value".to_string())?;
                pos += 4;
                Field::Date(Some(date))
            }
            DataType::Months => {
                if pos + 4 > data_end {
                    return Err("Unexpected end of data for Months field".to_string());
                }
                let byte_array = decode_bytes_array::<4>(&normalized_key[pos..pos + 4], asc);
                let val = i32::from_be_bytes(byte_array);
                pos += 4;
                Field::Months(Some(val))
            }
            DataType::Days => {
                if pos + 8 > data_end {
                    return Err("Unexpected end of data for Days field".to_string());
                }
                let byte_array = decode_bytes_array::<8>(&normalized_key[pos..pos + 8], asc);
                let val = i64::from_be_bytes(byte_array);
                pos += 8;
                Field::Days(Some(val))
            }
            DataType::Pointer => {
                if pos + 8 > data_end {
                    return Err("Unexpected end of data for Pointer field".to_string());
                }
                // For pointer, we need to decode if desc order
                if asc {
                    let ptr = RecordPointer::from_bytes(&normalized_key[pos..pos + 8]);
                    pos += 8;
                    Field::Pointer(Some(ptr))
                } else {
                    // Decode bytes for descending order
                    let byte_array = decode_bytes_array::<8>(&normalized_key[pos..pos + 8], asc);
                    let ptr = RecordPointer::from_bytes(&byte_array);
                    pos += 8;
                    Field::Pointer(Some(ptr))
                }
            }
        };

        fields.push(field);
    }

    Ok(fields)
}

/// Converts float32 from order-preserving bytes
fn f32_from_order_preserving_bytes(bytes: [u8; 4]) -> f32 {
    let bits = u32::from_be_bytes(bytes);
    if bits & (1 << 31) != 0 {
        // Positive number
        f32::from_bits(bits ^ (1 << 31))
    } else {
        // Negative number
        f32::from_bits(!bits)
    }
}

/// Converts float64 from order-preserving bytes
fn f64_from_order_preserving_bytes(bytes: [u8; 8]) -> f64 {
    let bits = u64::from_be_bytes(bytes);
    if bits & (1 << 63) != 0 {
        // Positive number
        f64::from_bits(bits ^ (1 << 63))
    } else {
        // Negative number
        f64::from_bits(!bits)
    }
}

pub struct Record {
    pub fields: Vec<Field>,
}

impl Default for Record {
    fn default() -> Self {
        Self::new()
    }
}

impl Record {
    pub fn new() -> Self {
        Record { fields: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Record {
            fields: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, field: Field) {
        self.fields.push(field);
    }

    pub fn remove(&mut self, index: usize) -> Field {
        self.fields.remove(index)
    }

    pub fn to_normalized_key(&self, key_indexes: &[(usize, bool, bool)]) -> Vec<u8> {
        to_normalized_key(&self.fields, key_indexes)
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Field::Int8(Some(v)) => write!(f, "{}", v),
            Field::Int8(None) => write!(f, "NULL"),
            Field::Int16(Some(v)) => write!(f, "{}", v),
            Field::Int16(None) => write!(f, "NULL"),
            Field::Int32(Some(v)) => write!(f, "{}", v),
            Field::Int32(None) => write!(f, "NULL"),
            Field::Int64(Some(v)) => write!(f, "{}", v),
            Field::Int64(None) => write!(f, "NULL"),
            Field::Uint8(Some(v)) => write!(f, "{}", v),
            Field::Uint8(None) => write!(f, "NULL"),
            Field::Uint16(Some(v)) => write!(f, "{}", v),
            Field::Uint16(None) => write!(f, "NULL"),
            Field::Uint32(Some(v)) => write!(f, "{}", v),
            Field::Uint32(None) => write!(f, "NULL"),
            Field::Uint64(Some(v)) => write!(f, "{}", v),
            Field::Uint64(None) => write!(f, "NULL"),
            Field::Float32(Some(v)) => write!(f, "{:.2}", v),
            Field::Float32(None) => write!(f, "NULL"),
            Field::Float64(Some(v)) => write!(f, "{:.2}", v),
            Field::Float64(None) => write!(f, "NULL"),
            Field::String(Some(v)) => write!(f, "'{}'", v),
            Field::String(None) => write!(f, "NULL"),
            Field::FixedBytes8(Some(v)) => write!(
                f,
                "0x{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]
            ),
            Field::FixedBytes8(None) => write!(f, "NULL"),
            Field::FixedBytes16(Some(v)) => {
                write!(f, "0x")?;
                for byte in v.iter() {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
            Field::FixedBytes16(None) => write!(f, "NULL"),
            Field::FixedBytes24(Some(v)) => {
                write!(f, "0x")?;
                for byte in v.iter() {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
            Field::FixedBytes24(None) => write!(f, "NULL"),
            Field::VarBytes(Some(v)) => {
                if v.len() <= 8 {
                    write!(f, "0x")?;
                    for byte in v.iter() {
                        write!(f, "{:02x}", byte)?;
                    }
                    Ok(())
                } else {
                    write!(f, "0x{:02x}{:02x}...({} bytes)", v[0], v[1], v.len())
                }
            }
            Field::VarBytes(None) => write!(f, "NULL"),
            Field::Bool(Some(v)) => write!(f, "{}", v),
            Field::Bool(None) => write!(f, "NULL"),
            Field::Date(Some(v)) => write!(f, "{}", v),
            Field::Date(None) => write!(f, "NULL"),
            Field::Months(Some(v)) => write!(f, "{} months", v),
            Field::Months(None) => write!(f, "NULL"),
            Field::Days(Some(v)) => write!(f, "{} days", v),
            Field::Days(None) => write!(f, "NULL"),
            Field::Pointer(Some(p)) => {
                write!(f, "Pointer(page:{}, frame:{})", p.page_id, p.frame_id)
            }
            Field::Pointer(None) => write!(f, "NULL"),
        }
    }
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Record[")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", field)?;
        }
        write!(f, "]")
    }
}

// ========================================================================
// Internal Helper Methods
// ========================================================================

#[inline(always)]
pub fn key_to_bytes(key: &[Field]) -> Vec<u8> {
    to_normalized_key(
        key,
        &key.iter()
            .enumerate()
            .map(|(i, _)| (i, true, false))
            .collect::<Vec<_>>(),
    )
}

#[inline(always)]
pub fn record_to_key_bytes(record: &[Field], schema: &Schema) -> Vec<u8> {
    to_normalized_key(
        &record,
        &schema
            .key_indices()
            .iter()
            .map(|&i| (i, true, false))
            .collect::<Vec<_>>(),
    )
}

#[inline(always)]
pub fn record_to_bytes(record: &[Field], schema: &Schema) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(record.len() * 8); // Estimate size
    for (i, field) in record.iter().enumerate() {
        let (is_nullable, _) = &schema.cols()[i];
        let field_bytes = field.to_bytes(*is_nullable);
        bytes.extend_from_slice(&field_bytes);
    }
    bytes
}

#[inline(always)]
pub fn bytes_to_record(bytes: &[u8], schema: &Schema) -> Vec<Field> {
    let mut all_fields = Vec::with_capacity(schema.cols().len());
    let mut offset = 0;

    for (is_nullable, data_type) in schema.cols().iter() {
        if offset >= bytes.len() {
            panic!(
                "Not enough bytes to read all fields. Expected {} fields, got {} bytes total, at offset {}. Schema: {:?}",
                schema.cols().len(),
                bytes.len(),
                offset,
                schema
            );
        }

        let remaining_bytes = &bytes[offset..];
        let field = Field::from_bytes(remaining_bytes, *is_nullable, *data_type);
        let consumed_bytes = field.size(*is_nullable);

        offset += consumed_bytes;
        all_fields.push(field);
    }

    all_fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_datatype_to_bytes_from_bytes_roundtrip() {
        let datatypes = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Uint8,
            DataType::Uint16,
            DataType::Uint32,
            DataType::Uint64,
            DataType::Float32,
            DataType::Float64,
            DataType::String,
            DataType::FixedBytes8,
            DataType::FixedBytes16,
            DataType::FixedBytes24,
            DataType::VarBytes,
            DataType::Bool,
            DataType::DateTime,
            DataType::Months,
            DataType::Days,
            DataType::Pointer,
        ];

        for datatype in datatypes {
            let bytes = datatype.to_bytes();
            let recovered = DataType::from_bytes(&bytes);
            assert_eq!(datatype, recovered);
        }
    }

    #[test]
    #[should_panic]
    fn test_datatype_from_bytes_invalid() {
        DataType::from_bytes(&[255]);
    }

    #[test]
    fn test_record_pointer() {
        let ptr = RecordPointer::new(42, 100);
        assert_eq!(ptr.page_id, 42);
        assert_eq!(ptr.frame_id, 100);

        let bytes = ptr.to_bytes();
        assert_eq!(bytes.len(), 8);

        let recovered = RecordPointer::from_bytes(&bytes);
        assert_eq!(recovered.page_id, 42);
        assert_eq!(recovered.frame_id, 100);
        assert_eq!(ptr, recovered);
    }

    #[test]
    fn test_field_is_null() {
        assert!(Field::Int8(None).is_null());
        assert!(!Field::Int8(Some(42)).is_null());

        assert!(Field::String(None).is_null());
        assert!(!Field::String(Some("test".to_string())).is_null());

        assert!(Field::Bool(None).is_null());
        assert!(!Field::Bool(Some(true)).is_null());

        assert!(Field::Date(None).is_null());
        assert!(!Field::Date(Some(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap())).is_null());
    }

    #[test]
    fn test_field_to_bytes_from_bytes_roundtrip_nullable() {
        let test_cases = vec![
            (Field::Int8(Some(42)), DataType::Int8),
            (Field::Int8(None), DataType::Int8),
            (Field::Int16(Some(-1000)), DataType::Int16),
            (Field::Int16(None), DataType::Int16),
            (Field::Int32(Some(123456)), DataType::Int32),
            (Field::Int32(None), DataType::Int32),
            (Field::Int64(Some(-9876543210)), DataType::Int64),
            (Field::Int64(None), DataType::Int64),
            (Field::Uint8(Some(255)), DataType::Uint8),
            (Field::Uint8(None), DataType::Uint8),
            (Field::Uint16(Some(65535)), DataType::Uint16),
            (Field::Uint16(None), DataType::Uint16),
            (Field::Uint32(Some(4294967295)), DataType::Uint32),
            (Field::Uint32(None), DataType::Uint32),
            (Field::Uint64(Some(18446744073709551615)), DataType::Uint64),
            (Field::Uint64(None), DataType::Uint64),
            (
                Field::Float32(Some(std::f32::consts::PI)),
                DataType::Float32,
            ),
            (Field::Float32(None), DataType::Float32),
            (Field::Float64(Some(std::f64::consts::E)), DataType::Float64),
            (Field::Float64(None), DataType::Float64),
            (
                Field::String(Some("Hello, World!".to_string())),
                DataType::String,
            ),
            (Field::String(None), DataType::String),
            (
                Field::FixedBytes8(Some([1, 2, 3, 4, 5, 6, 7, 8])),
                DataType::FixedBytes8,
            ),
            (Field::FixedBytes8(None), DataType::FixedBytes8),
            (
                Field::FixedBytes16(Some([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                ])),
                DataType::FixedBytes16,
            ),
            (Field::FixedBytes16(None), DataType::FixedBytes16),
            (
                Field::FixedBytes24(Some([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                    23, 24,
                ])),
                DataType::FixedBytes24,
            ),
            (Field::FixedBytes24(None), DataType::FixedBytes24),
            (
                Field::VarBytes(Some(vec![10, 20, 30, 40])),
                DataType::VarBytes,
            ),
            (Field::VarBytes(None), DataType::VarBytes),
            (Field::Bool(Some(true)), DataType::Bool),
            (Field::Bool(Some(false)), DataType::Bool),
            (Field::Bool(None), DataType::Bool),
            (
                Field::Date(Some(NaiveDate::from_ymd_opt(2023, 12, 25).unwrap())),
                DataType::DateTime,
            ),
            (Field::Date(None), DataType::DateTime),
            (Field::Months(Some(24)), DataType::Months),
            (Field::Months(None), DataType::Months),
            (Field::Days(Some(365)), DataType::Days),
            (Field::Days(None), DataType::Days),
            (
                Field::Pointer(Some(RecordPointer::new(123, 456))),
                DataType::Pointer,
            ),
            (Field::Pointer(None), DataType::Pointer),
        ];

        for (original_field, data_type) in test_cases {
            let bytes = original_field.to_bytes(true);
            let recovered_field = Field::from_bytes(&bytes, true, data_type);

            match (&original_field, &recovered_field) {
                (Field::Int8(a), Field::Int8(b)) => assert_eq!(a, b),
                (Field::Int16(a), Field::Int16(b)) => assert_eq!(a, b),
                (Field::Int32(a), Field::Int32(b)) => assert_eq!(a, b),
                (Field::Int64(a), Field::Int64(b)) => assert_eq!(a, b),
                (Field::Uint8(a), Field::Uint8(b)) => assert_eq!(a, b),
                (Field::Uint16(a), Field::Uint16(b)) => assert_eq!(a, b),
                (Field::Uint32(a), Field::Uint32(b)) => assert_eq!(a, b),
                (Field::Uint64(a), Field::Uint64(b)) => assert_eq!(a, b),
                (Field::Float32(a), Field::Float32(b)) => assert_eq!(a, b),
                (Field::Float64(a), Field::Float64(b)) => assert_eq!(a, b),
                (Field::String(a), Field::String(b)) => assert_eq!(a, b),
                (Field::FixedBytes8(a), Field::FixedBytes8(b)) => assert_eq!(a, b),
                (Field::FixedBytes16(a), Field::FixedBytes16(b)) => assert_eq!(a, b),
                (Field::FixedBytes24(a), Field::FixedBytes24(b)) => assert_eq!(a, b),
                (Field::VarBytes(a), Field::VarBytes(b)) => assert_eq!(a, b),
                (Field::Bool(a), Field::Bool(b)) => assert_eq!(a, b),
                (Field::Date(a), Field::Date(b)) => assert_eq!(a, b),
                (Field::Months(a), Field::Months(b)) => assert_eq!(a, b),
                (Field::Days(a), Field::Days(b)) => assert_eq!(a, b),
                (Field::Pointer(a), Field::Pointer(b)) => assert_eq!(a, b),
                _ => panic!("Field type mismatch"),
            }
        }
    }

    #[test]
    fn test_field_to_bytes_from_bytes_roundtrip_non_nullable() {
        let test_cases = vec![
            (Field::Int8(Some(42)), DataType::Int8),
            (Field::Int16(Some(-1000)), DataType::Int16),
            (Field::Int32(Some(123456)), DataType::Int32),
            (Field::Int64(Some(-9876543210)), DataType::Int64),
            (Field::Uint8(Some(255)), DataType::Uint8),
            (Field::Uint16(Some(65535)), DataType::Uint16),
            (Field::Uint32(Some(4294967295)), DataType::Uint32),
            (Field::Uint64(Some(18446744073709551615)), DataType::Uint64),
            (
                Field::Float32(Some(std::f32::consts::PI)),
                DataType::Float32,
            ),
            (Field::Float64(Some(std::f64::consts::E)), DataType::Float64),
            (
                Field::String(Some("Hello, World!".to_string())),
                DataType::String,
            ),
            (
                Field::FixedBytes8(Some([1, 2, 3, 4, 5, 6, 7, 8])),
                DataType::FixedBytes8,
            ),
            (
                Field::FixedBytes16(Some([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                ])),
                DataType::FixedBytes16,
            ),
            (
                Field::FixedBytes24(Some([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                    23, 24,
                ])),
                DataType::FixedBytes24,
            ),
            (
                Field::VarBytes(Some(vec![10, 20, 30, 40])),
                DataType::VarBytes,
            ),
            (Field::Bool(Some(true)), DataType::Bool),
            (Field::Bool(Some(false)), DataType::Bool),
            (
                Field::Date(Some(NaiveDate::from_ymd_opt(2023, 12, 25).unwrap())),
                DataType::DateTime,
            ),
            (Field::Months(Some(24)), DataType::Months),
            (Field::Days(Some(365)), DataType::Days),
            (
                Field::Pointer(Some(RecordPointer::new(789, 101112))),
                DataType::Pointer,
            ),
        ];

        for (original_field, data_type) in test_cases {
            let bytes = original_field.to_bytes(false);
            let recovered_field = Field::from_bytes(&bytes, false, data_type);

            match (&original_field, &recovered_field) {
                (Field::Int8(a), Field::Int8(b)) => assert_eq!(a, b),
                (Field::Int16(a), Field::Int16(b)) => assert_eq!(a, b),
                (Field::Int32(a), Field::Int32(b)) => assert_eq!(a, b),
                (Field::Int64(a), Field::Int64(b)) => assert_eq!(a, b),
                (Field::Uint8(a), Field::Uint8(b)) => assert_eq!(a, b),
                (Field::Uint16(a), Field::Uint16(b)) => assert_eq!(a, b),
                (Field::Uint32(a), Field::Uint32(b)) => assert_eq!(a, b),
                (Field::Uint64(a), Field::Uint64(b)) => assert_eq!(a, b),
                (Field::Float32(a), Field::Float32(b)) => assert_eq!(a, b),
                (Field::Float64(a), Field::Float64(b)) => assert_eq!(a, b),
                (Field::String(a), Field::String(b)) => assert_eq!(a, b),
                (Field::FixedBytes8(a), Field::FixedBytes8(b)) => assert_eq!(a, b),
                (Field::FixedBytes16(a), Field::FixedBytes16(b)) => assert_eq!(a, b),
                (Field::FixedBytes24(a), Field::FixedBytes24(b)) => assert_eq!(a, b),
                (Field::VarBytes(a), Field::VarBytes(b)) => assert_eq!(a, b),
                (Field::Bool(a), Field::Bool(b)) => assert_eq!(a, b),
                (Field::Date(a), Field::Date(b)) => assert_eq!(a, b),
                (Field::Months(a), Field::Months(b)) => assert_eq!(a, b),
                (Field::Days(a), Field::Days(b)) => assert_eq!(a, b),
                (Field::Pointer(a), Field::Pointer(b)) => assert_eq!(a, b),
                _ => panic!("Field type mismatch"),
            }
        }
    }

    #[test]
    fn test_record_new_and_with_capacity() {
        let record = Record::new();
        assert_eq!(record.fields.len(), 0);

        let record = Record::with_capacity(10);
        assert_eq!(record.fields.len(), 0);
        assert!(record.fields.capacity() >= 10);
    }

    #[test]
    fn test_record_push_and_remove() {
        let mut record = Record::new();

        record.push(Field::Int32(Some(42)));
        record.push(Field::String(Some("test".to_string())));
        record.push(Field::Bool(Some(true)));

        assert_eq!(record.fields.len(), 3);

        let removed = record.remove(1);
        match removed {
            Field::String(Some(s)) => assert_eq!(s, "test"),
            _ => panic!("Expected String field"),
        }

        assert_eq!(record.fields.len(), 2);
    }

    #[test]
    fn test_f32_to_order_preserving_bytes() {
        let values = [
            -f32::INFINITY,
            -1000.0,
            -1.0,
            -0.1,
            0.0,
            0.1,
            1.0,
            1000.0,
            f32::INFINITY,
        ];

        let mut encoded_values: Vec<([u8; 4], f32)> = values
            .iter()
            .map(|&v| (f32_to_order_preserving_bytes(v), v))
            .collect();

        encoded_values.sort_by(|a, b| a.0.cmp(&b.0));

        for i in 1..encoded_values.len() {
            assert!(
                encoded_values[i - 1].1 <= encoded_values[i].1,
                "Order not preserved: {} should be <= {}",
                encoded_values[i - 1].1,
                encoded_values[i].1
            );
        }
    }

    #[test]
    fn test_f64_to_order_preserving_bytes() {
        let values = [
            -f64::INFINITY,
            -1000.0,
            -1.0,
            -0.1,
            0.0,
            0.1,
            1.0,
            1000.0,
            f64::INFINITY,
        ];

        let mut encoded_values: Vec<([u8; 8], f64)> = values
            .iter()
            .map(|&v| (f64_to_order_preserving_bytes(v), v))
            .collect();

        encoded_values.sort_by(|a, b| a.0.cmp(&b.0));

        for i in 1..encoded_values.len() {
            assert!(
                encoded_values[i - 1].1 <= encoded_values[i].1,
                "Order not preserved: {} should be <= {}",
                encoded_values[i - 1].1,
                encoded_values[i].1
            );
        }
    }

    #[test]
    fn test_to_normalized_key_single_field_asc() {
        let mut record = Record::new();
        record.push(Field::Int32(Some(42)));

        let key_indexes = vec![(0, true, true)]; // field 0, ascending, nulls first
        let key = record.to_normalized_key(&key_indexes);

        // Should have non-null prefix (255) followed by the big-endian bytes of 42
        assert_eq!(key[0], 255); // non-null prefix for nulls_first=true
        assert_eq!(key[1..], 42i32.to_be_bytes());
    }

    #[test]
    fn test_to_normalized_key_single_field_desc() {
        let mut record = Record::new();
        record.push(Field::Int32(Some(42)));

        let key_indexes = vec![(0, false, true)]; // field 0, descending, nulls first
        let key = record.to_normalized_key(&key_indexes);

        // Should have non-null prefix (255) followed by the inverted big-endian bytes of 42
        assert_eq!(key[0], 255); // non-null prefix for nulls_first=true
        let expected_bytes: Vec<u8> = 42i32.to_be_bytes().iter().map(|b| !b).collect();
        assert_eq!(key[1..], expected_bytes);
    }

    #[test]
    fn test_to_normalized_key_null_field() {
        let mut record = Record::new();
        record.push(Field::Int32(None));

        let key_indexes = vec![(0, true, true)]; // field 0, ascending, nulls first
        let key = record.to_normalized_key(&key_indexes);

        // Should have only null prefix (0) for nulls_first=true
        assert_eq!(key, vec![0]);

        let key_indexes = vec![(0, true, false)]; // field 0, ascending, nulls last
        let key = record.to_normalized_key(&key_indexes);

        // Should have only null prefix (255) for nulls_first=false
        assert_eq!(key, vec![255]);
    }

    #[test]
    fn test_to_normalized_key_multiple_fields() {
        let mut record = Record::new();
        record.push(Field::Int32(Some(42)));
        record.push(Field::String(Some("test".to_string())));
        record.push(Field::Bool(Some(true)));

        let key_indexes = vec![
            (0, true, true),   // Int32, ascending, nulls first
            (1, false, false), // String, descending, nulls last
            (2, true, true),   // Bool, ascending, nulls first
        ];
        let key = record.to_normalized_key(&key_indexes);

        // Expected: [255, 42_bytes, 0, inverted_test_bytes, 255, 1]
        let mut expected = vec![255]; // non-null prefix for Int32
        expected.extend_from_slice(&42i32.to_be_bytes());
        expected.push(0); // non-null prefix for String (nulls_first=false)
        expected.extend("test".as_bytes().iter().map(|b| !b)); // inverted bytes for descending
        expected.push(1); // String terminator
        expected.push(255); // non-null prefix for Bool
        expected.push(1); // true as byte
        expected.push(0); // Var fields separator
        expected.extend_from_slice(&1u16.to_be_bytes()); // Index of String field
        expected.extend_from_slice(&4u32.to_be_bytes()); // Size of "test"

        assert_eq!(key, expected);
    }

    #[test]
    fn test_to_normalized_key_float_ordering() {
        let mut record1 = Record::new();
        record1.push(Field::Float32(Some(-1.0)));

        let mut record2 = Record::new();
        record2.push(Field::Float32(Some(1.0)));

        let key_indexes = vec![(0, true, true)];
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);

        // key1 should be less than key2 since -1.0 < 1.0
        assert!(key1 < key2);
    }

    #[test]
    fn test_multi_column_ordering_asc_asc() {
        let mut record1 = Record::new();
        record1.push(Field::Int32(Some(1)));
        record1.push(Field::String(Some("apple".to_string())));

        let mut record2 = Record::new();
        record2.push(Field::Int32(Some(1)));
        record2.push(Field::String(Some("banana".to_string())));

        let mut record3 = Record::new();
        record3.push(Field::Int32(Some(2)));
        record3.push(Field::String(Some("apple".to_string())));

        let key_indexes = vec![(0, true, true), (1, true, true)]; // Both ascending
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);
        let key3 = record3.to_normalized_key(&key_indexes);

        // Verify ordering: (1, "apple") < (1, "banana") < (2, "apple")
        assert!(key1 < key2);
        assert!(key2 < key3);
        assert!(key1 < key3);
    }

    #[test]
    fn test_multi_column_ordering_asc_desc() {
        let mut record1 = Record::new();
        record1.push(Field::Int32(Some(1)));
        record1.push(Field::String(Some("apple".to_string())));

        let mut record2 = Record::new();
        record2.push(Field::Int32(Some(1)));
        record2.push(Field::String(Some("banana".to_string())));

        let key_indexes = vec![(0, true, true), (1, false, true)]; // First asc, second desc
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);

        // With descending on second column: (1, "banana") < (1, "apple")
        assert!(key2 < key1);
    }

    #[test]
    fn test_multi_column_ordering_with_nulls() {
        let mut record1 = Record::new();
        record1.push(Field::Int32(Some(1)));
        record1.push(Field::String(None));

        let mut record2 = Record::new();
        record2.push(Field::Int32(Some(1)));
        record2.push(Field::String(Some("apple".to_string())));

        let mut record3 = Record::new();
        record3.push(Field::Int32(Some(2)));
        record3.push(Field::String(None));

        // Test nulls first
        let key_indexes = vec![(0, true, true), (1, true, true)]; // Both asc, nulls first
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);
        let key3 = record3.to_normalized_key(&key_indexes);

        // Ordering: (1, null) < (1, "apple") < (2, null)
        assert!(key1 < key2);
        assert!(key2 < key3);
        assert!(key1 < key3);

        // Test nulls last
        let key_indexes = vec![(0, true, false), (1, true, false)]; // Both asc, nulls last
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);
        let key3 = record3.to_normalized_key(&key_indexes);

        // Ordering: (1, "apple") < (1, null) < (2, null)
        assert!(key2 < key1);
        assert!(key1 < key3);
        assert!(key2 < key3);
    }

    #[test]
    fn test_multi_column_ordering_mixed_types() {
        let mut record1 = Record::new();
        record1.push(Field::Int32(Some(1)));
        record1.push(Field::Float64(Some(std::f64::consts::PI)));
        record1.push(Field::Bool(Some(false)));

        let mut record2 = Record::new();
        record2.push(Field::Int32(Some(1)));
        record2.push(Field::Float64(Some(std::f64::consts::PI)));
        record2.push(Field::Bool(Some(true)));

        let mut record3 = Record::new();
        record3.push(Field::Int32(Some(1)));
        record3.push(Field::Float64(Some(2.71)));
        record3.push(Field::Bool(Some(true)));

        let key_indexes = vec![(0, true, true), (1, true, true), (2, true, true)];
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);
        let key3 = record3.to_normalized_key(&key_indexes);

        // Ordering: (1, 2.71, true) < (1, PI, false) < (1, PI, true)
        assert!(key3 < key1);
        assert!(key1 < key2);
        assert!(key3 < key2);
    }

    #[test]
    fn test_multi_column_ordering_large_number_of_columns() {
        let mut record1 = Record::new();
        record1.push(Field::Int8(Some(1)));
        record1.push(Field::Int16(Some(100)));
        record1.push(Field::Int32(Some(1000)));
        record1.push(Field::Int64(Some(10000)));
        record1.push(Field::String(Some("test".to_string())));

        let mut record2 = Record::new();
        record2.push(Field::Int8(Some(1)));
        record2.push(Field::Int16(Some(100)));
        record2.push(Field::Int32(Some(1000)));
        record2.push(Field::Int64(Some(10000)));
        record2.push(Field::String(Some("test2".to_string())));

        let key_indexes = vec![
            (0, true, true),
            (1, true, true),
            (2, true, true),
            (3, true, true),
            (4, true, true),
        ];
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);

        // Should differ only on the last column: "test" < "test2"
        assert!(key1 < key2);
    }

    #[test]
    fn test_multi_column_ordering_descending_priority() {
        let mut record1 = Record::new();
        record1.push(Field::Int32(Some(2)));
        record1.push(Field::Int32(Some(1)));

        let mut record2 = Record::new();
        record2.push(Field::Int32(Some(1)));
        record2.push(Field::Int32(Some(2)));

        // First column descending, second ascending
        let key_indexes = vec![(0, false, true), (1, true, true)];
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);

        // With first column descending: (2, 1) < (1, 2)
        assert!(key1 < key2);
    }

    #[test]
    fn test_multi_column_ordering_date_and_numeric() {
        use chrono::NaiveDate;

        let mut record1 = Record::new();
        record1.push(Field::Date(Some(
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        )));
        record1.push(Field::Int32(Some(100)));

        let mut record2 = Record::new();
        record2.push(Field::Date(Some(
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        )));
        record2.push(Field::Int32(Some(200)));

        let mut record3 = Record::new();
        record3.push(Field::Date(Some(
            NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
        )));
        record3.push(Field::Int32(Some(50)));

        let key_indexes = vec![(0, true, true), (1, true, true)];
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);
        let key3 = record3.to_normalized_key(&key_indexes);

        // Ordering: (2023-01-01, 100) < (2023-01-01, 200) < (2023-01-02, 50)
        assert!(key1 < key2);
        assert!(key2 < key3);
        assert!(key1 < key3);
    }

    #[test]
    fn test_multi_column_ordering_consistent_with_single_column() {
        let mut record1 = Record::new();
        record1.push(Field::Int32(Some(1)));
        record1.push(Field::String(Some("test".to_string())));

        let mut record2 = Record::new();
        record2.push(Field::Int32(Some(2)));
        record2.push(Field::String(Some("test".to_string())));

        // Compare single column vs multi-column keys
        let single_key_indexes = vec![(0, true, true)];
        let multi_key_indexes = vec![(0, true, true), (1, true, true)];

        let single_key1 = record1.to_normalized_key(&single_key_indexes);
        let single_key2 = record2.to_normalized_key(&single_key_indexes);
        let multi_key1 = record1.to_normalized_key(&multi_key_indexes);
        let multi_key2 = record2.to_normalized_key(&multi_key_indexes);

        // Ordering should be consistent
        assert_eq!(single_key1 < single_key2, multi_key1 < multi_key2);
    }

    #[test]
    fn test_multi_column_ordering_edge_case_equal_prefixes() {
        // Test records that are equal on all but the last column
        let mut record1 = Record::new();
        record1.push(Field::Int32(Some(42)));
        record1.push(Field::String(Some("same".to_string())));
        record1.push(Field::Bool(Some(false)));
        record1.push(Field::Float32(Some(1.0)));

        let mut record2 = Record::new();
        record2.push(Field::Int32(Some(42)));
        record2.push(Field::String(Some("same".to_string())));
        record2.push(Field::Bool(Some(false)));
        record2.push(Field::Float32(Some(2.0)));

        let key_indexes = vec![
            (0, true, true),
            (1, true, true),
            (2, true, true),
            (3, true, true),
        ];
        let key1 = record1.to_normalized_key(&key_indexes);
        let key2 = record2.to_normalized_key(&key_indexes);

        // Should only differ on the last column: 1.0 < 2.0
        assert!(key1 < key2);
    }

    #[test]
    fn test_normalized_key_basic() {
        // Test with a simple key containing one string field
        let fields = vec![
            Field::Int32(Some(42)),
            Field::String(Some("hello".to_string())),
        ];
        let key_indexes = vec![(0, true, true), (1, true, true)];

        let normalized = to_normalized_key(&fields, &key_indexes);
        let recovered = from_normalized_key(
            &normalized,
            &key_indexes,
            &[DataType::Int32, DataType::String],
        )
        .unwrap();

        // Check the recovered fields match the original
        assert_eq!(recovered[0], Field::Int32(Some(42)));
        assert_eq!(recovered[1], Field::String(Some("hello".to_string())));
    }

    #[test]
    fn test_normalized_key_multiple_strings() {
        let fields = vec![
            Field::String(Some("first".to_string())),
            Field::Int32(Some(42)),
            Field::String(Some("second".to_string())),
            Field::VarBytes(Some(vec![1, 2, 3])),
        ];
        let key_indexes = vec![
            (0, true, true),
            (1, true, true),
            (2, true, true),
            (3, true, true),
        ];

        let normalized = to_normalized_key(&fields, &key_indexes);
        let recovered = from_normalized_key(
            &normalized,
            &key_indexes,
            &[
                DataType::String,
                DataType::Int32,
                DataType::String,
                DataType::VarBytes,
            ],
        )
        .unwrap();

        // Check the recovered fields match the original
        assert_eq!(recovered[0], Field::String(Some("first".to_string())));
        assert_eq!(recovered[1], Field::Int32(Some(42)));
        assert_eq!(recovered[2], Field::String(Some("second".to_string())));
        assert_eq!(recovered[3], Field::VarBytes(Some(vec![1, 2, 3])));
    }

    #[test]
    fn test_from_normalized_key_roundtrip() {
        let fields = vec![
            Field::Int32(Some(42)),
            Field::String(Some("hello world".to_string())),
            Field::Bool(Some(true)),
            Field::VarBytes(Some(vec![0xDE, 0xAD, 0xBE, 0xEF])),
        ];
        let key_indexes = vec![
            (0, true, true),
            (1, true, true),
            (2, true, true),
            (3, true, true),
        ];
        let field_types = vec![
            DataType::Int32,
            DataType::String,
            DataType::Bool,
            DataType::VarBytes,
        ];

        let normalized = to_normalized_key(&fields, &key_indexes);
        let recovered = from_normalized_key(&normalized, &key_indexes, &field_types).unwrap();

        // Check each field matches
        match (&fields[0], &recovered[0]) {
            (Field::Int32(Some(a)), Field::Int32(Some(b))) => assert_eq!(a, b),
            _ => panic!("Field 0 mismatch"),
        }
        match (&fields[1], &recovered[1]) {
            (Field::String(Some(a)), Field::String(Some(b))) => assert_eq!(a, b),
            _ => panic!("Field 1 mismatch"),
        }
        match (&fields[2], &recovered[2]) {
            (Field::Bool(Some(a)), Field::Bool(Some(b))) => assert_eq!(a, b),
            _ => panic!("Field 2 mismatch"),
        }
        match (&fields[3], &recovered[3]) {
            (Field::VarBytes(Some(a)), Field::VarBytes(Some(b))) => assert_eq!(a, b),
            _ => panic!("Field 3 mismatch"),
        }
    }

    #[test]
    fn test_from_normalized_key_with_nulls() {
        let fields = vec![
            Field::Int32(Some(42)),
            Field::String(None),
            Field::Bool(Some(false)),
            Field::VarBytes(None),
        ];
        let key_indexes = vec![
            (0, true, true),
            (1, true, true),
            (2, true, true),
            (3, true, true),
        ];
        let field_types = vec![
            DataType::Int32,
            DataType::String,
            DataType::Bool,
            DataType::VarBytes,
        ];

        let normalized = to_normalized_key(&fields, &key_indexes);
        let recovered = from_normalized_key(&normalized, &key_indexes, &field_types).unwrap();

        // Check nulls are preserved
        assert!(matches!(recovered[1], Field::String(None)));
        assert!(matches!(recovered[3], Field::VarBytes(None)));
    }

    #[test]
    fn test_normalized_key_desc_order() {
        let fields = vec![
            Field::Int32(Some(42)),
            Field::String(Some("test".to_string())),
        ];
        let key_indexes_asc = vec![(0, true, true), (1, true, true)];
        let key_indexes_desc = vec![(0, false, true), (1, false, true)];

        let normalized_asc = to_normalized_key(&fields, &key_indexes_asc);
        let normalized_desc = to_normalized_key(&fields, &key_indexes_desc);

        // They should be different due to desc ordering
        assert_ne!(normalized_asc, normalized_desc);

        // Test roundtrip with desc
        let field_types = vec![DataType::Int32, DataType::String];
        let recovered =
            from_normalized_key(&normalized_desc, &key_indexes_desc, &field_types).unwrap();

        match (&fields[0], &recovered[0]) {
            (Field::Int32(Some(a)), Field::Int32(Some(b))) => assert_eq!(a, b),
            _ => panic!("Field 0 mismatch"),
        }
        match (&fields[1], &recovered[1]) {
            (Field::String(Some(a)), Field::String(Some(b))) => assert_eq!(a, b),
            _ => panic!("Field 1 mismatch"),
        }
    }

    #[test]
    fn test_normalized_key_all_types() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let ptr = RecordPointer::new(123, 456);

        let fields = vec![
            Field::Int8(Some(-42)),
            Field::Int16(Some(-1000)),
            Field::Int32(Some(-100000)),
            Field::Int64(Some(-1000000000)),
            Field::Uint8(Some(255)),
            Field::Uint16(Some(65535)),
            Field::Uint32(Some(4294967295)),
            Field::Uint64(Some(18446744073709551615)),
            Field::Float32(Some(-std::f32::consts::PI)),
            Field::Float64(Some(std::f64::consts::E)),
            Field::String(Some("variable length".to_string())),
            Field::FixedBytes8(Some([1, 2, 3, 4, 5, 6, 7, 8])),
            Field::FixedBytes16(Some([0; 16])),
            Field::FixedBytes24(Some([255; 24])),
            Field::VarBytes(Some(vec![10, 20, 30, 40, 50])),
            Field::Bool(Some(true)),
            Field::Date(Some(date)),
            Field::Months(Some(12)),
            Field::Days(Some(365)),
            Field::Pointer(Some(ptr)),
        ];

        let key_indexes: Vec<_> = (0..fields.len()).map(|i| (i, true, true)).collect();
        let field_types = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Uint8,
            DataType::Uint16,
            DataType::Uint32,
            DataType::Uint64,
            DataType::Float32,
            DataType::Float64,
            DataType::String,
            DataType::FixedBytes8,
            DataType::FixedBytes16,
            DataType::FixedBytes24,
            DataType::VarBytes,
            DataType::Bool,
            DataType::DateTime,
            DataType::Months,
            DataType::Days,
            DataType::Pointer,
        ];

        let normalized = to_normalized_key(&fields, &key_indexes);
        let recovered = from_normalized_key(&normalized, &key_indexes, &field_types).unwrap();

        // Verify all fields match
        for (i, (original, recovered)) in fields.iter().zip(recovered.iter()).enumerate() {
            match (original, recovered) {
                (Field::Int8(Some(a)), Field::Int8(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Int16(Some(a)), Field::Int16(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Int32(Some(a)), Field::Int32(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Int64(Some(a)), Field::Int64(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Uint8(Some(a)), Field::Uint8(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Uint16(Some(a)), Field::Uint16(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Uint32(Some(a)), Field::Uint32(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Uint64(Some(a)), Field::Uint64(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Float32(Some(a)), Field::Float32(Some(b))) => {
                    assert!((a - b).abs() < f32::EPSILON, "Field {} mismatch", i)
                }
                (Field::Float64(Some(a)), Field::Float64(Some(b))) => {
                    assert!((a - b).abs() < f64::EPSILON, "Field {} mismatch", i)
                }
                (Field::String(Some(a)), Field::String(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::FixedBytes8(Some(a)), Field::FixedBytes8(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::FixedBytes16(Some(a)), Field::FixedBytes16(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::FixedBytes24(Some(a)), Field::FixedBytes24(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::VarBytes(Some(a)), Field::VarBytes(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Bool(Some(a)), Field::Bool(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Date(Some(a)), Field::Date(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Months(Some(a)), Field::Months(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Days(Some(a)), Field::Days(Some(b))) => {
                    assert_eq!(a, b, "Field {} mismatch", i)
                }
                (Field::Pointer(Some(a)), Field::Pointer(Some(b))) => {
                    assert_eq!(a.page_id, b.page_id, "Field {} page_id mismatch", i);
                    assert_eq!(a.frame_id, b.frame_id, "Field {} frame_id mismatch", i);
                }
                _ => panic!(
                    "Field {} type mismatch: {:?} vs {:?}",
                    i, original, recovered
                ),
            }
        }
    }

    #[test]
    fn test_normalized_key_empty_strings() {
        let fields = vec![
            Field::String(Some("".to_string())),
            Field::Int32(Some(42)),
            Field::String(Some("".to_string())),
        ];
        let key_indexes = vec![(0, true, true), (1, true, true), (2, true, true)];
        let field_types = vec![DataType::String, DataType::Int32, DataType::String];

        let normalized = to_normalized_key(&fields, &key_indexes);
        let recovered = from_normalized_key(&normalized, &key_indexes, &field_types).unwrap();

        match &recovered[0] {
            Field::String(Some(s)) => assert_eq!(s, ""),
            _ => panic!("Expected empty string"),
        }
        match &recovered[2] {
            Field::String(Some(s)) => assert_eq!(s, ""),
            _ => panic!("Expected empty string"),
        }
    }

    #[test]
    fn test_normalized_key_sort_order_preserved() {
        // Create several records with variable length fields
        let records = [
            vec![
                Field::String(Some("aaa".to_string())),
                Field::Int32(Some(1)),
            ],
            vec![
                Field::String(Some("aaa".to_string())),
                Field::Int32(Some(2)),
            ],
            vec![
                Field::String(Some("bbb".to_string())),
                Field::Int32(Some(1)),
            ],
            vec![Field::String(Some("bb".to_string())), Field::Int32(Some(1))],
            vec![Field::String(Some("c".to_string())), Field::Int32(Some(1))],
        ];

        let key_indexes = vec![(0, true, true), (1, true, true)];
        let field_types = vec![DataType::String, DataType::Int32];

        // Generate normalized keys
        let normalized_keys: Vec<_> = records
            .iter()
            .map(|fields| to_normalized_key(fields, &key_indexes))
            .collect();

        // Sort them
        let mut sorted_normalized = normalized_keys.clone();
        sorted_normalized.sort();

        // Verify the expected sort order
        // Expected order: ("aaa", 1), ("aaa", 2), ("bb", 1), ("bbb", 1), ("c", 1)
        // Note: "bb" < "bbb" in lexicographic order
        let expected_order = vec![0, 1, 3, 2, 4]; // indices into original records

        // Debug: print actual sorted order
        let mut actual_order = vec![];
        for key in sorted_normalized.iter() {
            let record_idx = normalized_keys.iter().position(|k| k == key).unwrap();
            actual_order.push(record_idx);
        }

        // If test fails, show the actual order
        if actual_order != expected_order {
            eprintln!("Expected order: {:?}", expected_order);
            eprintln!("Actual order: {:?}", actual_order);
            for (i, idx) in actual_order.iter().enumerate() {
                eprintln!(
                    "Position {} -> record {}: {:?}",
                    i,
                    idx,
                    records[*idx]
                        .iter()
                        .map(|f| format!("{:?}", f))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }

        assert_eq!(actual_order, expected_order, "Unexpected sort order");

        // Verify we can recover all fields from normalized keys
        for (fields, normalized_key) in records.iter().zip(normalized_keys.iter()) {
            let recovered =
                from_normalized_key(normalized_key, &key_indexes, &field_types).unwrap();
            for (i, &idx) in key_indexes.iter().map(|(idx, _, _)| idx).enumerate() {
                match (&recovered[i], &fields[idx]) {
                    (Field::String(Some(s1)), Field::String(Some(s2))) => assert_eq!(s1, s2),
                    (Field::Int32(Some(n1)), Field::Int32(Some(n2))) => assert_eq!(n1, n2),
                    _ => panic!("Field type mismatch at index {}", i),
                }
            }
        }
    }
}
