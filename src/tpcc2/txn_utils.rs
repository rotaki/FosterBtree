use chrono::NaiveDate;

use crate::txn_storage2::{field::Field, RecordPointer};

use super::record_definitions::string_to_address;
use crate::tpcc::record_definitions::Address;

// Helper functions for extracting fields from records

pub fn get_date_field(fields: &[Field], index: usize) -> NaiveDate {
    match &fields[index] {
        Field::Date(Some(s)) => s.clone(),
        other => panic!("Expected Date field at index {}, found {:?}", index, other),
    }
}

pub fn get_string_field(fields: &[Field], index: usize) -> String {
    match &fields[index] {
        Field::String(Some(s)) => s.clone(),
        other => panic!(
            "Expected String field at index {}, found {:?}",
            index, other
        ),
    }
}

pub fn get_u8_field(fields: &[Field], index: usize) -> u8 {
    match &fields[index] {
        Field::Uint8(Some(v)) => *v,
        other => panic!("Expected Uint8 field at index {}, found {:?}", index, other),
    }
}

pub fn get_u16_field(fields: &[Field], index: usize) -> u16 {
    match &fields[index] {
        Field::Uint16(Some(v)) => *v,
        other => panic!(
            "Expected Uint16 field at index {}, found {:?}",
            index, other
        ),
    }
}

pub fn get_u32_field(fields: &[Field], index: usize) -> u32 {
    match &fields[index] {
        Field::Uint32(Some(v)) => *v,
        other => panic!(
            "Expected Uint32 field at index {}, found {:?}",
            index, other
        ),
    }
}

pub fn get_u64_field(fields: &[Field], index: usize) -> u64 {
    match &fields[index] {
        Field::Uint64(Some(v)) => *v,
        other => panic!(
            "Expected Uint64 field at index {}, found {:?}",
            index, other
        ),
    }
}

pub fn get_i16_field(fields: &[Field], index: usize) -> i16 {
    match &fields[index] {
        Field::Int16(Some(v)) => *v,
        other => panic!("Expected Int16 field at index {}, found {:?}", index, other),
    }
}

pub fn get_i32_field(fields: &[Field], index: usize) -> i32 {
    match &fields[index] {
        Field::Int32(Some(v)) => *v,
        other => panic!("Expected Int32 field at index {}, found {:?}", index, other),
    }
}

pub fn get_f64_field(fields: &[Field], index: usize) -> f64 {
    match &fields[index] {
        Field::Float64(Some(v)) => *v,
        other => panic!(
            "Expected Float64 field at index {}, found {:?}",
            index, other
        ),
    }
}

pub fn get_f64_field_mut(fields: &mut [Field], index: usize) -> &mut f64 {
    match &mut fields[index] {
        Field::Float64(Some(v)) => v,
        other => panic!(
            "Expected Float64 field at index {}, found {:?}",
            index, other
        ),
    }
}

pub fn get_pointer_field(fields: &[Field], index: usize) -> RecordPointer {
    match &fields[index] {
        Field::Pointer(Some(ptr)) => *ptr,
        other => panic!(
            "Expected Pointer field at index {}, found {:?}",
            index, other
        ),
    }
}

pub fn get_address_field(fields: &[Field], index: usize) -> Address {
    match &fields[index] {
        Field::String(Some(s)) => string_to_address(s),
        other => panic!(
            "Expected String field for address at index {}, found {:?}",
            index, other
        ),
    }
}

pub fn get_optional_u8_field(fields: &[Field], index: usize) -> Option<u8> {
    match &fields[index] {
        Field::Uint8(v) => *v,
        other => panic!("Expected Uint8 field at index {}, found {:?}", index, other),
    }
}

pub fn get_optional_u64_field(fields: &[Field], index: usize) -> Option<u64> {
    match &fields[index] {
        Field::Uint64(v) => *v,
        other => panic!(
            "Expected Uint64 field at index {}, found {:?}",
            index, other
        ),
    }
}

// Helper to extract district info from stock s_dist field
pub fn get_district_info(s_dist_str: &str, d_id: u8) -> String {
    let parts: Vec<&str> = s_dist_str.split('|').collect();
    if d_id > 0 && (d_id as usize) <= parts.len() {
        parts[(d_id - 1) as usize].to_string()
    } else {
        String::new()
    }
}

// Helper to update district info in stock s_dist field
pub fn update_district_info(s_dist_str: &str, d_id: u8, new_info: &str) -> String {
    let mut parts: Vec<&str> = s_dist_str.split('|').collect();
    if d_id > 0 && (d_id as usize) <= parts.len() {
        parts[(d_id - 1) as usize] = new_info;
    }
    parts.join("|")
}

// Field indices for each table
pub mod warehouse_fields {
    pub const W_ID: usize = 0;
    pub const W_TAX: usize = 1;
    pub const W_YTD: usize = 2;
    pub const W_NAME: usize = 3;
    pub const W_ADDRESS: usize = 4;
}

pub mod district_fields {
    pub const D_W_ID: usize = 0;
    pub const D_ID: usize = 1;
    pub const D_NEXT_O_ID: usize = 2;
    pub const D_TAX: usize = 3;
    pub const D_YTD: usize = 4;
    pub const D_NAME: usize = 5;
    pub const D_ADDRESS: usize = 6;
}

pub mod customer_fields {
    pub const C_W_ID: usize = 0;
    pub const C_D_ID: usize = 1;
    pub const C_ID: usize = 2;
    pub const C_PAYMENT_CNT: usize = 3;
    pub const C_DELIVERY_CNT: usize = 4;
    pub const C_SINCE: usize = 5;
    pub const C_CREDIT_LIM: usize = 6;
    pub const C_DISCOUNT: usize = 7;
    pub const C_BALANCE: usize = 8;
    pub const C_YTD_PAYMENT: usize = 9;
    pub const C_FIRST: usize = 10;
    pub const C_MIDDLE: usize = 11;
    pub const C_LAST: usize = 12;
    pub const C_PHONE: usize = 13;
    pub const C_CREDIT: usize = 14;
    pub const C_DATA: usize = 15;
    pub const C_ADDRESS: usize = 16;
}

pub mod customer_secondary_fields {
    pub const C_W_ID: usize = 0;
    pub const C_D_ID: usize = 1;
    pub const C_LAST: usize = 2; // Secondary index on last name
    pub const C_ID: usize = 3; // Customer ID
    pub const C_POINTER: usize = 4; // Pointer to the customer record
}

pub mod item_fields {
    pub const I_ID: usize = 0;
    pub const I_IM_ID: usize = 1;
    pub const I_PRICE: usize = 2;
    pub const I_NAME: usize = 3;
    pub const I_DATA: usize = 4;
}

pub mod stock_fields {
    pub const S_W_ID: usize = 0;
    pub const S_I_ID: usize = 1;
    pub const S_QUANTITY: usize = 2;
    pub const S_YTD: usize = 3;
    pub const S_ORDER_CNT: usize = 4;
    pub const S_REMOTE_CNT: usize = 5;
    pub const S_DIST: usize = 6;
    pub const S_DATA: usize = 7;
}

pub mod order_fields {
    pub const O_W_ID: usize = 0;
    pub const O_D_ID: usize = 1;
    pub const O_ID: usize = 2;
    pub const O_C_ID: usize = 3;
    pub const O_CARRIER_ID: usize = 4;
    pub const O_OL_CNT: usize = 5;
    pub const O_ALL_LOCAL: usize = 6;
    pub const O_ENTRY_D: usize = 7;
}

pub mod order_secondary_fields {
    pub const O_W_ID: usize = 0;
    pub const O_D_ID: usize = 1;
    pub const O_C_ID: usize = 2; // Secondary index on customer ID
    pub const O_ID: usize = 3; // Order ID
    pub const O_POINTER: usize = 4; // Pointer to the order record
}

pub mod order_line_fields {
    pub const OL_W_ID: usize = 0;
    pub const OL_D_ID: usize = 1;
    pub const OL_O_ID: usize = 2;
    pub const OL_NUMBER: usize = 3;
    pub const OL_I_ID: usize = 4;
    pub const OL_SUPPLY_W_ID: usize = 5;
    pub const OL_DELIVERY_D: usize = 6;
    pub const OL_QUANTITY: usize = 7;
    pub const OL_AMOUNT: usize = 8;
    pub const OL_DIST_INFO: usize = 9;
}

pub mod new_order_fields {
    pub const NO_W_ID: usize = 0;
    pub const NO_D_ID: usize = 1;
    pub const NO_O_ID: usize = 2;
}

pub mod history_fields {
    pub const H_C_W_ID: usize = 0;
    pub const H_C_D_ID: usize = 1;
    pub const H_C_ID: usize = 2;
    pub const H_W_ID: usize = 3;
    pub const H_D_ID: usize = 4;
    pub const H_DATE: usize = 5;
    pub const H_AMOUNT: usize = 6;
    pub const H_DATA: usize = 7;
    pub const H_ID: usize = 8;
}
