use crate::txn_storage2::{
    field::{DataType, Field, Record},
    schema::Schema,
};

// Import types from original tpcc
use crate::tpcc::record_definitions::Address;

// Schema definitions for each table
pub fn item_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint32),  // i_id (primary key)
            (false, DataType::Uint32),  // i_im_id
            (false, DataType::Float64), // i_price
            (false, DataType::String),  // i_name
            (false, DataType::String),  // i_data
        ],
        vec![0], // i_id is primary key
    )
}

pub fn warehouse_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16),  // w_id (primary key)
            (false, DataType::Float64), // w_tax
            (false, DataType::Float64), // w_ytd
            (false, DataType::String),  // w_name
            (false, DataType::String),  // w_address (serialized)
        ],
        vec![0], // w_id is primary key
    )
}

pub fn stock_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16), // s_w_id (part of primary key)
            (false, DataType::Uint32), // s_i_id (part of primary key)
            (false, DataType::Int16),  // s_quantity
            (false, DataType::Uint32), // s_ytd
            (false, DataType::Uint16), // s_order_cnt
            (false, DataType::Uint16), // s_remote_cnt
            (false, DataType::String), // s_dist (serialized array)
            (false, DataType::String), // s_data
        ],
        vec![0, 1], // (s_w_id, s_i_id) is primary key
    )
}

pub fn district_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16),  // d_w_id (part of primary key)
            (false, DataType::Uint8),   // d_id (part of primary key)
            (false, DataType::Uint32),  // d_next_o_id
            (false, DataType::Float64), // d_tax
            (false, DataType::Float64), // d_ytd
            (false, DataType::String),  // d_name
            (false, DataType::String),  // d_address (serialized)
        ],
        vec![0, 1], // (d_w_id, d_id) is primary key
    )
}

pub fn customer_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16),  // c_w_id (part of primary key)
            (false, DataType::Uint8),   // c_d_id (part of primary key)
            (false, DataType::Uint32),  // c_id (part of primary key)
            (false, DataType::Uint16),  // c_payment_cnt
            (false, DataType::Uint16),  // c_delivery_cnt
            (false, DataType::Uint64),  // c_since
            (false, DataType::Float64), // c_credit_lim
            (false, DataType::Float64), // c_discount
            (false, DataType::Float64), // c_balance
            (false, DataType::Float64), // c_ytd_payment
            (false, DataType::String),  // c_first
            (false, DataType::String),  // c_middle
            (false, DataType::String),  // c_last
            (false, DataType::String),  // c_phone
            (false, DataType::String),  // c_credit
            (false, DataType::String),  // c_data
            (false, DataType::String),  // c_address (serialized)
        ],
        vec![0, 1, 2], // (c_w_id, c_d_id, c_id) is primary key
    )
}

pub fn customer_secondary_schema() -> Schema {
    // Secondary index on (w_id, d_id, c_last) - non-unique
    // Stores primary key (w_id, d_id, c_id) and pointer to primary record
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16),  // w_id (part of secondary key)
            (false, DataType::Uint8),   // d_id (part of secondary key)
            (false, DataType::String),  // c_last (part of secondary key - non-unique)
            (false, DataType::Uint32),  // c_id (primary key for lookup - unique)
            (false, DataType::Pointer), // pointer to primary record
        ],
        vec![0, 1, 2, 3], // Secondary key indices: (w_id, d_id, c_last, c_id)
    )
}

pub fn history_schema() -> Schema {
    // History doesn't have a primary key, we'll use a composite key
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16),  // h_c_w_id
            (false, DataType::Uint8),   // h_c_d_id
            (false, DataType::Uint32),  // h_c_id
            (false, DataType::Uint16),  // h_w_id
            (false, DataType::Uint8),   // h_d_id
            (false, DataType::Uint64),  // h_date
            (false, DataType::Float64), // h_amount
            (false, DataType::String),  // h_data
            (false, DataType::Uint64),  // h_id (artificial primary key for uniqueness)
        ],
        vec![8], // h_id is primary key
    )
}

pub fn order_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16), // o_w_id (part of primary key)
            (false, DataType::Uint8),  // o_d_id (part of primary key)
            (false, DataType::Uint32), // o_id (part of primary key)
            (false, DataType::Uint32), // o_c_id
            (true, DataType::Uint8),   // o_carrier_id (nullable)
            (false, DataType::Uint8),  // o_ol_cnt
            (false, DataType::Uint8),  // o_all_local
            (false, DataType::Uint64), // o_entry_d
        ],
        vec![0, 1, 2], // (o_w_id, o_d_id, o_id) is primary key
    )
}

pub fn order_secondary_schema() -> Schema {
    // Secondary index on (w_id, d_id, c_id) - non-unique (multiple orders per customer)
    // Stores primary key (o_id) and pointer to primary record
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16),  // w_id (part of secondary key)
            (false, DataType::Uint8),   // d_id (part of secondary key)
            (false, DataType::Uint32),  // c_id (part of secondary key - non-unique)
            (false, DataType::Uint32),  // o_id (primary key for lookup - unique)
            (false, DataType::Pointer), // pointer to primary record
        ],
        vec![0, 1, 2, 3], // Secondary key indices: (w_id, d_id, c_id, o_id)
    )
}

pub fn new_order_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16), // no_w_id (part of primary key)
            (false, DataType::Uint8),  // no_d_id (part of primary key)
            (false, DataType::Uint32), // no_o_id (part of primary key)
        ],
        vec![0, 1, 2], // (no_w_id, no_d_id, no_o_id) is primary key
    )
}

pub fn order_line_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16),  // ol_w_id (part of primary key)
            (false, DataType::Uint8),   // ol_d_id (part of primary key)
            (false, DataType::Uint32),  // ol_o_id (part of primary key)
            (false, DataType::Uint8),   // ol_number (part of primary key)
            (false, DataType::Uint32),  // ol_i_id
            (false, DataType::Uint16),  // ol_supply_w_id
            (true, DataType::Uint64),   // ol_delivery_d (nullable)
            (false, DataType::Uint8),   // ol_quantity
            (false, DataType::Float64), // ol_amount
            (false, DataType::String),  // ol_dist_info
        ],
        vec![0, 1, 2, 3], // (ol_w_id, ol_d_id, ol_o_id, ol_number) is primary key
    )
}

// Helper functions to convert between original structs and Field-based records

pub fn address_to_string(addr: &Address) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        String::from_utf8_lossy(&addr.street_1).trim_end_matches('\0'),
        String::from_utf8_lossy(&addr.street_2).trim_end_matches('\0'),
        String::from_utf8_lossy(&addr.city).trim_end_matches('\0'),
        String::from_utf8_lossy(&addr.state).trim_end_matches('\0'),
        String::from_utf8_lossy(&addr.zip).trim_end_matches('\0')
    )
}

pub fn string_to_address(s: &str) -> Address {
    let parts: Vec<&str> = s.split('|').collect();
    let mut addr = Address::new();
    if parts.len() >= 5 {
        let len = parts[0].len().min(Address::MAX_STREET);
        addr.street_1[..len].copy_from_slice(parts[0].as_bytes());
        let len = parts[1].len().min(Address::MAX_STREET);
        addr.street_2[..len].copy_from_slice(parts[1].as_bytes());
        let len = parts[2].len().min(Address::MAX_CITY);
        addr.city[..len].copy_from_slice(parts[2].as_bytes());
        let len = parts[3].len().min(Address::STATE);
        addr.state[..len].copy_from_slice(parts[3].as_bytes());
        let len = parts[4].len().min(Address::ZIP);
        addr.zip[..len].copy_from_slice(parts[4].as_bytes());
    }
    addr
}

// Conversion functions for Item
pub fn item_to_record(item: &crate::tpcc::Item) -> Record {
    Record {
        fields: vec![
            Field::Uint32(Some(item.i_id)),
            Field::Uint32(Some(item.i_im_id)),
            Field::Float64(Some(item.i_price)),
            Field::String(Some(
                String::from_utf8_lossy(&item.i_name)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::String(Some(
                String::from_utf8_lossy(&item.i_data)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
        ],
    }
}

// Conversion functions for Warehouse
pub fn warehouse_to_record(warehouse: &crate::tpcc::Warehouse) -> Record {
    Record {
        fields: vec![
            Field::Uint16(Some(warehouse.w_id)),
            Field::Float64(Some(warehouse.w_tax)),
            Field::Float64(Some(warehouse.w_ytd)),
            Field::String(Some(
                String::from_utf8_lossy(&warehouse.w_name)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::String(Some(address_to_string(&warehouse.w_address))),
        ],
    }
}

// Conversion functions for Stock
pub fn stock_to_record(stock: &crate::tpcc::Stock) -> Record {
    // Serialize s_dist array as a string
    let mut dist_str = String::new();
    for i in 0..10 {
        if i > 0 {
            dist_str.push('|');
        }
        dist_str.push_str(String::from_utf8_lossy(&stock.s_dist[i]).trim_end_matches('\0'));
    }

    Record {
        fields: vec![
            Field::Uint16(Some(stock.s_w_id)),
            Field::Uint32(Some(stock.s_i_id)),
            Field::Int16(Some(stock.s_quantity)),
            Field::Uint32(Some(stock.s_ytd)),
            Field::Uint16(Some(stock.s_order_cnt)),
            Field::Uint16(Some(stock.s_remote_cnt)),
            Field::String(Some(dist_str)),
            Field::String(Some(
                String::from_utf8_lossy(&stock.s_data)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
        ],
    }
}

// Conversion functions for District
pub fn district_to_record(district: &crate::tpcc::District) -> Record {
    Record {
        fields: vec![
            Field::Uint16(Some(district.d_w_id)),
            Field::Uint8(Some(district.d_id)),
            Field::Uint32(Some(district.d_next_o_id)),
            Field::Float64(Some(district.d_tax)),
            Field::Float64(Some(district.d_ytd)),
            Field::String(Some(
                String::from_utf8_lossy(&district.d_name)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::String(Some(address_to_string(&district.d_address))),
        ],
    }
}

// Conversion functions for Customer
pub fn customer_to_record(customer: &crate::tpcc::Customer) -> Record {
    Record {
        fields: vec![
            Field::Uint16(Some(customer.c_w_id)),
            Field::Uint8(Some(customer.c_d_id)),
            Field::Uint32(Some(customer.c_id)),
            Field::Uint16(Some(customer.c_payment_cnt)),
            Field::Uint16(Some(customer.c_delivery_cnt)),
            Field::Uint64(Some(customer.c_since)),
            Field::Float64(Some(customer.c_credit_lim)),
            Field::Float64(Some(customer.c_discount)),
            Field::Float64(Some(customer.c_balance)),
            Field::Float64(Some(customer.c_ytd_payment)),
            Field::String(Some(
                String::from_utf8_lossy(&customer.c_first)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::String(Some(
                String::from_utf8_lossy(&customer.c_middle)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::String(Some(
                String::from_utf8_lossy(&customer.c_last)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::String(Some(
                String::from_utf8_lossy(&customer.c_phone)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::String(Some(
                String::from_utf8_lossy(&customer.c_credit)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::String(Some(
                String::from_utf8_lossy(&customer.c_data)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::String(Some(address_to_string(&customer.c_address))),
        ],
    }
}

// Conversion functions for History
pub fn history_to_record(history: &crate::tpcc::History, h_id: u64) -> Record {
    Record {
        fields: vec![
            Field::Uint16(Some(history.h_c_w_id)),
            Field::Uint8(Some(history.h_c_d_id)),
            Field::Uint32(Some(history.h_c_id)),
            Field::Uint16(Some(history.h_w_id)),
            Field::Uint8(Some(history.h_d_id)),
            Field::Uint64(Some(history.h_date)),
            Field::Float64(Some(history.h_amount)),
            Field::String(Some(
                String::from_utf8_lossy(&history.h_data)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
            Field::Uint64(Some(h_id)),
        ],
    }
}

// Conversion functions for Order
pub fn order_to_record(order: &crate::tpcc::Order) -> Record {
    Record {
        fields: vec![
            Field::Uint16(Some(order.o_w_id)),
            Field::Uint8(Some(order.o_d_id)),
            Field::Uint32(Some(order.o_id)),
            Field::Uint32(Some(order.o_c_id)),
            if order.o_carrier_id == 0 {
                Field::Uint8(None)
            } else {
                Field::Uint8(Some(order.o_carrier_id))
            },
            Field::Uint8(Some(order.o_ol_cnt)),
            Field::Uint8(Some(order.o_all_local)),
            Field::Uint64(Some(order.o_entry_d)),
        ],
    }
}

// Conversion functions for NewOrder
pub fn new_order_to_record(new_order: &crate::tpcc::NewOrder) -> Record {
    Record {
        fields: vec![
            Field::Uint16(Some(new_order.no_w_id)),
            Field::Uint8(Some(new_order.no_d_id)),
            Field::Uint32(Some(new_order.no_o_id)),
        ],
    }
}

// Conversion functions for OrderLine
pub fn order_line_to_record(order_line: &crate::tpcc::OrderLine) -> Record {
    Record {
        fields: vec![
            Field::Uint16(Some(order_line.ol_w_id)),
            Field::Uint8(Some(order_line.ol_d_id)),
            Field::Uint32(Some(order_line.ol_o_id)),
            Field::Uint8(Some(order_line.ol_number)),
            Field::Uint32(Some(order_line.ol_i_id)),
            Field::Uint16(Some(order_line.ol_supply_w_id)),
            if order_line.ol_delivery_d == 0 {
                Field::Uint64(None)
            } else {
                Field::Uint64(Some(order_line.ol_delivery_d))
            },
            Field::Uint8(Some(order_line.ol_quantity)),
            Field::Float64(Some(order_line.ol_amount)),
            Field::String(Some(
                String::from_utf8_lossy(&order_line.ol_dist_info)
                    .trim_end_matches('\0')
                    .to_string(),
            )),
        ],
    }
}

// Container names
pub const ITEM_TABLE: &str = "item";
pub const WAREHOUSE_TABLE: &str = "warehouse";
pub const STOCK_TABLE: &str = "stock";
pub const DISTRICT_TABLE: &str = "district";
pub const CUSTOMER_TABLE: &str = "customer";
pub const CUSTOMER_SECONDARY_TABLE: &str = "customer_secondary";
pub const HISTORY_TABLE: &str = "history";
pub const ORDER_TABLE: &str = "order";
pub const ORDER_SECONDARY_TABLE: &str = "order_secondary";
pub const NEW_ORDER_TABLE: &str = "new_order";
pub const ORDER_LINE_TABLE: &str = "order_line";
