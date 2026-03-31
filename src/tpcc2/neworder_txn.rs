use std::sync::Arc;

use crate::{
    bp::{DatabaseId, MemPool},
    tpcc::record_definitions::get_timestamp,
    txn_storage2::{
        field::{Field, Record},
        field_level_storage_trait::{FieldLeveLStorageTrait, TxnOptions, TxnStorageStatus},
        transactional_storage::TransactionalStorage,
    },
};

use super::loader::PartitionMode;
use super::loader::TpccContainerIds;
use super::txn_helper::{not_successful, AbortID, TPCCStatus, TxHelper, TxnTypeStats};
use super::txn_utils::{
    customer_cold_fields, customer_fields, district_fields, item_fields, stock_fields,
    warehouse_fields, *,
};

pub struct NewOrderInput {
    pub w_id: u16,
    pub d_id: u8,
    pub c_id: u32,
    pub items: Vec<NewOrderItem>,
    pub rollback: bool, // Set to true for 1% of transactions
}

pub struct NewOrderItem {
    pub i_id: u32,
    pub supply_w_id: u16,
    pub quantity: u8,
}

pub struct NewOrderOutput {
    pub w_tax: f64,
    pub d_tax: f64,
    pub o_id: u32,
    pub c_discount: f64,
    pub c_last: String,
    pub c_credit: String,
    pub total_amount: f64,
    pub items: Vec<NewOrderItemOutput>,
}

pub struct NewOrderItemOutput {
    pub i_name: String,
    pub i_price: f64,
    pub s_quantity: i16,
    pub brand_generic: String,
    pub item_amount: f64,
}

pub fn run_neworder_txn<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &NewOrderInput,
) -> Result<NewOrderOutput, TxnStorageStatus> {
    let (status, output) = run_neworder_txn_with_stats(storage, db_id, containers, input, None);
    match status {
        TPCCStatus::Success => Ok(output.unwrap()),
        _ => Err(TxnStorageStatus::Aborted),
    }
}

pub fn run_neworder_txn_with_stats<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &NewOrderInput,
    stats: Option<&mut TxnTypeStats>,
) -> (TPCCStatus, Option<NewOrderOutput>) {
    // Create a dummy stats if none provided
    let mut dummy_stats = TxnTypeStats::new();
    let stats = stats.unwrap_or(&mut dummy_stats);

    let mut helper = TxHelper::new(storage, db_id, stats);
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    // Get warehouse tax
    let w_key = vec![Field::Uint16(Some(input.w_id))];
    let res = storage.get_fields(
        &txn,
        containers.warehouse_cid,
        w_key,
        &[warehouse_fields::W_TAX],
        None,
    );
    if not_successful(&res) {
        return (helper.kill(&txn, &res, AbortID::NewOrderGetWarehouse), None);
    }
    let (w_fields, _w_hint) = res.unwrap();
    let w_tax = get_f64_field(&w_fields, 0);

    // Get district info and increment next order id
    let d_key = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
    ];

    // Atomically read and increment d_next_o_id (acquires exclusive lock directly,
    // avoiding the shared→upgrade livelock with field-level locking)
    let mut d_next_o_id = 0u32;
    let res = storage.update_field_with_func(
        &txn,
        containers.district_cid,
        d_key.clone(),
        district_fields::D_NEXT_O_ID,
        |field| {
            if let Field::Uint32(Some(v)) = field {
                d_next_o_id = *v;
                *v += 1;
            } else {
                panic!("Expected Uint32 for D_NEXT_O_ID");
            }
        },
        None,
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::NewOrderUpdateDistrict),
            None,
        );
    }
    let o_id = d_next_o_id;

    // Read d_tax separately (shared lock on a different field — no conflict with
    // other NewOrders' exclusive on D_NEXT_O_ID under field-level locking)
    let res = storage.get_fields(
        &txn,
        containers.district_cid,
        d_key,
        &[district_fields::D_TAX],
        None,
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::NewOrderUpdateDistrict),
            None,
        );
    }
    let (d_fields, _d_hint) = res.unwrap();
    let d_tax = get_f64_field(&d_fields, 0);

    // Get customer info
    let c_key = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
        Field::Uint32(Some(input.c_id)),
    ];

    // NewOrder only needs stable customer fields (cold in all modes)
    let (disc_idx, last_idx, credit_idx) = if containers.partition_mode == PartitionMode::HotCold {
        (
            customer_cold_fields::C_DISCOUNT,
            customer_cold_fields::C_LAST,
            customer_cold_fields::C_CREDIT,
        )
    } else {
        (
            customer_fields::C_DISCOUNT,
            customer_fields::C_LAST,
            customer_fields::C_CREDIT,
        )
    };
    let res = storage.get_fields(
        &txn,
        containers.customer_cid,
        c_key,
        &[disc_idx, last_idx, credit_idx],
        None,
    );
    if not_successful(&res) {
        return (helper.kill(&txn, &res, AbortID::NewOrderGetCustomer), None);
    }
    let (c_fields, _c_hint) = res.unwrap();

    let c_discount = get_f64_field(&c_fields, 0);
    let c_last = get_string_field(&c_fields, 1);
    let c_credit = get_string_field(&c_fields, 2);

    // Check if all items are from local warehouse
    let all_local = input
        .items
        .iter()
        .all(|item| item.supply_w_id == input.w_id);

    // Create order
    let order_record = Record {
        fields: vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(input.d_id)),
            Field::Uint32(Some(o_id)),
            Field::Uint32(Some(input.c_id)),
            Field::Uint8(None), // o_carrier_id is null for new orders
            Field::Uint8(Some(input.items.len() as u8)),
            Field::Uint8(Some(if all_local { 1 } else { 0 })),
            Field::Uint64(Some(get_timestamp())),
        ],
    };
    let res = storage.insert_record(&txn, containers.order_cid, order_record, None);
    if not_successful(&res) {
        return (helper.kill(&txn, &res, AbortID::NewOrderInsertOrder), None);
    }
    let order_hint = res.unwrap();

    // Insert into order secondary index
    let order_secondary_record = Record {
        fields: vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(input.d_id)),
            Field::Uint32(Some(input.c_id)),
            Field::Uint32(Some(o_id)),
            Field::Pointer(Some(order_hint)),
        ],
    };
    let res = storage.insert_record(
        &txn,
        containers.order_secondary_cid,
        order_secondary_record,
        None,
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::NewOrderInsertOrderSecondary),
            None,
        );
    }
    let _order_secondary_hint = res.unwrap();

    // Create new order entry
    let new_order_record = Record {
        fields: vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(input.d_id)),
            Field::Uint32(Some(o_id)),
        ],
    };
    let res = storage.insert_record(&txn, containers.new_order_cid, new_order_record, None);
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::NewOrderInsertNewOrder),
            None,
        );
    }
    let _new_order_hint = res.unwrap();

    // Process order items
    let mut total_amount = 0.0;
    let mut item_outputs = Vec::new();

    for (ol_number, item) in input.items.iter().enumerate() {
        // Get item info
        let i_key = vec![Field::Uint32(Some(item.i_id))];

        // Check for rollback condition (1% of transactions with invalid item)
        if input.rollback && ol_number == input.items.len() - 1 {
            // This is the last item and we want to rollback
            // Use a non-existent item ID
            return (helper.user_abort(&txn), None);
        }

        // Check if item exists (invalid item check)
        let i_fields_result = storage.get_fields(
            &txn,
            containers.item_cid,
            i_key,
            &[
                item_fields::I_PRICE,
                item_fields::I_NAME,
                item_fields::I_DATA,
            ],
            None,
        );

        if i_fields_result.is_err() {
            // Item not found - system abort
            return (
                helper.kill(&txn, &i_fields_result, AbortID::NewOrderGetItem),
                None,
            );
        }

        let (i_fields, _i_hint) = i_fields_result.unwrap();
        let i_price = get_f64_field(&i_fields, 0);
        let i_name = get_string_field(&i_fields, 1);
        let i_data = get_string_field(&i_fields, 2);

        // Get and update stock
        let s_key = vec![
            Field::Uint16(Some(item.supply_w_id)),
            Field::Uint32(Some(item.i_id)),
        ];

        // Read stock fields
        let res = storage.get_fields(
            &txn,
            containers.stock_cid,
            s_key.clone(),
            &[
                stock_fields::S_QUANTITY,
                stock_fields::S_YTD,
                stock_fields::S_ORDER_CNT,
                stock_fields::S_REMOTE_CNT,
                stock_fields::S_DIST,
                stock_fields::S_DATA,
            ],
            None,
        );
        if not_successful(&res) {
            return (helper.kill(&txn, &res, AbortID::NewOrderGetStock), None);
        }
        let (s_fields, s_hint) = res.unwrap();

        let mut s_quantity = get_i16_field(&s_fields, 0);
        let s_ytd = get_u32_field(&s_fields, 1);
        let s_order_cnt = get_u16_field(&s_fields, 2);
        let s_remote_cnt = get_u16_field(&s_fields, 3);
        let s_dist_str = get_string_field(&s_fields, 4);
        let s_data = get_string_field(&s_fields, 5);

        // Update stock quantity
        if s_quantity >= item.quantity as i16 + 10 {
            s_quantity -= item.quantity as i16;
        } else {
            s_quantity = s_quantity - item.quantity as i16 + 91;
        }

        // Update stock fields
        let updates = vec![
            (stock_fields::S_QUANTITY, Field::Int16(Some(s_quantity))),
            (
                stock_fields::S_YTD,
                Field::Uint32(Some(s_ytd + item.quantity as u32)),
            ),
            (
                stock_fields::S_ORDER_CNT,
                Field::Uint16(Some(s_order_cnt + 1)),
            ),
        ];

        // Add remote count update if needed
        let final_updates = if item.supply_w_id != input.w_id {
            let mut u = updates;
            u.push((
                stock_fields::S_REMOTE_CNT,
                Field::Uint16(Some(s_remote_cnt + 1)),
            ));
            u
        } else {
            updates
        };

        let res = storage.update_fields(
            &txn,
            containers.stock_cid,
            s_key,
            final_updates,
            Some(s_hint),
        );
        if not_successful(&res) {
            return (helper.kill(&txn, &res, AbortID::NewOrderUpdateStock), None);
        }

        // Get district info for this item
        let dist_info = get_district_info(&s_dist_str, input.d_id);

        // Calculate item amount
        let item_amount = i_price * item.quantity as f64;
        total_amount += item_amount;

        // Create order line
        let order_line_record = Record {
            fields: vec![
                Field::Uint16(Some(input.w_id)),
                Field::Uint8(Some(input.d_id)),
                Field::Uint32(Some(o_id)),
                Field::Uint8(Some((ol_number + 1) as u8)),
                Field::Uint32(Some(item.i_id)),
                Field::Uint16(Some(item.supply_w_id)),
                Field::Uint64(None), // ol_delivery_d is null for new orders
                Field::Uint8(Some(item.quantity)),
                Field::Float64(Some(item_amount)),
                Field::String(Some(dist_info)),
            ],
        };
        let res = storage.insert_record(&txn, containers.order_line_cid, order_line_record, None);
        if not_successful(&res) {
            return (
                helper.kill(&txn, &res, AbortID::NewOrderInsertOrderLine),
                None,
            );
        }
        let _order_line_hint = res.unwrap();

        // Determine brand/generic
        let brand_generic = if i_data.contains("ORIGINAL") && s_data.contains("ORIGINAL") {
            "B".to_string()
        } else {
            "G".to_string()
        };

        item_outputs.push(NewOrderItemOutput {
            i_name,
            i_price,
            s_quantity,
            brand_generic,
            item_amount,
        });
    }

    // Calculate final total with taxes and discount
    total_amount = total_amount * (1.0 + w_tax + d_tax) * (1.0 - c_discount);

    // Commit transaction
    let status = helper.commit(&txn, AbortID::NewOrderCommit);
    if status != TPCCStatus::Success {
        return (status, None);
    }

    (
        TPCCStatus::Success,
        Some(NewOrderOutput {
            w_tax,
            d_tax,
            o_id,
            c_discount,
            c_last,
            c_credit,
            total_amount,
            items: item_outputs,
        }),
    )
}
