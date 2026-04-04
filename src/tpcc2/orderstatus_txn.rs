use std::sync::Arc;

use crate::{
    bp::{DatabaseId, MemPool},
    txn_storage2::{
        field::Field,
        field_level_storage_trait::{
            FieldLeveLStorageTrait, ScanOptions, TxnOptions, TxnStorageStatus,
        },
        transactional_storage::TransactionalStorage,
    },
};

use super::loader::TpccContainerIds;
use super::txn_helper::{not_successful, AbortID, TPCCStatus, TxHelper, TxnTypeStats};
use super::txn_utils::{customer_fields, order_fields, order_line_fields, *};

pub struct OrderStatusInput {
    pub w_id: u16,
    pub d_id: u8,
    pub c_id: Option<u32>,      // Either c_id or c_last must be provided
    pub c_last: Option<String>, // Either c_id or c_last must be provided
}

pub struct OrderStatusOutput {
    pub c_id: u32,
    pub c_first: String,
    pub c_middle: String,
    pub c_last: String,
    pub c_balance: f64,
    pub o_id: u32,
    pub o_entry_d: u64,
    pub o_carrier_id: Option<u8>,
    pub order_lines: Vec<OrderLineInfo>,
}

pub struct OrderLineInfo {
    pub ol_i_id: u32,
    pub ol_supply_w_id: u16,
    pub ol_quantity: u8,
    pub ol_amount: f64,
    pub ol_delivery_d: Option<u64>,
}

pub fn run_orderstatus_txn<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &OrderStatusInput,
) -> Result<OrderStatusOutput, TxnStorageStatus> {
    let (status, output) = run_orderstatus_txn_with_stats(storage, db_id, containers, input, None);
    match status {
        TPCCStatus::Success => Ok(output.unwrap()),
        _ => Err(TxnStorageStatus::Aborted),
    }
}

pub fn run_orderstatus_txn_with_stats<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &OrderStatusInput,
    stats: Option<&mut TxnTypeStats>,
) -> (TPCCStatus, Option<OrderStatusOutput>) {
    // Create a dummy stats if none provided
    let mut dummy_stats = TxnTypeStats::new();
    let stats = stats.unwrap_or(&mut dummy_stats);

    let mut helper = TxHelper::new(storage, db_id, stats);
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    // Find customer
    let (c_id, c_key, c_secondary_key, c_hint, c_secondary_hint) = if let Some(c_id) = input.c_id {
        // Customer specified by ID
        let key = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(input.d_id)),
            Field::Uint32(Some(c_id)),
        ];
        (c_id, key, None, None, None)
    } else if let Some(c_last) = &input.c_last {
        // Customer specified by last name - need to scan secondary index
        let scan_key_start = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(input.d_id)),
            Field::String(Some(c_last.clone())),
            Field::Uint32(Some(0)),
        ];
        let scan_key_end = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(input.d_id)),
            Field::String(Some(c_last.clone())),
            Field::Uint32(Some(u32::MAX)),
        ];

        // Scan secondary index
        let mut matching_customers = Vec::new();
        let res = storage.scan_range(
            &txn,
            containers.customer_secondary_cid,
            ScanOptions::new(&[customer_secondary_fields::C_POINTER])
                .with_bounds(scan_key_start, scan_key_end),
        );
        if not_successful(&res) {
            return (
                helper.kill(&txn, &res, AbortID::OrderStatusScanCustomerSecondary),
                None,
            );
        }
        let iter = res.unwrap();

        let fe_res =
            storage.iter_for_each_fields(&txn, &iter, &mut |key_fields, value_fields, hint| {
                let c_id = get_u32_field(key_fields, 3);
                matching_customers.push((
                    c_id,
                    key_fields.to_vec(),
                    get_pointer_field(value_fields, 0),
                    hint,
                ));
                true
            });
        let _ = storage.drop_iterator_handle(iter);
        if let Err(e) = fe_res {
            return (
                helper.kill::<()>(&txn, &Err(e), AbortID::OrderStatusScanCustomerSecondary),
                None,
            );
        }

        if matching_customers.is_empty() {
            return (
                helper.kill::<()>(
                    &txn,
                    &Err(TxnStorageStatus::KeyNotFound),
                    AbortID::OrderStatusScanCustomerSecondary,
                ),
                None,
            );
        }

        // Select middle customer (TPC-C requirement)
        matching_customers.sort_by_key(|(c_id, ..)| *c_id);
        let middle_idx = matching_customers.len() / 2;
        let selected_c = matching_customers.swap_remove(middle_idx);

        let key = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(input.d_id)),
            Field::Uint32(Some(selected_c.0)),
        ];
        (
            selected_c.0,
            key,
            Some(selected_c.1),
            Some(selected_c.2),
            Some(selected_c.3),
        )
    } else {
        return (
            helper.kill::<()>(
                &txn,
                &Err(TxnStorageStatus::AbortFailed),
                AbortID::InvalidInput,
            ),
            None,
        );
    };

    // Get customer info
    let res = storage.get_fields(
        &txn,
        containers.customer_cid,
        c_key,
        &[
            customer_fields::C_FIRST,
            customer_fields::C_MIDDLE,
            customer_fields::C_LAST,
            customer_fields::C_BALANCE,
        ],
        c_hint,
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::OrderStatusGetCustomer),
            None,
        );
    }
    let (c_fields, c_actual_hint) = res.unwrap();

    // Check if hint from secondary index is stale and update if needed
    if let (Some(c_secondary_key), Some(c_hint), Some(c_secondary_hint)) =
        (c_secondary_key, c_hint, c_secondary_hint)
    {
        if c_hint != c_actual_hint {
            // Hint is stale, update secondary index
            let res = storage.update_field(
                &txn,
                containers.customer_secondary_cid,
                c_secondary_key,
                4, // Pointer is at index 4 in the secondary index schema
                Field::Pointer(Some(c_actual_hint)),
                Some(c_secondary_hint),
            );
            if not_successful(&res) {
                return (
                    helper.kill(&txn, &res, AbortID::OrderStatusGetCustomer),
                    None,
                );
            }
        }
    }

    let c_first = get_string_field(&c_fields, 0);
    let c_middle = get_string_field(&c_fields, 1);
    let c_last = get_string_field(&c_fields, 2);
    let c_balance = get_f64_field(&c_fields, 3);

    // Find the latest order for this customer using secondary index
    let scan_start = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
        Field::Uint32(Some(c_id)),
        Field::Uint32(Some(1)),
    ];
    let scan_end = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
        Field::Uint32(Some(c_id)),
        Field::Uint32(Some(u32::MAX)),
    ];

    let res = storage.scan_range(
        &txn,
        containers.order_secondary_cid,
        ScanOptions::new(&[order_secondary_fields::O_POINTER]).with_bounds(scan_start, scan_end),
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::OrderStatusScanOrderSecondary),
            None,
        );
    }
    let iter = res.unwrap();

    let mut latest_o_id = 0u32;
    let mut latest_order_hint = None;
    let mut latest_order_secondary_key = None;
    let fe_res = storage.iter_for_each_fields(&txn, &iter, &mut |key_fields, value_fields, _| {
        let o_id = get_u32_field(key_fields, 3);
        if o_id > latest_o_id {
            latest_o_id = o_id;
            latest_order_secondary_key = Some(key_fields.to_vec());
            latest_order_hint = Some(get_pointer_field(value_fields, 0));
        }
        true
    });
    let _ = storage.drop_iterator_handle(iter);
    if let Err(e) = fe_res {
        return (
            helper.kill::<()>(&txn, &Err(e), AbortID::OrderStatusScanOrderSecondary),
            None,
        );
    }

    if latest_o_id == 0 {
        return (
            helper.kill::<()>(
                &txn,
                &Err(TxnStorageStatus::KeyNotFound),
                AbortID::OrderStatusScanOrderSecondary,
            ),
            None,
        );
    }

    // Get order details
    let o_key = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
        Field::Uint32(Some(latest_o_id)),
    ];

    let res = storage.get_fields(
        &txn,
        containers.order_cid,
        o_key,
        &[
            order_fields::O_ENTRY_D,
            order_fields::O_CARRIER_ID,
            order_fields::O_OL_CNT,
        ],
        latest_order_hint,
    );
    if not_successful(&res) {
        return (helper.kill(&txn, &res, AbortID::OrderStatusGetOrder), None);
    }
    let (o_fields, o_actual_hint) = res.unwrap();

    // Check if hint from order secondary index is stale and update if needed
    if latest_order_hint.unwrap() != o_actual_hint {
        // Hint is stale, update secondary index
        let update_res = storage.update_field(
            &txn,
            containers.order_secondary_cid,
            latest_order_secondary_key.unwrap(),
            4, // Pointer is at index 4 in the secondary index schema
            Field::Pointer(Some(o_actual_hint)),
            None,
        );
        // Log but don't fail the transaction if secondary index update fails
        if update_res.is_err() {
            eprintln!(
                "Warning: Failed to update stale secondary index pointer for order {}",
                latest_o_id
            );
        }
    }

    let o_entry_d = get_u64_field(&o_fields, 0);
    let o_carrier_id = get_optional_u8_field(&o_fields, 1);
    let o_ol_cnt = get_u8_field(&o_fields, 2);

    // Get order lines using range scan
    let mut order_lines = Vec::with_capacity(o_ol_cnt as usize);

    // Scan for all order lines of this order
    let ol_scan_start = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
        Field::Uint32(Some(latest_o_id)),
        Field::Uint8(Some(1)),
    ];
    let ol_scan_end = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
        Field::Uint32(Some(latest_o_id)),
        Field::Uint8(Some(u8::MAX)),
    ];

    let res = storage.scan_range(
        &txn,
        containers.order_line_cid,
        ScanOptions::new(&[
            order_line_fields::OL_I_ID,
            order_line_fields::OL_SUPPLY_W_ID,
            order_line_fields::OL_QUANTITY,
            order_line_fields::OL_AMOUNT,
            order_line_fields::OL_DELIVERY_D,
        ])
        .with_bounds(ol_scan_start, ol_scan_end),
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::OrderStatusGetOrderLine),
            None,
        );
    }
    let iter = res.unwrap();

    let fe_res = storage.iter_for_each_fields(&txn, &iter, &mut |_, value_fields, _| {
        let ol_i_id = get_u32_field(value_fields, 0);
        let ol_supply_w_id = get_u16_field(value_fields, 1);
        let ol_quantity = get_u8_field(value_fields, 2);
        let ol_amount = get_f64_field(value_fields, 3);
        let ol_delivery_d = get_optional_u64_field(value_fields, 4);

        order_lines.push(OrderLineInfo {
            ol_i_id,
            ol_supply_w_id,
            ol_quantity,
            ol_amount,
            ol_delivery_d,
        });
        true
    });
    let _ = storage.drop_iterator_handle(iter);
    if let Err(e) = fe_res {
        return (
            helper.kill::<()>(&txn, &Err(e), AbortID::OrderStatusGetOrderLine),
            None,
        );
    }

    // Commit transaction (read-only)
    let status = helper.commit(&txn, AbortID::OrderStatusCommit);
    if status != TPCCStatus::Success {
        return (status, None);
    }

    (
        TPCCStatus::Success,
        Some(OrderStatusOutput {
            c_id,
            c_first,
            c_middle,
            c_last,
            c_balance,
            o_id: latest_o_id,
            o_entry_d,
            o_carrier_id,
            order_lines,
        }),
    )
}
