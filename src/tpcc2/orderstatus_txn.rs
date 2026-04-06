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
    let (c_id, c_first, c_middle, c_last, c_balance) = if let Some(c_id) = input.c_id {
        // Customer specified by ID
        let key = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(input.d_id)),
            Field::Uint32(Some(c_id)),
        ];
        let res = storage.get_fields(
            &txn,
            containers.customer_cid,
            key,
            &[
                customer_fields::C_FIRST,
                customer_fields::C_MIDDLE,
                customer_fields::C_LAST,
                customer_fields::C_BALANCE,
            ],
            None,
        );
        if not_successful(&res) {
            return (
                helper.kill(&txn, &res, AbortID::OrderStatusGetCustomer),
                None,
            );
        }
        let (c_fields, _) = res.unwrap();
        (
            c_id,
            get_string_field(&c_fields, 0),
            get_string_field(&c_fields, 1),
            get_string_field(&c_fields, 2),
            get_f64_field(&c_fields, 3),
        )
    } else if let Some(c_last) = &input.c_last {
        // Customer specified by last name - scan secondary index
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

        // Scan secondary index - projects primary customer fields directly
        let mut customer_recs: Vec<(u32, String, String, String, f64)> = Vec::new();
        let res = storage.scan_range(
            &txn,
            containers.customer_secondary_cid,
            ScanOptions::new(&[
                customer_fields::C_FIRST,
                customer_fields::C_MIDDLE,
                customer_fields::C_LAST,
                customer_fields::C_BALANCE,
            ])
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
            storage.iter_for_each_fields(&txn, &iter, &mut |key_fields, value_fields, _| {
                let c_id = get_u32_field(key_fields, 3);
                customer_recs.push((
                    c_id,
                    get_string_field(value_fields, 0),
                    get_string_field(value_fields, 1),
                    get_string_field(value_fields, 2),
                    get_f64_field(value_fields, 3),
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

        if customer_recs.is_empty() {
            return (
                helper.kill::<()>(
                    &txn,
                    &Err(TxnStorageStatus::KeyNotFound),
                    AbortID::OrderStatusScanCustomerSecondary,
                ),
                None,
            );
        }

        // Sort by c_first and select the middle customer (TPC-C requirement)
        customer_recs.sort_by(|a, b| a.1.cmp(&b.1));
        let middle_idx = customer_recs.len().div_ceil(2) - 1;
        customer_recs.swap_remove(middle_idx)
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
        ScanOptions::new(&[
            order_fields::O_ENTRY_D,
            order_fields::O_CARRIER_ID,
            order_fields::O_OL_CNT,
        ])
        .with_bounds(scan_start, scan_end),
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::OrderStatusScanOrderSecondary),
            None,
        );
    }
    let iter = res.unwrap();

    let mut latest_o_id = 0u32;
    let mut latest_o_entry_d = 0u64;
    let mut latest_o_carrier_id = None;
    let mut latest_o_ol_cnt = 0u8;
    let fe_res = storage.iter_for_each_fields(&txn, &iter, &mut |key_fields, value_fields, _| {
        let o_id = get_u32_field(key_fields, 3);
        if o_id > latest_o_id {
            latest_o_id = o_id;
            latest_o_entry_d = get_u64_field(value_fields, 0);
            latest_o_carrier_id = get_optional_u8_field(value_fields, 1);
            latest_o_ol_cnt = get_u8_field(value_fields, 2);
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

    let o_entry_d = latest_o_entry_d;
    let o_carrier_id = latest_o_carrier_id;
    let o_ol_cnt = latest_o_ol_cnt;

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
