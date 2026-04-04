use std::sync::Arc;

use crate::{
    bp::{DatabaseId, MemPool},
    tpcc::record_definitions::get_timestamp,
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
use super::txn_utils::{order_fields, order_line_fields, *};

pub struct DeliveryInput {
    pub w_id: u16,
    pub o_carrier_id: u8,
}

pub struct DeliveryOutput {
    pub delivered_orders: Vec<DeliveredOrder>,
}

pub struct DeliveredOrder {
    pub d_id: u8,
    pub o_id: u32,
}

pub fn run_delivery_txn<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &DeliveryInput,
) -> Result<DeliveryOutput, TxnStorageStatus> {
    let (status, output) = run_delivery_txn_with_stats(storage, db_id, containers, input, None);
    match status {
        TPCCStatus::Success => Ok(output.unwrap()),
        _ => Err(TxnStorageStatus::Aborted),
    }
}

pub fn run_delivery_txn_with_stats<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &DeliveryInput,
    stats: Option<&mut TxnTypeStats>,
) -> (TPCCStatus, Option<DeliveryOutput>) {
    // Create a dummy stats if none provided
    let mut dummy_stats = TxnTypeStats::new();
    let stats = stats.unwrap_or(&mut dummy_stats);

    let mut helper = TxHelper::new(storage, db_id, stats);
    let txn = match storage.begin_txn(db_id, TxnOptions::default()) {
        Ok(txn) => txn,
        Err(_) => return (TPCCStatus::SystemAbort, None),
    };

    let mut delivered_orders = Vec::new();
    let delivery_date = get_timestamp();

    // Process each district
    for d_id in 1..=10 {
        // Find the oldest new order for this district
        let scan_start = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(d_id)),
            Field::Uint32(Some(1)),
        ];
        let scan_end = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(d_id)),
            Field::Uint32(Some(u32::MAX)),
        ];

        let res = storage.scan_range(
            &txn,
            containers.new_order_cid,
            ScanOptions::new(&[]).with_bounds(scan_start, scan_end),
        );
        if not_successful(&res) {
            return (helper.kill(&txn, &res, AbortID::DeliveryScanNewOrder), None);
        }
        let iter = res.unwrap();

        let mut oldest_no_o_id = None;
        let fe_res = storage.iter_for_each_fields(&txn, &iter, &mut |key_fields, _, _| {
            let o_id = get_u32_field(key_fields, 2);
            oldest_no_o_id = Some(o_id);
            false // only need the first (oldest) entry
        });
        let _ = storage.drop_iterator_handle(iter);
        if let Err(e) = fe_res {
            return (
                helper.kill::<()>(&txn, &Err(e), AbortID::DeliveryScanNewOrder),
                None,
            );
        }

        // If no new order found, skip this district
        let o_id = match oldest_no_o_id {
            Some(id) => id,
            None => continue,
        };

        // Delete the new order
        let no_key = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(d_id)),
            Field::Uint32(Some(o_id)),
        ];
        let res = storage.delete_record(&txn, containers.new_order_cid, no_key, None);
        if not_successful(&res) {
            return (
                helper.kill(&txn, &res, AbortID::DeliveryDeleteNewOrder),
                None,
            );
        }

        // Get the order and update carrier ID
        let o_key = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(d_id)),
            Field::Uint32(Some(o_id)),
        ];

        // Get customer ID and order line count
        let res = storage.get_fields(
            &txn,
            containers.order_cid,
            o_key.clone(),
            &[order_fields::O_C_ID],
            None,
        );
        if not_successful(&res) {
            return (helper.kill(&txn, &res, AbortID::DeliveryGetOrder), None);
        }
        let (o_fields, o_hint) = res.unwrap();

        let o_c_id = get_u32_field(&o_fields, 0);

        // Update order with carrier ID
        let res = storage.update_field(
            &txn,
            containers.order_cid,
            o_key,
            order_fields::O_CARRIER_ID,
            Field::Uint8(Some(input.o_carrier_id)),
            Some(o_hint),
        );
        if not_successful(&res) {
            return (helper.kill(&txn, &res, AbortID::DeliveryUpdateOrder), None);
        }

        // Update order lines and calculate total amount using range scan
        let mut total_amount = 0.0;

        // Scan for all order lines of this order
        let ol_scan_start = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(d_id)),
            Field::Uint32(Some(o_id)),
            Field::Uint8(Some(1)),
        ];
        let ol_scan_end = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(d_id)),
            Field::Uint32(Some(o_id)),
            Field::Uint8(Some(u8::MAX)),
        ];

        let res = storage.scan_range(
            &txn,
            containers.order_line_cid,
            ScanOptions::new(&[order_line_fields::OL_AMOUNT])
                .with_bounds(ol_scan_start, ol_scan_end),
        );
        if not_successful(&res) {
            return (helper.kill(&txn, &res, AbortID::DeliveryGetOrderLine), None);
        }
        let iter = res.unwrap();

        // First pass: collect order line keys, amounts, and hints
        let mut order_line_updates = Vec::new();
        let fe_res =
            storage.iter_for_each_fields(&txn, &iter, &mut |key_fields, value_fields, hint| {
                let ol_amount = get_f64_field(value_fields, 0);
                total_amount += ol_amount;
                order_line_updates.push((key_fields.to_vec(), hint));
                true
            });
        let _ = storage.drop_iterator_handle(iter);
        if let Err(e) = fe_res {
            return (
                helper.kill::<()>(&txn, &Err(e), AbortID::DeliveryGetOrderLine),
                None,
            );
        }

        // Second pass: update delivery dates
        for (ol_key, ol_hint) in order_line_updates {
            let res = storage.update_field(
                &txn,
                containers.order_line_cid,
                ol_key,
                order_line_fields::OL_DELIVERY_D,
                Field::Uint64(Some(delivery_date)),
                Some(ol_hint),
            );
            if not_successful(&res) {
                return (
                    helper.kill(&txn, &res, AbortID::DeliveryUpdateOrderLine),
                    None,
                );
            }
        }

        // Update customer balance and delivery count
        let c_key = vec![
            Field::Uint16(Some(input.w_id)),
            Field::Uint8(Some(d_id)),
            Field::Uint32(Some(o_c_id)),
        ];

        let res = storage.update_field_with_func(
            &txn,
            containers.customer_cid,
            c_key.clone(),
            customer_fields::C_BALANCE,
            |field| {
                if let Field::Float64(Some(balance)) = field {
                    *balance += total_amount;
                } else {
                    panic!("Expected C_BALANCE to be Float64");
                }
            },
            None,
        );
        if not_successful(&res) {
            return (
                helper.kill(&txn, &res, AbortID::DeliveryUpdateCustomer),
                None,
            );
        }

        let res = storage.update_field_with_func(
            &txn,
            containers.customer_cid,
            c_key.clone(),
            customer_fields::C_DELIVERY_CNT,
            |field| {
                if let Field::Uint16(Some(cnt)) = field {
                    *cnt += 1;
                } else {
                    panic!("Expected C_DELIVERY_CNT to be Uint16");
                }
            },
            None,
        );
        if not_successful(&res) {
            return (
                helper.kill(&txn, &res, AbortID::DeliveryUpdateCustomer),
                None,
            );
        }

        delivered_orders.push(DeliveredOrder { d_id, o_id });
    }

    // Commit transaction
    let status = helper.commit(&txn, AbortID::DeliveryCommit);
    if status != TPCCStatus::Success {
        return (status, None);
    }

    (
        TPCCStatus::Success,
        Some(DeliveryOutput { delivered_orders }),
    )
}
