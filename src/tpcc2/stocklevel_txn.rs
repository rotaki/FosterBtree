use std::collections::HashSet;
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
use super::txn_utils::{district_fields, order_line_fields, stock_fields, *};

pub struct StockLevelInput {
    pub w_id: u16,
    pub d_id: u8,
    pub threshold: i16,
}

pub struct StockLevelOutput {
    pub low_stock_count: usize,
}

pub fn run_stocklevel_txn<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &StockLevelInput,
) -> Result<StockLevelOutput, TxnStorageStatus> {
    let (status, output) = run_stocklevel_txn_with_stats(storage, db_id, containers, input, None);
    match status {
        TPCCStatus::Success => Ok(output.unwrap()),
        _ => Err(TxnStorageStatus::Aborted),
    }
}

pub fn run_stocklevel_txn_with_stats<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &StockLevelInput,
    stats: Option<&mut TxnTypeStats>,
) -> (TPCCStatus, Option<StockLevelOutput>) {
    // Create a dummy stats if none provided
    let mut dummy_stats = TxnTypeStats::new();
    let stats = stats.unwrap_or(&mut dummy_stats);

    let mut helper = TxHelper::new(storage, db_id, stats);
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    // Get the next order ID for this district
    let d_key = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
    ];

    let res = storage.get_fields(
        &txn,
        containers.district_cid,
        d_key,
        &[district_fields::D_NEXT_O_ID],
        None,
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::StockLevelGetDistrict),
            None,
        );
    }
    let (d_fields, _d_hint) = res.unwrap();
    let d_next_o_id = get_u32_field(&d_fields, 0);

    // Find all unique items from the last 20 orders
    let mut unique_items = HashSet::new();

    // Scan the last 20 orders (d_next_o_id - 20 to d_next_o_id - 1)
    let start_o_id = if d_next_o_id > 20 {
        d_next_o_id - 20
    } else {
        1
    };

    // Use scan to get all order lines for the last 20 orders
    let scan_start = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
        Field::Uint32(Some(start_o_id)),
        Field::Uint8(Some(1)),
    ];
    let scan_end = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
        Field::Uint32(Some(d_next_o_id)),
        Field::Uint8(Some(0)),
    ];

    let res = storage.scan_range(
        &txn,
        containers.order_line_cid,
        ScanOptions::new(&[order_line_fields::OL_I_ID]).with_bounds(scan_start, scan_end),
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::StockLevelScanOrderLine),
            None,
        );
    }
    let iter = res.unwrap();

    loop {
        match storage.iter_next(&txn, &iter) {
            Ok(Some((_, value_fields, _))) => {
                // Extract item ID from value fields
                let ol_i_id = get_u32_field(&value_fields, 0);
                unique_items.insert(ol_i_id);
            }
            Ok(None) => break,
            Err(e) => {
                let _ = storage.drop_iterator_handle(iter);
                return (
                    helper.kill::<()>(&txn, &Err(e), AbortID::StockLevelScanOrderLine),
                    None,
                );
            }
        }
    }
    let _ = storage.drop_iterator_handle(iter);

    // Count items with stock below threshold
    let mut low_stock_count = 0;

    for i_id in unique_items {
        let s_key = vec![Field::Uint16(Some(input.w_id)), Field::Uint32(Some(i_id))];

        let res = storage.get_field(
            &txn,
            containers.stock_cid,
            s_key,
            stock_fields::S_QUANTITY,
            None,
        );
        if not_successful(&res) {
            return (helper.kill(&txn, &res, AbortID::StockLevelGetStock), None);
        }
        let (s_field, _s_hint) = res.unwrap();

        let s_quantity = match s_field {
            Field::Int16(Some(v)) => v,
            _ => 0,
        };

        if s_quantity < input.threshold {
            low_stock_count += 1;
        }
    }

    // Commit transaction (read-only)
    let status = helper.commit(&txn, AbortID::StockLevelCommit);
    if status != TPCCStatus::Success {
        return (status, None);
    }

    (
        TPCCStatus::Success,
        Some(StockLevelOutput { low_stock_count }),
    )
}
