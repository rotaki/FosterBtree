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
use super::record_definitions::*;
use super::txn_helper::{not_successful, AbortID, TPCCStatus, TxHelper, TxnTypeStats};
use super::txn_utils::*;

pub struct PaymentInput {
    pub w_id: u16,
    pub d_id: u8,
    pub c_w_id: u16,
    pub c_d_id: u8,
    pub c_id: Option<u32>,      // Either c_id or c_last must be provided
    pub c_last: Option<String>, // Either c_id or c_last must be provided
    pub h_amount: f64,
}

pub struct PaymentOutput {
    pub c_id: u32,
    pub c_first: String,
    pub c_middle: String,
    pub c_last: String,
    pub c_street_1: String,
    pub c_street_2: String,
    pub c_city: String,
    pub c_state: String,
    pub c_zip: String,
    pub c_phone: String,
    pub c_since: u64,
    pub c_credit: String,
    pub c_credit_lim: f64,
    pub c_discount: f64,
    pub c_balance: f64,
    pub c_data: Option<String>, // Only if credit is "BC"
}

pub fn run_payment_txn<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &PaymentInput,
) -> Result<PaymentOutput, TxnStorageStatus> {
    let (status, output) = run_payment_txn_with_stats(storage, db_id, containers, input, None);
    match status {
        TPCCStatus::Success => Ok(output.unwrap()),
        _ => Err(TxnStorageStatus::Aborted),
    }
}

pub fn run_payment_txn_with_stats<M: MemPool>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    input: &PaymentInput,
    stats: Option<&mut TxnTypeStats>,
) -> (TPCCStatus, Option<PaymentOutput>) {
    // Create a dummy stats if none provided
    let mut dummy_stats = TxnTypeStats::new();
    let stats = stats.unwrap_or(&mut dummy_stats);

    let mut helper = TxHelper::new(storage, db_id, stats);
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    // Update warehouse year-to-date
    let w_key = vec![Field::Uint16(Some(input.w_id))];
    let res = storage.update_field_with_func(
        &txn,
        containers.warehouse_cid,
        w_key.clone(),
        warehouse_fields::W_YTD,
        |field| {
            if let Field::Float64(Some(ytd)) = field {
                *ytd += input.h_amount;
            } else {
                panic!("Expected Float64 field for W_YTD, found {:?}", field);
            }
        },
        None,
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::PaymentUpdateWarehouse),
            None,
        );
    }

    // Get warehouse info for history
    let res = storage.get_fields(
        &txn,
        containers.warehouse_cid,
        w_key,
        &[warehouse_fields::W_NAME, warehouse_fields::W_ADDRESS],
        None,
    );
    if not_successful(&res) {
        return (helper.kill(&txn, &res, AbortID::PaymentGetWarehouse), None);
    }
    let (w_fields, _w_hint) = res.unwrap();
    let _w_name = get_string_field(&w_fields, 0);
    let _w_address = string_to_address(&get_string_field(&w_fields, 1));

    // Update district year-to-date
    let d_key = vec![
        Field::Uint16(Some(input.w_id)),
        Field::Uint8(Some(input.d_id)),
    ];
    let res = storage.update_field_with_func(
        &txn,
        containers.district_cid,
        d_key.clone(),
        district_fields::D_YTD,
        |field| {
            if let Field::Float64(Some(ytd)) = field {
                *ytd += input.h_amount;
            } else {
                panic!("Expected Float64 field for D_YTD, found {:?}", field);
            }
        },
        None,
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::PaymentUpdateDistrict),
            None,
        );
    }

    // Get district info for history
    let res = storage.get_fields(
        &txn,
        containers.district_cid,
        d_key,
        &[district_fields::D_NAME, district_fields::D_ADDRESS],
        None,
    );
    if not_successful(&res) {
        return (helper.kill(&txn, &res, AbortID::PaymentGetDistrict), None);
    }
    let (d_fields, _d_hint) = res.unwrap();
    let _d_name = get_string_field(&d_fields, 0);
    let _d_address = string_to_address(&get_string_field(&d_fields, 1));

    // Find customer
    let c_id = if let Some(c_id) = input.c_id {
        c_id
    } else if let Some(c_last) = &input.c_last {
        // Customer specified by last name - scan secondary index
        let scan_key_start = vec![
            Field::Uint16(Some(input.c_w_id)),
            Field::Uint8(Some(input.c_d_id)),
            Field::String(Some(c_last.clone())),
            Field::Uint32(Some(0)),
        ];
        let scan_key_end = vec![
            Field::Uint16(Some(input.c_w_id)),
            Field::Uint8(Some(input.c_d_id)),
            Field::String(Some(c_last.clone())),
            Field::Uint32(Some(u32::MAX)),
        ];

        // Scan secondary index - project C_FIRST from primary for sorting
        let mut customer_recs: Vec<(u32, String)> = Vec::new();
        let res = storage.scan_range(
            &txn,
            containers.customer_secondary_cid,
            ScanOptions::new(&[customer_fields::C_ID, customer_fields::C_FIRST])
                .with_bounds(scan_key_start, scan_key_end),
        );
        if not_successful(&res) {
            return (
                helper.kill(&txn, &res, AbortID::PaymentScanCustomerSecondary),
                None,
            );
        }
        let iter = res.unwrap();

        let fe_res = storage.iter_for_each_fields(&txn, &iter, &mut |fields, _| {
            let c_id = get_u32_field(fields, 0);
            let c_first = get_string_field(fields, 1);
            customer_recs.push((c_id, c_first));
            true
        });
        let _ = storage.drop_iterator_handle(iter);
        if let Err(e) = fe_res {
            return (
                helper.kill::<()>(&txn, &Err(e), AbortID::PaymentScanCustomerSecondary),
                None,
            );
        }

        if customer_recs.is_empty() {
            return (
                helper.kill::<()>(
                    &txn,
                    &Err(TxnStorageStatus::KeyNotFound),
                    AbortID::PaymentScanCustomerSecondary,
                ),
                None,
            );
        }

        // Sort by c_first and select the middle customer (TPC-C requirement)
        customer_recs.sort_by(|a, b| a.1.cmp(&b.1));
        let middle_idx = customer_recs.len().div_ceil(2) - 1;
        customer_recs.swap_remove(middle_idx).0
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

    let c_key = vec![
        Field::Uint16(Some(input.c_w_id)),
        Field::Uint8(Some(input.c_d_id)),
        Field::Uint32(Some(c_id)),
    ];

    // Get customer info
    let res = storage.get_fields(
        &txn,
        containers.customer_cid,
        c_key.clone(),
        &[
            customer_fields::C_FIRST,
            customer_fields::C_MIDDLE,
            customer_fields::C_LAST,
            customer_fields::C_PHONE,
            customer_fields::C_SINCE,
            customer_fields::C_CREDIT,
            customer_fields::C_CREDIT_LIM,
            customer_fields::C_DISCOUNT,
            customer_fields::C_BALANCE,
            customer_fields::C_YTD_PAYMENT,
            customer_fields::C_PAYMENT_CNT,
            customer_fields::C_DATA,
            customer_fields::C_ADDRESS,
        ],
        None,
    );
    if not_successful(&res) {
        return (helper.kill(&txn, &res, AbortID::PaymentGetCustomer), None);
    }
    let (c_all_fields, c_actual_hint) = res.unwrap();

    let c_first = get_string_field(&c_all_fields, 0);
    let c_middle = get_string_field(&c_all_fields, 1);
    let c_last = get_string_field(&c_all_fields, 2);
    let c_phone = get_string_field(&c_all_fields, 3);
    let c_since = get_u64_field(&c_all_fields, 4);
    let c_credit = get_string_field(&c_all_fields, 5);
    let c_credit_lim = get_f64_field(&c_all_fields, 6);
    let c_discount = get_f64_field(&c_all_fields, 7);
    let c_balance = get_f64_field(&c_all_fields, 8);
    let c_ytd_payment = get_f64_field(&c_all_fields, 9);
    let c_payment_cnt = get_u16_field(&c_all_fields, 10);
    let c_data = get_string_field(&c_all_fields, 11);
    let c_address = string_to_address(&get_string_field(&c_all_fields, 12));

    // Update customer payment info
    let mut updates = vec![
        (
            customer_fields::C_BALANCE,
            Field::Float64(Some(c_balance - input.h_amount)),
        ),
        (
            customer_fields::C_YTD_PAYMENT,
            Field::Float64(Some(c_ytd_payment + input.h_amount)),
        ),
        (
            customer_fields::C_PAYMENT_CNT,
            Field::Uint16(Some(c_payment_cnt + 1)),
        ),
    ];

    // Handle bad credit customers
    let c_data_output = if c_credit == "BC" {
        // Construct new c_data
        let new_data = format!(
            "{} {} {} {} {} {} | {}",
            c_id,
            input.c_d_id,
            input.c_w_id,
            input.d_id,
            input.w_id,
            input.h_amount,
            if c_data.len() > 450 {
                &c_data[..450]
            } else {
                &c_data
            }
        );

        updates.push((
            customer_fields::C_DATA,
            Field::String(Some(new_data.clone())),
        ));
        Some(new_data)
    } else {
        None
    };

    let res = storage.update_fields(
        &txn,
        containers.customer_cid,
        c_key,
        updates,
        Some(c_actual_hint),
    );
    if not_successful(&res) {
        return (
            helper.kill(&txn, &res, AbortID::PaymentUpdateCustomer),
            None,
        );
    }

    // Commit transaction
    let status = helper.commit(&txn, AbortID::PaymentCommit);
    if status != TPCCStatus::Success {
        return (status, None);
    }

    (
        TPCCStatus::Success,
        Some(PaymentOutput {
            c_id,
            c_first,
            c_middle,
            c_last,
            c_street_1: String::from_utf8_lossy(&c_address.street_1)
                .trim_end_matches('\0')
                .to_string(),
            c_street_2: String::from_utf8_lossy(&c_address.street_2)
                .trim_end_matches('\0')
                .to_string(),
            c_city: String::from_utf8_lossy(&c_address.city)
                .trim_end_matches('\0')
                .to_string(),
            c_state: String::from_utf8_lossy(&c_address.state)
                .trim_end_matches('\0')
                .to_string(),
            c_zip: String::from_utf8_lossy(&c_address.zip)
                .trim_end_matches('\0')
                .to_string(),
            c_phone,
            c_since,
            c_credit,
            c_credit_lim,
            c_discount,
            c_balance: c_balance - input.h_amount,
            c_data: c_data_output,
        }),
    )
}
