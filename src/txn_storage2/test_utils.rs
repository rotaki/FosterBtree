use crate::{
    bp::{get_test_bp, ContainerId, DatabaseId, MemPool},
    schema,
    txn_storage2::{
        field::DataType,
        field_level_storage_trait::{
            ContainerDS, ContainerOptions, DBOptions, FieldLeveLStorageTrait,
        },
        non_transactional_storage::NonTransactionalStorage,
        schema::Schema,
    },
};

/// Create a simple test schema with primary key
pub fn simple_schema() -> Schema {
    schema!(pk: [0], cols: [
        (false, DataType::Int32),  // id (primary key)
        (true, DataType::String),  // name (nullable)
    ])
}

/// Create a test database and container with simple schema
pub fn setup_simple_db() -> (
    NonTransactionalStorage<impl MemPool>,
    DatabaseId,
    ContainerId,
) {
    let bp = get_test_bp(100);
    let storage = NonTransactionalStorage::new(bp);

    let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
    let schema = simple_schema();
    let container_id = storage
        .create_container(
            db_id,
            ContainerOptions::new("test_container", ContainerDS::BTree, schema),
        )
        .unwrap();

    (storage, db_id, container_id)
}

/// Create a test database and container with custom schema
pub fn setup_db_with_schema(
    schema: Schema,
) -> (
    NonTransactionalStorage<impl MemPool>,
    DatabaseId,
    ContainerId,
) {
    let bp = get_test_bp(100);
    let storage = NonTransactionalStorage::new(bp);

    let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
    let container_id = storage
        .create_container(
            db_id,
            ContainerOptions::new("test_container", ContainerDS::BTree, schema),
        )
        .unwrap();

    (storage, db_id, container_id)
}
