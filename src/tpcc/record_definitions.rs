// Type aliases
pub type Timestamp = u64;

// Function to get a timestamp
use std::cell::RefCell;
use std::cmp::Ordering;
use std::thread_local;

pub fn get_timestamp() -> Timestamp {
    thread_local! {
        static TIMESTAMP: RefCell<Timestamp> = const { RefCell::new(0) };
    }
    TIMESTAMP.with(|i| {
        let mut val = i.borrow_mut();
        let result = *val;
        *val += 1;
        result
    })
}

// ----------------- Key Definitions -----------------

// ----------------- ItemKey Struct -----------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct ItemKey {
    pub i_key: u32,
}

impl ItemKey {
    // Create key from i_id
    pub fn create_key(i_id: u32) -> Self {
        ItemKey { i_key: i_id }
    }

    // Create key from Item
    #[allow(dead_code)]
    fn create_key_from_item(item: &Item) -> Self {
        ItemKey { i_key: item.i_id }
    }

    pub fn into_bytes(&self) -> [u8; 4] {
        self.i_key.to_be_bytes()
    }
}

// Implement Ord and PartialOrd for ordering
impl PartialOrd for ItemKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.i_key.cmp(&other.i_key))
    }
}

impl Ord for ItemKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.i_key.cmp(&other.i_key)
    }
}

// ----------------- WarehouseKey Struct -----------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct WarehouseKey {
    pub w_key: u16,
}

impl WarehouseKey {
    pub fn create_key(w_id: u16) -> Self {
        WarehouseKey { w_key: w_id }
    }

    pub fn create_key_from_warehouse(w: &Warehouse) -> Self {
        WarehouseKey { w_key: w.w_id }
    }

    pub fn into_bytes(&self) -> [u8; 2] {
        self.w_key.to_be_bytes()
    }
}

impl PartialOrd for WarehouseKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.w_key.cmp(&other.w_key))
    }
}

impl Ord for WarehouseKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.w_key.cmp(&other.w_key)
    }
}

// ----------------- StockKey Struct -----------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct StockKey {
    pub s_key: u64,
}

impl StockKey {
    // Extract i_id and w_id using bit manipulation
    pub fn i_id(&self) -> u32 {
        (self.s_key & 0xFFFF_FFFF) as u32
    }

    pub fn w_id(&self) -> u16 {
        ((self.s_key >> 32) & 0xFFFF) as u16
    }

    pub fn into_bytes(&self) -> [u8; 8] {
        self.s_key.to_be_bytes()
    }

    pub fn create_key(w_id: u16, i_id: u32) -> Self {
        let s_key = ((w_id as u64) << 32) | (i_id as u64);
        StockKey { s_key }
    }

    pub fn create_key_from_stock(s: &Stock) -> Self {
        StockKey::create_key(s.s_w_id, s.s_i_id)
    }
}

impl PartialOrd for StockKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.s_key.cmp(&other.s_key))
    }
}

impl Ord for StockKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.s_key.cmp(&other.s_key)
    }
}

// ----------------- DistrictKey Struct -----------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct DistrictKey {
    pub d_key: u32,
}

impl DistrictKey {
    pub fn d_id(&self) -> u8 {
        (self.d_key & 0xFF) as u8
    }

    pub fn w_id(&self) -> u16 {
        ((self.d_key >> 8) & 0xFFFF) as u16
    }

    pub fn into_bytes(&self) -> [u8; 4] {
        self.d_key.to_be_bytes()
    }

    pub fn create_key(w_id: u16, d_id: u8) -> Self {
        let d_key = ((w_id as u32) << 8) | (d_id as u32);
        DistrictKey { d_key }
    }

    pub fn create_key_from_district(d: &District) -> Self {
        DistrictKey::create_key(d.d_w_id, d.d_id)
    }
}

impl PartialOrd for DistrictKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.d_key.cmp(&other.d_key))
    }
}

impl Ord for DistrictKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.d_key.cmp(&other.d_key)
    }
}

// ----------------- CustomerKey Struct -----------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct CustomerKey {
    pub c_key: u64,
}

impl CustomerKey {
    pub fn c_id(&self) -> u32 {
        (self.c_key & 0xFFFF_FFFF) as u32
    }

    pub fn d_id(&self) -> u8 {
        ((self.c_key >> 32) & 0xFF) as u8
    }

    pub fn w_id(&self) -> u16 {
        ((self.c_key >> 40) & 0xFFFF) as u16
    }

    pub fn into_bytes(&self) -> [u8; 8] {
        self.c_key.to_be_bytes()
    }

    pub fn create_key(w_id: u16, d_id: u8, c_id: u32) -> Self {
        let c_key = ((w_id as u64) << 40) | ((d_id as u64) << 32) | (c_id as u64);
        CustomerKey { c_key }
    }

    pub fn create_key_from_customer(c: &Customer) -> Self {
        CustomerKey::create_key(c.c_w_id, c.c_d_id, c.c_id)
    }
}

// ----------------- CustomerSecondaryKey Struct -----------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct CustomerSecondaryKey {
    pub num: u32,
    pub c_last: [u8; Customer::MAX_LAST],
}

impl CustomerSecondaryKey {
    fn new() -> Self {
        CustomerSecondaryKey {
            num: 0,
            c_last: [0; Customer::MAX_LAST],
        }
    }

    #[allow(dead_code)]
    fn from_num(num: u32) -> Self {
        CustomerSecondaryKey {
            num,
            c_last: [0; Customer::MAX_LAST],
        }
    }

    #[allow(dead_code)]
    fn copy_from(other: &Self) -> Self {
        let num = other.num & 0x00FFFFFF; // Zero out the 'not_used' bits
        let c_last = other.c_last;
        CustomerSecondaryKey { num, c_last }
    }

    #[allow(dead_code)]
    fn get_d_id(&self) -> u8 {
        (self.num & 0x000000FF) as u8
    }

    fn set_d_id(&mut self, value: u8) {
        self.num = (self.num & 0xFFFFFF00) | value as u32;
    }

    #[allow(dead_code)]
    fn get_w_id(&self) -> u16 {
        ((self.num & 0x00FFFF00) >> 8) as u16
    }

    fn set_w_id(&mut self, value: u16) {
        self.num = (self.num & 0xFF0000FF) | ((value as u32) << 8);
    }

    pub fn into_bytes(&self) -> [u8; 4 + Customer::MAX_LAST] {
        let mut bytes = [0; 4 + Customer::MAX_LAST];
        bytes[..4].copy_from_slice(&self.num.to_be_bytes());
        bytes[4..].copy_from_slice(&self.c_last);
        bytes
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &CustomerSecondaryKey {
        &*(bytes.as_ptr() as *const CustomerSecondaryKey)
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    /// Also, the caller must ensure that the bytes are mutable.
    pub unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut CustomerSecondaryKey {
        &mut *(bytes.as_mut_ptr() as *mut CustomerSecondaryKey)
    }

    pub fn create_key(w_id: u16, d_id: u8, c_last_in: &[u8]) -> Self {
        let mut k = Self::new();
        k.set_w_id(w_id);
        k.set_d_id(d_id);
        let len = c_last_in.len().min(Customer::MAX_LAST);
        k.c_last[..len].copy_from_slice(&c_last_in[..len]);
        k
    }

    pub fn create_key_from_customer(c: &Customer) -> Self {
        let mut k = Self::new();
        k.set_w_id(c.c_w_id);
        k.set_d_id(c.c_d_id);
        let len = c.c_last.len().min(Customer::MAX_LAST);
        k.c_last[..len].copy_from_slice(&c.c_last[..len]);
        k
    }
}

pub struct HistoryKey {
    pub h_key: u64,
}

impl HistoryKey {
    pub fn c_id(&self) -> u32 {
        (self.h_key & 0xFFFF_FFFF) as u32
    }

    pub fn d_id(&self) -> u8 {
        ((self.h_key >> 32) & 0xFF) as u8
    }

    pub fn w_id(&self) -> u16 {
        ((self.h_key >> 40) & 0xFFFF) as u16
    }

    pub fn into_bytes(&self) -> [u8; 8] {
        self.h_key.to_be_bytes()
    }

    pub fn create_key(w_id: u16, d_id: u8, c_id: u32) -> Self {
        let h_key = ((w_id as u64) << 40) | ((d_id as u64) << 32) | (c_id as u64);
        HistoryKey { h_key }
    }
}

// ----------------- OrderKey Struct -----------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct OrderKey {
    pub o_key: u64,
}

impl OrderKey {
    #[allow(dead_code)]
    fn o_id(&self) -> u32 {
        (self.o_key & 0xFFFF_FFFF) as u32
    }

    #[allow(dead_code)]
    fn d_id(&self) -> u8 {
        ((self.o_key >> 32) & 0xFF) as u8
    }

    #[allow(dead_code)]
    fn w_id(&self) -> u16 {
        ((self.o_key >> 40) & 0xFFFF) as u16
    }

    pub fn into_bytes(&self) -> [u8; 8] {
        self.o_key.to_be_bytes()
    }

    pub fn create_key(w_id: u16, d_id: u8, o_id: u32) -> Self {
        let o_key = ((w_id as u64) << 40) | ((d_id as u64) << 32) | (o_id as u64);
        OrderKey { o_key }
    }

    #[allow(dead_code)]
    fn create_key_from_order(o: &Order) -> Self {
        OrderKey::create_key(o.o_w_id, o.o_d_id, o.o_id)
    }
}

impl PartialOrd for OrderKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.o_key.cmp(&other.o_key))
    }
}

impl Ord for OrderKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.o_key.cmp(&other.o_key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderSecondaryKey {
    o_sec_key: u64,
}

impl Default for OrderSecondaryKey {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderSecondaryKey {
    // Constructor that initializes the key to zero
    pub fn new() -> Self {
        Self { o_sec_key: 0 }
    }

    // Constructor that accepts a raw u64 key
    pub fn from_raw(o_sec_key: u64) -> Self {
        Self { o_sec_key }
    }

    // Method to get the raw key
    pub fn get_raw_key(&self) -> u64 {
        self.o_sec_key
    }

    // Create a key from individual fields
    pub fn create_key(w_id: u16, d_id: u8, c_id: u32, o_id: u32) -> Self {
        let mut o_sec_key: u64 = 0;
        o_sec_key |= (w_id as u64 & 0xFFF) << 52; // 12 bits for w_id
        o_sec_key |= (d_id as u64 & 0xF) << 48; // 4 bits for d_id (actual: 10)
        o_sec_key |= (c_id as u64 & 0xFFFF) << 32; // 16 bits for c_id (actual: 3000)
        o_sec_key |= o_id as u64 & 0xFFFFFFFF; // 32 bits for o_id
        Self { o_sec_key }
    }

    pub fn create_key_from_order(o: &Order) -> Self {
        OrderSecondaryKey::create_key(o.o_w_id, o.o_d_id, o.o_c_id, o.o_id)
    }

    pub fn into_bytes(&self) -> [u8; 8] {
        self.o_sec_key.to_be_bytes()
    }

    // Extract fields from the raw key
    pub fn w_id(&self) -> u16 {
        ((self.o_sec_key >> 52) & 0xFFF) as u16
    }

    pub fn d_id(&self) -> u8 {
        ((self.o_sec_key >> 48) & 0xF) as u8
    }

    pub fn c_id(&self) -> u32 {
        ((self.o_sec_key >> 32) & 0xFFFF) as u32
    }

    pub fn o_id(&self) -> u32 {
        (self.o_sec_key & 0xFFFFFFFF) as u32
    }
}

// ----------------- OrderLineKey Struct -----------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct OrderLineKey {
    pub ol_key: u64,
}

impl OrderLineKey {
    #[allow(dead_code)]
    fn ol_number(&self) -> u8 {
        (self.ol_key & 0xFF) as u8
    }

    #[allow(dead_code)]
    fn o_id(&self) -> u32 {
        ((self.ol_key >> 8) & 0xFFFF_FFFF) as u32
    }

    #[allow(dead_code)]
    fn d_id(&self) -> u8 {
        ((self.ol_key >> 40) & 0xFF) as u8
    }

    #[allow(dead_code)]
    fn w_id(&self) -> u16 {
        ((self.ol_key >> 48) & 0xFFFF) as u16
    }

    pub fn into_bytes(&self) -> [u8; 8] {
        self.ol_key.to_be_bytes()
    }

    pub fn create_key(w_id: u16, d_id: u8, o_id: u32, ol_number: u8) -> Self {
        let ol_key = ((w_id as u64) << 48)
            | ((d_id as u64) << 40)
            | ((o_id as u64) << 8)
            | (ol_number as u64);
        OrderLineKey { ol_key }
    }

    #[allow(dead_code)]
    fn create_key_from_orderline(ol: &OrderLine) -> Self {
        OrderLineKey::create_key(ol.ol_w_id, ol.ol_d_id, ol.ol_o_id, ol.ol_number)
    }
}

impl PartialOrd for OrderLineKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.ol_key.cmp(&other.ol_key))
    }
}

impl Ord for OrderLineKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ol_key.cmp(&other.ol_key)
    }
}

// ----------------- NewOrderKey Struct -----------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct NewOrderKey {
    pub no_key: u64,
}

impl NewOrderKey {
    pub fn o_id(&self) -> u32 {
        (self.no_key & 0xFFFF_FFFF) as u32
    }

    pub fn d_id(&self) -> u8 {
        ((self.no_key >> 32) & 0xFF) as u8
    }

    pub fn w_id(&self) -> u16 {
        ((self.no_key >> 40) & 0xFFFF) as u16
    }

    pub fn into_bytes(&self) -> [u8; 8] {
        self.no_key.to_be_bytes()
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &NewOrderKey {
        &*(bytes.as_ptr() as *const NewOrderKey)
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    /// Also, the caller must ensure that the bytes are mutable.
    pub unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut NewOrderKey {
        &mut *(bytes.as_mut_ptr() as *mut NewOrderKey)
    }

    pub fn create_key(w_id: u16, d_id: u8, o_id: u32) -> Self {
        let no_key = ((w_id as u64) << 40) | ((d_id as u64) << 32) | (o_id as u64);
        NewOrderKey { no_key }
    }

    #[allow(dead_code)]
    fn create_key_from_new_order(no: &NewOrder) -> Self {
        NewOrderKey::create_key(no.no_w_id, no.no_d_id, no.no_o_id)
    }
}

impl PartialOrd for NewOrderKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.no_key.cmp(&other.no_key))
    }
}

impl Ord for NewOrderKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.no_key.cmp(&other.no_key)
    }
}

// ----------------- Record Definitions -----------------

// ----------------- Item Struct -----------------
#[repr(C)]
pub struct Item {
    pub i_id: u32,                    // 200,000 unique ids
    pub i_im_id: u32,                 // 200,000 unique ids
    pub i_price: f64,                 // numeric(5, 2)
    pub i_name: [u8; Item::MAX_NAME], // Fixed-size byte array
    pub i_data: [u8; Item::MAX_DATA], // Fixed-size byte array
}

impl Default for Item {
    fn default() -> Self {
        Self::new()
    }
}

impl Item {
    // Associated constants
    pub const ITEMS: usize = 100_000;
    pub const MIN_NAME: usize = 14;
    pub const MAX_NAME: usize = 24;
    pub const MIN_DATA: usize = 26;
    pub const MAX_DATA: usize = 50;
    pub const UNUSED_ID: u32 = 0;

    pub fn new() -> Self {
        Item {
            i_id: 0,
            i_im_id: 0,
            i_price: 0.0,
            i_name: [0; Item::MAX_NAME],
            i_data: [0; Item::MAX_DATA],
        }
    }

    // Method to generate an Item based on an id
    pub fn generate(i_id: u32) -> Self {
        let mut item = Item::new();
        item.i_id = i_id;
        item.i_im_id = urand_int(1, 10000); // 200000 unique ids
        item.i_price = urand_double(100, 10000, 100); // numeric(5, 2)
        make_random_astring(&mut item.i_name, Item::MIN_NAME, Item::MAX_NAME);
        make_random_astring(&mut item.i_data, Item::MIN_DATA, Item::MAX_DATA);
        if urand_int(0, 99) < 10 {
            make_original(&mut item.i_data);
        }
        item
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Item as *const u8,
                std::mem::size_of::<Item>(),
            )
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &Item {
        &*(bytes.as_ptr() as *const Item)
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    /// Also, the caller must ensure that the bytes are mutable.
    pub unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Item {
        &mut *(bytes.as_mut_ptr() as *mut Item)
    }

    // Method to print the Item
    pub fn print(&self) -> String {
        format!(
            "[ITEM] i_id:{} i_im_id:{} i_price:{:.2} i_name:{} i_data:{}",
            self.i_id,
            self.i_im_id,
            self.i_price,
            String::from_utf8_lossy(&self.i_name),
            String::from_utf8_lossy(&self.i_data)
        )
    }
}

// ----------------- Address Struct -----------------
#[repr(C)]
pub struct Address {
    pub street_1: [u8; Address::MAX_STREET],
    pub street_2: [u8; Address::MAX_STREET],
    pub city: [u8; Address::MAX_CITY],
    pub state: [u8; Address::STATE],
    pub zip: [u8; Address::ZIP],
}

impl Default for Address {
    fn default() -> Self {
        Self::new()
    }
}

impl Address {
    // Associated constants
    pub const MIN_STREET: usize = 10;
    pub const MAX_STREET: usize = 20;
    pub const MIN_CITY: usize = 10;
    pub const MAX_CITY: usize = 20;
    pub const STATE: usize = 2;
    pub const ZIP: usize = 9;

    pub fn new() -> Self {
        Address {
            street_1: [0; Address::MAX_STREET],
            street_2: [0; Address::MAX_STREET],
            city: [0; Address::MAX_CITY],
            state: [0; Address::STATE],
            zip: [0; Address::ZIP],
        }
    }

    pub fn print(&self) -> String {
        format!(
            "street_1:{} street_2:{} city:{} state:{} zip:{}",
            String::from_utf8_lossy(&self.street_1),
            String::from_utf8_lossy(&self.street_2),
            String::from_utf8_lossy(&self.city),
            String::from_utf8_lossy(&self.state),
            String::from_utf8_lossy(&self.zip)
        )
    }
}

// ----------------- Warehouse Struct -----------------
#[repr(C)]
pub struct Warehouse {
    pub w_id: u16,  // 2*W unique ids
    pub w_tax: f64, // signed numeric(4, 4)
    pub w_ytd: f64, // signed numeric(12, 2)
    pub w_name: [u8; Warehouse::MAX_NAME],
    pub w_address: Address,
}

impl Default for Warehouse {
    fn default() -> Self {
        Self::new()
    }
}

impl Warehouse {
    // Associated constants
    pub const MIN_NAME: usize = 6;
    pub const MAX_NAME: usize = 10;

    pub fn new() -> Self {
        Warehouse {
            w_id: 0,
            w_tax: 0.0,
            w_ytd: 0.0,
            w_name: [0; Warehouse::MAX_NAME],
            w_address: Address::new(),
        }
    }

    // Method to generate a Warehouse based on an id
    pub fn generate(w_id: u16) -> Self {
        let mut warehouse = Warehouse::new();
        warehouse.w_id = w_id;
        warehouse.w_tax = urand_double(0, 2000, 10000); // signed numeric(4, 4)
        warehouse.w_ytd = 300000.00; // signed numeric(12, 2)
        make_random_astring(
            &mut warehouse.w_name,
            Warehouse::MIN_NAME,
            Warehouse::MAX_NAME,
        );
        make_random_address(&mut warehouse.w_address);
        warehouse
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Warehouse as *const u8,
                std::mem::size_of::<Warehouse>(),
            )
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &Warehouse {
        &*(bytes.as_ptr() as *const Warehouse)
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    /// Also, the caller must ensure that the bytes are mutable.
    pub fn from_bytes_mut(bytes: &mut [u8]) -> &mut Warehouse {
        unsafe { &mut *(bytes.as_mut_ptr() as *mut Warehouse) }
    }

    // Method to print the Warehouse
    pub fn print(&self) -> String {
        format!(
            "[WARE] w_id:{} w_tax:{:.4} w_ytd:{:.2} w_name:{}, w_address:({})",
            self.w_id,
            self.w_tax,
            self.w_ytd,
            String::from_utf8_lossy(&self.w_name),
            self.w_address.print()
        )
    }
}

// ----------------- Stock Struct -----------------
#[repr(C)]
pub struct Stock {
    pub s_w_id: u16,
    pub s_i_id: u32,       // 200,000 unique ids
    pub s_quantity: i16,   // signed numeric(4)
    pub s_ytd: u32,        // numeric(8)
    pub s_order_cnt: u16,  // numeric(4)
    pub s_remote_cnt: u16, // numeric(4)
    pub s_dist: [[u8; Stock::DIST]; 10],
    pub s_data: [u8; Stock::MAX_DATA],
}

impl Default for Stock {
    fn default() -> Self {
        Self::new()
    }
}

impl Stock {
    // Associated constants
    pub const STOCKS_PER_WARE: usize = 100_000;
    pub const DIST: usize = 24;
    pub const MIN_DATA: usize = 26;
    pub const MAX_DATA: usize = 50;

    pub fn new() -> Self {
        Stock {
            s_w_id: 0,
            s_i_id: 0,
            s_quantity: 0,
            s_ytd: 0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
            s_dist: [[0; Stock::DIST]; 10],
            s_data: [0; Stock::MAX_DATA],
        }
    }

    // Method to generate a Stock
    pub fn generate(s_w_id: u16, s_i_id: u32) -> Self {
        let mut stock = Stock::new();
        stock.s_w_id = s_w_id;
        stock.s_i_id = s_i_id;
        stock.s_quantity = urand_int(10, 100); // signed numeric(4)
        stock.s_ytd = 0; // numeric(8)
        stock.s_order_cnt = 0; // numeric(4)
        stock.s_remote_cnt = 0; // numeric(4)
        for i in 0..10 {
            make_random_astring(&mut stock.s_dist[i], Stock::DIST, Stock::DIST);
        }
        make_random_astring(&mut stock.s_data, Stock::MIN_DATA, Stock::MAX_DATA);
        if urand_int(0, 99) < 10 {
            make_original(&mut stock.s_data);
        }
        stock
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Stock as *const u8,
                std::mem::size_of::<Stock>(),
            )
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &Stock {
        &*(bytes.as_ptr() as *const Stock)
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    /// Also, the caller must ensure that the bytes are mutable.
    pub unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Stock {
        &mut *(bytes.as_mut_ptr() as *mut Stock)
    }

    // Method to print the Stock
    pub fn print(&self) -> String {
        let mut dist = String::new();
        for i in 0..10 {
            dist.push_str(&format!("{} ", String::from_utf8_lossy(&self.s_dist[i])));
        }
        format!(
            "[STOCK] s_w_id:{} s_i_id:{} s_quantity:{} s_ytd:{} s_order_cnt:{} s_remote_cnt:{} s_dist:{} s_data:{}",
            self.s_w_id, self.s_i_id, self.s_quantity, self.s_ytd, self.s_order_cnt, self.s_remote_cnt, dist, String::from_utf8_lossy(&self.s_data)
        )
    }
}

// ----------------- District Struct -----------------
#[repr(C)]
pub struct District {
    pub d_w_id: u16,
    pub d_id: u8,         // 20 unique ids
    pub d_next_o_id: u32, // 10,000,000 unique ids
    pub d_tax: f64,       // signed numeric(4, 4)
    pub d_ytd: f64,       // signed numeric(12, 2)
    pub d_name: [u8; District::MAX_NAME],
    pub d_address: Address,
}

impl Default for District {
    fn default() -> Self {
        Self::new()
    }
}

impl District {
    // Associated constants
    pub const DISTS_PER_WARE: usize = 10;
    pub const MIN_NAME: usize = 6;
    pub const MAX_NAME: usize = 10;

    pub fn new() -> Self {
        District {
            d_w_id: 0,
            d_id: 0,
            d_next_o_id: 0,
            d_tax: 0.0,
            d_ytd: 0.0,
            d_name: [0; District::MAX_NAME],
            d_address: Address {
                street_1: [0; Address::MAX_STREET],
                street_2: [0; Address::MAX_STREET],
                city: [0; Address::MAX_CITY],
                state: [0; Address::STATE],
                zip: [0; Address::ZIP],
            },
        }
    }

    pub fn generate(d_w_id: u16, d_id: u8) -> Self {
        let mut district = District::new();
        district.d_w_id = d_w_id;
        district.d_id = d_id;
        district.d_next_o_id = 3001; // 10,000,000 unique ids
        district.d_tax = urand_double(0, 2000, 10000); // signed numeric(4, 4)
        district.d_ytd = 30000.00; // signed numeric(12, 2)
        make_random_astring(&mut district.d_name, District::MIN_NAME, District::MAX_NAME);
        make_random_address(&mut district.d_address);
        district
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const District as *const u8,
                std::mem::size_of::<District>(),
            )
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &District {
        &*(bytes.as_ptr() as *const District)
    }

    /// # Safety
    ///
    /// The caller must ensure that the bytes are of the correct length
    /// and that the lifetime of the returned reference is valid.
    /// Also, the caller must ensure that the bytes are mutable.
    pub unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut District {
        &mut *(bytes.as_mut_ptr() as *mut District)
    }

    // Method to print the District
    #[allow(dead_code)]
    fn print(&self) -> String {
        format!(
            "[DIST] d_w_id:{} d_id:{} d_next_o_id:{} d_tax:{:.4} d_ytd:{:.2} d_name:{} d_address:({})",
            self.d_w_id, self.d_id, self.d_next_o_id, self.d_tax, self.d_ytd, String::from_utf8_lossy(&self.d_name), self.d_address.print()
        )
    }
}

// ----------------- Customer Struct -----------------
#[repr(C)]
pub struct Customer {
    pub c_w_id: u16,
    pub c_d_id: u8,
    pub c_id: u32,           // 96,000 unique ids
    pub c_payment_cnt: u16,  // numeric(4)
    pub c_delivery_cnt: u16, // numeric(4)
    pub c_since: Timestamp,  // date and time
    pub c_credit_lim: f64,   // signed numeric(2, 2)
    pub c_discount: f64,     // signed numeric(4, 4)
    pub c_balance: f64,      // signed numeric(12, 2)
    pub c_ytd_payment: f64,  // signed numeric(12, 2)
    pub c_first: [u8; Customer::MAX_FIRST],
    pub c_middle: [u8; Customer::MAX_MIDDLE],
    pub c_last: [u8; Customer::MAX_LAST],
    pub c_phone: [u8; Customer::PHONE],
    pub c_credit: [u8; Customer::CREDIT], // "GC"=good, "BC"=bad
    pub c_data: [u8; Customer::MAX_DATA], // miscellaneous information
    pub c_address: Address,
}

impl Default for Customer {
    fn default() -> Self {
        Self::new()
    }
}

impl Customer {
    // Associated constants
    pub const CUSTS_PER_DIST: usize = 3000;
    pub const MIN_FIRST: usize = 8;
    pub const MAX_FIRST: usize = 16;
    pub const MAX_MIDDLE: usize = 2;
    pub const MAX_LAST: usize = 16;
    pub const PHONE: usize = 16;
    pub const CREDIT: usize = 2;
    pub const MIN_DATA: usize = 300;
    pub const MAX_DATA: usize = 500;
    pub const UNUSED_ID: u32 = 0;

    pub fn new() -> Self {
        Customer {
            c_w_id: 0,
            c_d_id: 0,
            c_id: 0,
            c_payment_cnt: 0,
            c_delivery_cnt: 0,
            c_since: 0,
            c_credit_lim: 0.0,
            c_discount: 0.0,
            c_balance: 0.0,
            c_ytd_payment: 0.0,
            c_first: [0; Customer::MAX_FIRST],
            c_middle: [0; Customer::MAX_MIDDLE],
            c_last: [0; Customer::MAX_LAST],
            c_phone: [0; Customer::PHONE],
            c_credit: [0; Customer::CREDIT],
            c_data: [0; Customer::MAX_DATA],
            c_address: Address {
                street_1: [0; Address::MAX_STREET],
                street_2: [0; Address::MAX_STREET],
                city: [0; Address::MAX_CITY],
                state: [0; Address::STATE],
                zip: [0; Address::ZIP],
            },
        }
    }

    // Method to generate a Customer
    pub fn generate(c_w_id: u16, c_d_id: u8, c_id: u32, t: Timestamp) -> Self {
        let mut customer = Customer::new();
        customer.c_w_id = c_w_id;
        customer.c_d_id = c_d_id;
        customer.c_id = c_id;
        customer.c_payment_cnt = 1; // numeric(4)
        customer.c_delivery_cnt = 0; // numeric(4)
        customer.c_since = t; // date and time
        customer.c_credit_lim = 50000.00; // signed numeric(2, 2)
        customer.c_discount = urand_double(0, 5000, 10000); // signed numeric(4, 4)
        customer.c_balance = -10.00; // signed numeric(12, 2)
        customer.c_ytd_payment = 10.00; // signed numeric(12, 2)
        make_random_astring(
            &mut customer.c_first,
            Customer::MIN_FIRST,
            Customer::MAX_FIRST,
        );
        customer.c_middle[0] = b'O';
        customer.c_middle[1] = b'E';
        if customer.c_id <= 1000 {
            // This makes sure that all the c_last patterns are used.
            make_clast(&mut customer.c_last, (customer.c_id - 1) as usize);
        } else {
            make_clast(
                &mut customer.c_last,
                nurand_int::<255, true>(0, 999) as usize,
            );
        }
        make_random_nstring(&mut customer.c_phone, Customer::PHONE, Customer::PHONE);
        if urand_int(0, 99) < 10 {
            customer.c_credit[0] = b'B';
            customer.c_credit[1] = b'C';
        } else {
            customer.c_credit[0] = b'G';
            customer.c_credit[1] = b'C';
        }
        make_random_astring(&mut customer.c_data, Customer::MIN_DATA, Customer::MAX_DATA);
        make_random_address(&mut customer.c_address);
        customer
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Customer as *const u8,
                std::mem::size_of::<Customer>(),
            )
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid Customer struct.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &Customer {
        unsafe { &*(bytes.as_ptr() as *const Customer) }
    }

    /// # Safety
    ///
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid Customer struct. The caller must also ensure that
    /// the input bytes are mutable.
    pub fn from_bytes_mut(bytes: &mut [u8]) -> &mut Customer {
        unsafe { &mut *(bytes.as_mut_ptr() as *mut Customer) }
    }

    // Method to print the Customer
    pub fn print(&self) -> String {
        format!(
            "[CUST] c_w_id:{} c_d_id:{} c_id:{} c_payment_cnt:{} c_delivery_cnt:{} c_since:{} c_credit_lim:{:.2} c_discount:{:.4} c_balance:{:.2} c_ytd_payment:{:.2} c_first:{} c_middle:{} c_last:{} c_phone:{} c_credit:{} c_data:{} c_address:({})",
            self.c_w_id, self.c_d_id, self.c_id, self.c_payment_cnt, self.c_delivery_cnt, self.c_since, self.c_credit_lim, self.c_discount, self.c_balance, self.c_ytd_payment, String::from_utf8_lossy(&self.c_first), String::from_utf8_lossy(&self.c_middle), String::from_utf8_lossy(&self.c_last), String::from_utf8_lossy(&self.c_phone), String::from_utf8_lossy(&self.c_credit), String::from_utf8_lossy(&self.c_data), self.c_address.print()
        )
    }
}

// ----------------- History Struct -----------------
#[repr(C)]
pub struct History {
    pub h_c_w_id: u16,
    pub h_c_d_id: u8,
    pub h_c_id: u32,
    pub h_w_id: u16,
    pub h_d_id: u8,
    pub h_date: Timestamp, // date and time
    pub h_amount: f64,     // signed numeric(6, 2)
    pub h_data: [u8; History::MAX_DATA],
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}

impl History {
    // Associated constants
    pub const HISTS_PER_CUST: usize = 1;
    pub const MIN_DATA: usize = 12;
    pub const MAX_DATA: usize = 24;

    pub fn new() -> Self {
        History {
            h_c_w_id: 0,
            h_c_d_id: 0,
            h_c_id: 0,
            h_w_id: 0,
            h_d_id: 0,
            h_date: 0,
            h_amount: 0.0,
            h_data: [0; History::MAX_DATA],
        }
    }

    pub fn generate(h_c_w_id: u16, h_c_d_id: u8, h_c_id: u32, h_w_id: u16, h_d_id: u8) -> Self {
        let mut history = History::new();
        history.h_c_w_id = h_c_w_id;
        history.h_c_d_id = h_c_d_id;
        history.h_c_id = h_c_id;
        history.h_w_id = h_w_id;
        history.h_d_id = h_d_id;
        history.h_date = get_timestamp();
        history.h_amount = 10.00; // signed numeric(6, 2)
        make_random_astring(&mut history.h_data, History::MIN_DATA, History::MAX_DATA);
        history
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const History as *const u8,
                std::mem::size_of::<History>(),
            )
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid History struct.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &History {
        &*(bytes.as_ptr() as *const History)
    }

    /// # Safety
    ///
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid History struct. The caller must also ensure that
    /// the input bytes are mutable.
    pub unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut History {
        &mut *(bytes.as_mut_ptr() as *mut History)
    }

    // Method to print the History
    pub fn print(&self) -> String {
        format!(
            "[HIST] h_c_w_id:{} h_c_d_id:{} h_c_id:{} h_w_id:{} h_d_id:{} h_date:{} h_amount:{:.2} h_data:{}",
            self.h_c_w_id, self.h_c_d_id, self.h_c_id, self.h_w_id, self.h_d_id, self.h_date, self.h_amount, String::from_utf8_lossy(&self.h_data)
        )
    }
}

// ----------------- Order Struct -----------------
#[repr(C)]
pub struct Order {
    pub o_w_id: u16,
    pub o_d_id: u8,
    pub o_id: u32, // 10,000,000 unique ids
    pub o_c_id: u32,
    pub o_carrier_id: u8,     // 10 unique ids or null
    pub o_ol_cnt: u8,         // numeric(2)
    pub o_all_local: u8,      // numeric(1)
    pub o_entry_d: Timestamp, // date and time
}

impl Default for Order {
    fn default() -> Self {
        Self::new()
    }
}

impl Order {
    // Associated constants
    pub const ORDS_PER_DIST: usize = 3000;

    pub fn new() -> Self {
        Order {
            o_w_id: 0,
            o_d_id: 0,
            o_id: 0,
            o_c_id: 0,
            o_carrier_id: 0,
            o_ol_cnt: 0,
            o_all_local: 0,
            o_entry_d: 0,
        }
    }

    pub fn generate(o_w_id: u16, o_d_id: u8, o_id: u32, o_c_id: u32) -> Self {
        let mut order = Order::new();
        order.o_w_id = o_w_id;
        order.o_d_id = o_d_id;
        order.o_id = o_id;
        order.o_c_id = o_c_id;
        order.o_carrier_id = if o_id < 2101 { urand_int(1, 10) } else { 0 }; // 10 unique ids or null
        order.o_ol_cnt = urand_int(
            OrderLine::MIN_ORDLINES_PER_ORD as u8,
            OrderLine::MAX_ORDLINES_PER_ORD as u8,
        ); // numeric(2)
        order.o_all_local = 1; // numeric(1)
        order.o_entry_d = get_timestamp(); // date and time
        order
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Order as *const u8,
                std::mem::size_of::<Order>(),
            )
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid Order struct.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &Order {
        &*(bytes.as_ptr() as *const Order)
    }

    /// # Safety
    ///
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid Order struct. The caller must also ensure that
    /// the input bytes are mutable.
    pub unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Order {
        &mut *(bytes.as_ptr() as *mut Order)
    }

    // Method to print the Order
    pub fn print(&self) -> String {
        format!(
            "[ORD] o_w_id:{} o_d_id:{} o_id:{} o_c_id:{} o_carrier_id:{} o_ol_cnt:{} o_all_local:{} o_entry_d:{}",
            self.o_w_id, self.o_d_id, self.o_id, self.o_c_id, self.o_carrier_id, self.o_ol_cnt, self.o_all_local, self.o_entry_d
        )
    }
}

// ----------------- NewOrder Struct -----------------
#[repr(C)]
pub struct NewOrder {
    pub no_w_id: u16,
    pub no_d_id: u8,
    pub no_o_id: u32,
}

impl Default for NewOrder {
    fn default() -> Self {
        Self::new()
    }
}

impl NewOrder {
    pub fn new() -> Self {
        NewOrder {
            no_w_id: 0,
            no_d_id: 0,
            no_o_id: 0,
        }
    }

    pub fn generate(no_w_id: u16, no_d_id: u8, no_o_id: u32) -> Self {
        let mut new_order = NewOrder::new();
        new_order.no_w_id = no_w_id;
        new_order.no_d_id = no_d_id;
        new_order.no_o_id = no_o_id;
        new_order
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const NewOrder as *const u8,
                std::mem::size_of::<NewOrder>(),
            )
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid NewOrder struct.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &NewOrder {
        &*(bytes.as_ptr() as *const NewOrder)
    }

    /// # Safety
    ///
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid NewOrder struct. The caller must also ensure that
    /// the input bytes are mutable.
    pub unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut NewOrder {
        &mut *(bytes.as_ptr() as *mut NewOrder)
    }

    // Method to print the NewOrder
    pub fn print(&self) -> String {
        format!(
            "[NEWORD] no_w_id:{} no_d_id:{} no_o_id:{}",
            self.no_w_id, self.no_d_id, self.no_o_id
        )
    }
}

// ----------------- OrderLine Struct -----------------
#[repr(C)]
pub struct OrderLine {
    pub ol_w_id: u16,
    pub ol_d_id: u8,
    pub ol_o_id: u32,
    pub ol_number: u8, // 15 unique ids
    pub ol_i_id: u32,  // 200,000 unique ids
    pub ol_supply_w_id: u16,
    pub ol_delivery_d: Timestamp,
    pub ol_quantity: u8, // numeric(2)
    pub ol_amount: f64,  // signed numeric(6, 2)
    pub ol_dist_info: [u8; OrderLine::DIST_INFO],
}

impl Default for OrderLine {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderLine {
    // Associated constants
    pub const MIN_ORDLINES_PER_ORD: usize = 5;
    pub const MAX_ORDLINES_PER_ORD: usize = 15;
    pub const DIST_INFO: usize = 24;

    pub fn new() -> Self {
        OrderLine {
            ol_w_id: 0,
            ol_d_id: 0,
            ol_o_id: 0,
            ol_number: 0,
            ol_i_id: 0,
            ol_supply_w_id: 0,
            ol_delivery_d: 0,
            ol_quantity: 0,
            ol_amount: 0.0,
            ol_dist_info: [0; OrderLine::DIST_INFO],
        }
    }

    pub fn generate(
        ol_w_id: u16,
        ol_d_id: u8,
        ol_o_id: u32,
        ol_number: u8,
        ol_supply_w_id: u16,
        ol_i_id: u32,
        o_entry_d: Timestamp,
    ) -> Self {
        let mut order_line = OrderLine::new();
        order_line.ol_w_id = ol_w_id;
        order_line.ol_d_id = ol_d_id;
        order_line.ol_o_id = ol_o_id;
        order_line.ol_number = ol_number;
        order_line.ol_supply_w_id = ol_supply_w_id;
        order_line.ol_i_id = ol_i_id;
        order_line.ol_delivery_d = if ol_o_id < 2101 { o_entry_d } else { 0 };
        order_line.ol_quantity = 5; // numeric(2)
        order_line.ol_amount = if ol_o_id < 2101 {
            0.00
        } else {
            urand_double(1, 999999, 100)
        }; // signed numeric(6, 2)
        make_random_astring(
            &mut order_line.ol_dist_info,
            OrderLine::DIST_INFO,
            OrderLine::DIST_INFO,
        );
        order_line
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const OrderLine as *const u8,
                std::mem::size_of::<OrderLine>(),
            )
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid OrderLine struct.
    pub unsafe fn from_bytes(bytes: &[u8]) -> &OrderLine {
        &*(bytes.as_ptr() as *const OrderLine)
    }

    /// # Safety
    /// The caller must ensure that the input bytes are valid and
    /// represent a valid OrderLine struct. The caller must also ensure that
    /// the input bytes are mutable.
    pub unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut OrderLine {
        &mut *(bytes.as_mut_ptr() as *mut OrderLine)
    }

    pub fn print(&self) -> String {
        format!(
            "[ORDL] ol_w_id:{} ol_d_id:{} ol_o_id:{} ol_number:{} ol_i_id:{} ol_supply_w_id:{} ol_delivery_d:{} ol_quantity:{} ol_amount:{:.2} ol_dist_info:{}",
            self.ol_w_id, self.ol_d_id, self.ol_o_id, self.ol_number, self.ol_i_id, self.ol_supply_w_id, self.ol_delivery_d, self.ol_quantity, self.ol_amount, String::from_utf8_lossy(&self.ol_dist_info)
        )
    }
}

// ----------------- get_constant_for_nurand Function -----------------
const fn get_constant_for_nurand(a: u64, is_load: bool) -> u64 {
    const C_FOR_C_LAST_IN_LOAD: u64 = 250;
    const C_FOR_C_LAST_IN_RUN: u64 = 150;
    const C_FOR_C_ID: u64 = 987;
    const C_FOR_OL_I_ID: u64 = 5987;

    // Static assertions converted to runtime checks
    // assert!(C_FOR_C_LAST_IN_LOAD <= 255);
    // assert!(C_FOR_C_LAST_IN_RUN <= 255);
    let delta = C_FOR_C_LAST_IN_LOAD - C_FOR_C_LAST_IN_RUN;
    assert!(65 <= delta && delta <= 119 && delta != 96 && delta != 112);
    // assert!(C_FOR_C_ID <= 1023);
    // assert!(C_FOR_OL_I_ID <= 8191);

    match a {
        255 => {
            if is_load {
                C_FOR_C_LAST_IN_LOAD
            } else {
                C_FOR_C_LAST_IN_RUN
            }
        }
        1023 => C_FOR_C_ID,
        8191 => C_FOR_OL_I_ID,
        _ => u64::MAX, // Bug
    }
}

// ----------------- nurand_int Function -----------------
pub fn nurand_int<const A: u64, const IS_LOAD: bool>(x: u64, y: u64) -> u64 {
    let c = get_constant_for_nurand(A, IS_LOAD);
    if c == u64::MAX {
        panic!("nurand_int bug");
    }
    // Assuming urand_int(x, y) returns a random u64 between x and y
    let rand_a = urand_int(0, A);
    let rand_xy = urand_int(x, y);
    (((rand_a | rand_xy) + c) % (y - x + 1)) + x
}

// ----------------- make_original Function -----------------
pub fn make_original(out: &mut [u8]) {
    let len = out.len();
    assert!(len >= 8);
    let pos = urand_int(0, (len - 8) as u64) as usize;
    out[pos..pos + 8].copy_from_slice(b"ORIGINAL");
}

// ----------------- make_clast Function -----------------
pub fn make_clast(out: &mut [u8], num: usize) -> usize {
    let candidates = [
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
    ];
    assert!(num < 1000);
    let mut len = 0;
    for &i in &[num / 100, (num % 100) / 10, num % 10] {
        let candidate = candidates[i];
        let bytes = candidate.as_bytes();
        let end = len + bytes.len();
        out[len..end].copy_from_slice(bytes);
        len = end;
    }
    debug_assert!(len < Customer::MAX_LAST);
    len
}

// ----------------- make_random_zip Function -----------------
pub fn make_random_zip(out: &mut [u8]) {
    make_random_nstring(&mut out[0..4], 4, 4);
    out[4..9].copy_from_slice(b"11111");
}

// ----------------- make_random_address Function -----------------
pub fn make_random_address(a: &mut Address) {
    make_random_astring(&mut a.street_1, Address::MIN_STREET, Address::MAX_STREET);
    make_random_astring(&mut a.street_2, Address::MIN_STREET, Address::MAX_STREET);
    make_random_astring(&mut a.city, Address::MIN_CITY, Address::MAX_CITY);
    make_random_astring(&mut a.state, Address::STATE, Address::STATE);
    make_random_zip(&mut a.zip);
}

// Placeholder functions for random number generation and string manipulation
use rand::Rng;

use crate::random::small_thread_rng;

pub fn urand_int<T>(x: T, y: T) -> T
where
    T: rand::distr::uniform::SampleUniform + PartialOrd + Copy,
{
    small_thread_rng().random_range(x..=y)
}

pub fn urand_double(min: u64, max: u64, divisor: u64) -> f64 {
    (small_thread_rng().random_range(min..=max) as f64) / divisor as f64
}

pub fn make_random_nstring(out: &mut [u8], min_len: usize, max_len: usize) {
    let len = small_thread_rng().random_range(min_len..=max_len);
    for v in out.iter_mut().take(len) {
        *v = small_thread_rng().random_range(b'0'..=b'9');
    }
}

pub fn make_random_astring(out: &mut [u8], min_len: usize, max_len: usize) {
    const CHARSET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let len = small_thread_rng().random_range(min_len..=max_len);
    for v in out.iter_mut().take(len) {
        *v = CHARSET[small_thread_rng().random_range(0..CHARSET.len())];
    }
}
