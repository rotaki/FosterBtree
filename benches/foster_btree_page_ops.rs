use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fbtree::access_method::fbt::{BTreeKey, FosterBtreePage};
use fbtree::prelude::Page;

/// Build a page with N keys inserted: "key_000", "key_001", ..., "key_{N-1}"
/// Fences: low = "key_" (less than all keys), high = "key_~~" (greater than all keys)
fn make_page(n: usize) -> Page {
    let mut page = Page::new_empty();
    page.init();
    page.set_low_fence(b"key_");
    page.set_high_fence(b"key_~~");
    for i in 0..n {
        let key = format!("key_{:03}", i);
        let val = format!("val_{:03}", i);
        if !page.insert(key.as_bytes(), val.as_bytes(), false) {
            break;
        }
    }
    page
}

fn bench_page_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("FosterBtreePage Ops");

    // Use a page with ~200 keys to exercise binary search
    let page = make_page(200);
    let slot_count = page.slot_count();
    let mid_key = format!("key_{:03}", 100);
    let missing_key = "key_100x"; // not present, between key_100 and key_101

    group.bench_function("binary_search (hit)", |b| {
        b.iter(|| {
            black_box(page.find_slot_id(&BTreeKey::Normal(black_box(mid_key.as_bytes()))));
        });
    });

    group.bench_function("binary_search (miss)", |b| {
        b.iter(|| {
            black_box(
                page.lower_bound_slot_id(&BTreeKey::Normal(black_box(missing_key.as_bytes()))),
            );
        });
    });

    group.bench_function("get_raw_key (iterate all)", |b| {
        b.iter(|| {
            for i in 1..slot_count - 1 {
                black_box(page.get_raw_key(i));
            }
        });
    });

    group.bench_function("get_val (iterate all)", |b| {
        b.iter(|| {
            for i in 1..slot_count - 1 {
                black_box(page.get_val(i));
            }
        });
    });

    group.bench_function("is_ghost (iterate all)", |b| {
        b.iter(|| {
            for i in 1..slot_count - 1 {
                black_box(page.is_ghost(i));
            }
        });
    });

    // Bench ghostify/unghostify cycle
    group.bench_function("ghostify + unghostify (iterate all)", |b| {
        let mut page_mut = make_page(200);
        let sc = page_mut.slot_count();
        b.iter(|| {
            for i in 1..sc - 1 {
                page_mut.ghostify_at(i);
            }
            for i in 1..sc - 1 {
                page_mut.unghostify_at(i);
            }
        });
    });

    // Bench insert + remove cycle
    group.bench_function("insert + remove", |b| {
        let mut page_mut = make_page(100);
        let key = b"key_100";
        let val = b"val_100";
        b.iter(|| {
            page_mut.insert(black_box(key), black_box(val), false);
            page_mut.remove(black_box(key));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_page_ops);
criterion_main!(benches);
