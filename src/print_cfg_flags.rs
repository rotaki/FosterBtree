pub fn print_cfg_flags() {
    #[cfg(feature = "no_tree_hint")]
    {
        println!("Tree hint disabled");
    }
    #[cfg(not(feature = "no_tree_hint"))]
    {
        println!("Tree hint enabled");
    }
    #[cfg(feature = "no_bp_hint")]
    {
        println!("BP hint disabled");
    }
    #[cfg(not(feature = "no_bp_hint"))]
    {
        println!("BP hint enabled");
    }
    #[cfg(feature = "vmcache")]
    {
        println!("Using VMCache");
    }
    #[cfg(feature = "bp_clock")]
    {
        println!("Using BufferPool with clock replacement policy");
    }
    #[cfg(feature = "bp_predicache")]
    {
        println!("Using Predictive Translation (PT) buffer pool");
    }
    #[cfg(feature = "bp_pt2")]
    {
        println!("Using Predictive Translation (PT) buffer pool with two hashes");
    }
    #[cfg(feature = "bp_pt2_bucket")]
    {
        println!("Using Predictive Translation (PT) two-hash bucket-validate-first buffer pool");
    }
    #[cfg(feature = "bp_pt4_bucket")]
    {
        println!("Using Predictive Translation (PT) four-hash bucket-validate-first buffer pool");
    }
    #[cfg(feature = "bp_pt_tlb")]
    {
        println!("Using Predictive Translation (PT) TLB-assisted buffer pool");
    }
    #[cfg(feature = "bp_pt_tlb_only")]
    {
        println!("Using Predictive Translation (PT) TLB-only buffer pool");
    }
    #[cfg(feature = "bp_pt_tlb_only_keys")]
    {
        println!(
            "Using Predictive Translation (PT) TLB-only experimental separate-key buffer pool"
        );
    }
    #[cfg(feature = "bp_tlb")]
    {
        println!("Using standalone TLB buffer pool (unified hash)");
    }
    #[cfg(feature = "bp_overflow")]
    {
        println!("Using OptimisticPageMap (custom HT) buffer pool");
    }
    #[cfg(not(any(
        feature = "vmcache",
        feature = "bp_clock",
        feature = "bp_predicache",
        feature = "bp_pt2",
        feature = "bp_pt2_bucket",
        feature = "bp_pt4_bucket",
        feature = "bp_pt_tlb",
        feature = "bp_pt_tlb_only",
        feature = "bp_pt_tlb_only_keys",
        feature = "bp_tlb",
        feature = "bp_overflow",
    )))]
    {
        println!("Using BufferPool with LRU replacement policy");
    }
}
