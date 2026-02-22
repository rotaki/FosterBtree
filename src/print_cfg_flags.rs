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
    #[cfg(feature = "bp_pt")]
    {
        println!("Using Predictive Translation (PT) buffer pool");
    }
    #[cfg(feature = "bp_dashmap")]
    {
        println!("Using DashMap baseline buffer pool");
    }
    #[cfg(feature = "bp_hashmap")]
    {
        println!("Using HashMap baseline buffer pool");
    }
    #[cfg(not(any(
        feature = "vmcache",
        feature = "bp_clock",
        feature = "bp_pt",
        feature = "bp_dashmap",
        feature = "bp_hashmap"
    )))]
    {
        println!("Using BufferPool with LRU replacement policy");
    }
}
