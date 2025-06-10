use crate::page::PAGE_SIZE;

pub fn print_cfg_flags() {
    println!("Page size: {}", PAGE_SIZE);

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
    #[cfg(not(any(feature = "vmcache", feature = "bp_clock")))]
    {
        println!("Using BufferPool with LRU replacement policy");
    }
}
