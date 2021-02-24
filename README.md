# TangramFS

User-level parallel file system with tunable consistency

![LOGL](https://png.pngtree.com/png-vector/20201128/ourmid/pngtree-tangram-png-image_2412176.jpg)


### TODO 

1. Local metadata - local interval tree
2. Metadata server and Metadata client - Use mercury+pthread
3. RCP for handle data requests from peers
4. Provide all POSIX I/O wrappers


### Naming Convention

1. tfs_*(): APIs provided and used by users
2. tangram_*(): Used internally
