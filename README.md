# TangramFS

User-level parallel file system with tunable consistency

![LOGL](https://png.pngtree.com/png-vector/20201128/ourmid/pngtree-tangram-png-image_2412176.jpg)


### TODO 

1. Local metadata - local interval tree
2. Metadata server and Metadata client - Use mercury+pthread
3. RCP for handle data requests from peers
4. Provide all POSIX I/O wrappers

### Limitations
1. Mercury with MPI plugin can not have the same rank running both as server and client. So have use one dedicated rank as the server.
2. The user application has to be initialized with MPI_Init_thread inorder for Mercury+pthread+MPI plugin to work.

### Naming Convention

1. tfs_*(): APIs provided and used by users
2. tangram_*(): Used internally


### In-situ systems for testing

1. [Damaris](https://project.inria.fr/damaris/documentation/): dedicated cores per node for aggregating I/O and send the data to Visit or ParaView.
2. [XImage](https://project.inria.fr/damaris/documentation/): provide APIs for generating images from HDF5 during simulations.
3. [SENSEI](https://sensei-insitu.org/learn-more/software.html)
