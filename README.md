
<img src="https://raw.githubusercontent.com/wangvsa/TangramFS/main/tangramfs-logo.png" width="200">

User-level parallel file system with tunable consistency.

[![build](https://github.com/wangvsa/TangramFS/actions/workflows/cmake.yml/badge.svg)](https://github.com/wangvsa/TangramFS/actions/workflows/cmake.yml)

----------

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
3. [SENSEI](https://sensei-insitu.org/learn-more/software.html): No disk I/O, all in-memory analysis.
4. [ADIOS](https://adios2.readthedocs.io/en/latest/): No document on in-situ analysis yet.
