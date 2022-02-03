#!/bin/bash

export TANGRAM_PERSIST_DIR=/home/wangchen/Sources/TangramFS/examples/ml-apps
export TANGRAM_BUFFER_DIR=/tmp
export TANGRAM_RPC_DEV=enp6s0
export TANGRAM_RPC_TL=tcp
export TANGRAM_RMA_DEV=enp6s0
export TANGRAM_RMA_TL=tcp
export TANGRAM_SEMANTICS=1
#export TANGRAM_DEBUG=1

~/Sources/TangramFS/install/bin/server start &

python3 ./flickr8k_tangram.py

~/Sources/TangramFS/install/bin/server stop
