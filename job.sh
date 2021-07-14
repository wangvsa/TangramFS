#!/usr/bin/bash
#SBATCH -N 10
#SBATCH -n 160
#SBATCH -t 00:05:00
#SBATCH -p pbatch
#SBATCH --job-name="hello"

# note: -e fsync() after write; -w write onley, default is write and read;

source /g/g90/wang116/.bash_profile


export I_MPI_EXTRA_FILESYSTEM=on
#export I_MPI_EXTRA_FILESYSTEM_LIST=lustre
export OMP_NUM_THREADS=1

work_dir=/p/lscratchh/wang116/applications/TangramFS
cd $work_dir 
export TANGRAM_PERSIST_DIR=$work_dir
export TANGRAM_BUFFER_DIR=/l/ssd

unset UCX_NET_DEVICES
#export UCX_NET_DEVICES=qib0:1,qib1:1

UCX_NET_DEVICES=hsi0,hsi1 ./server.out start &
sleep 2

for nodes in {2..10..2}
do
    procs=$(( 16 * $nodes))
    echo "nodes:" $nodes "procs:" $procs
    for i in {1..1..1}
    do
        srun -N $nodes -n $procs ./main.out
    done
done

./server.out stop
