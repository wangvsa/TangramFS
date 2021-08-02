#!/usr/bin/bash
#SBATCH -N 4
#SBATCH -n 32
#SBATCH -t 00:05:00
#SBATCH -p pbatch
#SBATCH --job-name="hello"

# note: -e fsync() after write; -w write onley, default is write and read;

source /g/g90/wang116/.bash_profile


export I_MPI_EXTRA_FILESYSTEM=on
#export I_MPI_EXTRA_FILESYSTEM_LIST=lustre
export OMP_NUM_THREADS=1

work_dir=/g/g90/wang116/sources/TangramFS
cd $work_dir 
export TANGRAM_PERSIST_DIR=$work_dir
export TANGRAM_BUFFER_DIR=/l/ssd

unset UCX_NET_DEVICES

./server.out start &
sleep 1

for nodes in {2..4..1}
do
    procs=$(( 8 * $nodes))
    echo "nodes:" $nodes "procs:" $procs
    for i in {1..5..1}
    do
        srun -N $nodes -n $procs ./main.out
    done
done

./server.out stop

