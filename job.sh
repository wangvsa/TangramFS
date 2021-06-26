#!/usr/bin/bash
#SBATCH -N 3
#SBATCH -n 96
#SBATCH -t 00:02:00
#SBATCH -p pdebug
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

UCX_NET_DEVICES=eno1 ./server.out start &
sleep 2

for nodes in {3..3..1}
do
    echo "CHEN" $nodes
    tasks=$((6*$nodes ))
    srun -n 24 ./main.out
done

./server.out stop
