#!/usr/bin/bash
#SBATCH -N 8
#SBATCH -n 64
#SBATCH -t 00:02:00
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

export UCX_NET_DEVICES=hsi0

./server.out start &
sleep 2

echo "Run 1"
mpirun -n 64 -env UCX_NET_DEVICES=hsi0 ./main.out


./server.out stop
