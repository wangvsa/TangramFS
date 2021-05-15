#!/usr/bin/bash
#SBATCH -N 4
#SBATCH -n 96
#SBATCH -t 00:10:00
#SBATCH -p pbatch
#SBATCH --job-name="hello"

# note: -e fsync() after write; -w write onley, default is write and read;

source /g/g90/wang116/.bash_profile

export I_MPI_EXTRA_FILESYSTEM=on
export I_MPI_EXTRA_FILESYSTEM_LIST=lustre
export OMP_NUM_THREADS=1

work_dir=/g/g90/wang116/sources/TangramFS
cd $work_dir 
export TANGRAM_PERSIST_DIR=$work_dir
export TANGRAM_BUFFER_DIR=/l/ssd


for nodes in {2..4..1}
do
    tasks=$(( 8*$nodes ))

    mpirun -np 1 ./server.out start ./ &
    sleep 2

    srun -N $nodes -n $tasks ./main.out ./

    mpirun -np 1 ./server.out stop ./
    sleep 2
done
