#!/usr/bin/bash
#SBATCH -N 1
#SBATCH -n 16
#SBATCH -t 00:10:00
#SBATCH -p pbatch
#SBATCH --job-name="hello"

# note: -e fsync() after write; -w write onley, default is write and read;

source /g/g90/wang116/.bash_profile

export I_MPI_EXTRA_FILESYSTEM=on
export I_MPI_EXTRA_FILESYSTEM_LIST=lustre
export OMP_NUM_THREADS=1

work_dir=/g/g90/wang116/sources/TangramFS/src
cd $work_dir

#for nodes in {2..2..1}
#do
    #tasks=$(( 8*$nodes ))

echo "CHEN" $nodes "nodes"

mpirun -np 1 ./server/server.out start ./ &
sleep 2

mpirun -np 4 ./client/main.out ./

mpirun -np 1 ./server/server.out stop ./

#done
