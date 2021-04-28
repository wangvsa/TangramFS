#!/usr/bin/bash
#SBATCH -N 2
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

for nodes in {2..2..1}
do
    tasks=$(( 8*$nodes ))
    echo "CHEN" $nodes "nodes"
    for i in {1..3..1}
    do
        srun -N $nodes -n $tasks ./main.out
    done
done
