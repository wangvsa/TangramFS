#!/usr/bin/bash
#SBATCH -N 4
#SBATCH -n 32
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

for nodes in {1..4..1}
do
    tasks=$(( 4*$nodes ))
    echo "CHEN" $nodes "nodes"
    LD_PRELOAD=/g/g90/wang116/sources/TangramFS/src/.libs/libtangramfs.so srun -N $nodes -n $tasks ./main.out
done
