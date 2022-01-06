#!/usr/bin/bash
#SBATCH -N 2
#SBATCH -n 16
#SBATCH -t 00:01:00
#SBATCH -p compute
#SBATCH --account=TG-CCR130058
#SBATCH --job-name="TangramFS"

export OMP_NUM_THREADS=1

source /home/wangvsa/.bashrc

work_dir=/home/wangvsa/sources/TangramFS/install/bin
cd $work_dir

export TANGRAM_PERSIST_DIR=$work_dir
export TANGRAM_BUFFER_DIR=/scratch/$USER/job_$SLURM_JOB_ID

# DEV=ib0, TL=tcp
# DEV=mlx5_2:1, TL=rc_verbs
export TANGRAM_RPC_DEV=mlx5_0:1
export TANGRAM_RPC_TL=dc_mlx5
#export TANGRAM_RMA_DEV=mlx5_2:1
#export TANGRAM_RMA_TL=rc_mlx5
export TANGRAM_RMA_DEV=ib0
export TANGRAM_RMA_TL=tcp


#-Z or random reorder
#-C for rank+1
#-Q set task offset

export TANGRAM_SEMANTICS=1
for nodes in {2..2..1}
do
    echo "CHEN nodes: " $nodes
    procs=$(( 8 * $nodes))

    /home/wangvsa/sources/TangramFS/install/bin/server start &
    sleep 1

    mpiexec -np $procs ./example

    /home/wangvsa/sources/TangramFS/install/bin/server stop
done
rm -f ./tfs.cfg
