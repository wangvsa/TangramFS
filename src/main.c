#include <stdio.h>
#include "mpi.h"
#include "tangramfs.h"

#define MB (1024*1024)

static int DATA_SIZE = 16*MB;

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    int size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    //tfs_init("./", "/l/ssd");
    tfs_init("./", "/tmp");
    TFILE* tf = tfs_open("./test.txt", "w");

    char* data = malloc(sizeof(char)*DATA_SIZE);
    for(int i = 0; i < 5; i++)
        tfs_write(tf, data, DATA_SIZE, size*DATA_SIZE*i+rank*DATA_SIZE);


    tfs_close(tf);
    MPI_Finalize();
    return 0;
}
