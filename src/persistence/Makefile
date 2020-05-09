HDF_INSTALL	=	${HDF5_HOME}
EXTLIB		=	-L$(HDF_INSTALL)/lib
CC			=	gcc
CFLAGS		=	-fopenmp -O3 -march=native -DDEBUG -DPROFILING
LIB			=	-L$(HDF5_HOME)/lib -lz -ldl -lm -lhdf5

INCLUDE	=	-I$(HDF_INSTALL)/include

prv_reader: prv_reader.c
	$(CC) $(CFLAGS) prv_reader.c -o prv_reader $(INCLUDE) $(LIB)

clean:
	rm -f prv_reader