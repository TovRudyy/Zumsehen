HDF_INSTALL	=	${HDF5_HOME}
EXTLIB		=	-L$(HDF_INSTALL)/lib
CC			=	gcc
CFLAGS		=	-fopenmp -O3 -DDEBUG -DPROFILING
LIB			=	-lz -ldl -lm

INCLUDE	=	-I$(HDF_INSTALL)/include
LIBHDF	=	$(EXTLIB) $(HDF_INSTALL)/lib/libhdf5.a

prv_reader: prv_reader.c
	$(CC) $(CFLAGS) prv_reader.c -o prv_reader $(INCLUDE) $(LIBHDF) $(LIB)

clean:
	rm -f prv_reader