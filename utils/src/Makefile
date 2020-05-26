CC          = gcc
CFLAGS      = -fopenmp -O3 -march=native -DDEBUG -DPROFILING
LDLIBS      = -lhdf5 -lhdf5_hl

ifneq (${HDF5_HOME},)
    HDF5_DIR    = $(HDF5_HOME)
    CFLAGS     += -I$(HDF5_DIR)/include
    LDFLAGS    += -L$(HDF5_DIR)/lib
endif

.PHONY: clean default

default: prv_reader

clean:
	$(RM) prv_reader
