#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <errno.h> 
#include <inttypes.h>

#ifdef PROFILING
#include <sys/time.h>
#define REGISTER_TIME(__t) gettimeofday(__t, NULL)
#define GET_ELAPSED_TIME(__t1, __t2) ((__t2).tv_sec + (__t2).tv_usec/1000000.0) - ((__t1).tv_sec + (__t1).tv_usec/1000000.0)
#endif

#include <omp.h>
#include "hdf5.h"

#define MAXBUF  32768
#define DEF_CHUNK_SIZE  262144000
#define DEF_READ_SIZE   4294967296
#define MIN_BYTES_LINE  16
#define DEF_MAX_HDF5_CHUNK_SIZE 1638400
#define STATE_RECORD_ELEM 7
#define EVENT_RECORD_ELEM 7
#define COMM_RECORD_ELEM 14
#define MASTER_THREAD 0

int retcode;
herr_t status;

char retbuff[MAXBUF];
long long __ReadSize = DEF_READ_SIZE;
long long __ChunkSize = DEF_CHUNK_SIZE;
long long __MinElem = DEF_READ_SIZE / MIN_BYTES_LINE;
long long __MaxHDF5ChunkSize = DEF_MAX_HDF5_CHUNK_SIZE;

typedef enum {
    STATE_RECORD    =   1,
    EVENT_RECORD    =   2,
    COMM_RECORD     =   3,
    INVALID_RECORD  =   -1,
} prv_states;

typedef struct recordArray {
    uint64_t *array;
    size_t rows;
} recordArray;

typedef struct Chunk {
    char **lines;
    size_t nlines;
} Chunk;

typedef struct ChunkSet {
    Chunk **chunk_array;
    size_t nchunks;
} ChunkSet;

size_t split(ChunkSet *chunk_p, const char *file, const size_t size, const size_t chunksize, const size_t displ) {
    #ifdef PROFILING
    struct timeval start, end;
    REGISTER_TIME(&start);
    #endif
    FILE *fp;
    size_t readB = 0, readcounter = 0;

    if ( (fp = fopen(file, "r")) == NULL) {
        retcode = errno;
        perror("split(...):fopen");
        exit(retcode);
    }
    /* Move the file pointer acording tot the displacement */
    if ( fseek(fp, displ, SEEK_SET) != 0) {
        retcode = errno;
        perror("split(...):fseek");
        exit(retcode);
    }

    char *line_buf = NULL;
    char **aux_lines_p = calloc(chunksize/MIN_BYTES_LINE, sizeof(char *));
    char ** lines_p = NULL;
    char *chunk_buf =malloc(chunksize+MAXBUF);
    chunk_p->chunk_array = calloc(size/chunksize + 1, sizeof(Chunk *)); chunk_p->nchunks = 0;
    size_t numchunks = 0;

    while( readB < size && 0 < (readcounter = fread(chunk_buf, 1, chunksize, fp))) {
        /* Check if chunk is aligned to line */
        if (chunk_buf[readcounter-1] != '\n' || chunk_buf[readcounter-1] != '\0') {
            size_t back = 0;
            int flag = 0;
            for (; back < readcounter && !flag; back++) {
                if (chunk_buf[(readcounter-back)-1] == '\n') {
                    readcounter -= back;
                    fseek(fp, -back, SEEK_CUR); // Move the file pointer to the start of the line
                    flag = 1;
                }
            }
        }
        readB += readcounter;

        size_t line_start_pos = 0;
        size_t numlines, i, lenght_line;
        numlines = i = lenght_line = 0;
        for (; i < readcounter; i++, lenght_line++) {
            // Check whether we've found one line
            if (chunk_buf[i] == '\n') {
                line_buf = malloc(lenght_line);  // Allocate memory to contain the line
                memcpy(line_buf, chunk_buf+line_start_pos, lenght_line);
                line_buf[lenght_line-1] = '\0';   // Change the last character '\n' for a null character ('\0') to mark end of string
                aux_lines_p[numlines] = line_buf;
                numlines++;
                line_start_pos = i+1;   // The next line starts at the next position
                lenght_line = 0; // Reset line length counter
            }
        }

        /* We have processed a chunk */
        Chunk *lines_p = malloc(sizeof(Chunk));
        lines_p->lines = calloc(numlines, sizeof(char *));
        lines_p->nlines = numlines;
        memcpy(lines_p->lines, aux_lines_p, numlines*sizeof(char *));
        (chunk_p->chunk_array)[numchunks] = lines_p;
        numchunks++;
    }
    /* Realloc the chunk_pointer to its real size (free the last position if it's the case) */
    chunk_p->chunk_array = realloc(chunk_p->chunk_array, sizeof(char **)*numchunks);
    chunk_p->nchunks = numchunks;
    free(aux_lines_p);
    free(chunk_buf);
    #ifdef PROFILING
    REGISTER_TIME(&end);
    printf("Elapsed time reading 1 block:    %.3f sec\n", GET_ELAPSED_TIME(start, end));
    #endif
    return readB;
}


prv_states get_record_type(char *line) {
    char record = line[0];
    if ('0' <= record && '9' >= record) {
        return record - '0';
    }
    else {
        return INVALID_RECORD;
    }
}

uint64_t *parse_state(char *line) {
    char *next, *rest = line;
    uint64_t *state = calloc(STATE_RECORD_ELEM, sizeof(uint64_t));
    size_t i = 0;
    //We discard the record type:
    strtok_r(rest, ":", &rest);
    while ((next = strtok_r(rest, ":", &rest))) {
        state[i] = (uint64_t) atoll(next);
        i++;
    }
    return state;
}

/* Not constant amount of events in each line. Thus, this routine returns a pointer with *nevents rows */
void parse_event(char *line, uint64_t **events, size_t *nevents) {
    char *next, *nnext, *rest = line;
    uint64_t *event = calloc(EVENT_RECORD_ELEM, sizeof(uint64_t));
    //We discard the record type:
    strtok_r(rest, ":", &rest);
    for (size_t i = 0; i < EVENT_RECORD_ELEM; i++) {
        next = strtok_r(rest, ":", &rest);
        event[i] = (uint64_t) atoll(next);
    }
    events[(*nevents)] = event;
    (*nevents)++;
    while ((next = strtok_r(rest, ":", &rest)) && (nnext = strtok_r(rest, ":", &rest))) {
        event = calloc(EVENT_RECORD_ELEM, sizeof(uint64_t));
        event = (uint64_t *)memcpy(event, events[(*nevents)-1], 5*sizeof(uint64_t));
        event[EVENT_RECORD_ELEM-2] = (uint64_t) atoll(next);
        event[EVENT_RECORD_ELEM-1] = (uint64_t) atoll(nnext);
        events[(*nevents)] = event;
        (*nevents)++;
    }
}

uint64_t *parse_comm(char *line) {
    char *next, *rest = line;
    uint64_t *comm = calloc(COMM_RECORD_ELEM, sizeof(uint64_t));
    size_t i = 0;
    //We discard the record type:
    strtok_r(rest, ":", &rest);
    while ((next = strtok_r(rest, ":", &rest))) {
        comm[i] = (uint64_t) atoll(next);
        i++;
    }
    return comm;
}

void parse_prv_records(ChunkSet * chunkS, recordArray *state_array, recordArray *event_array, recordArray *comm_array) {
    #ifdef PROFILING
    struct timeval start, end;
    REGISTER_TIME(&start);
    #endif
    size_t nchunks = chunkS->nchunks;
    int nthreads; 
    // OMP_NUM_THREADS must be <= nchunks
    nthreads = omp_get_max_threads() <= nchunks ? omp_get_max_threads() : nchunks;
    #ifdef DEBUG
    printf("DEBUG: Parallel parser using %d threads\n", nthreads);
    #endif
    uint64_t **thread_data[nthreads*3];
    size_t nrows[nthreads*3];
    for (int i = 0; i < nthreads; i++) {
        thread_data[i*3] = malloc((__MinElem/nthreads)*sizeof(uint64_t **));
        nrows[i*3] = 0;
        thread_data[i*3+1] = malloc((__MinElem/nthreads*2)*sizeof(uint64_t **));
        nrows[i*3+1] = 0;
        thread_data[i*3+2] = malloc((__MinElem/nthreads)*sizeof(uint64_t **));
        nrows[i*3+2] = 0;
    }
    size_t nstates = 0, nevents = 0, ncomms = 0;

    #pragma omp parallel num_threads(nthreads) shared(thread_data, nrows, nthreads, chunkS), reduction(+: nstates, nevents, ncomms)
    {
        int thread_id = omp_get_thread_num();
        nthreads = omp_get_num_threads();
        size_t *auxnstates, *auxnevents, *auxncomms;
        auxnstates = &nrows[thread_id*3];
        auxnevents = &nrows[thread_id*3+1];
        auxncomms = &nrows[thread_id*3+2];
        uint64_t **states = thread_data[thread_id*3];
        uint64_t **events = thread_data[thread_id*3+1];
        uint64_t **comms = thread_data[thread_id*3+2];
        #pragma omp for schedule (static)
        for (int i = 0; i < nchunks; i++) {
            char **lines = (chunkS->chunk_array[i])->lines;
            size_t nlines = (chunkS->chunk_array[i])->nlines;
            for (int j = 0; j < nlines; j++) {
                prv_states result = get_record_type(lines[j]);
                switch(result) {
                    case STATE_RECORD   :
                    states[nstates] = parse_state(lines[j]);
                    nstates++;
                    break;

                    case EVENT_RECORD   :
                    parse_event(lines[j], events, &nevents);
                    break;
            
                    case COMM_RECORD    :
                    comms[ncomms] = parse_comm(lines[j]);
                    ncomms++;
                    break;

                    case INVALID_RECORD :
                    #ifdef WARNING
                    printf("WARNING: Invalid/Not supported record type in the trace\n");
                    #endif
                    break;
                }
            }
            *auxnstates = nstates;
            *auxnevents = nevents;
            *auxncomms = ncomms;
        }
    }
    /* After parsing the block it copies the data to a contiguous memory location */
    state_array->array = malloc(nstates*STATE_RECORD_ELEM*sizeof(uint64_t));
    state_array -> rows = nstates;
    event_array->array = malloc(nevents*EVENT_RECORD_ELEM*sizeof(uint64_t));
    event_array -> rows = nevents;
    comm_array->array = malloc(ncomms*COMM_RECORD_ELEM*sizeof(uint64_t));
    comm_array -> rows = ncomms;
    size_t st_dspl, ev_dspl, cmm_dspl;
    st_dspl = ev_dspl = cmm_dspl = 0;
    for (size_t i = 0; i < nthreads; i++) {
        for (size_t j = 0; j < nrows[i*3]; j++) {
            for (size_t k = 0; k < STATE_RECORD_ELEM; k++) {
                (state_array->array)[(j+st_dspl)*STATE_RECORD_ELEM + k] = thread_data[i*3][j][k];   // (state_array->array)[j+st_dspl][k]
            }
            free(thread_data[i*3][j]);
        }
        st_dspl += nrows[i*3];
        free(thread_data[i*3]);
        for (size_t j = 0; j < nrows[i*3+1]; j++) {
            for (size_t k = 0; k < EVENT_RECORD_ELEM; k++) {
                (event_array->array)[j*EVENT_RECORD_ELEM + k] = thread_data[i*3+1][j][k];
            }
            free(thread_data[i*3+1][j]);
        }
        ev_dspl += nrows[i*3+1];
        free(thread_data[i*3+1]);
        for (size_t j = 0; j < nrows[i*3+2]; j++) {
            for (size_t k = 0; k < COMM_RECORD_ELEM; k++) {
                (comm_array->array)[(j+cmm_dspl)*COMM_RECORD_ELEM + k] = thread_data[i*3+2][j][k];
            }
            free(thread_data[i*3+2][j]);
        }
        cmm_dspl += nrows[i*3+2];
        free(thread_data[i*3+2]);
    }
    #ifdef PROFILING
    REGISTER_TIME(&end);
    printf("Elapsed time parsing 1 block:    %.3f sec\n", GET_ELAPSED_TIME(start, end));
    #endif
}

void get_env() {
    char __debug_buffer[MAXBUF];
    size_t ret = 0;
    char * env;
    ret += sprintf(&__debug_buffer[0], "DEBUG: Env. value ZMSHN_CHUNK_SIZE:\t");
    if ((env = getenv("ZMSHN_CHUNK_SIZE")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s)\n", env);
        __ChunkSize = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld)\n", DEF_CHUNK_SIZE);
        __ChunkSize = DEF_CHUNK_SIZE;
    }
    ret += sprintf(&__debug_buffer[ret], "DEBUG: Env. value ZMSHN_READ_SIZE:\t");
    if ((env = getenv("ZMSHN_READ_SIZE")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s)\n", env);
        __ReadSize = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld)\n", DEF_READ_SIZE);
        __ReadSize = DEF_READ_SIZE;
    }
    ret += sprintf(&__debug_buffer[ret], "DEBUG: Env. value ZMSHN_MAX_HDF5_CHUNK_SIZE:\t");
    if ((env = getenv("ZMSHN_MAX_HDF5_CHUNK_SIZE")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s)\n", env);
        __MaxHDF5ChunkSize = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld)\n", DEF_MAX_HDF5_CHUNK_SIZE);
        __MaxHDF5ChunkSize = DEF_MAX_HDF5_CHUNK_SIZE;
    }
    #ifdef DEBUG
    printf(__debug_buffer);
    #endif
    __MinElem = __ReadSize / MIN_BYTES_LINE;
}

hid_t create_HDF5(const char * name) {
    hid_t file_id;
    /* HDF5 file creation */
    file_id = H5Fcreate(name, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    return file_id;
}

hid_t create_HDF5_group(const hid_t parent, const char * name) {
    hid_t group_id;
    group_id = H5Gcreate(parent, name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    return group_id;
}

/* Returns one dataset for STATES, EVENTS and COMM respectively */
void create_datasets(const hid_t file, const hsize_t dimms[3][2], hid_t datasets[3], size_t size) {
    hsize_t nrows;
    nrows = ((size>>6) > __MaxHDF5ChunkSize) ? __MaxHDF5ChunkSize : (size>>6);
    hsize_t chunk_dimms[3][2] = {nrows, STATE_RECORD_ELEM, nrows,EVENT_RECORD_ELEM, nrows, COMM_RECORD_ELEM};
    hsize_t max_dimms[3][2] = {H5S_UNLIMITED, STATE_RECORD_ELEM, H5S_UNLIMITED, EVENT_RECORD_ELEM, H5S_UNLIMITED, COMM_RECORD_ELEM};   
    for (size_t i = 0; i < 3; i++) {
        /* HDF5 dataspace creation for STATES, EVENTS and COMMs records */
        hid_t dataspaces = H5Screate_simple(2, dimms[i], max_dimms[i]);
        /* HDF5 modify dataset creation properties (enable chunking) */
        hid_t props = H5Pcreate (H5P_DATASET_CREATE);
        status = H5Pset_chunk(props, 2, chunk_dimms[i]);
        /* HDF5 create 1 dataset for each record */
        switch (i) 
        {
            case 0: // STATE
                datasets[i] = H5Dcreate2 (file, "STATES", H5T_NATIVE_ULLONG, dataspaces, H5P_DEFAULT, props, H5P_DEFAULT);
                break;
            case 1: // EVENT
                datasets[i] = H5Dcreate2 (file, "EVENTS", H5T_NATIVE_ULLONG, dataspaces, H5P_DEFAULT, props, H5P_DEFAULT);
                break;
            case 2: // COMM
                datasets[i] = H5Dcreate2 (file, "COMMUNICATIONS", H5T_NATIVE_ULLONG, dataspaces, H5P_DEFAULT, props, H5P_DEFAULT);
            default:
                break;
        }
        status = H5Sclose(dataspaces);
        status = H5Pclose(props);
    }
}

void write_records_to_HDF5(const hid_t datasets[3], hsize_t dimms[3][2], hsize_t offsets[3][2],  recordArray *records[3]) {
    #ifdef PROFILING
    struct timeval start, end;
    REGISTER_TIME(&start);
    #endif
    /* HDF5 extend the dataset dimensions */
    hid_t filespace, memspace;
    for (size_t i = 0; i < 3; i++) {
        dimms[i][0] += records[i]->rows;    // Update dimensions
        status = H5Dset_extent(datasets[i], dimms[i]);   // Extent dataset's dimensions
        filespace = H5Dget_space(datasets[i]);   // Refresh dataset data
        hsize_t dimext[2];
        switch (i) {
        case 0: // STATE
            dimext[0] = records[i]->rows; dimext[1] = STATE_RECORD_ELEM;
            break;
        case 1: // EVENT
            dimext[0] = records[i]->rows; dimext[1] = EVENT_RECORD_ELEM;
            break;
        case 2: // COMM
            dimext[0] = records[i]->rows; dimext[1] = COMM_RECORD_ELEM;
        default:
            break;
        }
        status = H5Sselect_hyperslab (filespace, H5S_SELECT_SET, offsets[i], NULL, dimext, NULL);    // Define where to add the new data in the dataset
        memspace = H5Screate_simple (2, dimext, NULL);  // Allocate memory for the new data
        status = H5Dwrite (datasets[i], H5T_NATIVE_ULLONG, memspace, filespace, H5P_DEFAULT, records[i]->array);    // Append new data to the dataset
        // if (i == 1) {
        //     for (int k = 0; k < records[i]->rows; k++) {
        //     for (int j = 0; j < EVENT_RECORD_ELEM; j++) {
        //         printf("%"PRIu64":", records[i]->array[k][j]);
        //     }
        //     printf("\n");
        //     }
        // }
        /* Free innecesarry data structures */
        status = H5Sclose (memspace);
        status = H5Sclose (filespace);
        offsets[i][0] += records[i]->rows;  // Update the offsets
    }
    #ifdef PROFILING
    REGISTER_TIME(&end);
    printf("Elapsed time writing 1 block:    %.3f sec\n", GET_ELAPSED_TIME(start, end));
    #endif
}

void parse_prv_to_hdf5(const char * prv, const char * hdf5) {
    /* Parsing data structures */
    size_t read_bytes = 0;
    ChunkSet *chunkS = malloc(sizeof(ChunkSet));
    recordArray *records[3];
    for (size_t i = 0; i < 3; i++)  // i=0 -> STATES; i=1 -> EVENTS; i=2 -> COMMS
        records[i] = malloc(sizeof(recordArray));
    /* Writing to HDF5 data structures */
    hid_t file_id, record_group_id, datasets_id[3];
    hsize_t dimms[3][2] = {0, STATE_RECORD_ELEM, 0, EVENT_RECORD_ELEM, 0, COMM_RECORD_ELEM};
    hsize_t offsets[3][2] = {0, 0, 0, 0, 0, 0}; // Offsets to use later when extending HDF5 datasets
    int first = 1;  // Bool
    file_id = create_HDF5(hdf5);
    record_group_id = create_HDF5_group(file_id, "/RECORDS");
    while ( (read_bytes = split(chunkS, prv, __ReadSize, __ChunkSize, read_bytes)) > 0) {
        #ifdef DEBUG
        printf("DEBUG: Generated %u chunks after processing %.2f MB\n", chunkS->nchunks, (float)read_bytes/(1024*1024));
        #endif
        parse_prv_records(chunkS, records[0], records[1], records[2]);
        if (first) {
            create_datasets(record_group_id, dimms, datasets_id, read_bytes);
            first = 0;
        }
        write_records_to_HDF5(datasets_id, dimms, offsets, records);
    }
    /* Freeing data structures */
    for (size_t i = 0; i < 3; i++) {
        free(records[i]->array);
        free(records[i]);
        status = H5Dclose(datasets_id[i]);
    }
    free(chunkS);
    status = H5Fclose(file_id);
}

void Usage(int argc) {
    if (argc < 3) {
        printf("Usage: ./prv_reader prv_file hdf5_name\nThis parser parses prv_file generating the hdf5_name file in HDF5 format. The HDF5 file contains under /RECORDS 3 datasets, one for each record found in the prv file: STATES, EVENTS and COMMUNICATIONS.\n");
        exit(EXIT_FAILURE);
    }
}
int main(int argc, char **argv) {
    Usage(argc);
    get_env();  // Get environment variables
    parse_prv_to_hdf5(argv[1], argv[2]);
    return EXIT_SUCCESS;
}