#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <errno.h> 
#include <inttypes.h>
#include <omp.h>
#include "hdf5.h"

#define TRACE "/home/orudyy/apps/NPB3.4-MZ/NPB3.4-MZ-MPI/extrae/bt-mz.2x2-+A.x.prv"
#define TRACE_HUGE "/home/orudyy/apps/OpenFoam-Ashee/traces/rhoPimplExtrae5TimeSteps.01.FoA.prv"
#define MAXBUF  32768
#define DEF_CHUNK_SIZE  262144000
#define DEF_READ_SIZE   4294967296
#define MIN_BYTES_LINE  16
#define STATE_RECORD_ELEM 7
#define EVENT_RECORD_ELEM 7
#define COMM_RECORD_ELEM 14
#define MASTER_THREAD 0

int retcode;
char retbuff[MAXBUF];
long long __ReadSize = DEF_READ_SIZE;
long long __ChunkSize = DEF_CHUNK_SIZE;
long long __MinElem = DEF_READ_SIZE / MIN_BYTES_LINE;

typedef struct Chunk {
    char **lines;
    size_t nlines;
} Chunk;


typedef enum {
    STATE_RECORD    =   1,
    EVENT_RECORD    =   2,
    COMM_RECORD     =   3,
    INVALID_RECORD  =   -1,
} prv_states;

typedef struct recordArray {
    uint64_t **array;
    size_t rows;
} recordArray;

typedef struct ChunkSet {
    Chunk **chunk_array;
    size_t nchunks;
} ChunkSet;

size_t split(ChunkSet *chunk_p, const char *file, const size_t size, const size_t chunksize, const size_t displ) {
    FILE *fp;
    size_t readB = 0, readcounter = 0;

    if ( (fp = fopen(file, "r")) == NULL) {
        retcode = errno;
        perror("split(...):fopen");
        exit(retcode);
    }
    // Move the file pointer acording tot the displacement
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
        // Check if chunk is aligned to line
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

        // We have processed a chunk
        Chunk *lines_p = malloc(sizeof(Chunk));
        lines_p->lines = calloc(numlines, sizeof(char *));
        lines_p->nlines = numlines;
        memcpy(lines_p->lines, aux_lines_p, numlines*sizeof(char *));
        (chunk_p->chunk_array)[numchunks] = lines_p;
        numchunks++;
    }
    // Realloc the chunk_pointer to its real size (free the last position if it's the case)
    chunk_p->chunk_array = realloc(chunk_p->chunk_array, sizeof(char **)*numchunks);
    chunk_p->nchunks = numchunks;
    free(aux_lines_p);
    free(chunk_buf);
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

// Not constant amount of events in each line.
// Thus, this routine returns a pointer with *nevents rows
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
    size_t nchunks = chunkS->nchunks;
    int nthreads; 
    // OMP_NUM_THREADS must be <= nchunks
    nthreads = omp_get_max_threads() <= nchunks ? omp_get_max_threads() : nchunks;
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
        #ifdef DEBUG
        printf("I am thread %d of %d in the team\n", thread_id, nthreads);
        #endif
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
                    #ifdef DEBUG
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
    state_array->array = realloc(thread_data[MASTER_THREAD], nstates*STATE_RECORD_ELEM*sizeof(uint64_t));
    state_array->rows = nstates;
    event_array->array = realloc(thread_data[MASTER_THREAD+1], nevents*EVENT_RECORD_ELEM*sizeof(uint64_t));
    event_array->rows = nevents;
    comm_array->array = realloc(thread_data[MASTER_THREAD+2], ncomms*COMM_RECORD_ELEM*sizeof(uint64_t));
    comm_array->rows = ncomms;
    size_t st_dspl, ev_dspl, cmm_dspl;
    st_dspl = nrows[MASTER_THREAD*3];
    ev_dspl = nrows[MASTER_THREAD*3+1];
    cmm_dspl = nrows[MASTER_THREAD*3+2];
    for (int i = 1; i < nthreads; i++) {
        memcpy(&(state_array->array[st_dspl]), thread_data[i*3], nrows[i*3]*STATE_RECORD_ELEM*sizeof(uint64_t));
        free(thread_data[i*3]);
        memcpy(&(event_array->array[ev_dspl]), thread_data[i*3+1], nrows[i*3+1]*EVENT_RECORD_ELEM*sizeof(uint64_t));
        free(thread_data[i*3+1]);
        memcpy(&(comm_array->array[cmm_dspl]), thread_data[i*3+2], nrows[i*3+2]*COMM_RECORD_ELEM*sizeof(uint64_t));
        free(thread_data[i*3+2]);
        st_dspl += nrows[i*3];
        ev_dspl += nrows[i*3+1];
        cmm_dspl += nrows[i*3+2];
    }
}

void write_down(const char *output, const recordArray *states, const recordArray *events, const recordArray *comms) {
    FILE * fptr = fopen(output, "w");

    for (int i = 0; i < states->rows; i++) {
        fwrite(&(states->array[i][0]), STATE_RECORD_ELEM*sizeof(uint64_t), 1, fptr);
    }

    for (int i = 0; i < events->rows; i++) {
        // printf("%"PRIu64":", comms->array[i][j]);
        fwrite(&(events->array[i][0]), EVENT_RECORD_ELEM*sizeof(uint64_t), 1, fptr);
    }

    for (int i = 0; i < comms->rows; i++) {
        // printf("%"PRIu64":", comms->array[i][j]);
        fwrite(&(comms->array[i][0]), COMM_RECORD_ELEM*sizeof(uint64_t), 1, fptr);
    }

    fclose(fptr);
}

void get_env() {
    char __debug_buffer[MAXBUF];
    size_t ret = 0;
    char * env;
    ret += sprintf(&__debug_buffer[0], "Env. value ZMSHN_CHUNK_SIZE:\t");
    if ((env = getenv("ZMSHN_CHUNK_SIZE")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s)\n", env);
        __ChunkSize = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld)\n", DEF_CHUNK_SIZE);
        __ChunkSize = DEF_CHUNK_SIZE;
    }
    ret += sprintf(&__debug_buffer[ret], "Env. value ZMSHN_READ_SIZE:\t");
    if ((env = getenv("ZMSHN_READ_SIZE")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s)\n", env);
        __ReadSize = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld)\n", DEF_READ_SIZE);
        __ReadSize = DEF_READ_SIZE;
    }
    #ifdef DEBUG
    printf(__debug_buffer);
    #endif
    __MinElem = __ReadSize / MIN_BYTES_LINE;
}

int main(int argc, char **argv) {
    size_t read_bytes = 0;
    ChunkSet *chunkS = malloc(sizeof(ChunkSet));
    get_env();
    herr_t status;
    hid_t file_id;
    hid_t record_group_id;
    hid_t dataspaces_id[3];
    hid_t props[3];
    hid_t datasets_id[3];
    hsize_t chunk_dimms[3][2] = {__MinElem, STATE_RECORD_ELEM, __MinElem*2,EVENT_RECORD_ELEM, __MinElem, COMM_RECORD_ELEM};
    hsize_t dimms[3][2] = {0, STATE_RECORD_ELEM, 0, EVENT_RECORD_ELEM, 0, COMM_RECORD_ELEM};
    hsize_t max_dimms[3][2] = {H5S_UNLIMITED, STATE_RECORD_ELEM, H5S_UNLIMITED, EVENT_RECORD_ELEM, H5S_UNLIMITED, COMM_RECORD_ELEM};
    /* HDF5 file creation */
    if ( (file_id = H5Fcreate(argv[1], H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        retcode = errno;
        sprintf(retbuff, "H5Fopen(%s, ...) Failed creating the file", argv[1]);
        perror(retbuff);
        exit(retcode);
    }
    /* HDF5 "/Records" group creation */
    record_group_id = H5Gcreate(file_id, "/Records", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    /* HDF5 dataspace creation for STATES, EVENTS and COMMs records */
    dataspaces_id[0] = H5Screate_simple(2, dimms[0], max_dimms[0]);
    dataspaces_id[1] = H5Screate_simple(2, dimms[1], max_dimms[1]);
    dataspaces_id[2] = H5Screate_simple(2, dimms[2], max_dimms[2]);
    /* HDF5 modify dataset creation properties (enable chunking) */
    props[0] = H5Pcreate (H5P_DATASET_CREATE);
    status = H5Pset_chunk(props[0], 2, chunk_dimms[0]);
    props[1] = H5Pcreate (H5P_DATASET_CREATE);
    status = H5Pset_chunk(props[1], 2, chunk_dimms[1]);
    props[2] = H5Pcreate (H5P_DATASET_CREATE);
    status = H5Pset_chunk(props[2], 2, chunk_dimms[2]);
    /* HDF5 create 1 dataset for each record */
    datasets_id[0] = H5Dcreate2 (record_group_id, "STATES", H5T_NATIVE_ULLONG, dataspaces_id[0], H5P_DEFAULT, props[0], H5P_DEFAULT);
    datasets_id[1] = H5Dcreate2 (record_group_id, "EVENTS", H5T_NATIVE_ULLONG, dataspaces_id[1], H5P_DEFAULT, props[1], H5P_DEFAULT);
    datasets_id[2] = H5Dcreate2 (record_group_id, "COMMUNICATIONS", H5T_NATIVE_ULLONG, dataspaces_id[2], H5P_DEFAULT, props[2], H5P_DEFAULT);

    /* offsets to use later when extending HDF5 datasets */
    hsize_t offsets[3][2] = {0, 0, 0, 0, 0, 0};
    recordArray *records[3];
    /* i=0 -> STATES; i=1 -> EVENTS; i=2 -> COMMS */
    for (size_t i = 0; i < 3; i++)
        records[i] = malloc(sizeof(recordArray));
    while ( (read_bytes = split(chunkS, TRACE_HUGE, __ReadSize, __ChunkSize, read_bytes)) > 0) {
        printf("Number of chunks and lines: %u, %u. Procesed bytes: %u\n", chunkS->nchunks, (chunkS->chunk_array[0])->nlines, read_bytes);
        parse_prv_records(chunkS, records[0], records[1], records[2]);
        /* HDF5 extend the dataset dimensions */
        hid_t filespace, memspace;
        for (size_t i = 0; i < 3; i++) {
            dimms[i][1] += records[i]->rows;
            status = H5Dset_extent(datasets_id[i], dimms[i]);   // Extent dataset's dimensions
            filespace = H5Dget_space(datasets_id[i]);   // Refresh dataset data
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
            status = H5Dwrite (datasets_id[i], H5T_NATIVE_ULLONG, memspace, filespace, H5P_DEFAULT, records[i]);    // Append new data to the dataset
            /* Free innecesarry data structures */
            status = H5Sclose (memspace);
            status = H5Sclose (filespace);
            offsets[i][0] += records[i]->rows;
        }
    }
    /* Free innecessary data structures */
    for (size_t i = 0; i < 3; i++) {
        status = H5Dclose(datasets_id[i]);
        status = H5Dclose(dataspaces_id[i]);
        status = H5Pclose(props[i]);
    }
    status = H5Fclose(file_id);
    // hid_t file_id, dataset_id;
    // file_id = H5Fcreate("test.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    // H5Fclose(file_id);
    // for (int i = 0; i < events->rows; i++) {
    //     for (int j = 0; j < EVENT_RECORD_ELEM; j++) {
    //         printf("%"PRIu64":", events->array[i][j]);
    //     }
    //     printf("\n");

    // }

    return 0;
}