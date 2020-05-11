#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <errno.h> 
#include <inttypes.h>
#include <assert.h>

#ifdef PROFILING
#include <sys/time.h>
#define REGISTER_TIME(__t) gettimeofday(__t, NULL)
#define GET_ELAPSED_TIME(__t1, __t2) ((__t2).tv_sec + (__t2).tv_usec/1000000.0) - ((__t1).tv_sec + (__t1).tv_usec/1000000.0)
#endif

#include <omp.h>
#include "hdf5.h"

#define MAXBUF  32768
#define DEF_READ_SIZE   1024*1024*1024
#define DEF_EXTEND_EVENT_CAPACITY   2
#define DEF_COMPRESSION_LEVEL   0
#define AV_LINE_LENGTH_BYTES  32
#define MIN_BYTES_STATES_LINE   16
#define MIN_BYTES_EVENTS_LINE   32
#define MIN_BYTES_COMMS_LINE    64
#define MIN_EVENT_CAP   512
#define DEF_MAX_HDF5_CHUNK_SIZE 1198372
#define DEF_MIN_HDF5_CHUNK_SIZE 2048
#define STATE_RECORD_ELEM 7
#define EVENT_RECORD_ELEM 7
#define COMM_RECORD_ELEM 14
#define MASTER_THREAD 0
#define TRUE    1
#define FALSE   0


int retcode;
herr_t status;

char retbuff[MAXBUF];
long long __ReadSize = DEF_READ_SIZE;
long long __MaxHDF5ChunkSize = DEF_MAX_HDF5_CHUNK_SIZE;
long long __MinHDF5ChunkSize = DEF_MIN_HDF5_CHUNK_SIZE;
long long __ExtendEventCapacity = DEF_EXTEND_EVENT_CAPACITY;
int __CompressionLevel = DEF_COMPRESSION_LEVEL;

typedef enum {
    STATE_RECORD    =   1,
    EVENT_RECORD    =   2,
    COMM_RECORD     =   3,
    INVALID_RECORD  =   -1,
} recordTypes_enum;

typedef struct uint64_Array {
    uint64_t *array;    // array[]
    size_t elements;
    size_t capacity;
} uint64_Array;

typedef struct file_data {
    char *buffer;
    size_t counter_states;
    size_t counter_events;
    size_t counter_comms;
    size_t counter_unknowns;
    uint16_t *lengths;
    int8_t *types;
} file_data;

recordTypes_enum get_record_type(char *line) {
    char record = line[0];
    if ('0' <= record && record <= '3') {
        return record - '0';
    }
    else {
        return INVALID_RECORD;
    }
}

size_t align_to_line(FILE *fp, const size_t read_bytes, const char* buf) {
    size_t back = 0;
    char xivato = buf[read_bytes-1];
    if (xivato != '\n' && xivato != '\0') {
    uint8_t stop = FALSE;
        for (; back < read_bytes && !stop; back++) {
            xivato = buf[(read_bytes-back)-1];
            if (xivato == '\n') {
                fseek(fp, -back, SEEK_CUR); // Move the file pointer to the start of the line
                stop = TRUE;
            }
        }
    }
    return back;
}

size_t parse_state(char *line, uint64_t *state) {
    char *next, *rest = line;
    size_t i = 0;
    // Discards the record type:
    strtok_r(rest, ":", &rest);
    while ((next = strtok_r(rest, ":", &rest))) {
        state[i] = (uint64_t) atoll(next);
        i++;
    }
    return i;
}

/* Not constant amount of events in each line. Thus, this routine returns a pointer with *nevents rows */
size_t parse_event(char *line, uint64_t **events) {
    char *next, *nnext, *rest = line;
    size_t nevents = 0;
    uint64_t *event = (uint64_t *)calloc(EVENT_RECORD_ELEM, sizeof(uint64_t));
    //We discard the record type:
    strtok_r(rest, ":", &rest);
    for (size_t i = 0; i < EVENT_RECORD_ELEM; i++) {
        next = strtok_r(rest, ":", &rest);
        event[i] = (uint64_t) atoll(next);
    }
    events[nevents] = event;
    nevents++;
    while ((next = strtok_r(rest, ":", &rest)) && (nnext = strtok_r(rest, ":", &rest))) {
        event = (uint64_t *)calloc(EVENT_RECORD_ELEM, sizeof(uint64_t));
        for (size_t __i = 0; __i < 5; __i++)
            event[__i] = events[nevents-1][__i];
        event[EVENT_RECORD_ELEM-2] = (uint64_t) atoll(next);
        event[EVENT_RECORD_ELEM-1] = (uint64_t) atoll(nnext);
        events[nevents] = event;
        nevents++;
    }
    return nevents;
}

size_t parse_comm(char *line, uint64_t *comm) {
    char *next, *rest = line;
    size_t i = 0;
    //We discard the record type:
    strtok_r(rest, ":", &rest);
    while ((next = strtok_r(rest, ":", &rest))) {
        comm[i] = (uint64_t) atoll(next);
        i++;
    }
    return i;
}

/* Takes the read data from the .prv file and fills the states, events and comms arrays */
void data_parser(const file_data *file_d, uint64_Array *states, uint64_Array *events, uint64_Array *comms) {
    #ifdef PROFILING
    struct timeval start, end;
    REGISTER_TIME(&start);
    #endif
    size_t ret;
    size_t st_cap, ev_cap, cmm_cap;
    size_t st_elem, ev_elem, cmm_elem;
    uint64_t *states_a, *events_a, *comms_a;
    st_cap = (states->capacity) = (file_d->counter_states)*STATE_RECORD_ELEM;
    states_a = (states->array) = (uint64_t *)malloc((states->capacity)*sizeof(uint64_t));
    ev_cap = (events->capacity) = (file_d->counter_events)*EVENT_RECORD_ELEM+MIN_EVENT_CAP;
    events_a = (events->array) = (uint64_t *)malloc((events->capacity)*sizeof(uint64_t));
    cmm_cap = (comms->capacity) = (file_d->counter_comms)*COMM_RECORD_ELEM;
    comms_a = (comms->array) = (uint64_t *)malloc((comms->capacity)*sizeof(uint64_t));
    st_elem = ev_elem = cmm_elem = (states->elements) = (events->elements) = (comms->elements) = 0;
    char * buffer= (file_d->buffer);
    uint16_t *lengths = (file_d->lengths);
    int8_t *types = (file_d->types);
    size_t total_lines;
    size_t iterator;
    size_t offset;
    iterator = offset = 0;
    total_lines = (file_d->counter_states) + (file_d->counter_events) + (file_d->counter_comms) + (file_d->counter_unknowns);
    uint64_t aux_buf[512];
    uint64_t **aux_events = (uint64_t **)malloc(128*sizeof(uint64_t *));
    while(iterator < total_lines) {
        switch(types[iterator]) {
            case STATE_RECORD   :
            ret = parse_state(&buffer[offset], aux_buf);
            assert(ret == STATE_RECORD_ELEM);
            for (size_t __i = 0; __i < ret; __i++)
                states_a[st_elem*STATE_RECORD_ELEM+__i] = aux_buf[__i];
            st_elem++;
            break;

            case EVENT_RECORD   :
            ret = parse_event(&buffer[offset], aux_events);
            for (size_t __i = 0; __i < ret; __i++, ev_elem++) {
                for (size_t __j = 0; __j < EVENT_RECORD_ELEM; __j++)
                    events_a[ev_elem*EVENT_RECORD_ELEM+__j] = aux_events[__i][__j];
                free(aux_events[__i]);
            }
            if ((ev_elem*EVENT_RECORD_ELEM + EVENT_RECORD_ELEM*64) >= ev_cap) {   // Checks if capacity of events buffer is enough to hold more rows
                ev_cap = ev_cap*__ExtendEventCapacity;
                (events->capacity) = ev_cap;
                uint64_t *replacement = (uint64_t *)malloc(ev_cap*sizeof(uint64_t));
                memcpy(replacement, (events->array), ev_elem*EVENT_RECORD_ELEM*sizeof(uint64_t));
                free(events->array);
                events_a = (events->array) = replacement;
            }
            break;

            case COMM_RECORD    :
            ret = parse_comm(&buffer[offset], aux_buf);
            assert(ret == COMM_RECORD_ELEM);
            for (size_t __i = 0; __i < ret; __i++)
                comms_a[cmm_elem*STATE_RECORD_ELEM+__i] = aux_buf[__i];
            cmm_elem++;
            break;
            default:
            break;
        }
        offset += lengths[iterator];
        iterator++;
    }
    free(aux_events);
    (states->elements) = st_elem;
    (events->elements) = ev_elem;
    (comms->elements) = cmm_elem;
    #ifdef PROFILING
    REGISTER_TIME(&end);
    printf("Elapsed time parsing 1 block:    %.3f sec\n", GET_ELAPSED_TIME(start, end));
    #endif
}

/* Reads <MaxBytesRead> aligned to line of the file <file> with an offset. Preprocess the read data saving file's structure in <file_d>.
/* Returns the amount of bytes it has processed from the file. */
size_t read_and_preprocess( const char *file, const size_t MaxBytesRead, const size_t offset, file_data *file_d) {
    #ifdef PROFILING
    struct timeval start, end;
    REGISTER_TIME(&start);
    #endif
    FILE *fp;
    size_t read_bytes_counter, ret;
    read_bytes_counter = ret = 0;

    if ( (fp = fopen(file, "r")) == NULL) {
        retcode = errno;
        perror("split(...):fopen");
        exit(retcode);
    }
    /* Move the file pointer acording tot the displacement */
    if ( fseek(fp, offset, SEEK_SET) != 0) {
        retcode = errno;
        perror("split(...):fseek");
        exit(retcode);
    }

    file_d->lengths = (uint16_t *)malloc(MaxBytesRead/AV_LINE_LENGTH_BYTES * sizeof(uint16_t));
    file_d->types = (int8_t *)malloc(MaxBytesRead/AV_LINE_LENGTH_BYTES * sizeof(int8_t));

    size_t counter_states, counter_events, counter_comms, counter_unknowns;
    counter_states = counter_events = counter_comms = counter_unknowns = 0;
    recordTypes_enum record_t;
    int8_t start_of_line = TRUE;  // Bool indicating start of a new line
    file_d->buffer = (char *)malloc(MaxBytesRead+MAXBUF); // Memory buffer where to store disk's data
    if ((read_bytes_counter = fread(file_d->buffer, 1, MaxBytesRead, fp)) > 0) {
        read_bytes_counter = read_bytes_counter - align_to_line(fp, read_bytes_counter, file_d->buffer);   //  Ensures that read block is aligned to a line
        ret += read_bytes_counter;
        size_t counter_lines;
        size_t counter_line_length;
        size_t iterator;
        for(counter_line_length = 1, counter_lines = iterator = 0; iterator <= read_bytes_counter; iterator++, counter_line_length++) {
            if (start_of_line) {
                record_t = get_record_type(&(file_d->buffer)[iterator]);
                switch(record_t) {
                    case STATE_RECORD   :
                    counter_states++;
                    break;
                    case EVENT_RECORD   :
                    counter_events++;
                    break;
                    case COMM_RECORD    :
                    counter_comms++;
                    break;
                    default :
                    counter_unknowns++;
                    #ifdef WARNING
                    printf("WARNING: Invalid/Not supported record type in the trace\n");
                    #endif
                    break;
                }
                (file_d->types)[counter_lines] = record_t;
                start_of_line = FALSE;
            }
            /* Check whether it've reached line's end */
            if ((file_d->buffer)[iterator] == '\n') {
                (file_d->buffer)[iterator] = '\0';  // Replaces '\n' with NULL

                (file_d->lengths)[counter_lines] = counter_line_length;
                start_of_line = TRUE;
                counter_lines++;
                counter_line_length = 0;
            }
        }
    }
    file_d->counter_states = counter_states;
    file_d->counter_events = counter_events;
    file_d->counter_comms = counter_comms;
    file_d->counter_unknowns = counter_unknowns;
    size_t total_elements = counter_states + counter_events + counter_comms + counter_unknowns;
    for (size_t o = 0; o < total_elements; o++) 
    /* Realloc file_structure size to minimize memory consumption */
    file_d->lengths = realloc(file_d->lengths, total_elements*sizeof(uint16_t));
    file_d->types = realloc(file_d->types, total_elements*sizeof(int8_t));
    fclose(fp);
    #ifdef PROFILING
    REGISTER_TIME(&end);
    printf("Elapsed time read & pre-process 1 block:    %.3f sec\n", GET_ELAPSED_TIME(start, end));
    #endif
    return ret;
}

void get_env() {
    char __debug_buffer[MAXBUF];
    size_t ret = 0;
    char * env;
    ret += sprintf(&__debug_buffer[ret], "DEBUG: Env. value ZMSHN_READ_SIZE:\t");
    if ((env = getenv("ZMSHN_READ_SIZE")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s Bytes)\n", env);
        __ReadSize = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld Bytes)\n", DEF_READ_SIZE);
        __ReadSize = DEF_READ_SIZE;
    }
    ret += sprintf(&__debug_buffer[ret], "DEBUG: Env. value ZMSHN_MAX_HDF5_CHUNK_SIZE:\t");
    if ((env = getenv("ZMSHN_MAX_HDF5_CHUNK_SIZE")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s elements)\n", env);
        __MaxHDF5ChunkSize = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld elements)\n", DEF_MAX_HDF5_CHUNK_SIZE);
        __MaxHDF5ChunkSize = DEF_MAX_HDF5_CHUNK_SIZE;
    }
    ret += sprintf(&__debug_buffer[ret], "DEBUG: Env. value ZMSHN_MIN_HDF5_CHUNK_SIZE:\t");
    if ((env = getenv("ZMSHN_MIN_HDF5_CHUNK_SIZE")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s elements)\n", env);
        __MinHDF5ChunkSize = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld elements)\n", DEF_MIN_HDF5_CHUNK_SIZE);
        __MinHDF5ChunkSize = DEF_MIN_HDF5_CHUNK_SIZE;
    }
    ret += sprintf(&__debug_buffer[ret], "DEBUG: Env. value ZMSHN_EXTEND_EVENT_CAPACITY:\t");
    if ((env = getenv("ZMSHN_EXTEND_EVENT_CAPACITY")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s factor)\n", env);
        __ExtendEventCapacity = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld factor)\n", DEF_EXTEND_EVENT_CAPACITY);
        __ExtendEventCapacity = DEF_EXTEND_EVENT_CAPACITY;
    }
    ret += sprintf(&__debug_buffer[ret], "DEBUG: Env. value ZMSHN_COMPRESSION_LEVEL:\t");
    if ((env = getenv("ZMSHN_COMPRESSION_LEVEL")) != NULL) {
         ret += sprintf(&__debug_buffer[ret], "defined (%s level)\n", env);
        __CompressionLevel = atoll(env);
    }
    else {
        ret += sprintf(&__debug_buffer[ret], "not defined (default value %lld level)\n", DEF_EXTEND_EVENT_CAPACITY);
        __CompressionLevel = DEF_EXTEND_EVENT_CAPACITY;
    }
    #ifdef DEBUG
    printf(__debug_buffer);
    #endif
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
void create_datasets(const hid_t file, const hsize_t dimms[3][2], hid_t datasets[3], const size_t rows[3]) {
    size_t chunk_state, chunk_event, chunk_comm;
    if (rows[0] > __MinHDF5ChunkSize && rows[0] < __MaxHDF5ChunkSize)
        chunk_state = rows[0];
    else if (rows[0] > __MaxHDF5ChunkSize) chunk_state = __MaxHDF5ChunkSize;
    else chunk_state = __MinHDF5ChunkSize;
    if (rows[1] > __MinHDF5ChunkSize && rows[1] < __MaxHDF5ChunkSize)
        chunk_event = rows[1];
    else if (rows[1] > __MaxHDF5ChunkSize) chunk_event = __MaxHDF5ChunkSize;
    else chunk_event = __MinHDF5ChunkSize;
    if (rows[2] > __MinHDF5ChunkSize && rows[2] < __MaxHDF5ChunkSize)
        chunk_comm = rows[2];
    else if (rows[2] > __MaxHDF5ChunkSize) chunk_comm = __MaxHDF5ChunkSize;
    else chunk_comm = __MinHDF5ChunkSize;

    hsize_t chunk_dimms[3][2] = {chunk_state, STATE_RECORD_ELEM, chunk_event, EVENT_RECORD_ELEM, chunk_comm, COMM_RECORD_ELEM};
    hsize_t max_dimms[3][2] = {H5S_UNLIMITED, STATE_RECORD_ELEM, H5S_UNLIMITED, EVENT_RECORD_ELEM, H5S_UNLIMITED, COMM_RECORD_ELEM};   
    for (size_t i = 0; i < 3; i++) {
        /* HDF5 dataspace creation for STATES, EVENTS and COMMs records */
        hid_t dataspaces = H5Screate_simple(2, dimms[i], max_dimms[i]);
        /* HDF5 modifies dataset creation properties (enable chunking & compression) */
        hid_t props = H5Pcreate (H5P_DATASET_CREATE);
        status = H5Pset_chunk(props, 2, chunk_dimms[i]);
        status = H5Pset_deflate(props, __CompressionLevel);
        /* HDF5 creates 1 dataset for each record */
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

void write_records_to_HDF5(const hid_t datasets[3], hsize_t dimms[3][2], hsize_t offsets[3][2],  uint64_Array *records) {
    #ifdef PROFILING
    struct timeval start, end;
    REGISTER_TIME(&start);
    #endif
    /* HDF5 extends dataset dimensions */
    hid_t filespace, memspace;
    for (size_t i = 0; i < 3; i++) {
        dimms[i][0] += records[i].elements;    // Updates dimensions
        status = H5Dset_extent(datasets[i], dimms[i]);   // Extends dataset's dimensions
        filespace = H5Dget_space(datasets[i]);   // Refreshes dataset data
        hsize_t dimext[2];
        switch (i) {
        case 0: // STATE
            dimext[0] = records[i].elements; dimext[1] = STATE_RECORD_ELEM;
            break;
        case 1: // EVENT
            dimext[0] = records[i].elements; dimext[1] = EVENT_RECORD_ELEM;
            break;
        case 2: // COMM
            dimext[0] = records[i].elements; dimext[1] = COMM_RECORD_ELEM;
        default:
            break;
        }
        status = H5Sselect_hyperslab (filespace, H5S_SELECT_SET, offsets[i], NULL, dimext, NULL);    // Define where to add the new data in the dataset
        memspace = H5Screate_simple (2, dimext, NULL);  // Allocate memory for new data
        status = H5Dwrite (datasets[i], H5T_NATIVE_ULLONG, memspace, filespace, H5P_DEFAULT, records[i].array);    // Append new data to the dataset
        /* Free innecesarry data structures */
        status = H5Sclose (memspace);
        status = H5Sclose (filespace);
        offsets[i][0] += records[i].elements;  // Update the offsets
    }
    #ifdef PROFILING
    REGISTER_TIME(&end);
    printf("Elapsed time writing 1 block:    %.3f sec\n", GET_ELAPSED_TIME(start, end));
    #endif
}

void parse_prv_to_hdf5(const char * prv, const char * hdf5) {
    size_t read_bytes, ret;
    read_bytes = ret = 0;
    file_data file_d;
    uint64_Array records[3];
    hid_t file_id, record_group_id, datasets_id[3];
    hsize_t dimms[3][2] = {0, STATE_RECORD_ELEM, 0, EVENT_RECORD_ELEM, 0, COMM_RECORD_ELEM};
    hsize_t offsets[3][2] = {0, 0, 0, 0, 0, 0}; // Offsets to use later when extending HDF5 datasets
    int8_t first = TRUE;  // Bool
    file_id = create_HDF5(hdf5);
    record_group_id = create_HDF5_group(file_id, "/RECORDS");
    while ( (ret = read_and_preprocess(prv, __ReadSize, read_bytes, &file_d)) > 0) {
        #ifdef DEBUG
        printf("DEBUG: read %.2f MB\n", (float)ret/(1024*1024));
        #endif
        read_bytes += ret;
        data_parser(&file_d, &records[0], &records[1], &records[2]);
        /* Frees buffers holdings file's data */
        free(file_d.buffer);
        free(file_d.lengths);
        free(file_d.types);
        if (first) {
            size_t rows[3] = {records[0].elements, records[1].elements, records[2].elements};
            create_datasets(record_group_id, dimms, datasets_id, rows);
            first = FALSE;
        }
        write_records_to_HDF5(datasets_id, dimms, offsets, records);
        for (size_t i = 0; i < 3; i++) {
            free(records[i].array);
        }
    }
    /* Frees HDF5 data structures */
    for (size_t i = 0; i < 3; i++) {
        status = H5Dclose(datasets_id[i]);
    }
    status = H5Fclose(file_id);
    #ifdef DEBUG
    printf("DEBUG: Total read %.2f MB\n", (float)read_bytes/(1024*1024));
    #endif
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