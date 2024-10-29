#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <zlib.h>
#include "gcsv.h"

#define CHUNK_SIZE 16384  // bytes

typedef struct {
    char *data;
    size_t size;
    FILE *out_file;
    pthread_mutex_t *file_mutex;
} Chunk;

void *compress_chunk(void *arg) {
    Chunk *chunk = (Chunk *)arg;
    char out[CHUNK_SIZE];
    z_stream strm;

    // initialize zlib stream
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    deflateInit2(&strm, Z_BEST_COMPRESSION, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY);

    strm.avail_in = chunk->size;
    strm.next_in = (unsigned char *)chunk->data;

    do {
        strm.avail_out = CHUNK_SIZE;
        strm.next_out = (unsigned char *)out;

        deflate(&strm, Z_FINISH);

        size_t have = CHUNK_SIZE - strm.avail_out;

        // write compressed data to the output file
        pthread_mutex_lock(chunk->file_mutex);
        fwrite(out, 1, have, chunk->out_file);
        pthread_mutex_unlock(chunk->file_mutex);

    } while (strm.avail_out == 0);

    deflateEnd(&strm);
    free(chunk->data);
    free(chunk);
    return NULL;
}

void gcsv_compress(const char *input_file, const char *output_file) {
    FILE *in = fopen(input_file, "r");
    FILE *out = fopen(output_file, "wb");
    if (!in || !out) {
        perror("Failed to open files");
        exit(EXIT_FAILURE);
    }

    pthread_mutex_t file_mutex;
    pthread_mutex_init(&file_mutex, NULL);

    char *buffer = malloc(CHUNK_SIZE);
    size_t bytes_read;
    pthread_t threads[8]; // 8 threads start cap
    int thread_count = 0;

    // read and compress the file in chunks
    while ((bytes_read = fread(buffer, 1, CHUNK_SIZE, in)) > 0) {
        Chunk *chunk = malloc(sizeof(Chunk));
        chunk->data = buffer;
        chunk->size = bytes_read;
        chunk->out_file = out;
        chunk->file_mutex = &file_mutex;

        pthread_create(&threads[thread_count++], NULL, compress_chunk, chunk);

        // wait for threads to finish if max threads are reached
        if (thread_count >= 8) {
            for (int i = 0; i < thread_count; i++) {
                pthread_join(threads[i], NULL);
            }
            thread_count = 0;
        }

        buffer = malloc(CHUNK_SIZE);  // allocate a new buffer for the next chunk
    }

    // join remaining threads
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    pthread_mutex_destroy(&file_mutex);
    fclose(in);
    fclose(out);
}

void gcsv_decompress(const char *input_file, const char *output_file) {
    FILE *in = fopen(input_file, "rb");
    FILE *out = fopen(output_file, "w");
    if (!in || !out) {
        perror("Failed to open files");
        exit(EXIT_FAILURE);
    }

    char in_buffer[CHUNK_SIZE];
    char out_buffer[CHUNK_SIZE];
    z_stream strm;

    // initialize zlib stream for decompression
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    inflateInit2(&strm, 15 + 16);  // 15 + 16 for gzip decoding

    int ret;
    do {
        strm.avail_in = fread(in_buffer, 1, CHUNK_SIZE, in);
        if (ferror(in)) {
            inflateEnd(&strm);
            perror("File read error");
            exit(EXIT_FAILURE);
        }

        if (strm.avail_in == 0) break;
        strm.next_in = (unsigned char *)in_buffer;

        do {
            strm.avail_out = CHUNK_SIZE;
            strm.next_out = (unsigned char *)out_buffer;

            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_STREAM_ERROR) {
                perror("Stream error");
                exit(EXIT_FAILURE);
            }

            size_t have = CHUNK_SIZE - strm.avail_out;
            fwrite(out_buffer, 1, have, out);
        } while (strm.avail_out == 0);

    } while (ret != Z_STREAM_END);

    inflateEnd(&strm);
    fclose(in);
    fclose(out);
}

void print_usage(const char *program_name) {
    printf("Usage:\n");
    printf("  %s compress <input.csv> <output.gcsv>\n", program_name);
    printf("  %s decompress <input.gcsv> <output.csv>\n", program_name);
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    const char *command = argv[1];
    const char *input_file = argv[2];
    const char *output_file = argv[3];

    if (strcmp(command, "compress") == 0) {
        printf("Compressing %s to %s...\n", input_file, output_file);
        gcsv_compress(input_file, output_file);
        printf("Compression completed successfully.\n");
    } 
    else if (strcmp(command, "decompress") == 0) {
        printf("Decompressing %s to %s...\n", input_file, output_file);
        gcsv_decompress(input_file, output_file);
        printf("Decompression completed successfully.\n");
    } 
    else {
        printf("Unknown command: %s\n", command);
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}