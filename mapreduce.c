#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mapreduce.h"
#include "common.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <string.h>

void mapreduce(MAPREDUCE_SPEC *spec, MAPREDUCE_RESULT *result) {
    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);

    if (spec == NULL || result == NULL) {
        EXIT_ERROR(ERROR, "Error: 'spec' or 'result' is NULL.\n");
    }

    // Variables initialization
    long input_file_size;
    int i, worker_exit_status;
    int total_splits = spec->split_num;

    // Open the input file
    FILE *input_file = fopen(spec->input_data_filepath, "r");
    if (input_file == NULL) {
        EXIT_ERROR(ERROR, "Error: Unable to open input file: %s\n", spec->input_data_filepath);
    }

    // Calculate input file size
    fseek(input_file, 0, SEEK_END);
    input_file_size = ftell(input_file);
    rewind(input_file);

    long split_size = input_file_size / total_splits;

    // Allocate memory for split and intermediate file names
    char **split_filenames = malloc(total_splits * sizeof(char *));
    char **intermediate_filenames = malloc(total_splits * sizeof(char *));
    if (split_filenames == NULL || intermediate_filenames == NULL) {
        fclose(input_file);
        EXIT_ERROR(ERROR, "Error: Memory allocation failed for file name arrays.\n");
    }

    // Phase 1: Splitting the input file into chunks
    for (i = 0; i < total_splits; i++) {
        split_filenames[i] = malloc(20);
        snprintf(split_filenames[i], 20, "split-%d", i);

        FILE *split_file = fopen(split_filenames[i], "w");
        if (split_file == NULL) {
            fclose(input_file);
            EXIT_ERROR(ERROR, "Error: Failed to create split file: %s\n", split_filenames[i]);
        }

        char buffer[1024];
        long bytes_read = 0;

        // Write data to split files
        while (bytes_read < split_size && fgets(buffer, sizeof(buffer), input_file)) {
            fputs(buffer, split_file);
            bytes_read += strlen(buffer);
        }

        fclose(split_file);

        // Allocate memory for intermediate file names
        intermediate_filenames[i] = malloc(20);
        snprintf(intermediate_filenames[i], 20, "mr-%d.itm", i);
    }
    fclose(input_file);

    // Phase 2: Forking processes for map phase
    int *map_worker_pids = malloc(total_splits * sizeof(int));
    if (map_worker_pids == NULL) {
        EXIT_ERROR(ERROR, "Error: Memory allocation failed for map worker PIDs.\n");
    }

    for (i = 0; i < total_splits; i++) {
        if ((map_worker_pids[i] = fork()) == 0) {
            // Child process logic
            DATA_SPLIT split = {0};
            split.fd = open(split_filenames[i], O_RDONLY);
            split.usr_data = spec->usr_data;

            if (split.fd < 0) {
                _EXIT_ERROR(ERROR, "Error: Unable to open split file: %s\n", split_filenames[i]);
            }

            // Create intermediate file for map output
            int intermediate_fd = open(intermediate_filenames[i], O_WRONLY | O_CREAT | O_TRUNC, 0666);
            if (intermediate_fd < 0) {
                _EXIT_ERROR(ERROR, "Error: Unable to create intermediate file: %s\n", intermediate_filenames[i]);
            }

            // Execute map function
            int map_status = spec->map_func(&split, intermediate_fd);
            close(split.fd);
            close(intermediate_fd);

            if (map_status != SUCCESS) {
                _EXIT_ERROR(ERROR, "Error: Map function failed for split file: %s\n", split_filenames[i]);
            }

            _exit(SUCCESS);
        } else if (map_worker_pids[i] < 0) {
            EXIT_ERROR(ERROR, "Error: Fork failed for map worker %d.\n", i);
        } else {
            // Parent process: Store the PID in result->map_worker_pid
            result->map_worker_pid[i] = map_worker_pids[i];
        }
    }

    // Phase 3: Wait for all map workers to complete
    for (i = 0; i < total_splits; i++) {
        waitpid(map_worker_pids[i], &worker_exit_status, 0);
        if (WIFEXITED(worker_exit_status) && WEXITSTATUS(worker_exit_status) != SUCCESS) {
            fprintf(stderr, "Error: Map worker %d failed.\n", i);
        }
    }

    // Phase 4: Fork a reduce worker process
    int reduce_worker_pid;
    if ((reduce_worker_pid = fork()) == 0) {
        // Child process logic for reduce
        int *intermediate_fds = malloc(total_splits * sizeof(int));
        if (intermediate_fds == NULL) {
            _EXIT_ERROR(ERROR, "Error: Memory allocation failed for intermediate file descriptors.\n");
        }

        // Open intermediate files
        for (i = 0; i < total_splits; i++) {
            intermediate_fds[i] = open(intermediate_filenames[i], O_RDONLY);
            if (intermediate_fds[i] < 0) {
                _EXIT_ERROR(ERROR, "Error: Unable to open intermediate file: %s\n", intermediate_filenames[i]);
            }
        }

        int result_fd = open("mr.rst", O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (result_fd < 0) {
            _EXIT_ERROR(ERROR, "Error: Unable to create result file.\n");
        }

        // Execute reduce function
        if (spec->reduce_func(intermediate_fds, total_splits, result_fd) != SUCCESS) {
            close(result_fd);
            _EXIT_ERROR(ERROR, "Error: Reduce function execution failed.\n");
        }

        close(result_fd);
        for (i = 0; i < total_splits; i++) {
            close(intermediate_fds[i]);
        }
        free(intermediate_fds);
        _exit(SUCCESS);
    } else if (reduce_worker_pid < 0) {
        EXIT_ERROR(ERROR, "Error: Fork failed for reduce worker.\n");
    } else {
        result->reduce_worker_pid = reduce_worker_pid; // Store reduce worker PID
    }

    // Wait for reduce worker to complete
    waitpid(reduce_worker_pid, &worker_exit_status, 0);
    if (WIFEXITED(worker_exit_status) && WEXITSTATUS(worker_exit_status) != SUCCESS) {
        fprintf(stderr, "Error: Reduce worker execution failed.\n");
    }

    // Phase 5: Cleanup resources
    for (i = 0; i < total_splits; i++) {
        free(split_filenames[i]);
        free(intermediate_filenames[i]);
    }
    free(split_filenames);
    free(intermediate_filenames);
    free(map_worker_pids);

    // Record processing time
    gettimeofday(&end_time, NULL);
    result->processing_time = (end_time.tv_sec - start_time.tv_sec) * US_PER_SEC + (end_time.tv_usec - start_time.tv_usec);
}
