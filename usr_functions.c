#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

#include "common.h"
#include "usr_functions.h"

/* User-defined map function for the "Letter counter" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */

int letter_counter_map(DATA_SPLIT *split, int fd_out) {
    // Validate input parameters for the DATA_SPLIT structure and output file descriptor
    if (!split || split->fd < 0) {
        fprintf(stderr, "Error: Invalid input structure or file descriptor in map function.\n");
        return -1;
    }

    // Initialize an array to store counts for letters A-Z
    int letter_frequencies[26] = {0};
    char read_buffer[1024]; // Buffer to hold file data during reads
    ssize_t bytes_read;

    // Read data from the input file
    while ((bytes_read = read(split->fd, read_buffer, sizeof(read_buffer))) > 0) {
        // Process each character in the buffer
        for (ssize_t idx = 0; idx < bytes_read; idx++) {
            if (read_buffer[idx] >= 'A' && read_buffer[idx] <= 'Z') {
                letter_frequencies[read_buffer[idx] - 'A']++;
            } else if (read_buffer[idx] >= 'a' && read_buffer[idx] <= 'z') {
                letter_frequencies[read_buffer[idx] - 'a']++;
            }
        }
    }

    // Check if reading encountered an error
    if (bytes_read < 0) {
        perror("File read error in map function");
        return -1;
    }

    // Write the letter counts to the intermediate file
    for (int letter_idx = 0; letter_idx < 26; letter_idx++) {
        if (letter_frequencies[letter_idx] > 0) {
            char output_line[16];
            int output_length = snprintf(output_line, sizeof(output_line), "%c %d\n", 'A' + letter_idx, letter_frequencies[letter_idx]);
            
            if (output_length < 0 || write(fd_out, output_line, output_length) != output_length) {
                perror("Error writing to intermediate file in map function");
                return -1;
            }
        }
    }

    return 0; // Indicate successful completion
}


/* User-defined reduce function for the "Letter counter" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/

int letter_counter_reduce(int *p_fd_in, int fd_in_num, int fd_out) {
    // Validate input: Ensure file descriptor array and count are valid
    if (!p_fd_in || fd_in_num <= 0) {
        fprintf(stderr, "Error: Invalid input file descriptors or count in reduce function.\n");
        return -1;
    }

    // Initialize an array to store the aggregated letter counts
    int aggregated_counts[26] = {0};
    char read_buffer[256]; // Buffer for reading lines from intermediate files

    // Iterate over each input file descriptor
    for (int fd_idx = 0; fd_idx < fd_in_num; fd_idx++) {
        // Reset file position to the beginning of the current file
        if (lseek(p_fd_in[fd_idx], 0, SEEK_SET) < 0) {
            perror("Error resetting file position in intermediate file (reduce function)");
            return -1;
        }

        ssize_t bytes_read;
        // Read lines from the current file
        while ((bytes_read = read(p_fd_in[fd_idx], read_buffer, sizeof(read_buffer) - 1)) > 0) {
            read_buffer[bytes_read] = '\0'; // Null-terminate the buffer for safe string manipulation
            char *line = strtok(read_buffer, "\n"); // Tokenize the buffer by line

            // Parse each line to extract letter counts
            while (line) {
                char letter;
                int count;
                if (sscanf(line, "%c %d", &letter, &count) == 2 && letter >= 'A' && letter <= 'Z') {
                    aggregated_counts[letter - 'A'] += count; // Update the aggregated count
                }
                line = strtok(NULL, "\n"); // Get the next line
            }
        }

        // Check for errors during file reading
        if (bytes_read < 0) {
            perror("Error reading from intermediate file (reduce function)");
            return -1;
        }
    }

    // Write the aggregated letter counts to the output file
    for (int letter_idx = 0; letter_idx < 26; letter_idx++) {
        if (aggregated_counts[letter_idx] > 0) {
            char output_line[16];
            int output_length = snprintf(output_line, sizeof(output_line), "%c %d\n", 'A' + letter_idx, aggregated_counts[letter_idx]);
            
            if (output_length < 0 || write(fd_out, output_line, output_length) != output_length) {
                perror("Error writing aggregated results to output file mr.rst(reduce function)");
                return -1;
            }
        }
    }

    return 0; // Indicate successful completion
}

/* User-defined map function for the "Word finder" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */


int word_finder_map(DATA_SPLIT *split, int fd_out) {
    // Validate input: Ensure DATA_SPLIT, user data (word to find), and file descriptor are valid
    if (!split || !split->usr_data || split->fd < 0) {
        fprintf(stderr, "Error: Invalid input parameters to word_finder_map function.\n");
        return -1;
    }

    char *target_word = (char *)split->usr_data; // Word to search for
    size_t target_word_len = strlen(target_word); // Length of the word to find
    char read_buffer[1024]; // Buffer for file reading
    char current_line[1024]; // Buffer for constructing a line
    char original_line_copy[1024]; // Copy of the current line for output
    ssize_t bytes_read; // Bytes read from the file
    ssize_t current_line_len = 0; // Length of the current line being constructed

    // Read data from the input file
    while ((bytes_read = read(split->fd, read_buffer, sizeof(read_buffer))) > 0) {
        for (ssize_t buffer_idx = 0; buffer_idx < bytes_read; buffer_idx++) {
            // Check for end of a line or line length exceeding buffer size
            if (read_buffer[buffer_idx] == '\n' || current_line_len == sizeof(current_line) - 1) {
                current_line[current_line_len] = '\0'; // Null-terminate the line
                strcpy(original_line_copy, current_line); // Create a copy for output

                char *search_ptr = current_line; // Pointer for searching the target word
                // Search for the target word in the line
                while ((search_ptr = strstr(search_ptr, target_word)) != NULL) {
                    // Check word boundaries to ensure an exact match
                    if ((search_ptr == current_line || *(search_ptr - 1) == ' ') &&
                        (*(search_ptr + target_word_len) == ',' || *(search_ptr + target_word_len) == '.' ||
                         *(search_ptr + target_word_len) == ' ' || *(search_ptr + target_word_len) == '\0')) {
                        // Write the matching line to the output file
                        if (write(fd_out, original_line_copy, current_line_len) != current_line_len || 
                            write(fd_out, "\n", 1) != 1) {
                            perror("Error writing matching line to output file (word_finder_map function)");
                            return -1;
                        }
                        break; // Stop searching the current line after a match
                    }
                    search_ptr += target_word_len; // Move past the current match
                }
                current_line_len = 0; // Reset the line length for the next line
            } else {
                current_line[current_line_len++] = read_buffer[buffer_idx]; // Append character to the current line
            }
        }
    }

    // Check for errors during file reading
    if (bytes_read < 0) {
        perror("Error reading input file (word_finder_map function)");
        return -1;
    }

    return 0; // Indicate successful completion
}



/* User-defined reduce function for the "Word finder" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/

int word_finder_reduce(int *input_fds, int num_input_fds, int output_fd) {
    // Validate input: Ensure file descriptor array and count are valid
    if (!input_fds || num_input_fds <= 0) {
        fprintf(stderr, "Error: Invalid input file descriptors or count in word_finder_reduce.\n");
        return -1;
    }

    char read_buffer[1024]; // Buffer for reading data from intermediate files
    ssize_t bytes_read;     // Number of bytes read in each read operation

    // Loop through each intermediate file descriptor
    for (int fd_idx = 0; fd_idx < num_input_fds; fd_idx++) {
        // Reset the file offset to the beginning of the file
        if (lseek(input_fds[fd_idx], 0, SEEK_SET) < 0) {
            perror("Error resetting file position in intermediate file (word_finder_reduce)");
            return -1;
        }

        // Read data from the current intermediate file
        while ((bytes_read = read(input_fds[fd_idx], read_buffer, sizeof(read_buffer))) > 0) {
            // Write the read data to the final output file
            if (write(output_fd, read_buffer, bytes_read) != bytes_read) {
                perror("Error writing data to output file (word_finder_reduce)");
                return -1;
            }
        }

        // Check for errors during reading
        if (bytes_read < 0) {
            perror("Error reading from intermediate file (word_finder_reduce)");
            return -1;
        }
    }

    return 0; // Indicate successful completion
}
