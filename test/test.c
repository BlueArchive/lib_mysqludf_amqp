//
// Created by jeff on 9/3/19.
//

#include <stdio.h>
#include <string.h>
#include <mysql.h>
#include <mysql_com.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>
#include "lib_mysqludf_amqp.h"

// Test parameters
static int number_of_threads = 64;
static int loops_per_thread = 10000;

// Count the number of errors
static _Atomic int error_count = 0;

void set_string_arg(UDF_ARGS *args, int index, char *value) {
    args->arg_type[index] = STRING_RESULT;
    args->args[index] = value;
    args->lengths[index] = strlen(value);
}

void init_args(UDF_ARGS *args) {
    static long long port = 5672;

    args->arg_count = 7;
    args->arg_type = (enum Item_result *) calloc(7, sizeof(enum Item_result));
    args->args = (char **) calloc(7, sizeof(char *));
    args->lengths = (unsigned long *) calloc(7, sizeof(unsigned long));

    set_string_arg(args, 0, "172.17.0.1");
    set_string_arg(args, 2, "guest");
    set_string_arg(args, 3, "guest");
    set_string_arg(args, 4, "udf");
    set_string_arg(args, 5, "udf.test");
    set_string_arg(args, 6, "{ \"json\" : \"string\" }");

    // Set the port as number
    args->arg_type[1] = INT_RESULT;
    args->args[1] = (char *) &port;
}

_Atomic int running_threads = 0;        // number of running threads
struct timeval start;

void *test_thread(void *arg) {
    // Build the args list
    UDF_ARGS args;
    init_args(&args);
    UDF_INIT initid;
    char message[MYSQL_ERRMSG_SIZE];
    char error[1];
    char *result;
    unsigned long length;
    char is_null[1];

    for (int loop = 1; loop <= loops_per_thread; loop++) {
        message[0] = 0;
        if (lib_mysqludf_amqp_sendjson_init(&initid, &args, message) != 0 || strlen(message) > 0) {
            fprintf(stderr, "ERROR in lib_mysqludf_amqp_sendjson_init: %s\n", message);
            ++error_count;
        }
        result = lib_mysqludf_amqp_sendjson(&initid, &args, NULL, &length, is_null, error);
        if (result == NULL || strlen(result) == 0 || *error != 0) {
            fprintf(stderr, "ERROR in lib_mysqludf_amqp_sendjson\n");
            ++error_count;
        }
        lib_mysqludf_amqp_sendjson_deinit(&initid);
    }

    if (--running_threads == 0) {
        struct timeval end;
        gettimeofday(&end, NULL);
        printf("Completed test of %d threads with %d loops, time = %f seconds.\n", number_of_threads, loops_per_thread,
                ((double) (end.tv_sec - start.tv_sec)) + ((double) (end.tv_usec - start.tv_usec) / 1000000.0));
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    printf("Starting test of lib_mysqludf_ampq...\n");
    // Build the args list
    UDF_ARGS args;
    init_args(&args);
    UDF_INIT initid;
    char message[MYSQL_ERRMSG_SIZE];
    char *result;
    unsigned long length;
    char is_null[1];

    gettimeofday(&start, NULL);
    running_threads = number_of_threads;
    for (int thread = 1; thread <= number_of_threads; thread++) {
        pthread_t thread_id;
        pthread_create(&thread_id, NULL, test_thread, NULL);
    }
    while (running_threads > 0) {
        sleep(1);
    }

    // If errors, return non zero for unit test system
    if (error_count > 0) {
        fprintf(stderr, "Total %d errors!\n", error_count);
        exit(1);
    }
    return 0;
}
