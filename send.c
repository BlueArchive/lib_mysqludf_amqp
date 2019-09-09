#include "config.h"

#include <ctype.h>
#include <mysql.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <pthread.h>

#include "send.h"
#include "uuid.h"

#define CONN_INFO_URL_SIZE  1024        // maximum size of URL for host:port:username:password

typedef struct conn_info {
    amqp_socket_t *socket;
    amqp_connection_state_t conn;
    char *message_id;
    // State added for free pool connection management
    int in_free_pool;             // true if in the free pool
    struct conn_info *next;       // when in free pool, next on the list
    char url[CONN_INFO_URL_SIZE]; // original URL for the connection
    time_t last_used;             // time last used
} conn_info_t;

// ***** Free Connection Pooling Code *****

// Head and tail of the free list
static volatile conn_info_t *conn_free_head = NULL;
static volatile conn_info_t *conn_free_tail = NULL;

// Number of connections on the list
static volatile int conn_free_count = 0;

// Mutex to lock all operations
static pthread_mutex_t conn_free_mutex;

// Maximum connections allowed in free pool (static so we can change for testing)
static int conn_free_max_conns = 250;
// Maximum time in seconds for use of a connection from the pool
static int conn_max_timeout_secs = 15;

// Standard error printing
void print_error(char *message) {
    // Format time like MySQL
    char buffer[64];

    time_t now = time(&now);
    struct tm *ptm = gmtime(&now);
    strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S.000000Z", ptm);
    fprintf(stderr, "%s [ERROR] lib_mysqludf_ampq: %s\n", buffer, message);
}

// Initialize the UUID
int set_message_id(conn_info_t *conn_info) {
    // Free any existing id
    if (conn_info == NULL) {
        return 0;
    }
    if (conn_info->message_id != NULL) {
        free(conn_info->message_id);
    }
    // Get a new message id
    conn_info->message_id = uuidgen();
    if (conn_info->message_id == NULL) {
        print_error("lib_mysqludf_amqp_send: uuidgen error");
        return 0;
    }
    return 1;
}

// Create a new ampq connection
conn_info_t *
create_connection(UDF_ARGS *args, char *message)
{
    int rc;
    amqp_rpc_reply_t reply;
    conn_info_t *conn_info;

    conn_info = (conn_info_t *) calloc(1, sizeof(conn_info_t));
    if (conn_info == NULL) {
        (void) strncpy(message, "lib_mysqludf_amqp_send: calloc error", MYSQL_ERRMSG_SIZE);
        return NULL;
    }
    // Save the URL for the connection
    snprintf(conn_info->url, sizeof(conn_info->url), "%s:%d:%s:%s", args->args[0], (int)(*((long long *) args->args[1])), args->args[2], args->args[3]);
    // Time last used is now
    conn_info->last_used = time(NULL);
    // Create the connection
    conn_info->conn = amqp_new_connection();
    conn_info->socket = amqp_tcp_socket_new(conn_info->conn);
    if (conn_info->socket == NULL) {
        (void) strncpy(message, "lib_mysqludf_amqp_send: socket error", MYSQL_ERRMSG_SIZE);
        goto init_error_destroy;;
    }

    // wait no longer than 100ms for a connection to RabbitMQ
    // Cache invalidation signaling is (should be) robust
    // enough to miss an invalidation signal
    // Given than this can be called on every insert/update/delete row
    // We don't want to tie up a MySQL thread when connecting to RabbitMQ
    // as that could be a Bad Thing
    struct timeval timeout;
    memset(&timeout, 0, sizeof(struct timeval));
    timeout.tv_usec = 100 * 1000;   // wait no longer than 100ms for connection

    rc = amqp_socket_open_noblock(
            conn_info->socket,
            args->args[0],
            (int)(*((long long *) args->args[1])),
            &timeout
    );
    if (rc < 0) {
        (void) strncpy(message, "lib_mysqludf_amqp_send: socket open error", MYSQL_ERRMSG_SIZE);
        goto init_error_destroy;
    }

    // ==> Do we need heartbeat to keep connection alive?
    reply = amqp_login(conn_info->conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, args->args[2], args->args[3]);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        (void) strncpy(message, "lib_mysqludf_amqp_send: login error", MYSQL_ERRMSG_SIZE);
        goto init_error_close;
    }

    amqp_channel_open(conn_info->conn, 1);
    reply = amqp_get_rpc_reply(conn_info->conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        (void) strncpy(message, "lib_mysqludf_amqp_send: channel error", MYSQL_ERRMSG_SIZE);
        goto init_error_close;
    }

    // Make sure we have unique message id
    if (conn_info != NULL) {
        if (!set_message_id(conn_info)) {
            (void) strncpy(message, "lib_mysqludf_amqp_send: uuidgen error", MYSQL_ERRMSG_SIZE);
            goto init_error_close;
        }
    }

    // Return our connection information
    return conn_info;

init_error_close:
    (void) amqp_connection_close(conn_info->conn, AMQP_REPLY_SUCCESS);

init_error_destroy:
    (void) amqp_destroy_connection(conn_info->conn);
    free(conn_info);

    return NULL;
}

// Close a ampq connection
void
close_connection(conn_info_t *conn_info)
{
    if (conn_info != NULL) {
        (void) amqp_channel_close(conn_info->conn, 1, AMQP_REPLY_SUCCESS);
        (void) amqp_connection_close(conn_info->conn, AMQP_REPLY_SUCCESS);
        (void) amqp_destroy_connection(conn_info->conn);
        if (conn_info->message_id != NULL) {
            free(conn_info->message_id);
        }
        free(conn_info);
    }
}

// Get an ampq connection either from free pool or create a new one
conn_info_t *
get_connection(UDF_ARGS *args, char *message) {

    conn_info_t *conn_info = NULL;

    // Form our URL from the request
    char url[CONN_INFO_URL_SIZE];
    snprintf(url, sizeof(url), "%s:%d:%s:%s", args->args[0], (int)(*((long long *) args->args[1])), args->args[2], args->args[3]);

    // Lock before checking the static variables
    pthread_mutex_lock(&conn_free_mutex);

    // Must pick one that matches our URL
    conn_info_t *prev = NULL;
    for (conn_info = (conn_info_t *) conn_free_head; conn_info != NULL; ) {
        // Check if entry expired
        int expired = (int) difftime(time(NULL), conn_info->last_used) >= conn_max_timeout_secs;
        // If expired or this entry url matches...
        if (expired || strcmp(conn_info->url, url) == 0) {
            // Remove this entry from the list
            conn_info->in_free_pool = 0;
            if (conn_free_count > 0) {
                conn_free_count -= 1;
            }
            // Remove this entry from the free list
            if (prev == NULL) {
                conn_free_head = conn_info->next;
            } else {
                prev->next = conn_info->next;
            }
            if (conn_info == conn_free_tail) {
                conn_free_tail = prev;
            }
            // If this entry expired, release it
            if (expired) {
                conn_info_t *next = conn_info->next;
                close_connection(conn_info);
                conn_info = next;
                continue;
            }
            // All done
            break;
        }
        // Advance to next entry
        prev = conn_info;
        conn_info = conn_info->next;
    };

    // Unlock before checking the static variables
    pthread_mutex_unlock(&conn_free_mutex);

    // If no free one found, create a new one
    if (conn_info == NULL) {
        conn_info = create_connection(args, message);
    } else {
        // Otherwise, make sure we have a new message id
        set_message_id(conn_info);
    }

    // Return what we found or created
    return conn_info;
}

// Free an ampq connection either to the free pool or close it
void
free_connection(conn_info_t *conn_info) {

    // If we have some info...
    if (conn_info != NULL) {

        // Time last used is now
        conn_info->last_used = time(NULL);

        // Lock before checking the static variables
        pthread_mutex_lock(&conn_free_mutex);

        // If we can keep more connections, put this one in the free pool
        if (conn_free_count < conn_free_max_conns) {
            conn_free_count += 1;
            conn_info->in_free_pool = 1;
            conn_info->next = NULL;
            if (conn_free_tail == NULL) {
                conn_free_head = conn_info;
            } else {
                conn_free_tail->next = conn_info;
            }
            conn_free_tail = conn_info;
            // This signals we are done with free
            conn_info = NULL;
        }

        // Unlock when done with the static variables
        pthread_mutex_unlock(&conn_free_mutex);

        // If we still have the connection, close it
        if (conn_info != NULL) {
            close_connection(conn_info);
        }
    }
}

// ***** MySQL UDF Functions *****

my_bool
lib_mysqludf_amqp_send_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{

    conn_info_t *conn_info;

    // Output message the first time with the version.
    static _Atomic int init = 0;
    if (!init) {
        init = 1;
        printf("MySQL plugin %s initialized.\n", PACKAGE_STRING);
    }

    // Check the parameters one time on the init
    if (args->arg_count != 7 || (args->arg_type[0] != STRING_RESULT)        /* host */
                             || (args->arg_type[1] != INT_RESULT)           /* port */
                             || (args->arg_type[2] != STRING_RESULT)        /* username */
                             || (args->arg_type[3] != STRING_RESULT)        /* password */
                             || (args->arg_type[4] != STRING_RESULT)        /* exchange */
                             || (args->arg_type[5] != STRING_RESULT)        /* routing key */
                             || (args->arg_type[6] != STRING_RESULT)) {     /* message */
        (void) strncpy(message, "lib_mysqludf_amqp_send: invalid arguments", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    // Default initialization of initid data
    initid->ptr = NULL;
    initid->maybe_null = 1; /* returns NULL */
    initid->const_item = 0; /* may return something different if called again */

    // Get a connection.
    conn_info = get_connection(args, message);
    // Save our conn_info
    initid->ptr = (char *) conn_info;
    // Get the maximum length of the result
    if (conn_info != NULL && conn_info->message_id != NULL) {
        initid->max_length = strlen(conn_info->message_id); // maximum length of message
    }
    // Any error is printed
    if (conn_info == NULL && strlen(message) > 0) {
        print_error(message);
    }

    // Note that we always return success so that a failure doesn't stop our MySQL triggers from working.
    return 0;
}

char*
lib_mysqludf_amqp_send(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length, char *is_null, char *error, char *content_type)
{

    int rc;
    char message[MYSQL_ERRMSG_SIZE];

    conn_info_t *conn_info = (conn_info_t *) initid->ptr;

    if (conn_info == NULL) {
        // uninitialized, so error out
        *is_null = 1;
        *error = 1;
        return NULL;
    }

    amqp_table_entry_t headers[1];
    amqp_basic_properties_t props;
    props._flags = 0;

    /* contentType */
    props.content_type = amqp_cstring_bytes(content_type);
    props._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;

    /* deliveryMode */
    props.delivery_mode = 2;
    props._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;

    /* timestamp */
    props.timestamp = time(NULL);
    props._flags |= AMQP_BASIC_TIMESTAMP_FLAG;

    /* headers */
    headers[0].key = amqp_cstring_bytes("User-Agent");
    headers[0].value.kind = AMQP_FIELD_KIND_UTF8;
    headers[0].value.value.bytes = amqp_cstring_bytes(PACKAGE_NAME "/" PACKAGE_VERSION);
    props.headers.entries = headers;
    props.headers.num_entries = sizeof(headers) / sizeof(headers[0]);
    props._flags |= AMQP_BASIC_HEADERS_FLAG;

    /* appId */
    props.app_id = amqp_cstring_bytes(PACKAGE_NAME);
    props._flags |= AMQP_BASIC_APP_ID_FLAG;

    // If our first attempt fails due to socket error, allow second retry on a new connection
    for (int loop = 1; loop <= 2; loop++) {
        // Set the message id since it can change in the loops
        props.message_id = amqp_cstring_bytes(conn_info->message_id);
        props._flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
        // Try to publish the message
        rc = amqp_basic_publish(conn_info->conn, 1, amqp_cstring_bytes(args->args[4]),
                                amqp_cstring_bytes(args->args[5]), 0, 0, &props, amqp_cstring_bytes(args->args[6]));
        // If we got a socket error on the first try, release this connection and retry with a new connection
        if (rc == -9 && loop == 1) {
            close_connection(conn_info);
            initid->ptr = NULL;
            conn_info = create_connection(args, message);
            if (conn_info == NULL) {
                goto error_exit;
            }
            initid->ptr = (void *) conn_info;
            continue;
        }
        if (rc < 0) {
            snprintf(message, MYSQL_ERRMSG_SIZE, "amqp_basic_publish (rc = %d): %s", rc, amqp_error_string2(rc));
            close_connection(conn_info);
            initid->ptr = NULL;
            goto error_exit;
        } else {
            break;
        }
    }

    *is_null = 0;
    *error = 0;
    if (conn_info->message_id != NULL) {
        *length = (unsigned long) strlen(conn_info->message_id);
    }

    return conn_info->message_id;

error_exit:
    // All errors come here
    initid->ptr = NULL;
    *is_null = 1;
    *error = 1;
    print_error(message);
    return NULL;
}

void
lib_mysqludf_amqp_send_deinit(UDF_INIT *initid)
{
    if (initid->ptr != NULL) {
        conn_info_t *conn_info = (conn_info_t *) initid->ptr;
        free_connection(conn_info);
        initid->ptr = NULL;
    }

    return;
}
