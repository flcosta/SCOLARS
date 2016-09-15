// This file is part of BOINC-MapReduce (MapReduce extension for BOINC)
// http://boinc.berkeley.edu
//
// BOINC is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation,
// either version 3 of the License, or (at your option) any later version.
//
// BOINC is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with BOINC.  If not, see <http://www.gnu.org/licenses/>.


#ifndef MR_TASKTRACKER_H_INCLUDED
#define MR_TASKTRACKER_H_INCLUDED

//#include <vector>
#ifndef _WIN32
#include <string>
#include <vector>
#endif

using std::string;
using std::vector;

#include "client_types.h"


// default value for timeout (maximum time serving a file)
#define MR_SOCKET_SERVER_TIMEOUT    (60*60*24)  // 1 day
#define MR_SOCK_TEST                86400

/// MR_FILE_SERVER is a thread that listens to incoming download
/// requests from other users.
/// It controls incoming access to the client through:
/// - list of files available for upload
/// - list of accepted users (IP)

/// BOINC-MR TODO - control users with signatures/hashes, or is IP enough?


/// MR_CLIENT_CONNECTION represents an open upload socket, serving a MapReduce file
/// output to another client that requires it as input.
/// Each object has its beginning serving instant, time it has spent serving file so far,
/// the FILE_INFO corresponding to the file it is serving, and the open socket itself.
/// When the connection is closed, it is removed from the list of open sockets, and if no
/// more files should be served/uploaded, the client stops receiving connections

/// a MR_CLIENT_CONNECTION is created and added to mr_client_connection_set
/// 1) when read from the client state file
///   in (FILE_INFO::parse(), CLIENT_STATE::parse_state_file()
/// 2) when a FILE_INFO is ready to be served (uploaded to another client)
///   in CLIENT_STATE::mr_handle_file_uploads()


/// BOINC-MR TODO
// Listening thread (ServerSocket)
class MR_FILE_SERVER {
private:
    std::vector<std::string> mr_user_access_list; // list of users that have access
    /// BOINC-MR TODO - each user has access to all files OR grant access per-file AND per-user (user1:output2; user2:output4; etc.)
    std::vector<FILE_INFO*> mr_file_upload_list;        // list of files available for upload

    bool mr_is_listening;

    // get sockaddr, IPv4 or IPv6:
    void *get_in_addr(struct sockaddr*);

    //FILE_INFO *mr_check_req_file(char*);
        // check if requested file is being served. Returns corresponding FILE_INFO if it is, NULL otherwise

    //pid_t mr_pid;
        // pid of process that is running mr_file_server

public:
    pthread_t running_thread;
    int file_server_retval;
        // 0 if no errors
    int mr_sockfd;
        // socket (file descriptor) to listen for incoming connections

    MR_FILE_SERVER();
    ~MR_FILE_SERVER();
    // add/remove file from list of available files for upload
    bool mr_add_file(FILE_INFO*);
    bool mr_remove_file(FILE_INFO*);
    // stop accepting requests (e.g. network suspended, no more files to serve)
    bool mr_suspend();

    // add/remove users from list of users with permission to access files
    bool mr_add_user(std::string);
    bool mr_remove_user(std::string);

    // start listening to incoming connections with mr_lsock
    //int mr_start_listening();

    /// BOINC-MR TODO necessary? when stopped/suspended kills/deletes this object?
    bool mr_resume();

    bool mr_is_server_listening();
    void set_is_listening(bool is){
        mr_is_listening = is;
    };

    bool accept_connections();
};

// handle finished processes - socket closed / connection finished on SIGCHLD signal
#ifdef __cplusplus
extern "C" {
#endif
   extern void sigchld_handler(int);
#ifdef __cplusplus
}
#endif


/// BOINC-MR TODO - set values of MR_CLIENT_CONNECTION::mr_conn_state
// values of MR_CLIENT_CONNECTION::mr_conn_state
// similar to (active) tasks
//

#define MR_CONN_UNINITIALIZED   0
    // connection doesn't exist yet
#define MR_CONN_ACTIVE          1
    // connection is active, as far as we know
#define MR_CONN_ABORT_PENDING   5    /// BOINC-MR TODO (only aborted if overal serving/uploading time is over max threshold) ?
    // connection exceeded limits; send "abort" message, waiting to exit


// states in which the connection has exited/terminated
#define MR_CONN_EXITED          2
///#define MR_CONN_WAS_SIGNALED    3
///#define MR_CONN_EXIT_UNKNOWN    4
#define MR_CONN_ABORTED         6
    // aborted connection has exited
#define MR_CONN_COULDNT_START   7
    // error creating thread - could not start upload

#define MR_CLIENT_REQUEST_PREFIX "REQ_FILE:"
    // tag that identifies file name requested by client reducer

#define MR_DATASIZE_SEND        512
    // size of buffer used to send data to client reducer
#define MR_DATASIZE_RECV        512
    // size of buffer used to receive data from client mapper


// Existing connection (upload) to another client of MapReduce output
class MR_CLIENT_CONNECTION {
private:
    double mr_conn_start_t;        // instant when connection was initiated
    int _mr_conn_state;          // connection state
    int _sock_fd;                // socket file descriptor for this connection
public:
#ifdef _WIN32
    HANDLE pid_handle, shm_handle;
    bool kill_all_children();
#endif

    volatile bool is_running;
        // used on while loop to control when the connection is closed
        /// Volatile tells compiler not to use it for optimization - variable used to tell connection whether to stop running
        /// BOINC-MR TODO - used only for threads!

    /// Used only with fork() - new process to run MR_CLIENT_CONNECTION
    //PROCESS_ID pid;
	//PROCINFO procinfo;


    FILE_INFO* mr_fip;          // file currently being served
    int mr_client_sockfd;       // socket (fd) connected to client
    char mr_downloading_user[256];  // user that is downloading this file (IP)
    /// BOINC-MR TODO - save it as char* or IP address? (struct sockaddr_storage)
    std::vector<FILE_INFO*> mr_available_files;
        // current list of available files - required to check if requested file is available for upload [NOTE: not to be changed here, only in MR_FILE_SERVER]

    pthread_t connection_thread;

    /// BOINC-MR TODO - time stats of upload (upload_so_far)
    double mr_upload_start_time;
        // instant when upload first started
    double mr_upload_time_so_far;
        // Total time this file has been uploading
    double mr_upload_last_time;
        /// when the above was last updated.
        /// Defined only while file is being uploaded
    int client_connection_retval;
        // errors defined in error_numbers.h (0 if no error) at the end of connection

    void set_mr_conn_state(int, const char*);
    inline int mr_conn_state() {
        return _mr_conn_state;
    }

    void set_sock_fd(int);
    inline int get_sock_fd(){
        return _sock_fd;
    }

    MR_CLIENT_CONNECTION();
    int upload_file(FILE_INFO*);
        // upload corresponding file to requesting user

    bool conn_exists();
    int mr_request_exit();
    int kill_conn();
        // tell a connection to terminate
    bool has_conn_exited();

    void init();
        // initialize variable values

    bool set_available_files(const std::vector<FILE_INFO*>&);
    FILE_INFO* mr_check_req_file(char*, const std::vector<FILE_INFO*>&);
        // check if requested file is being served. Returns corresponding FILE_INFO if it is, NULL otherwise
};


class MR_CLIENT_CONNECTION_SET {
private:
    //std::vector<PERS_FILE_XFER*>pers_file_xfers;
    std::vector<MR_CLIENT_CONNECTION*>mr_client_connections;
    void request_connections_exit(PROJECT* p=0);
    int mr_wait_for_exit(double, PROJECT* p=0);
    void kill_connections(PROJECT* p=0);
public:

    MR_CLIENT_CONNECTION_SET();
    int insert(MR_CLIENT_CONNECTION*);
    int remove(MR_CLIENT_CONNECTION*);

    int mr_terminate_connections(PROJECT* p=0);
    //int exit_tasks(PROJECT* p=0);

};


/// PERS_FILE_XFER represents a "persistent file transfer",
/// i.e. a long-term effort to upload or download a file.
/// This may consist of several "episodes",
/// which are HTTP operations to a particular server.
/// PERS_FILE_XFER manages
/// - the choice of data servers
/// - the retry and giveup policies
/// - restarting partial transfers
///
/// The FILE_INFO has a list of URLs.
/// For download, the object attempts to download the file
/// from any combination of the URLs.
/// For upload, try to upload the file in its entirety to one of the URLs.

/// a PERS_FILE_XFER is created and added to pers_file_xfer_set
/// 1) when read from the client state file
///   in (FILE_INFO::parse(), CLIENT_STATE::parse_state_file()
/// 2) when a FILE_INFO is ready to transfer
///   in CLIENT_STATE::handle_pers_file_xfers()

// MR_CLIENT_OP
class MR_CLIENT_XFER {
public:
    char dest_ip_addr[256];
    int dest_port;
    bool is_downloading;
    pthread_t running_thread;
    // same as FILE_XFER
    FILE_INFO* mr_fip;
    char pathname[256];
    char fname[256];

        /// time at which transfer started
    double start_time;

        /// File size at start of transfer, used for:
        /// 1) lets us recover when client ignored offset request
        /// and sent us whole file
    double starting_size;

    /// BOINC-MR TODO: calculate transfer speed - statistical purposes
    /// this includes previous count (i.e. file offset)
    double bytes_xferred;
        /// bytes_xferred at the start of this operation;
        /// used to compute transfer speed
	double start_bytes_xferred;
        /// tranfer rate based on elapsed time and bytes_xferred
    double xfer_speed;

    MR_CLIENT_XFER();
    ~MR_CLIENT_XFER();

    int init_download(FILE_INFO&);
    bool mr_client_xfer_done;
        // set to true just before thread exits (used to warn main thread when it
        // can process finished xfers on MR_CLIENT_XFER_SET::poll)
    bool mr_client_conn_over;
        // errors defined in error_numbers.h (0 if no error)
    int mr_client_xfer_retval;

        // set to true when pers_file_xfer::suspend() is called.
        // used to identify cases when file_info pointed by mr_fip may have been deleted
    bool suspended;

    bool suspend(){
        is_downloading = false;
    };
        // reset previous transfer - called in transient_failure(), and by constructor
        // resets all variables except mr_fip [same file being downloaded]
    void reset();
};

class MR_CLIENT_XFER_SET {
public:
    // http_ops;
    std::vector<MR_CLIENT_XFER*> client_xfers;

    // need to limit number of transfers/connections to other clients - 256 limit of created threads
    // sensible size: 50~100 limit
    int max_num_conn;

    int insert(MR_CLIENT_XFER*);
    int remove(MR_CLIENT_XFER*);
        // called by FILE_XFER_SET::suspend
    bool poll();

};

// thread functions
void *mr_download_from_mapper(void*);
    // download Map output from Mapper client

void *mr_start_listening(void*);
    // start listening to connections from clients (serving Map outputs)

void *serve_file(void *);
    // handle accepted connection from client - upload file


/*
class FILE_XFER_SET {
    HTTP_OP_SET* http_ops;
public:
        /// has there been transfer activity since last call to check_active()?
    bool up_active, down_active;
    std::vector<FILE_XFER*> file_xfers;
    FILE_XFER_SET(HTTP_OP_SET*);
    int insert(FILE_XFER*);
    int remove(FILE_XFER*);
    bool poll();
    void check_active(bool&, bool&);
    void set_bandwidth_limits(bool is_upload);
};

class PERS_FILE_XFER_SET {
public:
    FILE_XFER_SET* file_xfers;
    std::vector<PERS_FILE_XFER*>pers_file_xfers;

    PERS_FILE_XFER_SET(FILE_XFER_SET*);
    int insert(PERS_FILE_XFER*);
    int remove(PERS_FILE_XFER*);
    bool poll();
    void suspend();
};
*/
#endif // MR_TASKTRACKER_H_INCLUDED
