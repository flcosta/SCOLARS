#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>  // for inet_ntop()
#include <signal.h>     // for signal handling - sigaction()
#include <sys/wait.h>   // waitpid()
#include <errno.h>      // global variable errno
#include <fcntl.h>      // set socket non blocking

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>

// for itoa()
//#include <stdio.h>
//#include <stdlib.h>

#include "client_state.h"
#include "util.h"
#include "client_msgs.h"        // msg_printf()
#include "file_names.h"         // get_pathname()
#include "filesys.h"            // boinc_fopen()
#include "client_types.h"       // mapper_addr() functions
#include "str_util.h"           // boincerror()

#include <sys/time.h>           // struct timeval

#include "mr_tasktracker.h"

//#define PORT "8000"

#define MR_BACKLOG 10 // how many pending connections queue will hold

/// BOINC-MR DEBUG : handle this more gracefully
/// Enable inter-process communication between client process and server Socket listening (or just use sockets)...
//FILE_INFO ** pointer_mr_input_files;

MR_FILE_SERVER::MR_FILE_SERVER(){
    mr_is_listening = false;
    // reset user and file access lists
    mr_file_upload_list.clear();
    mr_user_access_list.clear();
    file_server_retval = 0;
}

// if it is still listening, stop
MR_FILE_SERVER::~MR_FILE_SERVER(){
    if (mr_is_listening){
        mr_is_listening = false;
    }
}

// set socket 'sock' as non-blocking to be used by listening/server socket in select()
bool setnonblocking(int sock){
	int opts;

	opts = fcntl(sock,F_GETFL);
	if (opts < 0) {
		perror("fcntl(F_GETFL)");
		//exit(EXIT_FAILURE);
		return false;
	}
	opts = (opts | O_NONBLOCK);
	if (fcntl(sock,F_SETFL,opts) < 0) {
		perror("fcntl(F_SETFL)");
		//exit(EXIT_FAILURE);
		return false;
	}
	return true;
}

// Suspend serving files
// - Set mr_is_listening to false, to stop listening
bool MR_FILE_SERVER::mr_suspend(){
    // BOINC-MR DEBUG
    printf("Called MR_FILE_SERVER::mr_suspend() - stop listening to connections.\n");
    mr_is_listening = false;
    return true;
}

bool MR_FILE_SERVER::mr_is_server_listening(){
    return mr_is_listening;
}

// Add new user to list to give him access to files
bool MR_FILE_SERVER::mr_add_user(std::string u){
    mr_user_access_list.push_back(u);
    /// BOINC-MR TODO : always true; no error can come from push_back. Make other checks or just return void?

    /// BOINC-MR TODO : if there were no users before AND ?there are files to be served?, start listening for incoming connections

    return true;
}

// Remove user from list to deny him access to files
bool MR_FILE_SERVER::mr_remove_user(std::string u){
    bool success = false;
    std::vector<std::string>::iterator user_iter;

    user_iter = mr_user_access_list.begin();
    while (user_iter != mr_user_access_list.end()) {
        if ((*user_iter).compare(u) == 0){
            user_iter = mr_user_access_list.erase(user_iter);
            success = true;
        }
        else
            user_iter++;
    }

    /// BOINC-MR TODO - if there are no more users, we should stop serving files

    return success;
}

// Add entry to list of available files
// If there were no files previously, start acting as server and start listening for incoming connections
bool MR_FILE_SERVER::mr_add_file(FILE_INFO* fip){
    std::vector<FILE_INFO*>::iterator file_iter;

    /// BOINC-MR DEBUG
    printf("Entered [MR_FILE_SERVER::mr_add_file()] Adding file: %s\n", fip->name);

    // First,check if the file isn't already there
    file_iter = mr_file_upload_list.begin();
    while (file_iter != mr_file_upload_list.end()) {

        // BOINC-MR DEBUG
        //printf("[MR_FILE_SERVER::mr_add_file()] Going through file list. Current file: %s | Comparing with: %s\n", (*file_iter)->name, fip->name);

        if (*file_iter == fip){

            // BOINC-MR DEBUG
            //printf("[MR_tasktracker] ERROR: File %s already being served for upload.\n", fip->name);

            // if this function was called, the mr_status is not correct, set status as available for upload
            fip->mr_status = MR_OUTPUT_FILE_UPLOADING;
            return false;
        }
        else{

            // BOINC-MR DEBUG
            //printf("[MR_FILE_SERVER::mr_add_file()] File %s not being served yet for upload. *file_iter: %p | fip: %p\n", fip->name, *file_iter, fip);
            file_iter++;
        }
    }

    mr_file_upload_list.push_back(fip);

    // BOINC-MR DEBUG
//    time_t now = time(0);
//    char* time_string = time_to_string((double)now);
//    printf("%s [MR_FILE_SERVER::mr_add_file()] Added file %s\n", time_string, fip->name);

    // if this is the only file in the list, user should start listening for incoming connections
    /// BOINC-MR TODO : if there are no accepted users, no point in start listening
    if (mr_file_upload_list.size() == 1){

        // BOINC-MR DEBUG
        printf("[MR_Tasktracker] First file added to upload list. Calling mr_start_listening()\n");
        fflush(stdout);

        if (!mr_is_listening){

            /// BOINC-MR TODO fork new process here or in mr_start_listening?
            // LINUX / MAC / UNIX case (windows and EMX must be done with other function - _WIN32 | __EMX__)

            /// move to thread...
            // BOINC-MR DEBUG
            int retval = pthread_create(&running_thread, NULL, mr_start_listening, this);
            //int retval = 0;
            if (!retval){
                // BOINC-MR DEBUG
                printf("[MR_Tasktracker] MR_FILE_SERVER thread created succesfully. \n");
                fflush(stdout);
            }
            else{
                // BOINC-MR DEBUG
                printf("Error in MR_FILE_SERVER::mr_add_file - pthread_create() failed\n");
                fflush(stdout);
                mr_is_listening = false;
                return false;
            }



            //int childpid = fork();
            //if (!childpid){   // child process



                // save ID of process running the server
                //mr_pid = getpid();
                //mr_start_listening();

                // BOINC-MR DEBUG
                //printf("[MR_Tasktracker] Child process running - left mr_start_listening. Calling exit(0). \n");
                //fflush(stdout);

                //exit(0);
//            }
            //else if (childpid == -1){
                // BOINC-MR DEBUG
                //printf("Error in MR_FILE_SERVER::mr_add_file - fork() failed\n");
                //return false;
            //}
            //else{
                // BOINC-MR DEBUG
            //printf("MR_Tasktracker - Main thread here. MR_FILE_SERVER thread running.\n");
            //printf("[MR_Tasktracker] Parent called fork, created child process with pid %d\n", childpid);
                // save ID of process running the server
                //mr_pid = childpid;
            //}
        }
        // there was an error, and client continued listening for connections although no files were being served
        else{
            /// BOINC-MR TODO : how to handle this? restart listening? make sure it never gets here?
            /// mr_listening = true ; but just added first file for serving [maybe server did not stop serving even after last file
            /// was removed from list]

        }
    }
    return true;
}


// Remove file from list to stop serving it to other users
bool MR_FILE_SERVER::mr_remove_file(FILE_INFO* fip){

    // BOINC-MR DEBUG
    printf("[MR_FILE_SERVER::mr_remove_file] Removing File: %s", fip->name);

    bool success = false;
    std::vector<FILE_INFO*>::iterator file_iter;

    file_iter = mr_file_upload_list.begin();
    while (file_iter != mr_file_upload_list.end()) {

        // BOINC-MR DEBUG
        printf("[MR_FILE_SERVER::mr_remove_file] Going through file list. Current file: %s | Comparing with: %s\n", (*file_iter)->name, fip->name);

        if (*file_iter == fip){
            file_iter = mr_file_upload_list.erase(file_iter);
            success = true;

            // BOINC-MR DEBUG
            printf("BOINC-MR DEBUG [MR_Tasktracker] Inside mr_remove_file() - successfully removed file %s from upload list\n", fip->name);

            // File is done being uploaded
            fip->mr_status = MR_OUTPUT_FILE_DONE;
            /// BOINC-MR TODO - change sticky value for this file? No longer needed, so it can be erased...
            fip->sticky = false;
            continue;

        }
        else
            file_iter++;
    }


    // suspend listening socket if there are no files being served
    if (success){
        if (mr_file_upload_list.empty()){
            success = mr_suspend();
        }
    }

    return success;
}



/// BOINC-MR TODO - go through MR_CLIENT_CONNECTIONS?
// scan all FILE_INFOs and MR_CLIENT_CONNECTIONs.
// start and stop serving files as needed/required
//
bool CLIENT_STATE::mr_handle_file_uploads(){
    unsigned int i;
    FILE_INFO* fip;
    //MR_CLIENT_CONNECTION *conn;
    bool action = false;
    static double last_time;
    double diff;

    // BOINC-MR DEBUG
    double tempd = 0;

    if (now - last_time < MR_CLIENT_CONNECTION_START_PERIOD) return false;
    last_time = now;

    // D_VAL DEBUG
    //printf("[MR_TASKTRACKER] Entered CLIENT_STATE::mr_handle_file_uploads. File_infos size: %zu\n", file_infos.size());
    //fflush(stdout);

    // Look for FILE_INFOs for which we should serving MAP outputs for reducers,
    // and make MR_CLIENT_CONNECTIONs for them
    //
    for (i=0; i<file_infos.size(); i++) {
        fip = file_infos[i];

        /// BOINC-MR DEBUG
        /// not mapreduce = error
        //if (!fip->mapreduce && !strstr(fip->name, "in")){
        //    msg_printf(NULL, MSG_INFO, "mr_handle_file_uploads:: ERROR: Not a MapReduce file: %s", fip->name);
        //}

        // D_VAL DEBUG
//        printf("[CLIENT_STATE::mr_handle_file_uploads] Inside file_infos loop (#%d). File: %s\n", i, fip->name);
//        fflush(stdout);

        // must be output (generated locally)
        // C2C: unless it is explicitely defined as available for upload (<c2c_up/> tag)
        if (!fip->generated_locally && !fip->c2c_upload) continue;

        // D_VAL DEBUG
//        printf("[CLIENT_STATE::mr_handle_file_uploads] OUTPUT File (gen locally): %s\n", fip->name);
//        fflush(stdout);

        // must be part of MapReduce job, scheduled for distributed validation, or be explicitely defined as available for upload
        if (!fip->mapreduce && !fip->dist_val && !fip->c2c_upload) continue;

        // D_VAL DEBUG
        //printf("[Dist val or MR] File: %s | mr_status: %d | status: %d | dist_val: %d\n", fip->name, fip->mr_status, fip->status, fip->dist_val);
        //fflush(stdout);

        // BOINC-MR DEBUG
        // check if there is something wrong with this file (for not being added to upload list)
//        if (fip->mr_status != MR_OUTPUT_FILE_UPLOADING && fip->mr_file_type == MR_MAP_OUTPUT && fip->status != FILE_NOT_PRESENT){
//            printf("File: %s | mr_status: %d | status: %d | mr_file_type: %d", fip->name, fip->mr_status, fip->status, fip->mr_file_type);
//            fflush(stdout);
//        }

        //D_VAL DEBUG
//        if (fip->mr_status != MR_OUTPUT_FILE_UPLOADING && fip->dist_val && fip->status != FILE_NOT_PRESENT){
//            printf("File: %s | mr_status: %d | dist_val: %d | status: %d\n", fip->name, fip->mr_status, fip->dist_val, fip->status);
//            fflush(stdout);
//        }

        //else if (fip->mr_file_type == MR_MAP_OUTPUT && fip->status != FILE_NOT_PRESENT){
        //    msg_printf(NULL, MSG_INFO, "[mr_status = UPLOADING] File: %s | mr_status: %d | status: %d | mr_file_type: %d", fip->name, fip->mr_status, fip->status, fip->mr_file_type);
        //}
        //else if(fip->mr_status != MR_OUTPUT_FILE_UPLOADING && fip->status != FILE_NOT_PRESENT && !strstr(fip->name, "in")){
        //    msg_printf(NULL, MSG_INFO, "[mr_file_type != Map Output] File: %s | mr_status: %d | status: %d | mr_file_type: %d", fip->name, fip->mr_status, fip->status, fip->mr_file_type);
        //}
        //else if(!strstr(fip->name, "in") && fip->status != FILE_NOT_PRESENT){
        //    msg_printf(NULL, MSG_INFO, "[mr_file NOT in] File: %s | mr_status: %d | status: %d | mr_file_type: %d", fip->name, fip->mr_status, fip->status, fip->mr_file_type);
        //}

        // If file is present, but it hasn't already been uploaded and is not currently being uploaded (and is a Map output)
        if (fip->mr_status != MR_OUTPUT_FILE_DONE && fip->mr_status != MR_OUTPUT_FILE_UPLOADING && fip->status == FILE_PRESENT &&
            fip->mr_file_type == MR_MAP_OUTPUT){

            // BOINC-MR DEBUG
//            printf("[MR_FILE_SERVER::mr_handle_file_uploads] Adding file: %s to upload list, since its status is not uploading or done\n", fip->name);
//            fflush(stdout);

            // add file (file_info*) to list of files available for upload
            if (!mr_file_server.mr_add_file(fip)){
                // BOINC-MR DEBUG
                printf("[MR_FILE_SERVER::mr_handle_file_uploads] ERROR Adding file %s to upload list\n", fip->name);
                fflush(stdout);
                // if there was an error
                continue;
            }

            // set status as available for upload
            fip->mr_status = MR_OUTPUT_FILE_UPLOADING;

            // save instant when file starts being served and set as last update on total uploading time so far
            fip->mr_serving_start_time = now;
            fip->mr_serving_last_time = now;
            action = true;
        }
        /// Distributed Validation: D_VAL
        // check if these files must be hosted for distributed validation purposes (will be downloaded and validated by another client, not the server)
        else if (fip->mr_status != MR_OUTPUT_FILE_DONE && fip->mr_status != MR_OUTPUT_FILE_UPLOADING && fip->status == FILE_PRESENT &&
                 fip->dist_val){
            // D_VAL DEBUG
            printf("[MR_FILE_SERVER::mr_handle_file_uploads] Adding Dist_val output file: %s to upload list, since its status is not uploading or done\n", fip->name);
            fflush(stdout);

            // add file (file_info*) to list of files available for upload
            if (!mr_file_server.mr_add_file(fip)){
                // D_VAL DEBUG
                printf("[MR_FILE_SERVER::mr_handle_file_uploads] ERROR Adding D_VAL output file %s to upload list\n", fip->name);
                fflush(stdout);
                // if there was an error
                continue;
            }

            // set status as available for upload
            fip->mr_status = MR_OUTPUT_FILE_UPLOADING;

            // save instant when file starts being served and set as last update on total uploading time so far
            fip->mr_serving_start_time = now;
            fip->mr_serving_last_time = now;
            action = true;
        }
        // C2C : if file is set as being available for upload (tag <c2c_up/>), host it as well
        else if(fip->mr_status != MR_OUTPUT_FILE_DONE && fip->mr_status != MR_OUTPUT_FILE_UPLOADING && fip->status == FILE_PRESENT &&
                 fip->c2c_upload){
            // C2C DEBUG
            printf("[MR_FILE_SERVER::mr_handle_file_uploads] Adding file (C2C_upload): %s to upload list, since its status is not uploading or done\n", fip->name);
            fflush(stdout);

            // add file (file_info*) to list of files available for upload
            if (!mr_file_server.mr_add_file(fip)){
                // C2C DEBUG
                printf("[MR_FILE_SERVER::mr_handle_file_uploads] ERROR Adding D_VAL output file %s to upload list\n", fip->name);
                fflush(stdout);
                // if there was an error
                continue;
            }

            // set status as available for upload
            fip->mr_status = MR_OUTPUT_FILE_UPLOADING;

            // save instant when file starts being served and set as last update on total uploading time so far
            fip->mr_serving_start_time = now;
            fip->mr_serving_last_time = now;
            action = true;
        }
        // D-VAL DEBUG
        else if (fip->status == FILE_PRESENT && fip->mr_status != MR_OUTPUT_FILE_UPLOADING && fip->mr_status != MR_OUTPUT_FILE_DONE){
//            printf("[MR_FILE_SERVER::mr_handle_file_uploads] File present, but not added to upload list: %s | d_val: %d\n", fip->name, fip->dist_val);
//            fflush(stdout);
        }

        // go through list of hosted/served files and check if any have gone over the max upload period (MR_SOCKET_SERVER_TIMEOUT)
        // - check mr_upload_start_time (instant when it last started uploading) AND mr_upload_total_time (total upload time so far)
        else if(fip->mr_status == MR_OUTPUT_FILE_UPLOADING && fip->status == FILE_PRESENT){

            // BOINC-MR DEBUG
            //tempd = MR_SOCKET_SERVER_TIMEOUT;
            //printf("tempd (MR_SOCKET_SERVER_TIMEOUT) [f] = %f\n", tempd);
            //printf("[MR_FILE_SERVER::mr_handle_file_uploads] Updating upload times. File: %s\n", fip->name);
            //printf("U/L time so far: %f | Max: %i\n", fip->mr_serving_time_so_far, MR_SOCKET_SERVER_TIMEOUT);

            // don't count suspended periods in total time
            //
            diff = now - fip->mr_serving_last_time;
            if (diff <= 2) {
                fip->mr_serving_time_so_far += diff;
            }
            fip->mr_serving_last_time = now;

            /// BOINC-MR DEBUG
            /// do not delete for now... OR increase timeout
            // if over threshold, stop uploading
            if (fip->mr_serving_time_so_far > MR_SOCKET_SERVER_TIMEOUT){

                // BOINC-MR DEBUG
                printf("CLIENT_STATE::mr_handle_file_uploads - Removing file %s from upload list\n", fip->name);
                fflush(stdout);

                // remove file from list
                if (!mr_file_server.mr_remove_file(fip)){
                    printf("[MR_Trasktracker] handle_mr_file_uploads - Error deleting file %s\n", fip->name);
                    continue;
                }
                action = true;
            }
        }


    }

    /// BOINC-MR - go through MR_CLIENT_CONNECTION_SET and update uploading times for active uploads
    // don't count suspended periods in total time
            //
            /*
            MR_CLIENT_CONNECTION *mrconn;
            diff = now - mrconn->mr_upload_last_time;
            if (diff <= 2) {
                mrconn->mr_upload_time_so_far += diff;
            }
            mrconn->mr_upload_last_time = now;*/

    // BOINC-MR DEBUG
    //printf("[MR_TASKTRACKER] Leaving CLIENT_STATE::mr_handle_file_uploads\n");
    //fflush(stdout);

    return action;
}


//  get info on host connected to other side of socket
void get_info_on_host(int s){

    /// --- getpeername() ---

    // assume s is a connected socket
    socklen_t len;
    struct sockaddr_storage addr;
    char ipstr[INET6_ADDRSTRLEN];
    int port;

    len = sizeof addr;
    getpeername(s, (struct sockaddr*)&addr, &len);

    // deal with both IPv4 and IPv6:
    if (addr.ss_family == AF_INET) {

        // BOINC-MR DEBUG
        printf("get_info_on_host() Family = AF_INET (IPv4)");

        struct sockaddr_in *s = (struct sockaddr_in *)&addr;
        port = ntohs(s->sin_port);
        inet_ntop(AF_INET, &s->sin_addr, ipstr, sizeof ipstr);
    } else{ // AF_INET6

        // BOINC-MR DEBUG
        printf("get_info_on_host() Family = AF_INET6 (IPv6)");

        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&addr;
        port = ntohs(s->sin6_port);
        inet_ntop(AF_INET6, &s->sin6_addr, ipstr, sizeof ipstr);
    }

    printf("Peer IP address: %s\n", ipstr);
    printf("Peer port      : %d\n", port);
}



/**
* BOINC-MR TODO
* handle error numbers returned by this function
*
**/
//int MR_FILE_SERVER::mr_start_listening(){
void *mr_start_listening(void *f_server){
    MR_FILE_SERVER* file_server;

    file_server = (MR_FILE_SERVER *) f_server;

    //int mr_sockfd; // defined on mr_tasktracker.h
    struct addrinfo hints, *servinfo, *p;
    // sigaction structure used to define the actions to be taken on receipt of specified signal
    struct sigaction sa;
    int yes=1;
    int rv;

    int temp_port = gstate.mr_get_output_port();     // cast to char* to pass it to getaddrinfo()
    char str_port[33];
    sprintf(str_port,"%d",temp_port); // converts to decimal base - standard-compliant alternative

    // BOINC-MR DEBUG
    printf("mr_start_listening - thread\n");
    fflush(stdout);

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;        // don't care IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE; // use my IP

    // get info on this client's address
    if ((rv = getaddrinfo(NULL, str_port, &hints, &servinfo)) != 0) {
        fprintf(stderr, "[mr_tasktracker::mr_start_listening] Error in getaddrinfo: %s\n", gai_strerror(rv));
        file_server->set_is_listening(false);
        file_server->file_server_retval = ERR_MR_GETADDRINFO;
        pthread_exit(NULL);
        // return ERR_MR_GETADDRINFO;
    }

    // loop through all the results and bind to the first we can
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((file_server->mr_sockfd = socket(p->ai_family, p->ai_socktype,
                p->ai_protocol)) == -1) {
            msg_printf(0, MSG_INTERNAL_ERROR, "[mr_tasktracker::mr_start_listening] server: socket. retval: %d", errno);
            perror("[mr_tasktracker::mr_start_listening] server: socket");
            continue;
        }

        // lose the "Address already in use" error message
        if (setsockopt(file_server->mr_sockfd, SOL_SOCKET, SO_REUSEADDR, &yes,
                sizeof(int)) == -1) {
            msg_printf(0, MSG_INTERNAL_ERROR, "[mr_tasktracker::mr_start_listening] server: setsockopt. retval: %d", errno);
            perror("[mr_tasktracker::mr_start_listening] setsockopt");
            file_server->set_is_listening(false);
            pthread_exit(NULL);
            //exit(1);
        }

        if (bind(file_server->mr_sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(file_server->mr_sockfd);
            msg_printf(0, MSG_INTERNAL_ERROR, "[mr_tasktracker::mr_start_listening] server: bind. retval: %d", errno);
            perror("[mr_tasktracker::mr_start_listening] server: bind");
            continue;
        }

        break;
    }

    // Check if successful with at least one of the addresses
    if (p == NULL) {
        msg_printf(0, MSG_INTERNAL_ERROR, "[mr_tasktracker::mr_start_listening] server: failed to bind");
        fprintf(stderr, "[mr_tasktracker::mr_start_listening] server: failed to bind\n");
        // check if address is in use - in that case, try next port
        gstate.mr_set_output_port(temp_port+1);
        file_server->set_is_listening(false);
        file_server->file_server_retval = ERR_BIND;
        pthread_exit(NULL);
        //return ERR_BIND;
    }

    // BOINC-MR DEBUG
    //printf("MR_FILE_SERVER::mr_start_listening() - Server's info\n");
    void *addr;
    char ipver[16];
    char ipstr[INET6_ADDRSTRLEN];
    int port;
    // get the pointer to the address itself,
    // different fields in IPv4 and IPv6:
    if (p->ai_family == AF_INET) { // IPv4
        struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
        addr = &(ipv4->sin_addr);
        port = ntohs(ipv4->sin_port);
        //ipver = "IPv4";
        sprintf(ipver, "IPv4");
    } else { // IPv6
        struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)p->ai_addr;
        addr = &(ipv6->sin6_addr);
        port = ntohs(ipv6->sin6_port);
        //ipver = "IPv6";
        sprintf(ipver, "IPv6");
    }
    // convert the IP to a string and print it:
    inet_ntop(p->ai_family, addr, ipstr, sizeof ipstr);
    //printf(" %s:: %s | port: %i\n", ipver, ipstr, port);

    freeaddrinfo(servinfo); // all done with this structure
    // listen for incoming connections (queue max = MR_BACKLOG
    if (listen(file_server->mr_sockfd, MR_BACKLOG) == -1) {
        perror("[mr_tasktracker::mr_start_listening] listen\n");
        fflush(stderr);
        file_server->set_is_listening(false);
        /// BOINC-MR TODO - leave here or warn main process that it has encountered an error
        // return ERR_LISTEN;
        pthread_exit(NULL);
        //exit(1);
    }

    // responsible for reaping zombie processes that appear as the fork()ed child processes exit
    // sa_handler is a pointer to a function called when signal is received
    /*sa.sa_handler = sigchld_handler; // reap all dead processes
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    // sets the action associated with the signal SIGCHLD
    if (sigaction(SIGCHLD, &sa, NULL) == -1) {
        perror("[mr_tasktracker] sigaction");
        file_server->set_is_listening(false);
        pthread_exit(NULL);
        //exit(1);
    }
    */

    // BOINC-MR DEBUG
    printf("[BOINC-MR DEBUG] mr_tasktracker start_listening() | server: waiting for connections...\n");
    fflush(stdout);

    file_server->accept_connections();

    pthread_exit(NULL);

}


// keep accepting connections while network is up AND there are files to serve/upload
bool MR_FILE_SERVER::accept_connections(){

    int new_fd; // listen on mr_sockfd, new connection on new_fd
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;
    char s[INET6_ADDRSTRLEN];
    mr_is_listening = true;
    int retval;

    /// BOINC-MR TODO - handle each accepted connection
    // New function (e.g.: accept_conn())
    // If from allowed user AND requesting existing file:
    // - Create new MR_CLIENT_CONNECTION object and add it to the MR_CLIENT_CONNECTION_SET
    // - save socket that is connecting to client; client's IP; requested file's file_info
    // - set connection start time/instant (now)? Save as statistics on how much time it took to upload file / avg txf speed
    //
    //while(1) { // main accept() loop

    // BOINC-MR DEBUG
    //printf("Inside MR_FILE_SERVER::accept_connections, entering while loop\n");

    while(mr_is_listening) {
        sin_size = sizeof their_addr;

        // BOINC-MR DEBUG
        //printf("Inside MR_FILE_SERVER::accept_connections, entered while loop; just before calling accept(). mr_is_listening=%i\n", mr_is_listening);
        //fflush(stdout);

        /// BOINC-MR TODO : replace accept() with select()
        /*fd_set set;
        struct timeval timeout;

        // Set socket to non-blocking with our setnonblocking routine
        /// Set before bind()
        setnonblocking(mr_sockfd);

        // Initialize the file descriptor set.
        FD_ZERO (&set);
        //FD_SET (filedes, &set);
        FD_SET (mr_sockfd, &set);

        // Initialize the timeout data structure. 3 seconds? BOINC waits 5 seconds for apps to finish
        timeout.tv_sec = seconds;
        timeout.tv_usec = 0;

        // select returns 0 if timeout, 1 if input available, -1 if error.
        /// Since we start with only one socket, the listening socket,
        //it is the highest socket so far.
        /// int highsock = mr_sockfd; // unnecessary - only one socket listening
        //int readsocks = select(highsock+1, &socks, (fd_set *) 0, (fd_set *) 0, &timeout);
        /* The first argument to select is the highest file
			descriptor value plus 1. In most cases, you can
			just pass FD_SETSIZE and you'll be fine. */
        /* The third parameter is an fd_set that you want to
			know if you can write on -- this example doesn't
			use it, so it passes 0, or NULL. The fourth parameter
			is sockets you're waiting for out-of-band data for,
			which usually, you're not. */
        /*int readsocks = select(mr_sockfd+1, &socks, (fd_set *) 0, (fd_set *) 0, &timeout);

        /* select() returns the number of sockets that had
			things going on with them -- i.e. they're readable. */

		/* Once select() returns, the original fd_set has been
			modified so it now reflects the state of why select()
			woke up. i.e. If file descriptor 4 was originally in
			the fd_set, and then it became readable, the fd_set
			contains file descriptor 4 in it. */

        /*if (readsocks < 0) {
			perror("select");
			file_server_retval = ERR_MR_SELECT;
			pthread_exit(NULL);
			exit(EXIT_FAILURE);
		}

		if (readsocks == 0) {
			// Nothing ready to read, just show that we're alive
			// BOINC-MR DEBUG
			printf(".");
			fflush(stdout);
		} else
			read_socks();

        // read_socks():
        if (FD_ISSET(sock,&socks))
            //handle_new_connection();
            pthread_create... [serve_file()]
            /// move all checks into thread

        ///return TEMP_FAILURE_RETRY (select (FD_SETSIZE, &set, NULL, NULL, &timeout));
        */
        new_fd = accept(mr_sockfd, (struct sockaddr *)&their_addr, &sin_size);

        // BOINC-MR DEBUG
        //printf("Inside MR_FILE_SERVER::accept_connections. Just accepted incoming connection!!!\n");
        //fflush(stdout);

        if (new_fd == -1) {
            msg_printf(NULL, MSG_INTERNAL_ERROR,"MR_FILE_SERVER::accept_connections. Error in accept()");
            continue;
        }
        inet_ntop(their_addr.ss_family,
            get_in_addr((struct sockaddr *)&their_addr),
            s, sizeof s);

        // BOINC-MR DEBUG
        time_t now = time(0);
        char* time_string = time_to_string((double)now);
        printf("%s [MR_FILE_SERVER::accept_connections()] server: got connection from %s\n", time_string, s);
        fflush(stdout);

        MR_CLIENT_CONNECTION *mrconn = new MR_CLIENT_CONNECTION;
        // Initialize connection variables
        mrconn->init();
        mrconn->set_mr_conn_state(MR_CONN_ACTIVE, "accept_connections");
        mrconn->set_sock_fd(new_fd);

        /// BOINC-MR TODO add connection to MR_CONNECTION_SET - defined for client_state
        /// Problem: always returns 0 - never error
        if (gstate.mr_client_connections->insert(mrconn)){
            // error
            msg_printf(NULL, MSG_INTERNAL_ERROR, "BOINC-MR accept_connections() Error adding client connection to list in client_state.");
            close(new_fd);
            mrconn = NULL;
            delete mrconn;
            continue;
        }

        /// BOINC-MR - change from fork() to threads
        // Example: int start_timer_thread() - lib/boinc_api.cpp
        //pthread_t nThreadID;
        /// Continue...
        // give client connection the current list of files being served (needed inside thread to check if file requested
        // by downloading client is available)
        mrconn->set_available_files(mr_file_upload_list);
        mrconn->is_running = true;
        retval = pthread_create(&(mrconn->connection_thread), NULL, serve_file, mrconn);
        if (!retval){
            // BOINC-MR DEBUG
            //printf("[MR_Tasktracker] MR_CLIENT_CONNECTION thread created succesfully. \n");
            //fflush(stdout);
        }
        else{
            // BOINC-MR DEBUG
            printf("Error in MR_FILE_SERVER::accept_connections - pthread_create() failed\n");
            fflush(stdout);
            close(new_fd);
            mrconn->set_mr_conn_state(MR_CONN_COULDNT_START, "accept_connections");
            mrconn->is_running = false;
            continue;
        }
    }

    close(mr_sockfd);
    // after mr_is_listening has been set to false, thread is not needed any longer and exits
    //exit(0);
    pthread_exit(NULL);

}

/// BOINC-MR - function run by client connection Thread
//
// Should be similar to upload_file(). differences:
// - check for requesting IP / requested file here?
//
// Parameters
//
//void * runMe(void *generic_pointer)
void *serve_file(void *client_conn){
    int retval;
    FILE_INFO *f_info;                  // file info requested by user
    MR_CLIENT_CONNECTION* mr_conn = (MR_CLIENT_CONNECTION*)client_conn;
    // Serve file until told to stop

    //if (!fork()) { // this is the child process
    int new_fd = mr_conn->get_sock_fd();

    // BOINC-MR DEBUG
    // hello world
    //if (send(new_fd, "Hello, world!", 13, 0) == -1)
      //  perror("send");

    // Process request - get name of requested file
    char buf[256];
    int recv_bytes = 0;
    // problem with receiving extra bytes (maybe buf was not completely 'clean', so memset to 0)
    //buf =
    //if (recv(new_fd, buf, sizeof(buf)-1, 0) == -1) {
    if ((recv_bytes = recv(new_fd, buf, sizeof(buf)-1, 0)) == -1) {
        msg_printf(NULL, MSG_INFO, "accept_connections - Error serving file. Error calling function recv(): %d", errno);
        perror("recv [serve_file]");
        close(new_fd);
        mr_conn->client_connection_retval = ERR_MR_CLIENT_SOCKET_RECV;
        pthread_exit(NULL);
        //exit(1);
    }

    if (buf == NULL){
        msg_printf(NULL, MSG_INFO, "accept_connections - Error serving file. Did not received any data from recv.\n");
        close(new_fd);
        mr_conn->client_connection_retval = ERR_MR_CLIENT_REQ_PREFIX;
        pthread_exit(NULL);
    }

    // must add the '\0' at the end of the recv_bytes
    buf[recv_bytes] = '\0';

    // BOINC-MR DEBUG
    msg_printf(NULL, MSG_INFO, "Client request: %s\n", buf);
    fflush(stdout);

    // BOINC-MR DEBUG
//    msg_printf(NULL, MSG_INFO, "Received %d bytes. Client Request with 0 at end (buf[nbytes]): %s\n", recv_bytes, buf);
//    fflush(stdout);

    // parse request
    char *p;
    // check if this is a request
    p = strstr(buf, MR_CLIENT_REQUEST_PREFIX);
    if (!p){
        msg_printf(NULL, MSG_INFO, "accept_connections - Error serving file. Client request did not follow specified template\n");
        close(new_fd);
        mr_conn->client_connection_retval = ERR_MR_CLIENT_REQ_PREFIX;
        pthread_exit(NULL);
        //exit(0);
    }

    p += strlen(MR_CLIENT_REQUEST_PREFIX);
    char fname[256];
    strncpy(fname, p, sizeof(fname)-1);
    fname[sizeof(fname)-1] = '\0';
    // BOINC-MR DEBUG
    //printf("Requested file: %s\n", fname);
    //fflush(stdout);

    /// BOINC-MR TODO check if file is in file list - mr_file_upload_list
    //strcpy(fname, "mapred_map_3-min_wu_1_0");
    //f_info = mr_conn->mr_check_req_file(fname);
    if (!mr_conn->mr_available_files.empty())
        f_info = mr_conn->mr_check_req_file(fname, mr_conn->mr_available_files);
    else{

        // BOINC-MR DEBUG
        msg_printf(NULL, MSG_INTERNAL_ERROR,"serve_file(). mr_available_files empty");
        fprintf(stderr, "serve_file(). mr_available_files empty\n");
        fflush(stderr);

        // error - MR_FILE_SERVER may have terminated
        close(new_fd);
        mr_conn->client_connection_retval = ERR_MR_FILE_NOT_SERVED;
        pthread_exit(NULL);
    }

    //printf("after mr_check_req_file\n");
    //fflush(stdout);

    /// Check if requested file is on upload list and return corresponding FILE_INFO if it is
    if (f_info == NULL){
        // BOINC-MR DEBUG
        msg_printf(NULL, MSG_INTERNAL_ERROR, "f_info = NULL. File name: %s", fname);
        fflush(stdout);
        fprintf(stderr, "serve_file(): f_info = NULL. File name: %s\n", fname);
        fflush(stderr);
        //if (log_flags.mr_debug) {
        //msg_printf(NULL, MSG_INFO, "[mr_file_server_debug] Error: User=%s asked for file %s not being served\n",
        //                   user, fname
        //);
        //}

        close(new_fd);
        /// BOINC-MR TODO - delete mr_conn and remove it from set? Or done by main thread?
        //gstate.mr_client_connections.remove(mrconn);
        //del mrconn;
        mr_conn->client_connection_retval = ERR_MR_FILE_NOT_SERVED;
        pthread_exit(NULL);
        //exit(0);
        ///continue; - this thread will exit (new thread will be responsible for another connection)
    }
    else{
        // BOINC-MR DEBUG
        //printf("f_info != NULL\n");
        //fflush(stdout);
    }


    // BOINC-MR DEBUG
    /// check if thread is being saved correctly
    /*if (!mr_conn->connection_thread){
        printf("[BOINC-MR] serve_file() Thread not being stored correctly in mr_conn.connection_thread!\n");
        fflush(stdout);
    }
    else{
        if (pthread_equal(pthread_self(), mr_conn->connection_thread)){
            printf("[BOINC-MR] serve_file() Success: Thread running successfully stored in mr_conn.connection_thread...\n");
            fflush(stdout);
        }
        else{
            printf("[BOINC-MR] serve_file() Thread running NOT the same as the one stored in mr_conn.connection_thread!!\n");
            //exit(0);
        }
    }*/

    /// BOINC-MR TODO: version 2 Check if user is on accepted list
    char user[256];
    sprintf(user,"test");
    //char *user_addr;

    // save file_info
    mr_conn->mr_fip = f_info;

    // BOINC-MR DEBUG
    //printf("FILE_INFO name: %s\n", mr_conn->mr_fip->name);
    //fflush(stdout);

    retval = mr_conn->upload_file(mr_conn->mr_fip);
    if (retval){
        // error
        msg_printf(NULL, MSG_INTERNAL_ERROR,
                    "Can't upload file %s to client. %s", mr_conn->mr_fip->name,
                    boincerror(retval));
    }

    /// BOINC-MR TODO - delete mr_conn and remove it from set? Or done by main thread?
    //gstate.mr_client_connections.remove(mrconn);
    //del mrconn;

    // BOINC-MR DEBUG
    time_t now = time(0);
    char* time_string = time_to_string((double)now);
    printf("%s Finished upload of: %s\n", time_string, mr_conn->mr_fip->name);
    fflush(stdout);

    mr_conn->client_connection_retval = retval;
    close(mr_conn->get_sock_fd());
    //mr_conn->is_running = false;
    // set state as exited (normal exit) if the state has not been altered yet (to aborted, for example)
    if (mr_conn->mr_conn_state() == MR_CONN_UNINITIALIZED)
        mr_conn->set_mr_conn_state(MR_CONN_EXITED, "serve_file");
    pthread_exit(NULL);

    //return;
}

bool MR_CLIENT_CONNECTION::set_available_files(const std::vector<FILE_INFO*> &vec){
    mr_available_files = vec;
}


//
// Checks if file 'fname' is part of the Map outputs available for upload
//
/// D_VAL TODO: compare wu name with alt_name for each workunit
//
//FILE_INFO *MR_CLIENT_CONNECTION::mr_check_req_file(char *fname){
FILE_INFO* MR_CLIENT_CONNECTION::mr_check_req_file(char* fname, const std::vector<FILE_INFO*> &file_list){

    // BOINC-MR DEBUG
    //printf("Entered mr_check_req_file. available upload files size: %d\n", file_list.size());
    //fflush(stdout);

    //FILE_INFO *fip;
    std::vector<FILE_INFO*>::const_iterator file_iter;
    char workunit_name[256];
    char* char_divider, *p;         // position of character '_'
    int len;

    // D_VAL
    char d_val_workunit_name[256];  // workunit name taken by reading file name up to next to last '_' (WUNAME_CLIENT#_OUTPUT# instead of WU_OUTPUT#)
    char* d_val_char_div;           // pointer to next to last '_'

    // parse wu name from requested file name
    if (!(char_divider = strrchr(fname, '_'))){
        msg_printf(NULL, MSG_INFO, "Requested file %s does not follow naming guidelines [no '_' character to identify index]", fname);
        return NULL;
    }
    len = (int) (char_divider-fname);
    if (len >= sizeof(workunit_name)) len = sizeof(workunit_name)-1;
    strncpy(workunit_name, fname, len);
    workunit_name[len] = '\0';

    // D_VAL: parse wu name from workunit_name (remove string starting from last '_' in workunit_name)
    if (!(d_val_char_div = strrchr(workunit_name, '_'))){
        msg_printf(NULL, MSG_INFO, "Distributed Validation: Requested file %s does not follow D_VAL naming guidelines [no '_' character to identify client ID]", workunit_name);
        strcpy(d_val_workunit_name, "");
    }
    else{
        len = (int) (d_val_char_div-workunit_name);
        if (len >= sizeof(d_val_workunit_name)) len = sizeof(d_val_workunit_name)-1;
        strncpy(d_val_workunit_name, workunit_name, len);
        d_val_workunit_name[len] = '\0';
        // D_VAL DEBUG
        msg_printf(NULL, MSG_INFO, "Distributed Validation: Requested file %s 2nd work unit name [reading two '_']: %s", fname, d_val_workunit_name);
    }



    // BOINC-MR DEBUG
    //printf("Requested file's Work Unit: %s.\n", workunit_name);
    //fflush(stdout);

    msg_printf(NULL, MSG_INFO,
               "D_VAL. Comparing to: workunit_name: %s | 2nd possible work unit name: %s", workunit_name, d_val_workunit_name);


    file_iter = file_list.begin();
    // move pointer to after '_', to get index
    char_divider++;
    while (file_iter != file_list.end()) {

        // D_VAL DEBUG
        //msg_printf(NULL, MSG_INFO,"File (%s) alt WU name: %s",(*file_iter)->name , (*file_iter)->alt_wu_name.c_str());
        //fflush(stdout);

        // BOINC-MR DEBUG
        //printf("Inside loop of mr_check_req_file. Comparing [%s] to file: %s\n", fname, (*file_iter)->name);
        //fflush(stdout);

        /// Distributed Validation: D_VAL
        /// DEPRECATED - alternative_name no longer saved in FILE_INFO, but WORKUNIT
        // check if the requested file name corresponds to  the alternative name
        /*if (!((*file_iter)->alternative_name.empty())){
            if (strcmp(fname, (*file_iter)->alternative_name.c_str())== 0){
                return *file_iter;
            }
        }*/

        /*if ( strcmp((*file_iter)->name, fname) == 0 ){
            return *file_iter;
        }*/

        // BOINC-MR DEBUG
        //printf("Comparing [%s]'s Workunit %s to file's workunit: %s\n", fname, workunit_name, (*file_iter)->mr_wu_name);
        //fflush(stdout);

        // Check if Work Unit name corresponds to this file's wu
        if (strcmp((*file_iter)->mr_wu_name, workunit_name) == 0){

            // BOINC-MR DEBUG
            //printf("Success! File %s has same workunit name: %s\n", (*file_iter)->name, (*file_iter)->mr_wu_name);
            //fflush(stdout);

            p = strrchr((*file_iter)->name, '_');
            // move pointer to this file's Index (after '_')
            p++;
            // If indexes match, this is the file

            // BOINC-MR DEBUG
            //printf("Comparing indexes. [File||index]: %s||%s *** req file index: %s\n", (*file_iter)->name, p, char_divider);
            //fflush(stdout);


            if (strcmp(p, char_divider) == 0){
                return *file_iter;
            }
            else{
                file_iter++;
            }
        }
        /// D_VAL TODO : check if Work Unit alternative name corresponds to this file's wu
        else if (strcmp((*file_iter)->alt_wu_name.c_str(), workunit_name) == 0){

            // D_VAL DEBUG
            //printf("Success! File %s has same ALTERNATIVE workunit name: %s\n", (*file_iter)->name, (*file_iter)->alt_wu_name.c_str());
            //fflush(stdout);

            p = strrchr((*file_iter)->name, '_');
            // move pointer to this file's Index (after '_')
            p++;
            // If indexes match, this is the file

            // D_VAL DEBUG
            //printf("Comparing indexes. [File||index]: %s||%s *** req file index: %s\n", (*file_iter)->name, p, char_divider);
            //fflush(stdout);


            if (strcmp(p, char_divider) == 0){

                // D_VAL DEBUG
                printf("Success! Comparing indexes. [File||index]: %s||%s *** req file index: %s\n", (*file_iter)->name, p, char_divider);
                fflush(stdout);

                return *file_iter;
            }
            else{
                file_iter++;
            }
        }
        /// D_VAL TODO : check if Work Unit alternative name corresponds to this file's wu (save up to the '_' next to last)
        /// file_name: ALTNAME_CLIENT#_OUTPUT#
        else if (strcmp((*file_iter)->alt_wu_name.c_str(), d_val_workunit_name) == 0){
            // D_VAL DEBUG
            //printf("Success! File %s has same ALTERNATIVE workunit name: %s\n", (*file_iter)->name, (*file_iter)->alt_wu_name.c_str());
            //fflush(stdout);

            p = strrchr((*file_iter)->name, '_');
            // move pointer to this file's Index (after '_')
            p++;
            // If indexes match, this is the file

            // D_VAL DEBUG
            //printf("Comparing indexes. [File||index]: %s||%s *** req file index: %s\n", (*file_iter)->name, p, char_divider);
            //fflush(stdout);


            if (strcmp(p, char_divider) == 0){

                // D_VAL DEBUG
                printf("Success! Comparing indexes. [File||index]: %s||%s *** req file index: %s\n", (*file_iter)->name, p, char_divider);
                fflush(stdout);

                return *file_iter;
            }
            else{
                file_iter++;
            }
        }
        else{
            file_iter++;
        }
    }

    return NULL;
}


// Initialize variable values
void MR_CLIENT_CONNECTION::init(){
    mr_conn_start_t = gstate.now;
    _mr_conn_state = MR_CONN_UNINITIALIZED;
    _sock_fd = 0;

#ifdef _WIN32
    //HANDLE pid_handle, shm_handle;
#endif

    /// only for threads
    is_running = false;

    /// for fork() [process]
    PROCESS_ID pid;
	PROCINFO procinfo;

    //mr_fip = NULL;
    mr_client_sockfd = 0;
    //mr_downloading_user = "0.0.0.0";
    sprintf(mr_downloading_user, "0.0.0.0");

    mr_upload_start_time = 0;
    mr_upload_time_so_far = 0;
    mr_upload_last_time = gstate.now;
    client_connection_retval = 0;
}


int MR_CLIENT_CONNECTION_SET::insert(MR_CLIENT_CONNECTION* conn){
    mr_client_connections.push_back(conn);
    return 0;
}



// get sockaddr, IPv4 or IPv6:
void *MR_FILE_SERVER::get_in_addr(struct sockaddr *sa){
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }
    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}


/// BOINC-MR TODO : deprecated
/// Only necessary if using processes to serve files (for each incoming connection - fork())
// Called when SIGCHLD signal is received - reaps all dead processes
//void sigchld_handler(int s){
//    while(waitpid(-1, NULL, WNOHANG) > 0);
//}


// Send quit signal to all processes handling existing connections
// in the project (or all tasks, if proj==0).
// If they don't exit in 5 seconds,
// send them a kill signal and wait up to 5 more seconds to exit.
// This is called when the core client exits,
// or when a project is detached or reset
//
/// BOINC-MR TODO
//
//int ACTIVE_TASK_SET::exit_tasks(PROJECT* proj) {
int MR_CLIENT_CONNECTION_SET::mr_terminate_connections(PROJECT* proj){
    request_connections_exit(proj);

    // Wait 5 seconds for them to exit normally; if they don't then kill them
    //
    if (mr_wait_for_exit(5, proj)) {
        kill_connections(proj);
    }
    // BOINC-MR DEBUG
    else{
        // BOINC-MR DEBUG
        printf("[MR_TASKTRACKER] - Inside MR_CLIENT_CONNECTION_SET::mr_terminate_connections(). Connections exited.\n");
    }

    mr_wait_for_exit(5, proj);

    // delete MR_CLIENT_CONNECTION objects from mr_client_connections
    MR_CLIENT_CONNECTION* mrconn;
    int i;
    for (i=0; i<mr_client_connections.size(); i++) {
        //mr_client_connections[i] = NULL;
        delete mr_client_connections[i];
    }
    // unnecessary - already called in destructor
    mr_client_connections.clear();

    return 0;
}


// Send kill signal to all existing connections
// Don't wait for them to exit
//
void MR_CLIENT_CONNECTION_SET::kill_connections(PROJECT* proj) {
    unsigned int i;
    MR_CLIENT_CONNECTION* mrconn;
    for (i=0; i<mr_client_connections.size(); i++) {
        mrconn = mr_client_connections[i];

        if (proj && mrconn->mr_fip->project != proj) continue;
        if (!mrconn->conn_exists()) continue;
        mrconn->kill_conn();
        //atp->kill_task(false);
    }
}

// Kill the connection by OS-specific means.
//
/// restart not necessary since when we close a connection, it must be restarted by downloading client, not by this client serving the file
//int MR_CLIENT_CONNECTION::kill_conn(bool restart) {
int MR_CLIENT_CONNECTION::kill_conn() {

    /// BOINC-MR TODO: kill thread (with a signal)
    /// pthread_kill(); - does not cleanup
    /// cancel thread - more complex:
    /// set cancellation options when thread is called [serve_file]
    /* set the cancellation parameters --
   - Enable thread cancellation
   - Defer the action of the cancellation
    */

    ///pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    ///pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    /* push the cleanup routine (print_it) onto the thread
       cleanup stack.  This routine will be called when the
       thread is cancelled.  Also note that the pthread_cleanup_push
       call must have a matching pthread_cleanup_pop call.  The
       push and pop calls MUST be at the same lexical level
       within the code */

    /* pass address of `i' since the current value of `i' is not
       the one we want to use in the cleanup function */
    ///pthread_cleanup_push(print_it, (void *)&i);

    /* every 100 tries check to see if the thread has been cancelled
           if the thread has not been cancelled then yield the thread's
	   LWP to another thread that may be able to run */

	///if (i%100 == 0) {
	///	pthread_testcancel();
	///	sched_yield();
	///	}

	/// IF Thread cleanup not needed - call pthread_cleanup_pop(0)
	/* The only way we can get here is when the thread breaks out
   of the while loop.  In this case the thread that makes it here
   has found the number we are looking for and does not need to run
   the thread cleanup function.  This is why the pthread_cleanup_pop
   function is called with a 0 argument; this will pop the cleanup
   function off the stack without executing it */

    ///pthread_cleanup_pop(0);


	set_mr_conn_state(MR_CONN_ABORTED, "kill_connection");

    return 0;
}

// Send quit message to all connections (set is_running to false)
// This is called when the core client exits,
// or when a project is detached or reset
//
void MR_CLIENT_CONNECTION_SET::request_connections_exit(PROJECT* proj) {
    unsigned int i;
    MR_CLIENT_CONNECTION* mrconn;
//    ACTIVE_TASK *atp;

    // BOINC-MR DEBUG
    printf("[MR_TASKTRACKER] - Entered MR_CLIENT_CONNECTION_SET::request_connections_exit()\n");

    //for (i=0; i<active_tasks.size(); i++) {
    for (i=0; i<mr_client_connections.size(); i++) {
        mrconn = mr_client_connections[i];
        //atp = active_tasks[i];
        if (proj && mrconn->mr_fip->project != proj) continue;
        if (!mrconn->conn_exists()) continue;
        mrconn->mr_request_exit();
    }
}


/// BOINC-MR TODO - Send a quit message (set is_running to false)
//
//
int MR_CLIENT_CONNECTION::mr_request_exit(){

    // BOINC-MR DEBUG
    printf("[MR_TASKTRACKER] - Entered MR_CLIENT_CONNECTION::mr_request_exit()\n");

    is_running = false;
    set_mr_conn_state(MR_CONN_ABORT_PENDING, "mr_request_exit");

    //quit_time = gstate.now;
    return 0;

}


/// TODO - Handle incoming connection - upload file
// If requested file exists (already checked)
//
int MR_CLIENT_CONNECTION::upload_file(FILE_INFO* fip){
    //init(fip, false);

    int new_fd = get_sock_fd();
    int bytes_sent, missing_data;

    // BOINC-MR DEBUG
    printf("[MR_TASKTRACKER] - Entered MR_CLIENT_CONNECTION::upload_file(). is_running: %d\n", is_running);
    fflush(stdout);

    char pathname[1024];
    get_pathname(fip, pathname, sizeof(pathname));
    //if (file_size(pathname, size)) continue;

    // BOINC-MR DEBUG
    printf(NULL, MSG_INFO, "Serving file: %s\n", pathname);
    fflush(stdout);

    FILE* fp = boinc_fopen(pathname, "rb");
    if (!fp) {
        //fprintf(stderr,
        printf(
            "accept_connections() Client server Error: can't open %s\n",
            pathname
        );
        fflush(stdout);
        //is_running = false;
        return ERR_FOPEN;
    }

    char send_buf[MR_DATASIZE_SEND];
    int len;
    char* temp;
    //char buf[1024];

    while(!feof(fp) && is_running){
        len = fread(send_buf, sizeof(char), sizeof(send_buf),fp);
        if (len != sizeof(char)*sizeof(send_buf) && !feof(fp)){
            // BOINC-MR DEBUG
            msg_printf(fip->project, MSG_INFO, "Error reading file %s: %d", fip->name, len);
            set_mr_conn_state(MR_CONN_EXITED, "serve_file");
            return ERR_FREAD;
        }
        //mfcc[len]='\0';
        //write(new_fd,mfcc,len);
        bytes_sent = send(new_fd, send_buf, len, 0);
        if (bytes_sent == -1){
            // BOINC-MR DEBUG
            msg_printf(fip->project, MSG_INFO, "Error sending first bytes of file %s through socket. Errno: %d", fip->name, errno);
            set_mr_conn_state(MR_CONN_EXITED, "serve_file");
            return ERR_MR_CLIENT_SOCKET_SEND;
        }
        else if (bytes_sent != len){
            // update buffer pointer to send only remaining data
            // only do this if we sent something (> 0 bytes), otherwise we will have an overlap in strncpy (same source and destination)
            // which causes undefined behaviour
            if (bytes_sent > 0){
                //send_buf += bytes_sent;
                temp = send_buf;
                temp += bytes_sent;
                missing_data = len - bytes_sent;
                strncpy(send_buf, temp, missing_data);
            }

            // retry until all data is sent
            while ((bytes_sent = send(new_fd, send_buf, missing_data, 0)) != missing_data){
                // if error
                if (bytes_sent == -1){
                    // BOINC-MR DEBUG
                    msg_printf(fip->project, MSG_INFO, "Error sending file %s through socket.", fip->name);
                    set_mr_conn_state(MR_CONN_EXITED, "serve_file");
                    return ERR_MR_CLIENT_SOCKET_SEND;
                }
                // again, only update send buffer (use strncpy) if some data was sent
                if (bytes_sent > 0){
                    temp = send_buf;
                    temp += bytes_sent;
                    missing_data -= bytes_sent;
                    strncpy(send_buf, temp, missing_data);
                    //send_buf += bytes_sent;
                }

            }

        }

        /// BOINC-MR TODO : update how many bytes have already been sent
        // sent+=len;
    }
    //char test[8] = "fileend";
    //if (send(new_fd, test, sizeof(test), 0) == -1)
      //  perror("send 'fileend'");
    //write(new_fd,"fileend",16);

    // connection was aborted - may or may not have finished sending file
    if (!is_running){
        if (!feof(fp))
            msg_printf(fip->project, MSG_INFO, "Upload of file %s interrupted.", fip->name);
        set_mr_conn_state(MR_CONN_ABORTED, "upload_file");
        //return 0;
    }
    return 0;
}

// *** Same as ACTIVE_TASKS::wait_for_exit() ***
// Wait up to wait_time seconds for processes to exit
// If proj is zero, wait for all processes, else that project's
// NOTE: it's bad form to sleep, but it would be complex to avoid it here
//
int MR_CLIENT_CONNECTION_SET::mr_wait_for_exit(double wait_time, PROJECT* proj) {
    bool all_exited;
    unsigned int i,n;
    MR_CLIENT_CONNECTION *mrconn;

    for (i=0; i<10; i++) {
        all_exited = true;

        for (n=0; n<mr_client_connections.size(); n++) {
            mrconn = mr_client_connections[n];
            if (proj && mrconn->mr_fip->is_project_file && mrconn->mr_fip->project != proj) continue;
            if (!mrconn->has_conn_exited()) {
                all_exited = false;
                break;
            }
        }

        if (all_exited) return 0;
        boinc_sleep(wait_time/10.0);
    }

    return ERR_NOT_EXITED;
}

/// BOINC-MR TODO
//
// We have sent a quit request to the thread (is_running = false); see if it's exited.
// This is called when the core client exits,
// or when a project is detached or reset
//
bool MR_CLIENT_CONNECTION::has_conn_exited() {
    // join thread
    ///pthread_join();

    bool exited = false;

    if (!conn_exists()) return true;

    if (exited) {
        set_mr_conn_state(MR_CONN_EXITED, "has_conn_exited");
    }
    return exited;
}

// Check if connection is running
/// BOINC-MR TODO - check if is_running is TRUE? Or simply use state?
//
bool MR_CLIENT_CONNECTION::conn_exists() {
    switch (mr_conn_state()) {
    case MR_CONN_ACTIVE:
    case MR_CONN_ABORT_PENDING:
        return true;
    }
    return false;
}


// similar to task_state_name
static const char* mr_conn_state_name(int val) {
    switch (val) {
    case MR_CONN_UNINITIALIZED: return "MR_CONN_UNINITIALIZED";
    case MR_CONN_ACTIVE: return "MR_CONN_ACTIVE";
    case MR_CONN_ABORT_PENDING: return "MR_CONN_ABORT_PENDING";
    case MR_CONN_EXITED: return "MR_CONN_EXITED";
    //case MR_CONN_WAS_SIGNALED: return "MR_CONN_WAS_SIGNALED";
    //case MR_CONN_EXIT_UNKNOWN: return "MR_CONN_EXIT_UNKNOWN";
    case MR_CONN_ABORTED: return "MR_CONN_ABORTED";
    case MR_CONN_COULDNT_START: return "MR_CONN_COULDNT_START";
    }
    return "Unknown";
}


void MR_CLIENT_CONNECTION::set_mr_conn_state(int val, const char* where) {
    _mr_conn_state = val;
    /// BOINC-MR TODO - LOG flags - mr_debug
    // BOINC-MR DEBUG
    /*msg_printf(NULL, MSG_INFO,
            "[task_debug] mr_conn_state=%s for MR_CLIENT_CONNECTION from %s",
            mr_conn_state_name(val), where
    );*/

    /*if (log_flags.mr_debug) {
        msg_printf(result->project, MSG_INFO,
            "[task_debug] task_state=%s for %s from %s",
            task_state_name(val), result->name, where
        );
    }*/
}



MR_CLIENT_CONNECTION::MR_CLIENT_CONNECTION(){
    mr_conn_start_t = 0;
    _mr_conn_state = MR_CONN_UNINITIALIZED;
    mr_fip = NULL;
}


// Set socket (file descriptor) for this connection
void MR_CLIENT_CONNECTION::set_sock_fd(int s){
    _sock_fd = s;
}

MR_CLIENT_CONNECTION_SET::MR_CLIENT_CONNECTION_SET(){
}

void *get_in_addr(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }
    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

/// BOINC-MR TODO : enable partial transfers (errors during file transfer, continue downloading from same offset instead of the whole
/// file again)

// initialize download from Mapper client - create thread to handle transfer
//
int MR_CLIENT_XFER::init_download(FILE_INFO& file_info){
    mr_fip = &file_info;
    //pthread_t thr;
    get_pathname(mr_fip, pathname, sizeof(pathname));

    // if file is already as large or larger than it's supposed to be,
    // something's screwy; start reading it from the beginning.
    //
    if (file_size(pathname, starting_size) || starting_size >= mr_fip->nbytes) {
        starting_size = 0;
    }

    /// BOINC-MR TODO: modify for partial transfers
    bytes_xferred = 0;
    ///bytes_xferred = starting_size;
    // update this value for speed calculations
    ///start_bytes_xferred = off;
    //if (off != 0) {
    //    bytes_xferred = off;
    //    start_bytes_xferred = off;
    //}

    const char* mapper_addr = mr_fip->get_current_mapper_addr();
    if (!mapper_addr) return ERR_MR_INVALID_ADDR;
    strncpy(dest_ip_addr, mapper_addr, sizeof(dest_ip_addr)-1);
    dest_ip_addr[sizeof(dest_ip_addr)-1] = '\0';
    // useful for debuggin purposes - identifying file being downloaded [if there is an error]
    strncpy(fname, mr_fip->name, sizeof(fname)-1);
    fname[sizeof(fname)-1] = '\0';
    /// BOINC-MR TODO : read destination port from mapper address
    dest_port = MR_DEFAULT_PORT;

    /// BOINC-MR DEBUG
    /// initialize thread attributes manually (instead of using NULL)
    pthread_attr_t attr;
    int retval;
    //pthread_attr_init(&attr);;

    if(retval = pthread_attr_init(&attr)){
        fprintf(stderr,"error pthread_attr_init()\n");
        fflush(stderr);
        /// BOINC-MR DEBUG
        msg_printf(0, MSG_INFO, "MR_CLIENT_XFER::init_download() - Error intializing thread attributes [pthread_attr_init]. retval: %d", retval);
    }

    if (retval = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)){
        //fprintf(stderr,"error pthread_attr_setdetachstate()\n");
        perror("pthread_attr_setdetachstate()\n");
        fflush(stderr);
        /// BOINC-MR DEBUG
        msg_printf(0, MSG_INFO, "MR_CLIENT_XFER::init_download() - Error setting thread to detached [pthread_attr_setdetachstate]. retval: %d", retval);
    }

    // create thread to connect to mapper and download file
    is_downloading = true;
    retval = pthread_create(&running_thread, &attr, mr_download_from_mapper, this);
    ///int retval = pthread_create(&running_thread, NULL, mr_download_from_mapper, this);
    if (!retval){
        //running_thread = thr;
        //is_downloading = true;
    }
    else{

        /// BOINC-MR DEBUG
        msg_printf(0, MSG_INFO, "MR_CLIENT_XFER::init_download() - Error starting thread [pthread_create]. retval: %d", retval);

        is_downloading = false;
        retval = ERR_THREAD;
    }
    // free attributes
    pthread_attr_destroy(&attr);
    return retval;
}

/// BOINC-MR TODO - connecting to server
//bool connect_to_mapper(int port){
void *mr_download_from_mapper(void *xfer){

    MR_CLIENT_XFER *client_xfer;

    //client_download_op = (struct thread_data *) threadarg;
    client_xfer = (MR_CLIENT_XFER *) xfer;
    int sockfd, numbytes, retval;
    char buf[MR_DATASIZE_RECV];
    char mapper_ip[INET6_ADDRSTRLEN];

    char fip_name[256];            // file name [to use in debug messages - in case file_info has been deleted]

    struct addrinfo hints;
    struct addrinfo *servinfo, *p;   // will point to the results

/// BOINC-MR TODO
//#ifndef _WIN32
    // timeout for recv() function - LINUX
    struct timeval timeout;
    timeout.tv_sec = 30;  // 150 Secs Timeout
    timeout.tv_usec = 0;  // 0 microseconds
//#else

//#endif


    char str_port[128];
    sprintf(str_port,"%d", client_xfer->dest_port); // converts to decimal base - standard-compliant alternative

    memset(&hints, 0, sizeof hints); // make sure the struct is empty
    hints.ai_family = AF_UNSPEC;      // don't care IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM; // TCP stream sockets

    char ip_addr[256];

    /// mr_mappers_ip_addrs - current result saved in client_xfer->dest_ip_addr
    if (client_xfer != NULL && client_xfer->is_downloading){
        strncpy(ip_addr, client_xfer->dest_ip_addr, sizeof(ip_addr)-1);
        ip_addr[sizeof(ip_addr)-1] = '\0';
        strncpy(fip_name, client_xfer->fname, sizeof(fip_name)-1);
        fip_name[sizeof(fip_name)-1] = '\0';
    }
    else{
        perror("mr_download_from_mapper: client_xfer does not exist or is_downloading = false. Just before getaddrinfo()\n");
        fflush(stderr);

        if (!client_xfer->is_downloading){
        // BOINC-MR DEBUG
            msg_printf(NULL, MSG_INFO, "Download of file %s was suspended. Thread about to exit...\n", fip_name);
            client_xfer->mr_client_conn_over = true;
            pthread_exit(NULL);
        }
        //client_xfer->mr_client_conn_over = true;
        //pthread_exit(NULL);
    }

    // get ready to connect
    //if ((retval = getaddrinfo(client_xfer->dest_ip_addr, str_port, &hints, &servinfo)) != 0) {
    if ((retval = getaddrinfo(ip_addr, str_port, &hints, &servinfo)) != 0) {

        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(retval));
        client_xfer->mr_client_xfer_retval = ERR_MR_GETADDRINFO;
        client_xfer->is_downloading = false;
        client_xfer->mr_client_conn_over = true;
        pthread_exit(NULL);
    }

    // periodic check to see if this thread has been "told" to terminate
    if (!client_xfer->is_downloading){
        // BOINC-MR DEBUG
        msg_printf(NULL, MSG_INFO, "Download of file %s was suspended. Thread about to exit...\n", fip_name);
        client_xfer->mr_client_conn_over = true;
        pthread_exit(NULL);
    }

    // loop through all the results and connect to the first that works
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype,
                 p->ai_protocol)) == -1) {
            perror("client: socket");
            fflush(stderr);
            continue;
        }

        // set a timeout value for recv() - avoid blocking indefinitely
        /// BOINC-MR TODO : separate between WINDOWS AND LINUX version
//#ifndef _WIN32
        // LINUX + MAC
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (struct timeval *)&timeout, sizeof(struct timeval)) == -1) {
//#else
        // Windows
//        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(struct timeval)) == -1) {
//#endif

            msg_printf(0, MSG_INTERNAL_ERROR, "[mr_tasktracker::mr_download_from_mapper] client: setsockopt (rcvtimeo). retval: %d", errno);
            perror("[mr_tasktracker::mr_download_from_mapper] setsockopt");
            continue;
            //exit(1);
        }

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            perror("client: connect");
            fflush(stderr);
            continue;
        }
        break;
    }

    if (p == NULL) {
        fprintf(stderr, "mr_download_from mapper: failed to connect to client Mapper: %s | File: %s\n",
                client_xfer->dest_ip_addr, fip_name);
        fflush(stderr);
        client_xfer->mr_client_xfer_retval = ERR_CONNECT;
        client_xfer->is_downloading = false;
        client_xfer->mr_client_conn_over = true;
        pthread_exit(NULL);
    }


    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),
            mapper_ip, sizeof mapper_ip);

    // periodic check to see if this thread has been "told" to terminate
    if (!client_xfer->is_downloading){
        // BOINC-MR DEBUG
        msg_printf(NULL, MSG_INFO, "Download of file %s was suspended. Thread about to exit...\n", fip_name);
        client_xfer->mr_client_conn_over = true;
        pthread_exit(NULL);
    }

    // BOINC-MR DEBUG
    printf("client: connecting to %s\nFile: %s\n", mapper_ip, fip_name);
    fflush(stdout);

    freeaddrinfo(servinfo); // all done with this structure

    // BOINC-MR DEBUG
    // Test to check if it is connected - receives Hello World
    /*if ((numbytes = recv(sockfd, buf, sizeof(buf)-1, 0)) == -1) {
        perror("recv Hello World");
        close(sockfd);
        exit(1);
    }
    buf[numbytes] = '\0';*/
    /// BOINC-MR DEBUG
    //msg_printf(client_xfer->mr_fip->project, MSG_INFO, "mr_download_from_mapper() -File %s | received '%s'\n", fip_name, buf);


    /// BOINC-MR TODO - identify which file to request (naming conventions must match when requesting file)
    // tell Mapper/server which file is needed
    char req[256];
    strncpy(req, MR_CLIENT_REQUEST_PREFIX, sizeof(MR_CLIENT_REQUEST_PREFIX));
    req[sizeof(req)-1] = '\0';

    // +1 added for the '\0' char added at then end
    if ((strlen(MR_CLIENT_REQUEST_PREFIX) + strlen(fip_name) +1) > sizeof(req)){

        fprintf(stderr, "[mr_tascktracker::mr_download_from_mapper] Client request is too long [%zu], and does not fir into the request buffer (char req[256]). Truncating it...",
                strlen(MR_CLIENT_REQUEST_PREFIX) + strlen(fip_name));
        fflush(stderr);
        strncat(req, fip_name, sizeof(req)-strlen(MR_CLIENT_REQUEST_PREFIX));
    }
    else {
        strncat(req, fip_name, strlen(fip_name));
        req[strlen(MR_CLIENT_REQUEST_PREFIX) + strlen(fip_name)] = '\0';
    }



    /// BOINC-MR DEBUG
//    printf("C2C download REQUEST string: %s\n", req);
//    fflush(stdout);

    req[sizeof(req)-1] = '\0';

    /// BOINC-MR DEBUG
//    printf("C2C download REQUEST string (after req[N]=0): %s\n", req);
//    fflush(stdout);

    //if (send(sockfd, "REQ_FILE:File_XPTO", 19, 0) == -1){
    if (send(sockfd, req, strlen(req), 0) == -1){
        client_xfer->mr_client_xfer_retval = ERR_MR_CLIENT_SOCKET_SEND;

        // BOINC-MR DEBUG
        perror("ERROR: send [mr_download_from_mapper]");
        fflush(stderr);
        msg_printf(NULL, MSG_INFO, "mr_download_from_mapper - Error sending file request, calling function send(): %d", errno);
        //printf("ERROR: send");
        //fflush(stdout);
        client_xfer->is_downloading = false;
        close(sockfd);
        client_xfer->mr_client_conn_over = true;
        pthread_exit(NULL);
    }

    if (!client_xfer->is_downloading){
        // BOINC-MR DEBUG
        msg_printf(NULL, MSG_INFO, "Download of file %s was suspended. Thread about to exit...\n", fip_name);
        close(sockfd);
        client_xfer->mr_client_conn_over = true;
        pthread_exit(NULL);
    }


    // open file where we will save downloaded data
    char pathname[1024];
    strncpy(pathname, fip_name, sizeof(pathname));
    get_pathname(client_xfer->mr_fip, pathname, sizeof(pathname));

    FILE *fp = fopen(pathname, "wb");

    if (!fp){
        msg_printf(NULL, MSG_INTERNAL_ERROR, "mr_download_from_mapper - error opening file %s", pathname);
        client_xfer->mr_client_xfer_retval = ERR_FOPEN;
        client_xfer->is_downloading = false;
        client_xfer->mr_client_conn_over = true;
        pthread_exit(NULL);
    }

    int bytes_written;

    /// BOINC-MR DEBUG
    printf("Beginning download for %s [is_downloading: %d]\n", fip_name, client_xfer->is_downloading);
    fflush(stdout);

    // Receive file
    double delta_t;
    int n_retries = 0;
    while (client_xfer->is_downloading){

        if ((numbytes = recv(sockfd, buf, sizeof(buf), 0)) == -1) {
            // check if there was a timeout in receiving data from buffer - if the timeout has been reached over 3 times, abort this transfer
            if (errno == EAGAIN || errno == EWOULDBLOCK){
                n_retries ++;
                if (n_retries <= 3){
                    fprintf(stderr, "recv [mr_download_from_mapper] TIMEOUT for file %s. Trying again [tried %d times so far]. Errno: %d\n",
                            fip_name, n_retries, errno);
                    fflush(stderr);
                    // try again
                    continue;
                }
                // else just show error and cleanup
                else{
                    fprintf(stderr, "recv [mr_download_from_mapper] TIMEOUT. Giving up after %d tries. Errno: %d", n_retries, errno);
                    fflush(stderr);
                    msg_printf(NULL, MSG_INFO, "mr_download_from_mapper - Timeout while receiving file %s, calling function recv(): %d",
                       fip_name, errno);
                }
            }

            client_xfer->mr_client_xfer_retval = ERR_MR_CLIENT_SOCKET_RECV;
            fprintf(stderr, "File: %s. ERROR: ", fip_name);
            perror("recv [mr_download_from_mapper]\n");
            fflush(stderr);
            // BOINC-MR TODO - BOINC-MR DEBUG
            printf("mr_download_from_mapper - Error receiving file %s, calling function recv(): %d\n",
                       fip_name, errno);
            fflush(stdout);
            //printf("Error: recv\n");
            //fflush(stdout);
            client_xfer->is_downloading = false;
            close(sockfd);
            if (fp) {
                fclose(fp);
                fp = NULL;
            }
            client_xfer->mr_client_conn_over = true;
            pthread_exit(NULL);
            //exit(1);
        }

        // reset number of retries after successful recv()
        n_retries = 0;

        // update transfer bytes and speed
        /// update_speed
        client_xfer->bytes_xferred += numbytes;
        delta_t = dtime() - client_xfer->start_time;
        if (delta_t > 0) {
            client_xfer->xfer_speed = (client_xfer->bytes_xferred-client_xfer->start_bytes_xferred) / delta_t;
        }

        //if (numbytes < (MAXDATASIZE-1)) break;
        if (numbytes == 0){
            // BOINC-MR DEBUG
            fprintf(stderr, "Received 0 bytes from mapper. File: %s\n", fip_name);
            fflush(stderr);
            break;
        }

        bytes_written = (int)fwrite(buf, 1, numbytes, fp);
        if (bytes_written != numbytes) {
            client_xfer->mr_client_xfer_retval = ERR_FWRITE;
            // BOINC-MR TODO - BOINC-MR DEBUG
            msg_printf(NULL, MSG_INTERNAL_ERROR, "Error writing to file [fwrite]: %s", pathname);
            fprintf(stderr, "Error writing to file [fwrite]: %s\n", pathname);
            fflush(stderr);
            client_xfer->is_downloading = false;
            close(sockfd);
            if (fp) {
                fclose(fp);
                fp = NULL;
            }
            client_xfer->mr_client_conn_over = true;
            pthread_exit(NULL);
        }

        //buf[numbytes] = '\0';
        /// BOINC-MR DEBUG
        //printf("client: received from FILE: '%s'\nWrote %d bytes\n",buf, bytes_written);
        //fflush(stdout);

        // Check if it is finished (received specific TAG from server)
        /*if(strncmp(buf,"fileend", 8)==0)
		{
		    printf("Finished downloading file. Received 'fileend'\n");
			break;
		}
		*/
    }

    close(sockfd);
    if (fp) {
        fclose(fp);
        fp = NULL;
    }

    if (!client_xfer->is_downloading){
        // BOINC-MR DEBUG
        msg_printf(NULL, MSG_INFO, "Download of file %s was suspended. Thread about to exit...\n", fip_name);
        client_xfer->mr_client_conn_over = true;
        pthread_exit(NULL);
    }

    /// If you're using Windows and Winsock that you should call closesocket() instead of close().

    /*
    void boinc_close_socket(int sock) {
    #if defined(_WIN32) && defined(USE_WINSOCK)
        closesocket(sock);
    #else
        close(sock);
    #endif
    }
    */

    // BOINC-MR DEBUG
    printf("Finished download of file %s. Thread about to exit...\n", fip_name);
    fflush(stdout);

    // if did not download anything, set transfer as error
    double size;
    if (file_size(pathname, size)){
        client_xfer->mr_client_xfer_retval = ERR_FILE_NOT_FOUND;

        // BOINC-MR DEBUG
        msg_printf(NULL, MSG_INFO, "Did not download anything from file %s...", fip_name);
    }

    client_xfer->is_downloading = false;
    client_xfer->mr_client_conn_over = true;
    pthread_exit(NULL);
}

// Call this to get the next Mapper's address.
// NULL return means you've tried them all.
//
const char* FILE_INFO::get_next_mapper_addr() {
    if (!mr_mappers_ip_addrs.size()) return NULL;
    while(1) {
        current_mapper_addr = (current_mapper_addr + 1)%((int)mr_mappers_ip_addrs.size());
        if (current_mapper_addr == start_mapper_addr) {
            return NULL;
        }
        return mr_mappers_ip_addrs[current_mapper_addr].c_str();
    }
}

const char* FILE_INFO::get_current_mapper_addr() {
    if (current_mapper_addr < 0) {
        return get_init_mapper_addr();
    }
    if (current_mapper_addr >= (int)mr_mappers_ip_addrs.size()) {
        msg_printf(project, MSG_INTERNAL_ERROR,
            "File %s has no Mapper address", name
        );
        return NULL;
    }
    return mr_mappers_ip_addrs[current_mapper_addr].c_str();
}

/// BOINC-MR DEPRECATED
/*int FILE_INFO::mr_add_mapper_addresses(std::vector<std::string> list_mappers){
    for (int i=0; i<list_mappers.size(); i++){
        //strdup();
        mr_mappers_ip_addrs.push_back(strdup(list_mappers[i].c_str()));
        //struct file_info
    }
}*/

const char* FILE_INFO::get_init_mapper_addr() {
    if (!mr_mappers_ip_addrs.size()) {
        return NULL;
    }

// if there are several mappers holding the file, try them in order
/// BOINC-MR TODO: how to order Mappers' addresses - load, nw bandwidth?
//
    current_mapper_addr = 0;
    start_mapper_addr = current_mapper_addr;
    return mr_mappers_ip_addrs[current_mapper_addr].c_str();
}


bool MR_CLIENT_XFER_BACKOFF::ok_to_transfer() {
    double dt = next_xfer_time - gstate.now;
    if (dt > gstate.pers_retry_delay_max) {
        // must have changed the system clock
        //
        dt = 0;
    }
    return (dt <= 0);
}

void MR_CLIENT_XFER_BACKOFF::client_xfer_failed(char* fname) {
    client_xfer_failures++;
    if (client_xfer_failures < MR_CLIENT_XFER_FAILURE_LIMIT) {
        next_xfer_time = 0;
    } else {
        double backoff = calculate_exponential_backoff(
            client_xfer_failures,
            gstate.pers_retry_delay_min,
            gstate.pers_retry_delay_max
        );
        /*if (log_flags.mr_client_xfer_debug) {
            msg_printf(p, MSG_INFO,
                "[mr_client_xfer_debug] xfer delay for file %s for %f sec", fip->name, backoff);
            );
        }*/
        // BOINC-MR DEBUG
        msg_printf(NULL, MSG_INFO, "[mr_client_xfer_debug] xfer delay for file %s for %f sec", fname, backoff);
        next_xfer_time = gstate.now + backoff;
    }
}

void MR_CLIENT_XFER_BACKOFF::client_xfer_succeeded() {
    client_xfer_failures = 0;
    next_xfer_time  = 0;
}

// Insert a MR_CLIENT_XFER object into the set
//
int MR_CLIENT_XFER_SET::insert(MR_CLIENT_XFER* cxp) {
    client_xfers.push_back(cxp);

    /// BOINC-MR TODO : set bw limits for client transfers - coordinate with file_xfers
    //set_bandwidth_limits();
    return 0;
}

// Remove a MR_CLIENT_XFER object from the set
//
int MR_CLIENT_XFER_SET::remove(MR_CLIENT_XFER* cxp){
    std::vector<MR_CLIENT_XFER*>::iterator iter;

    /// BOINC-MR DEBUG
    printf(NULL, MSG_INTERNAL_ERROR,
        "Removing file transfer for file: %s", cxp->fname
    );
    fflush(stdout);

    iter = client_xfers.begin();
    while (iter != client_xfers.end()) {
        if (*iter == cxp) {
            delete *iter;
            iter = client_xfers.erase(iter);
            //set_bandwidth_limits();
            return 0;
        }
        iter++;
    }

    msg_printf(NULL, MSG_INTERNAL_ERROR,
        "Client file transfer for %s not found", cxp->fname
    );
    return ERR_NOT_FOUND;
}

//
// Called from:
// - MR_CLIENT_XFER constructor [shared initializations];
// - transient_failure() when attempting to start a new transfer from next mapper address on list
//
// Variables that are not reset:
// - mr_fip: file info is still the same, only the source is different
//
void MR_CLIENT_XFER::reset() {
    dest_port = MR_DEFAULT_PORT;
    xfer_speed = 0;
    bytes_xferred = 0;
    start_bytes_xferred = 0;
    mr_client_xfer_retval = 0;
    start_time = gstate.now;
    strcpy(dest_ip_addr, "");
    strcpy(pathname, "");
    strcpy(fname, "");
    mr_client_xfer_done = false;
    mr_client_conn_over = false;
    is_downloading = false;     // similar to close_socket() - tells previous thread to terminate (it should have already if we're here)
    suspended = false;
}

MR_CLIENT_XFER::MR_CLIENT_XFER() {
    mr_fip = NULL;
    starting_size = 0;
    //strcpy(pathname, "");
    //is_downloading = false;
    //mr_client_conn_over = false;
    //xfer_speed = 0;
    //bytes_xferred = 0;
    //start_bytes_xferred = 0;
    //start_time = gstate.now;


    // reset is called in transient_failure, when starting a new transfer from the next mapper in "line" (in list given by server)
    // shared initializations at that point are moved to reset() instead of staying in constructor [avoid repetition]
    reset();
}

/// BOINC-MR TODO - unnecessary? Constructor called only once, no need for init() [unlike reset()]
/*void MR_CLIENT_XFER::init() {
    reset();
    start_time = gstate.now;
    start_bytes_xferred = 0;
}*/

MR_CLIENT_XFER::~MR_CLIENT_XFER() {
    /// BOINC-MR DEBUG
    printf("Inside ~MR_CLIENT_XFER. About to set mr_fip->pers_file_xfer->mr_cxp = NULL\n");
    fflush(stdout);

    // if this transfer was suspended, file_info (mr_fip pointer) may have been deleted
    if (!suspended){
        if (mr_fip && mr_fip->pers_file_xfer) {
            mr_fip->pers_file_xfer->mr_cxp = NULL;
        }
    }
}

// Run through the MR_CLIENT_XFER_SET and determine if any of the client file
// transfers are complete or had an error
//
bool MR_CLIENT_XFER_SET::poll() {
    unsigned int i;
    MR_CLIENT_XFER* cxp;
    bool action = false;
    static double mr_last_time=0;
    char pathname[256];
    double size;

    if (gstate.now - mr_last_time < MR_CLIENT_XFER_POLL_PERIOD) return false;

    mr_last_time = gstate.now;

    for (i=0; i<client_xfers.size(); i++) {
        cxp = client_xfers[i];
        if (!cxp->mr_client_conn_over) continue;

        action = true;

        if (cxp->suspended){
            //if (log_flags.client_xfer_debug) {
            // BOINC-MR DEBUG
            msg_printf(NULL, MSG_INFO,
                "[mr_client_xfer_debug] MR_CLIENT_XFER_SET::poll(): client xfer suspended. Delete [remove] cxp. File: %s; retval %d\n",
                cxp->fname,
                cxp->mr_client_xfer_retval
            );
            //}

            /// BOINC-MR DEBUG
            //msg_printf(0, MSG_INFO, "Deleting cxp. MR_CLIENT_XFER_SET::poll() - suspended");

            remove(cxp);
            //delete cxp;   // unnecessary. MR_CLIENT_XFER already deleted in remove(cxp) - MR_CLIENT_XFER_SET::remove()
            cxp = NULL;
            continue;
        }

        // transfer was not suspended, so handle error cases

        cxp->mr_client_xfer_done = true;

        //if (log_flags.client_xfer_debug) {
        //msg_printf(NULL, MSG_INFO,
        printf(NULL, MSG_INFO,
            "[mr_client_xfer_debug] MR_CLIENT_XFER_SET::poll(): client xfer done. File: %s; retval %d\n",
            cxp->fname,
            cxp->mr_client_xfer_retval
        );
        fflush(stdout);
        //}

        // deal with various error cases
        //
        // check if file_info has not been deleted (not sticky or error after map task [fip->status < 0])
        if (cxp->mr_fip){
            if (cxp->mr_fip->project){
                get_pathname(cxp->mr_fip, pathname, sizeof(pathname));
                // if no file present, move on - no file size validation tests
                if (file_size(pathname, size)) continue;
            }
        }

        // BOINC-MR DEBUG
        printf(NULL, MSG_INFO, "Got pathname. Checking file size for file: %s", cxp->fname);
        fflush(stdout);
        //printf("[mr_client_xfer_debug] Pathname: %s\n", pathname);

        //double diff = size - cxp->starting_size;
        if (cxp->mr_client_xfer_retval == 0) {
            // If no error
            if (cxp->mr_fip && cxp->mr_fip->nbytes){
                if (size == cxp->mr_fip->nbytes) continue;
                // however, file size does not correspond to expected value
                msg_printf(cxp->mr_fip->project, MSG_INFO, "MR_CLIENT_XFER_SET::poll() - Error: File [%s] size %f does not "
                            "correspond to expected value: %f", cxp->fname, size, cxp->mr_fip->nbytes);
                cxp->mr_client_xfer_retval = ERR_MR_FILE_SIZE;
            }
        } else {
            /// BOINC-MR TODO : handle error conditions from client transfer
            /// - only tackle problems that can be dealt with here, PERS_FILE_XFER handles final retval: permanent or transient failure
        }
    }
    return action;
}

