// This file is part of BOINC.
// http://boinc.berkeley.edu
// Copyright (C) 2008 University of California
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

#include "cpp.h"

#ifdef _WIN32
#include "boinc_win.h"
#endif

#ifndef _WIN32
#include "config.h"
#include <cmath>
#include <cstdlib>
#endif

#include "error_numbers.h"
#include "md5_file.h"
#include "parse.h"
#include "str_util.h"
#include "filesys.h"

#include "log_flags.h"
#include "file_names.h"
#include "client_state.h"
#include "client_types.h"
#include "client_msgs.h"

using std::vector;

PERS_FILE_XFER::PERS_FILE_XFER() {
    nretry = 0;
    first_request_time = gstate.now;
    next_request_time = first_request_time;
    time_so_far = 0;
    last_bytes_xferred = 0;
    pers_xfer_done = false;
    fip = NULL;
    fxp = NULL;
    // BOINC-MR
    mr_cxp = NULL;
    mr_conn_attempts = 0;
}

PERS_FILE_XFER::~PERS_FILE_XFER() {
    if (fip) {
        fip->pers_file_xfer = NULL;
    }
}

// BOINC-MR
int PERS_FILE_XFER::init(FILE_INFO* f, bool is_file_upload) {
    fxp = NULL;
    fip = f;
    is_upload = is_file_upload;
    pers_xfer_done = false;
    // BOINC-MR
    mr_cxp = NULL;
    mr_conn_attempts = 0;
    /// BOINC-MR DIST_VERSION: number of addresses does not matter: if there was an error (or not enough
    /// addreses) report to server/ask for new Map task to be distributed; delete FILE_INFO, do NOT try d/l again
    /* if (f->mr_file_type == MR_REDUCE_INPUT){
        const char* p = f->get_init_mapper_addr();
        if (!p) {
                msg_printf(NULL, MSG_INTERNAL_ERROR, "No Mapper address for client file transfer of %s", f->name);
                return ERR_NULL;
        }
    }
    else{
    */
    /// BOINC-MR CENTRALIZED_VERSION
    // Try to download from server if this is not Reduce input or if there are no Mapper addresses
    if (!f->mr_file_type == MR_REDUCE_INPUT || f->mr_mappers_ip_addrs.size() == 0 || is_upload){
        const char* p = f->get_init_url();
        if (!p) {
            msg_printf(NULL, MSG_INTERNAL_ERROR, "No URL for file transfer of %s", f->name);
            return ERR_NULL;
        }
    }

    return 0;
}

int PERS_FILE_XFER::start_xfer() {
    if (is_upload) {
        if (gstate.exit_before_upload) {
            exit(0);
        }
        return fxp->init_upload(*fip);
    } else if (fxp) {
        return fxp->init_download(*fip);
    }
    /// BOINC-MR TODO - continue...
    else if (mr_cxp){
        // create thread and call function to start download
        return mr_cxp->init_download(*fip);
    }
}

// Possibly create and start a file transfer
//
// BOINC-MR
int PERS_FILE_XFER::create_xfer() {
    FILE_XFER *file_xfer;
    MR_CLIENT_XFER* client_xfer;
    int retval;

    // Decide whether to start a new file transfer
    //
    if (!gstate.start_new_file_xfer(*this)) {
        return ERR_IDLE_PERIOD;
    }

    // if download, see if file already exists and is valid
    //
    if (!is_upload) {
        char pathname[256];
        get_pathname(fip, pathname, sizeof(pathname));

        if (!fip->verify_file(true, false)) {
            retval = fip->set_permissions();
            fip->status = FILE_PRESENT;
            pers_xfer_done = true;

            if (log_flags.file_xfer) {
                msg_printf(
                    fip->project, MSG_INFO,
                    "File %s exists already, skipping download", fip->name
                );
            }

            return 0;
        } else {
            // Mark file as not present but don't delete it.
            // It might partly downloaded.
            //
            fip->status = FILE_NOT_PRESENT;
        }
    }

    // Default scenario - not a Reduce input for a MapReduce job
    /// DIST_VERSION
    // if (fip->mr_file_type != MR_REDUCE_INPUT){
    /// CENTRALIZED_VERSION
    // D_VAL
    // check if this is not a c2c transfer

    // D_VAL DEBUG
//    msg_printf(NULL, MSG_INFO, "create_xfer. File: %s | fip->c2c_txf: %d | mappers addr size: %zu | fip->req_file :%d\n",
//                fip->name, fip->c2c_transfer, fip->mr_mappers_ip_addrs.size(), fip->requested_file);
//                fflush(stdout);

    if ((fip->mr_file_type != MR_REDUCE_INPUT && !fip->c2c_transfer) || fip->mr_mappers_ip_addrs.size() == 0){
        file_xfer = new FILE_XFER;
        fxp = file_xfer;
        retval = start_xfer();
        if (!retval) retval = gstate.file_xfers->insert(file_xfer);
        if (retval) {
            if (log_flags.http_debug) {
                msg_printf(
                    fip->project, MSG_INFO, "[file_xfer_debug] Couldn't start %s of %s",
                    (is_upload ? "upload" : "download"), fip->name
                );
                msg_printf(
                    fip->project, MSG_INFO, "[file_xfer_debug] URL %s: %s",
                    fip->get_current_url(), boincerror(retval)
                );
            }

            fxp->file_xfer_retval = retval;
            transient_failure(retval);
            delete fxp;
            fxp = NULL;
            return retval;
        }
        if (log_flags.file_xfer) {
            msg_printf(
                fip->project, MSG_INFO, "Started %s of %s",
                (is_upload ? "upload" : "download"), fip->name
            );
        }
        if (log_flags.file_xfer_debug) {
            msg_printf(fip->project, MSG_INFO,
                "[file_xfer_debug] URL: %s\n",
                fip->get_current_url()
            );
        }
    }
    /// D_VAL TODO: handle the case in which this file is requested by server (output file for a D_VAL validation Workunit)
    /// Create an upload in this case (set is_upload = true);
    else if (fip->requested_file){
        //is_upload = true;         /// done before
        file_xfer = new FILE_XFER;
        fxp = file_xfer;
        retval = start_xfer();
        if (!retval) retval = gstate.file_xfers->insert(file_xfer);
        if (retval) {
            if (log_flags.http_debug) {
                msg_printf(
                    fip->project, MSG_INFO, "[file_xfer_debug] Couldn't start %s of %s",
                    (is_upload ? "upload" : "download"), fip->name
                );
                msg_printf(
                    fip->project, MSG_INFO, "[file_xfer_debug] URL %s: %s",
                    fip->get_current_url(), boincerror(retval)
                );
            }

            fxp->file_xfer_retval = retval;
            transient_failure(retval);
            delete fxp;
            fxp = NULL;
            return retval;
        }
        if (log_flags.file_xfer) {
            msg_printf(
                fip->project, MSG_INFO, "Started %s of %s",
                (is_upload ? "upload" : "download"), fip->name
            );
        }
        if (log_flags.file_xfer_debug) {
            msg_printf(fip->project, MSG_INFO,
                "[file_xfer_debug] URL: %s\n",
                fip->get_current_url()
            );
        }
    }
    // BOINC-MR
    else{
        // create MapReduce client transfer
        client_xfer = new MR_CLIENT_XFER;
        mr_cxp = client_xfer;
        ///mr_cxp->init();      // unnecessary - all done in constructor
        retval = start_xfer();
        if (!retval) retval = gstate.mr_client_xfers->insert(client_xfer);
        if (retval) {
            //if (log_flags.client_xfer_debug) {
            // BOINC-MR DEBUG
            msg_printf(
                fip->project, MSG_INFO, "[mr_client_xfer_debug] Couldn't start download of %s from client", fip->name
            );
            msg_printf(
                fip->project, MSG_INFO, "[mr_client_xfer_debug] Mapper IP Address - %s: %s",
                fip->get_current_mapper_addr(), boincerror(retval)
            );
            //}

            /// BOINC-MR DEBUG
            msg_printf(fip->project, MSG_INFO, "create_xfer() [retval: %d] File: %s || Entering transient_failure...", retval, fip->name);

            mr_cxp->mr_client_xfer_retval = retval;
            transient_failure(retval);

            /// BOINC-MR TODO: if another mapper is available and transfer starts immediatelly, mr_cxp will be deleted while being
            /// accessed by thread responsible by substitute download
            // delete mr_cxp only if another download did not start immediatelly after this 1st failed
            //      - is_downloading set to true by
            // make sure mr_cxp was not deleted inside transient_failure [by calling permanent_failure()]

            // transient error without an alternative download started - still error
            if (mr_cxp && !mr_cxp->is_downloading){
                /// BOINC-MR DEBUG
                msg_printf(0, MSG_INFO, "Deleting mr_cxp. create_xfer() - is_downloading = false");

                delete mr_cxp;
                mr_cxp = NULL;
                return retval;
            }
            // new transfer started immediately - no error so far. Try to insert it into set
            else if (mr_cxp && mr_cxp->is_downloading){
                retval = gstate.mr_client_xfers->insert(client_xfer);
                // if there is an error inserting client_xfer, try again later... but do not delete mr_cxp (need to stop transfer first [is_downloading = false])
                if (retval){
                    // BOINC-MR DEBUG
                    msg_printf(
                        fip->project, MSG_INFO, "[mr_client_xfer_debug] Couldn't insert transfer of file %s from client into SET", fip->name
                    );
                    // stop transfer
                    mr_cxp->is_downloading = false;
                    // after asking transfer to stop [thread], wait until transfer is actually over before deleting mr_cxp
                    return retval;
                }
                else
                    return 0;
            }
            // mr_cxp has been already set to NULL and deleted - just return error val
            return retval;
        }
    }
    return 0;
}

// Poll the status of this persistent file transfer.
// If it's time to start it, then attempt to start it.
// If it has finished or failed, then deal with it appropriately
//
bool PERS_FILE_XFER::poll() {
    int retval;

    if (pers_xfer_done) {
        return false;
    }

    // BOINC-MR
    // if there is no active file or client transfer
    if (!fxp && !mr_cxp){
    //if (!fxp) {
        // No file xfer is active.
        // Either initial or resume after failure.
        // See if it's time to try again.
        //
        if (gstate.now < next_request_time) {
            return false;
        }

        /// DIST_VERSION
        // For reduce inputs, only use Client-to-Client transfers
        //if (fip->mr_file_type != MR_REDUCE_INPUT)
        /// CENTRALIZED_VERSION
        // Use server file transfer as backup if there are no Mapper addresses
        if (fip->mr_file_type != MR_REDUCE_INPUT || fip->mr_mappers_ip_addrs.size() == 0){
            FILE_XFER_BACKOFF& fxb = fip->project->file_xfer_backoff(is_upload);
            if (!fxb.ok_to_transfer()) {
#if 0
                if (log_flags.file_xfer_debug) {
                    msg_printf(fip->project, MSG_INFO,
                        "[file_xfer_debug] delaying %s of %s: project-wide backoff %f sec",
                        is_upload?"upload":"download", fip->name,
                        fxb.next_xfer_time - gstate.now
                    );
                }
#endif
                return false;
            }
        }
        else{
            /// BOINC-MR TODO: use backoffs; do not constantly attempt to contact other clients that are unavailable
            MR_CLIENT_XFER_BACKOFF& cxb = fip->mr_client_xfer_backoff();
            if (!cxb.ok_to_transfer()) {
                return false;
            }
        }

        last_time = gstate.now;
        retval = create_xfer();
        return (retval == 0);
    }

    // copy bytes_xferred for use in GUI
    //
    if (fxp){
        last_bytes_xferred = fxp->bytes_xferred;
        if (fxp->is_upload) {
            last_bytes_xferred += fxp->file_offset;
        }
    }
    // BOINC-MR - mr_cxp
    else if (mr_cxp)
        last_bytes_xferred = mr_cxp->bytes_xferred;

    // don't count suspended periods in total time
    //
    double diff = gstate.now - last_time;
    if (diff <= 2) {
        time_so_far += diff;
    }
    last_time = gstate.now;

    // BOINC-MR : default operation, with a FILE_XFER
    if (fxp){
        if (fxp->file_xfer_done) {
            if (log_flags.file_xfer_debug) {
                msg_printf(fip->project, MSG_INFO,
                    "[file_xfer_debug] file transfer status %d",
                    fxp->file_xfer_retval
                );
            }
            switch (fxp->file_xfer_retval) {
            case 0:
                fip->project->file_xfer_backoff(is_upload).file_xfer_succeeded();
                if (log_flags.file_xfer) {
                    msg_printf(
                        fip->project, MSG_INFO, "Finished %s of %s",
                        is_upload?"upload":"download", fip->name
                    );
                }
                if (log_flags.file_xfer_debug) {
                    if (fxp->xfer_speed < 0) {
                        msg_printf(fip->project, MSG_INFO, "[file_xfer_debug] No data transferred");
                    } else {
                        msg_printf(
                            fip->project, MSG_INFO, "[file_xfer_debug] Throughput %d bytes/sec",
                            (int)fxp->xfer_speed
                        );
                    }
                }
                pers_xfer_done = true;
                break;
            case ERR_UPLOAD_PERMANENT:
                permanent_failure(fxp->file_xfer_retval);
                break;
            case ERR_NOT_FOUND:
            case ERR_FILE_NOT_FOUND:
            case HTTP_STATUS_NOT_FOUND:     // won't happen - converted in http_curl.C
                if (is_upload) {
                    // if we get a "not found" on an upload,
                    // the project must not have a file_upload_handler.
                    // Treat this as a transient error.
                    //
                    msg_printf(fip->project, MSG_INFO,
                        "Project file upload handler is missing"
                    );
                    transient_failure(fxp->file_xfer_retval);
                } else {
                    permanent_failure(fxp->file_xfer_retval);
                }
                break;
            default:
                if (log_flags.file_xfer) {
                    msg_printf(
                        fip->project, MSG_INFO, "Temporarily failed %s of %s: %s",
                        is_upload?"upload":"download", fip->name,
                        boincerror(fxp->file_xfer_retval)
                    );
                }
                transient_failure(fxp->file_xfer_retval);
            }

            // If we transferred any bytes, or there are >1 URLs,
            // set upload_offset back to -1
            // so that we'll query file size on next retry.
            // Otherwise leave it as is, avoiding unnecessary size query.
            //
            if (last_bytes_xferred || (fip->urls.size() > 1)) {
                fip->upload_offset = -1;
            }

            // fxp could have already been freed and zeroed above
            // so check before trying to remove
            //
            if (fxp) {
                gstate.file_xfers->remove(fxp);
                delete fxp;
                fxp = NULL;
            }
            return true;
        }
    }
    // BOINC-MR - MR_CLIENT_XFER mr_cxp is done
    else if (mr_cxp->mr_client_xfer_done){
        ///if (log_flags.client_xfer_debug) {
        msg_printf(fip->project, MSG_INFO,
            "[client_xfer_debug] client transfer done. File: %s | status %d",
            mr_cxp->fname, mr_cxp->mr_client_xfer_retval
        );
        //}

        switch (mr_cxp->mr_client_xfer_retval) {
        case 0:
            fip->mr_client_xfer_backoff().client_xfer_succeeded();
            ///if (log_flags.mr_client_xfer) {
                msg_printf(
                    fip->project, MSG_INFO, "Finished download of %s", fip->name
                );
            //}
            ///if (log_flags.mr_client_xfer_debug) {
                if (mr_cxp->xfer_speed < 0) {
                    msg_printf(fip->project, MSG_INFO, "[mr_client_xfer_debug] No data transferred");
                } else {
                    msg_printf(
                        fip->project, MSG_INFO, "[mr_client_xfer_debug] File: %s | Throughput %d bytes/sec",
                        fip->name, (int)mr_cxp->xfer_speed
                    );
                }
            //}
            pers_xfer_done = true;
            break;
        case ERR_FOPEN:                     // Error opening file to write Map output data to
            //permanent_failure(mr_cxp->mr_client_xfer_retval);
            //break;
        case ERR_FILE_NOT_FOUND:            // Requested a file that client did not have (already deleted or wrong name)
        case ERR_CONNECT:                   // Error connecting to Mapper client, try next in list
        case ERR_MR_CLIENT_SOCKET_RECV:     // error receiving data from socket - possibly closed, retry to connect
        case ERR_MR_GETADDRINFO:            // on getaddrinfo() - could be a DNS error, retry
        case ERR_MR_CLIENT_SOCKET_SEND:     // sending data - retry to connect [if new error then, stop retrying]
        case ERR_FWRITE:                    // writing data to file to be used as input - retry from beginning

            /// BOINC-MR DEBUG
            msg_printf(fip->project, MSG_INFO, "pers_file_xfer::poll() - client_xfer case ERR_* [retval:%d] File: %s || Entering transient_failure...",
                        mr_cxp->mr_client_xfer_retval, fip->name);

            transient_failure(mr_cxp->mr_client_xfer_retval);
            break;
        default:
            ///if (log_flags.file_xfer) {
                msg_printf(
                    fip->project, MSG_INFO, "Temporarily failed download of %s: %s", fip->name,
                    boincerror(mr_cxp->mr_client_xfer_retval)
                );
            //}
            /// BOINC-MR DEBUG
            printf("pers_file_xfer::poll() -  case default [retval:%d] File: %s || Entering transient_failure...\n",
                        mr_cxp->mr_client_xfer_retval, fip->name);
            fflush(stdout);
            transient_failure(mr_cxp->mr_client_xfer_retval);
        }

        // mr_cxp could have already been freed and zeroed above
        // so check before trying to remove
        //
        // BOINC-MR : do not simply delete it, if there was a transient failure and another mapper was available.
        // - on transient failure with another mapper available, do not delete since a new transfer is started,
        //   and uses the same CLIENT_XFER (mr_cxp)
        //          - new transfer sets is_downloading to true unless there was an error (in which case it is safe to delete)
        // in a permanent failure, mr_cxp is already deleted inside permanent_failure()...
        //
        // delete here in the following case(s):
        // - xfer was finished, without transient (restarts transfer with same mr_cxp) or permanent (has its own delete) error
        //      -> is_downloading = false [no restart]
        // - xfer had transient error, AND no other mapper was available. Backoff required [do_backoff()]
        //      -> is_downloading = false [no restart]
        if (mr_cxp && !mr_cxp->is_downloading) {
            /// BOINC-MR DEBUG
//            time_t now = time(0);
//            char* time_string = time_to_string((double)now);
//            printf("%s Deleting mr_cxp. pers_file_xfer::poll() - is_downloading = false\n", time_string);

            gstate.mr_client_xfers->remove(mr_cxp);
            delete mr_cxp;
            mr_cxp = NULL;
        }
        return true;

    }
    return false;
}

void PERS_FILE_XFER::permanent_failure(int retval) {
    // BOINC-MR
    if (fxp){

        // D_VAL DEBUG
        msg_printf(0, MSG_INFO, "Deleting [remove] fxp. permanent_failure()");

        gstate.file_xfers->remove(fxp);
        delete fxp;
        fxp = NULL;
        if (log_flags.file_xfer) {
            msg_printf(
                fip->project, MSG_INFO, "Giving up on %s of %s: %s",
                is_upload?"upload":"download", fip->name, boincerror(retval)
            );
        }
    }
    // BOINC-MR - handle permanent error on client transfer
    else {

        /// BOINC-MR DEBUG
        msg_printf(0, MSG_INFO, "Deleting [remove] mr_cxp. permanent_failure()");

        // client_xfer_set::remove() already deletes mr_cxp
        gstate.mr_client_xfers->remove(mr_cxp);

        /// BOINC-MR DEBUG
        msg_printf(0, MSG_INFO, "Deleted mr_cxp. permanent_failure()");


        //delete mr_cxp;        // unnecessary. MR_CLIENT_XFER already deleted in remove(cxp) - MR_CLIENT_XFER_SET::remove()
        mr_cxp = NULL;
        ///if (log_flags.client_xfer) {
            msg_printf(
                fip->project, MSG_INFO, "Giving up on download of %s: %s",
                fip->name, boincerror(retval)
            );
        //}

        /// BOINC-MR TODO : tell scheduler that it was not possible to obtain file - request new Map wu distributed and executed

    }
    fip->status = retval;
    pers_xfer_done = true;
    fip->error_msg = boincerror(retval);
}

// Handle a transient failure
//
void PERS_FILE_XFER::transient_failure(int retval) {

    // D_VAL DEBUG
    msg_printf(fip->project, MSG_INFO, "Entered transient_failure() - comparison between now and first req time [%f VS. %d].",
                    (gstate.now - first_request_time), gstate.mr_client_xfer_giveup_period);
    fflush(stdout);

    // If it was a bad range request, delete the file and start over
    //
    if (retval == HTTP_STATUS_RANGE_REQUEST_ERROR) {
        fip->delete_file();
        return;
    }

    // If too much time has elapsed, give up
    //
    // BOINC-MR
    if (fxp && (gstate.now - first_request_time) > gstate.file_xfer_giveup_period) {
        permanent_failure(ERR_TIMEOUT);
    }
    else if (mr_cxp && (gstate.now - first_request_time) > gstate.mr_client_xfer_giveup_period){
        /// BOINC-MR DEBUG
        msg_printf(fip->project, MSG_INFO, "transient_failure() - time over giveup_period [%f > %d] Entering permanent_failure...",
                    (gstate.now - first_request_time), gstate.mr_client_xfer_giveup_period);

        permanent_failure(ERR_TIMEOUT);
    }

    // Cycle to the next URL to try.
    // If we reach the URL that we started at, then back off.
    // Otherwise immediately try the next URL
    //

    // BOINC-MR
    if (fxp){
        if (fxp && fip->get_next_url()) {
            start_xfer();
        } else {

            // D_VAL DEBUG
            msg_printf(fip->project, MSG_INFO, "Inside transient_failure() - Calling do_backoff (fip->get_next_url not found).");
            fflush(stdout);

            do_backoff();
        }
    }
    else {
        if (mr_cxp && fip->get_next_mapper_addr()) {

            /// BOINC-MR DEBUG
            msg_printf(0, MSG_INFO, "pers_file_xfer::transient_failure() - get_next_mapper_add() = true. File: %s", fip->name);

            // create MapReduce client transfer
            /// BOINC-MR TODO - delete previous instance first: new download starts with same mr_cxp
            /// instead of deleting mr_cxp, reset its attributes
            mr_cxp->reset();
            //delete mr_cxp;
            //mr_cxp = NULL;
            //mr_cxp = new MR_CLIENT_XFER;
            //mr_cxp = client_xfer;
            start_xfer();
        }
        else{
            // update number of attempts so far, and check if it is over the maximum allowed
            mr_conn_attempts++;
            if (mr_conn_attempts >= gstate.mr_max_client_conn_attempts)
                permanent_failure(ERR_MR_CONN_ATTEMPTS);
            else
                do_backoff();
        }
    }
}

// per-file backoff policy: sets next_request_time
//
void PERS_FILE_XFER::do_backoff() {
    double backoff = 0;

    // don't count it as a server failure if network is down
    //
    if (!net_status.need_physical_connection) {
        nretry++;
    }

    // keep track of transient failures per project (not currently used)
    //
    PROJECT* p = fip->project;
    p->file_xfer_backoff(is_upload).file_xfer_failed(p);

    // Do an exponential backoff of e^nretry seconds,
    // keeping within the bounds of pers_retry_delay_min and
    // pers_retry_delay_max
    //
    backoff = calculate_exponential_backoff(
        nretry, gstate.pers_retry_delay_min, gstate.pers_retry_delay_max
    );
    next_request_time = gstate.now + backoff;
    msg_printf(fip->project, MSG_INFO,
        "Backing off %s on %s of %s",
        timediff_format(backoff).c_str(),
        is_upload?"upload":"download",
        fip->name
    );
}

void PERS_FILE_XFER::abort() {
    if (fxp) {
        gstate.file_xfers->remove(fxp);
        delete fxp;
        fxp = NULL;
    }
    fip->status = ERR_ABORTED_VIA_GUI;
    fip->error_msg = "user requested transfer abort";
    pers_xfer_done = true;
}

// Parse XML information about a persistent file transfer
//
int PERS_FILE_XFER::parse(MIOFILE& fin) {
    char buf[256];

    while (fin.fgets(buf, 256)) {
        if (match_tag(buf, "</persistent_file_xfer>")) return 0;
        else if (parse_int(buf, "<num_retries>", nretry)) continue;
        else if (parse_double(buf, "<first_request_time>", first_request_time)) {
            continue;
        }
        else if (parse_double(buf, "<next_request_time>", next_request_time)) {
            continue;
        }
        else if (parse_double(buf, "<time_so_far>", time_so_far)) continue;
        else if (parse_double(buf, "<last_bytes_xferred>", last_bytes_xferred)) continue;
        else {
            if (log_flags.unparsed_xml) {
                msg_printf(NULL, MSG_INFO,
                    "[unparsed_xml] Unparsed line in file transfer info: %s", buf
                );
            }
        }
    }
    return ERR_XML_PARSE;
}

// Write XML information about a persistent file transfer
//
int PERS_FILE_XFER::write(MIOFILE& fout) {
    fout.printf(
        "    <persistent_file_xfer>\n"
        "        <num_retries>%d</num_retries>\n"
        "        <first_request_time>%f</first_request_time>\n"
        "        <next_request_time>%f</next_request_time>\n"
        "        <time_so_far>%f</time_so_far>\n"
        "        <last_bytes_xferred>%f</last_bytes_xferred>\n"
        "    </persistent_file_xfer>\n",
        nretry, first_request_time, next_request_time, time_so_far, last_bytes_xferred
    );
    if (fxp) {
        fout.printf(
            "    <file_xfer>\n"
            "        <bytes_xferred>%f</bytes_xferred>\n"
            "        <file_offset>%f</file_offset>\n"
            "        <xfer_speed>%f</xfer_speed>\n"
            "        <url>%s</url>\n"
            "    </file_xfer>\n",
            fxp->bytes_xferred,
            fxp->file_offset,
            fxp->xfer_speed,
            fxp->m_url
        );
    }
    return 0;
}

// suspend file transfers by killing them.
// They'll restart automatically later.
//
void PERS_FILE_XFER::suspend() {
    if (fxp) {
        last_bytes_xferred = fxp->bytes_xferred;  // save bytes transferred
        if (fxp->is_upload) {
            last_bytes_xferred += fxp->file_offset;
        }
        gstate.file_xfers->remove(fxp);     // this removes from http_op_set too
        delete fxp;
        fxp = 0;
    }
    // BOINC-MR
    // cannot simply suspend, since there may a racing condition - thread may be writing/reading from mr_cxp
    /// BOINC-MR TODO - set is_downloading to false, and then wait for threads to exit (mr_conn_over)
    if (mr_cxp){

        /// BOINC-MR DEBUG
        msg_printf(0, MSG_INFO, "Trying to delete mr_cxp. pers_file_xfer::suspend(). File: %s", mr_cxp->fname);

        last_bytes_xferred = mr_cxp->bytes_xferred;

        // set suspended to true - separate from normal or error returns and avoid trying to read from file_info which was possibly deleted
        mr_cxp->suspended = true;

        // do not remove client_xfers from here, unless thread has already exited
        /// BOINC-MR TODO : problem - this never happens. This is where we are telling connection to stop
        /// cannot delete mr_cxp here, otherwise downloading thread will throw an exception
        if (mr_cxp->mr_client_conn_over){
            /// BOINC-MR DEBUG
            msg_printf(0, MSG_INFO, "Deleting mr_cxp. mr_client_conn_over = TRUE. pers_file_xfer::suspend(). File: %s", mr_cxp->fname);

            gstate.mr_client_xfers->remove(mr_cxp);
            //delete mr_cxp;          // unnecessary. MR_CLIENT_XFER already deleted in remove(cxp) - MR_CLIENT_XFER_SET::remove()
            mr_cxp = NULL;
        }
        else
            mr_cxp->is_downloading = false;
    }
    fip->upload_offset = -1;
}

PERS_FILE_XFER_SET::PERS_FILE_XFER_SET(FILE_XFER_SET* p) {
    file_xfers = p;
}

// Run through the set, starting any transfers that need to be
// started and deleting any that have finished
//
bool PERS_FILE_XFER_SET::poll() {
    unsigned int i;
    bool action = false;
    static double last_time=0;

    if (gstate.now - last_time < PERS_FILE_XFER_POLL_PERIOD) return false;
    last_time = gstate.now;

    for (i=0; i<pers_file_xfers.size(); i++) {
        action |= pers_file_xfers[i]->poll();
    }

    if (action) gstate.set_client_state_dirty("pers_file_xfer_set poll");

    return action;
}

// Insert a PERS_FILE_XFER object into the set.
// We will decide which ones to start when we hit the polling loop
//
int PERS_FILE_XFER_SET::insert(PERS_FILE_XFER* pfx) {
    pers_file_xfers.push_back(pfx);
    return 0;
}

// Remove a PERS_FILE_XFER object from the set.
// What should the action here be?
//
int PERS_FILE_XFER_SET::remove(PERS_FILE_XFER* pfx) {
    vector<PERS_FILE_XFER*>::iterator iter;

    iter = pers_file_xfers.begin();
    while (iter != pers_file_xfers.end()) {
        if (*iter == pfx) {
            iter = pers_file_xfers.erase(iter);
            return 0;
        }
        iter++;
    }
    msg_printf(
        pfx->fip->project, MSG_INTERNAL_ERROR,
        "Persistent file transfer object not found"
    );
    return ERR_NOT_FOUND;
}

// suspend all PERS_FILE_XFERs
//
void PERS_FILE_XFER_SET::suspend() {
    unsigned int i;

    for (i=0; i<pers_file_xfers.size(); i++) {
        pers_file_xfers[i]->suspend();
    }
}

const char *BOINC_RCSID_76edfcfb49 = "$Id: pers_file_xfer.cpp 20750 2010-02-27 01:04:14Z davea $";
