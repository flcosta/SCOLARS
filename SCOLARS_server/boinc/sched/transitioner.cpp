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

// transitioner - handle transitions in the state of a WU
//    - a result has become DONE (via timeout or client reply)
//    - the WU error mask is set (e.g. by validater)
//    - assimilation is finished
//
// cmdline:
//   [ -one_pass ]          do one pass, then exit
//   [ -d x ]               debug level x
//   [ -mod n i ]           process only WUs with (id mod n) == i
//   [ -sleep_interval x ]  sleep x seconds if nothing to do

#include "config.h"
#include <vector>
#include <unistd.h>
#include <cstring>
#include <climits>
#include <cstdlib>
#include <string>
#include <signal.h>
#include <sys/time.h>

#include "boinc_db.h"
#include "util.h"
#include "backend_lib.h"
#include "common_defs.h"
#include "error_numbers.h"
#include "str_util.h"
#include "svn_version.h"

#include "sched_config.h"
#include "sched_util.h"
#include "sched_msgs.h"
#ifdef GCL_SIMULATOR
#include "gcl_simulator.h"
#endif

// BOINC-MR
#include "mr_jobtracker.h"
// BOINC-MR DEBUG
//#include <unistd.h>

#include <iostream>         // std::cerr

// D_VAL
#include "distributed_validation.h"

#define LOCKFILE                "transitioner.out"
#define PIDFILE                 "transitioner.pid"

#define SELECT_LIMIT    1000

#define DEFAULT_SLEEP_INTERVAL  5

int startup_time;
R_RSA_PRIVATE_KEY key;
int mod_n, mod_i;
bool do_mod = false;
bool one_pass = false;
int sleep_interval = DEFAULT_SLEEP_INTERVAL;

// BOINC-MR
MAPREDUCE_JOB_SET mapreduce_jobs;

// D_VAL
D_VAL_SET dist_val_apps;

void signal_handler(int) {
    log_messages.printf(MSG_NORMAL, "Signaled by simulator\n");
    return;
}

int result_suffix(char* name) {
    char* p = strrchr(name, '_');
    if (p) return atoi(p+1);
    return 0;
}

// A result just timed out.
// Update the host's avg_turnaround and max_results_day.
//
int penalize_host(int hostid, double delay_bound) {
    DB_HOST host;
    char buf[256];
    int retval = host.lookup_id(hostid);
    if (retval) return retval;
    compute_avg_turnaround(host, delay_bound);
    if (host.max_results_day == 0 || host.max_results_day > config.daily_result_quota) {
        host.max_results_day = config.daily_result_quota;
    }
    host.max_results_day -= 1;
    if (host.max_results_day < 1) {
        host.max_results_day = 1;
    }
    sprintf(buf,
        "avg_turnaround=%f, max_results_day=%d",
        host.avg_turnaround, host.max_results_day
    );
    return host.update_field(buf);
}


// BOINC-MR
// Add this WU's id 'wuid' to list of available Map inputs and check if all inputs have become available.
// If so, create reduce workunit(s)
int check_create_red_wus(MAPREDUCE_JOB* mrj, int wuid){

    int retval = 0;

    // BOINC-MR DEBUG
    //std::vector<char*> tmp = mrj->get_mr_map_wu_names();
    std::vector<std::string> tmp = mrj->get_mr_map_wu_names();
    log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: Transitioner - check_create_red_wus(). Wu_name size: %zu.\n", tmp.size());
    fflush(stderr);
    for (int l=0; l<tmp.size();l++){
        log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: Transitioner - check_create_red_wus(). [MR JOB #%d]Wu_name[%d]: %s.\n", mrj->mr_job_id, l, tmp[l].c_str());
                    fflush(stderr);
    }


    /// BOINC-MR DEPRECATED: done before calling this function
    // Add this wu to list of available inputs (Map tasks done)
    //mrj->mr_add_input_task(wuid);

    /// BOINC-MR TODO
    /// DELAYED (for now) - check if project would like Map output files to be available as soon as they are available (validated),
    /// or if we should wait until ALL Map work units have been finished and returned/validated
    // Check if number of available WUs matches number of Map jobs. If so,
    // - create Reduce WUs
    // - set this MR job to Reduce phase
    if (!mrj->mr_all_inputs_available()){
        log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: Transitioner - check_create_red_wus(). Only %d out of %d tasks have finished and "
                    "are available as Reduce input.\n", mrj->get_num_available_inputs(), mrj->get_num_map_tasks()
        );


    }


    // Check if number of available WUs matches number of Map jobs. If so,
    // - create Reduce WUs
    // - set this MR job to Reduce phase
    if (mrj->mr_all_inputs_available()){
        log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: Transitioner - check_create_red_wus(). All Map tasks [%d] have finished and "
                    "their output is available.\n", mrj->get_num_map_tasks()
        );
        fflush(stderr);

    // D_VAL
    // see if this reduce work unit will be validated by clients (DIST VAL)
    bool red_dist_val = false;
    D_VAL_APP* dv_app;
    // get corresponding dist val application info, through the Reduce app name
    dv_app = dist_val_apps.get_dv_app_name(mrj->get_reduce_app_name());

    // D_VAL DEBUG
    log_messages.printf(MSG_NORMAL,
        "Called dist_val_apps.get_dv_app(%s)\n", mrj->get_reduce_app_name()
    );


    // D_VAL DEBUG
    if (!dv_app){
        log_messages.printf(MSG_DEBUG,
            "check_create_red_wus(): Reduce application [%s] not found in dist_val.xml. It is validated at server.",
            mrj->get_reduce_app_name());
    }
    else{
        // this reduce work unit is D_VAL

        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG,
            "check_create_red_wus(): Reduce application [%s] found in dist_val.xml. It is validated at server.",
            mrj->get_reduce_app_name());
        red_dist_val = true;
    }


        // Create Reduce WUs
        /// BOINC-MR TODO: send mrj instead of all this info?
        retval = mr_create_reduce_work(mrj->get_reduce_re_template(), mrj->get_reduce_wu_template(),  // result and wu templates
                                       mrj->get_reduce_app_name(), mrj->get_num_map_tasks(),          // Reduce App name, number of Reduce inputs (=map tasks)
                                       mrj->get_num_reduce_tasks(), mrj->get_mr_map_wu_names(),       // number of reduce tasks to create, names of Map Workunits [required to know name of input files]
                                       mrj->get_reduce_name_index(), mrj,
                                       red_dist_val);                                                 // true if this reduce work unit


        /*log_messages.printf(MSG_NORMAL, "Left mr_create_reduce_work()\n");
        fflush(stderr);
        log_messages.printf(MSG_DEBUG, "[mr_jobtracker.cpp] Got out of mr_create_reduce_work(). Retval: %d\n", retval);
        fflush(stderr);*/

        // BOINC-MR DEBUG
        fprintf(stderr, "Left mr_create_reduce_work() - Using fprintf...\n");
        fflush(stderr);


        //mr_create_reduce_work(char* result_template_file, char* wu_template_file,
        //char* app_name, int num_inputs, int num_reducers, std::vector<char*> wu_name){


        // There was an error
        if (retval){
            log_messages.printf(MSG_CRITICAL, "handle_wu() - error returned from mr_create_reduce_work()\n");
            fflush(stderr);
        }
        else {
            log_messages.printf(MSG_CRITICAL, "handle_wu() - Successfully created work with mr_create_reduce_work()\n");
            fflush(stderr);

            // Increment counter to guarantee next WU name is unique
            mrj->increment_reduce_name_index();
            /// BOINC-MR TODO - save this index to mr_jobtracker.xml [and parse it at the beginning]

            mrj->set_job_status(MR_JOB_RED_RUNNING);

        }

    }
    return retval;
}



// D_VAL
//
// modify template of validator WUs (validate other WUs), to include xml_signature for all input files. This is necessary
// since they may be requested by server (which will not be able to accept its upload without a signature)
//
// @ key: pvt key used to create xml_signature for each file_info content
// @ wu: transitioner item with Workunit ID and name
//int dval_modify_validator_wu_template(R_RSA_PRIVATE_KEY& key, SCHED_CONFIG& config_loc, TRANSITIONER_ITEM wu){
int dval_modify_validator_wu_template(R_RSA_PRIVATE_KEY& key, TRANSITIONER_ITEM wu){
    int retval;

    //if (!config_loc.dont_generate_upload_certificates) {
    char wu_template[MEDIUM_BLOB_SIZE];
    // The WORKUNIT wu does not have the xml_doc field stored, therefore we must fetch it from the DB in order to change it
    DB_WORKUNIT dbwu;
    dbwu.id = wu.id;
    /// D_VAL TODO: optimize these two DB queries into ("SELECT validating_wuid, xml_doc" in single query)
    // check if this workunit is a validator WU (only then must we insert a xml_signature for all inputs (in case they are required for upload later)
    retval = dbwu.get_field_int("validating_wuid", dbwu.validating_wuid);
    if (!retval && dbwu.validating_wuid >= 0) {

        // D_VAL DEBUG
        fprintf(stderr,
            "dval_modify_validator_wu_template: [%s] This is a Validator Workunit.\n", wu.name
        );
        fflush(stderr);

        retval = dbwu.get_field_str("xml_doc", dbwu.xml_doc, sizeof(dbwu.xml_doc));

        // D_VAL DEBUG
        fprintf(stderr,
            "dval_modify_validator_wu_template: After getting xml_doc:\n%s\n", dbwu.xml_doc
        );
        fflush(stderr);

        if (!retval) {
            // only add signatures if they're not already present (possible if it is not the first time we have created results for this WU
            // e.g.: error during the execution of the first three results)
            if (!strstr(dbwu.xml_doc, "<xml_signature>")){

                retval = add_signatures(dbwu.xml_doc, key);
                if (retval) {
                    fprintf(stderr,
                        "dval_modify_validator_wu_template: [%s] WU Error trying to add xml_signature [add_signatures()] to xml_doc: %d\n", wu.name, retval
                    );
                    return retval;
                }

                // D_VAL DEBUG
                fprintf(stderr,
                    "dval_modify_validator_wu_template: After inserting xml_signature in xml_doc:\n%s\n", dbwu.xml_doc
                );
                fflush(stderr);

                // update DB with new xml_doc value
                char buf2[MEDIUM_BLOB_SIZE+256];
                sprintf(buf2, "xml_doc=\"%s\"", dbwu.xml_doc);
                // update this value on the WU that was validated by wu
                retval = dbwu.update_field(buf2);
                if (retval) {
                    fprintf(stderr,
                        "dval_modify_validator_wu_template: [%s] WU update failed (inserting xml_signature): %d\n", wu.name, retval
                    );
                    return retval;
                }
            }
            // xml_doc already has an <xml_signature>
            else {
                // D_VAL DEBUG
                log_messages.printf(MSG_DEBUG,
                    "d_val_request_output_files: xml_doc already has signatures. Returning 0.\n");
                return 0;
            }
        }
        // error fetching xml_doc field
        else{
            fprintf(stderr,
                "dval_modify_validator_wu_template: [%s] Error trying to fetch WU's xml_doc from DB: %d\n", wu.name, retval
            );
            return retval;
        }
    }
    else if (retval){
        fprintf(stderr,
            "dval_modify_validator_wu_template: [%s] Error trying to fetch WU's validating_wuid from DB: %d\n", wu.name, retval
        );
        return retval;
    }
    //}
    return 0;
}

// called by transitioner. Check if current workunit requires its results to be returned to server. If so,
// set req_out_file_state = D_VAL_REQUEST_FILE in DB [WU]
//
// Since D_VAL WU with ID wuid has been successfully validated by clients, check if this D_VAL_APP wants its output files to be sent back to server
// Since this is the validator, we must ask the scheduler to communicate with hosts. Therefore, change the req_out_file_state field in the DB to
// D_VAL_REQUEST_FILE.
//
// @ wuid: ID of workunit to request files from (which WORKUNIT to change in DB)
//
int d_val_request_output_files(int wuid){
    char query[MAX_QUERY_LEN];
    DB_CONN* db = &boinc_db;
    int retval;
    char app_name[256];

//    char buf2[256];
//    DB_WORKUNIT wu;
//    wu.id = result.workunitid;
//    sprintf(buf2, "req_out_file_state=%d", D_VAL_REQUEST_FILE);
//    wu.update_field(buf2);

    // look in DB for app corresponding to this wuid
    sprintf(query,
            "SELECT app.name from workunit, app "
            "WHERE workunit.id=%d AND "
            "workunit.appid = app.id",
            wuid);

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "d_val_request_output_files: Looking for app that corresponds to WU [#%d] with query: %s\n",
        wuid, query
    );

    retval = db->do_query(query);
    if (retval){
        log_messages.printf(MSG_CRITICAL,
            "d_val_request_output_files: Error updating Workunit's req_out_file_state through query: %s\n",
            query
        );
        return retval;
    }

    // the following stores the entire result set in memory
    //
    MYSQL_RES* rp;
    MYSQL_ROW row;
    rp = mysql_store_result(db->mysql);
    if (!rp) return mysql_errno(db->mysql);

    //do {
    // only need first row (only one app should correspond to this workunit
    row = mysql_fetch_row(rp);
    if (!row) {
        mysql_free_result(rp);

        // No application was found - error
        log_messages.printf(MSG_CRITICAL,
            "d_val_request_output_files: Error. Did not find app that corresponded to workunit [#%d]\n",
            wuid
        );
        return ERR_DB_NOT_FOUND;


    } else {
        // save app_name
        strncpy(app_name, row[0], sizeof(app_name));
    }
    //} while (row);



    // go through list of D_VAL_APPs and see if this one has requested that its results be returned to server after validation (must_return_results())
    D_VAL_APP* dval_temp;
    bool return_result = false;
    int i;

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "d_val_req_out_files. dist_val_apps size: %zu\n",
        dist_val_apps.dval_apps.size()
    );

    for (i=0; i<dist_val_apps.dval_apps.size(); i++){
        dval_temp = dist_val_apps.dval_apps[i];

        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG,
            "d_val_req_out_files: Going through dist_val_apps. Current app [validator]: %s | validating: %s\n",
            dval_temp->get_d_val_app_name(), dval_temp->get_app_name_wu_to_validate()
        );

        // check
        if (strcmp(dval_temp->get_app_name_wu_to_validate(), app_name) == 0){

            // D_VAL DEBUG
            log_messages.printf(MSG_DEBUG,
                "d_val_req_out_files: Found application [(dval_temp) %s = %s (app_name)].  must_return_result:%d\n",
                dval_temp->get_app_name_wu_to_validate(), app_name, dval_temp->must_return_result()
            );


            return_result = dval_temp->must_return_result();
            break;
        }
    }

    if (!return_result){
        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG,
            "d_val_request_output_files: Application [%s] does not want its results to be returned to server.\n",
            app_name
        );
        return 0;
    }

    // results need to be returned, so update DB, in order to let scheduler know that hosts must be asked to return files (if they have not been requested or received yet)
    sprintf(query, "UPDATE workunit SET req_out_file_state = %d WHERE id=%d and req_out_file_state = %d",
             D_VAL_REQUEST_FILE, wuid, D_VAL_REQUEST_INIT);

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "d_val_request_output_files: Updating d_val Workunit's req_out_file_state through query: %s\n",
        query
    );

    retval = db->do_query(query);
    if (retval){
        log_messages.printf(MSG_CRITICAL,
            "d_val_request_output_files: Error updating Workunit's req_out_file_state through query: %s\n",
            query
        );
        return retval;
    }
}


// BOINC-MR
// Rename and move Map Result's output to input directory to be used as input for Reduce tasks
/// BOINC-MR TODO: check for errors in previous Map tasks. If error: do not create Reduce tasks, warn admin
//
/// D_VAL TODO: if a new result is available, check if there are enough returned results of a WU that is going to be validated by clients.
/// If so, check if validator wu has already been created.
/// - If validator WU does not exist: create WU
/// - If it has already been created: -- if successful, create a new val WU that compares new result's output with a single valid output
///                                   -- if considered invalid or unable to decide, create a new val WU that compares all current available results


int handle_wu(
    DB_TRANSITIONER_ITEM_SET& transitioner,
    std::vector<TRANSITIONER_ITEM>& items
) {
    int ntotal, nerrors, retval, ninprogress, nsuccess;
    int nunsent, ncouldnt_send, nover, ndidnt_need, nno_reply;
    int canonical_result_index, j;
    char suffix[256];
    time_t now = time(0), x;
    bool all_over_and_validated, have_new_result_to_validate, do_delete;
    unsigned int i;

    TRANSITIONER_ITEM& wu_item = items[0];
    TRANSITIONER_ITEM wu_item_original = wu_item;

    // "assigned" WUs aren't supposed to pass through the transitioner.
    // If we get one, it's an error
    //
    if (config.enable_assignment && strstr(wu_item.name, ASSIGNED_WU_STR)) {
        DB_WORKUNIT wu;
        char buf[256];

        wu.id = wu_item.id;
        log_messages.printf(MSG_CRITICAL,
            "Assigned WU %d unexpectedly found by transitioner\n", wu.id
        );
        sprintf(buf, "transition_time=%d", INT_MAX);
        retval = wu.update_field(buf);
        if (retval) {
            log_messages.printf(MSG_CRITICAL,
                "update_field failed %d\n", retval
            );
        }
        return 0;
    }

    // count up the number of results in various states,
    // and check for timed-out results
    //
    ntotal = 0;
    nunsent = 0;
    ninprogress = 0;
    nover = 0;
    nerrors = 0;
    nsuccess = 0;
    ncouldnt_send = 0;
    nno_reply = 0;
    ndidnt_need = 0;
    have_new_result_to_validate = false;
    int rs, max_result_suffix = -1;

    // Scan the WU's results, and find the canonical result if there is one
    //
    canonical_result_index = -1;
    if (wu_item.canonical_resultid) {
        for (i=0; i<items.size(); i++) {
            TRANSITIONER_ITEM& res_item = items[i];
            if (!res_item.res_id) continue;
            if (res_item.res_id == wu_item.canonical_resultid) {
                canonical_result_index = i;
            }
        }
    }

    if (wu_item.canonical_resultid && (canonical_result_index == -1)) {
        log_messages.printf(MSG_CRITICAL,
            "[WU#%d %s] can't find canonical result\n",
            wu_item.id, wu_item.name
        );
    }

    // if there is a canonical result, see if its file are deleted
    //
    bool canonical_result_files_deleted = false;
    if (canonical_result_index >= 0) {
        TRANSITIONER_ITEM& cr = items[canonical_result_index];
        if (cr.res_file_delete_state == FILE_DELETE_DONE) {
            canonical_result_files_deleted = true;
        }
    }

    // Scan this WU's results, and
    // 1) count those in various server states;
    // 2) identify time-out results and update their server state and outcome
    // 3) find the max result suffix (in case need to generate new ones)
    // 4) see if we have a new result to validate
    //    (outcome SUCCESS and validate_state INIT)
    //
    for (i=0; i<items.size(); i++) {
        TRANSITIONER_ITEM& res_item = items[i];

        if (!res_item.res_id) continue;
        ntotal++;

        rs = result_suffix(res_item.res_name);
        if (rs > max_result_suffix) max_result_suffix = rs;

        switch (res_item.res_server_state) {
        case RESULT_SERVER_STATE_UNSENT:
            nunsent++;
            break;
        case RESULT_SERVER_STATE_IN_PROGRESS:
            if (res_item.res_report_deadline < now) {
                log_messages.printf(MSG_NORMAL,
                    "[WU#%d %s] [RESULT#%d %s] result timed out (%d < %d) server_state:IN_PROGRESS=>OVER; outcome:NO_REPLY\n",
                    wu_item.id, wu_item.name, res_item.res_id, res_item.res_name,
                    res_item.res_report_deadline, (int)now
                );
                res_item.res_server_state = RESULT_SERVER_STATE_OVER;
                res_item.res_outcome = RESULT_OUTCOME_NO_REPLY;
                retval = transitioner.update_result(res_item);
                if (retval) {
                    log_messages.printf(MSG_CRITICAL,
                        "[WU#%d %s] [RESULT#%d %s] update_result(): %d\n",
                        wu_item.id, wu_item.name, res_item.res_id,
                        res_item.res_name, retval
                    );
                }
                penalize_host(res_item.res_hostid, (double)wu_item.delay_bound);
                nover++;
                nno_reply++;
            } else {
                ninprogress++;
            }
            break;
        case RESULT_SERVER_STATE_OVER:
            nover++;
            switch (res_item.res_outcome) {
            case RESULT_OUTCOME_COULDNT_SEND:
                log_messages.printf(MSG_NORMAL,
                    "[WU#%d %s] [RESULT#%d %s] result couldn't be sent\n",
                    wu_item.id, wu_item.name, res_item.res_id, res_item.res_name
                );
                ncouldnt_send++;
                break;
            case RESULT_OUTCOME_SUCCESS:
                if (res_item.res_validate_state == VALIDATE_STATE_INIT) {
                    if (canonical_result_files_deleted) {
                        res_item.res_validate_state = VALIDATE_STATE_TOO_LATE;
                        retval = transitioner.update_result(res_item);
                        log_messages.printf(MSG_NORMAL,
                            "[WU#%d %s] [RESULT#%d %s] validate_state:INIT=>TOO_LATE retval %d\n",
                            wu_item.id, wu_item.name, res_item.res_id,
                            res_item.res_name, retval
                        );
                    } else {
                        have_new_result_to_validate = true;
                    }
                }
                nsuccess++;
                break;
            case RESULT_OUTCOME_CLIENT_ERROR:
            case RESULT_OUTCOME_VALIDATE_ERROR:
                nerrors++;
                break;
            case RESULT_OUTCOME_CLIENT_DETACHED:
            case RESULT_OUTCOME_NO_REPLY:
                nno_reply++;
                break;
            case RESULT_OUTCOME_DIDNT_NEED:
                ndidnt_need++;
                break;
            }
            break;
        }
    }

    log_messages.printf(MSG_DEBUG,
        "[WU#%d %s] %d results: unsent %d, in_progress %d, over %d (success %d, error %d, couldnt_send %d, no_reply %d, didnt_need %d)\n",
        wu_item.id, wu_item.name, ntotal, nunsent, ninprogress, nover,
        nsuccess, nerrors, ncouldnt_send, nno_reply, ndidnt_need
    );

    // if there's a new result to validate, trigger validation
    //
    /// Distribute Validation: D_VAL
    // do not trigger validation if this workunit is supposed to be validated by another client (dist_val)
    //
    if (have_new_result_to_validate && (nsuccess >= wu_item.min_quorum) && !wu_item.dist_val) {
        wu_item.need_validate = true;
        log_messages.printf(MSG_NORMAL,
            "[WU#%d %s] need_validate:=>true\n", wu_item.id, wu_item.name
        );
    }
    // distributed validation is needed (new result is available)
    if (have_new_result_to_validate && (nsuccess >= wu_item.min_quorum) && wu_item.dist_val) {
        log_messages.printf(MSG_NORMAL,
            "[WU#%d %s] Needs distributed validation\n", wu_item.id, wu_item.name
        );
        // Therefore, create a new dist_val WU
        /// TODO: get dist_val WU information:
        /// - templates (result + workunit)
        /// - validator app name
        /// - number of available results (including the last available one) = nsuccess
        /// - ID of last result set for validation [go through list of results with validate_state_init]
        int last_res_id = -1;
        std::vector<TRANSITIONER_ITEM> d_val_res_items;
        std::vector<int> resultids;                         // vector to fill with IDs of results to validate
        if (!items.empty()){
            //TRANSITIONER_ITEM& temp_res_item = items[(items.size() - 1)];
            for (int k = (items.size() - 1); k>=0; k--){

                log_messages.printf(MSG_NORMAL,
                    "Inside loop. k: %d | items.size(): %zu\n", k, items.size()
                );

                TRANSITIONER_ITEM& temp_res_item = items[k];

                if (!temp_res_item.res_id) continue;

                // D_VAL DEBUG
                log_messages.printf(MSG_NORMAL,
                    "[WU#%d %s] Checking transitioner item #%d. Result ID: %d | last_res_id: %d\n", wu_item.id, wu_item.name, k, temp_res_item.res_id, last_res_id
                );

                // save information of one result (its ID) that is going to be validated (validate_state = VALIDATE_STATE_INIT - not VALIDATE_STATE_UNDER_DIST_VAL yet)
                if (temp_res_item.res_server_state == RESULT_SERVER_STATE_OVER && temp_res_item.res_outcome == RESULT_OUTCOME_SUCCESS &&
                    temp_res_item.res_validate_state == VALIDATE_STATE_INIT){

                    // update only last known value. Needed to create D_VAL WU with unique name
                    if (last_res_id == -1)
                        last_res_id = temp_res_item.res_id;
                    // list of results to be validated by distributed validation WU
                    // necessary to change their validate_state to VALIDATE_STATE_UNDER_DIST_VAL if dist val wu is created successfully
                    d_val_res_items.push_back(temp_res_item);
                    //break;
                }

                // save all results that are ready for DIST_VAL (total number of results ready: nsuccess)
                if (temp_res_item.res_server_state == RESULT_SERVER_STATE_OVER && temp_res_item.res_outcome == RESULT_OUTCOME_SUCCESS){
                    resultids.push_back(temp_res_item.res_id);
                }
            }
            if (last_res_id != -1){

                // Sort
                std::sort(resultids.begin(), resultids.end());

                // D_VAL DEBUG
                log_messages.printf(MSG_NORMAL,
                    "Finished sorting RESULT id vector. Current order\n");
                for (vector<int>::size_type i = 0; i != resultids.size(); ++i)
                    std::cerr << resultids[i] << " ";


                // D_VAL DEBUG
                log_messages.printf(MSG_NORMAL,
                    "[WU#%d %s] Gone through items. Last result ID: %d\n", wu_item.id, wu_item.name, last_res_id
                );

                /// D_VAL TODO: obtain information on templates that correspond to this app
                D_VAL_APP* dv_app;
                // get corresponding dist val application info, through the app id stored by wu_item
                dv_app = dist_val_apps.get_dv_app_wu(wu_item.name);

                // D_VAL DEBUG
                log_messages.printf(MSG_NORMAL,
                    "Called dist_val_apps.get_dv_app_wu(%s)\n", wu_item.name
                );


                if (!dv_app){
                    log_messages.printf(MSG_CRITICAL,
                        "handle_wu(): Error: This workunit should be valitaded by client (dist val), but Transitioner did not find app corresponding to this "
                        "workunit [%s] in dist_val.xml.",
                        wu_item.name);

                }
                else{

                    log_messages.printf(MSG_NORMAL,
                    "handle_wu(): Found app [ID:%d] to be validated: %s. Validating app: %s\n", wu_item.appid, dv_app->get_app_name_wu_to_validate(), dv_app->get_d_val_app_name()
                    );


                    /// D_VAL TODO - check nsuccess is correct. If there is a single new result to validate, check if previous validation is complete.
                    /// - If previous validation is complete and successful: compare new result with only one (valid) output;
                    ///     -- If prev valid is complete and UNSUCCESSFUL: compare ALL available results (try to find quorum again);
                    /// - If prev valid is incomplete: compare new result with all others.
                    /// * In cases where prev valid is running, add validators to list of peers that have the necessary inputs for validating wu.
                    // new result to validate, but there already exists a d_val WU that is validating other results returned previously
                    if (nsuccess != d_val_res_items.size()){
                        // check if that previous WU has already been successful in validating a result
                        if (canonical_result_index){
                            // If so, only compare the new available result(s) to the canonical result
                            /// TODO - add canonical result index to function input, to use only that result as the remaining input
                            ///      - insert canon_result_index to command line for app
                            //retval = create_dist_val_wu(dv_app->get_re_template(), dv_app->get_wu_template(), dv_app->get_d_val_app_name(), wu_item.alt_name,
                            //                            dv_app->get_num_wu_outputs(), dv_app->get_min_quorum(), nsuccess, last_res_id, wu_item.id,
                            //                            wu_item.canonical_resultid, previous_wu);
                            //                            canonical_result_index, previous_wu); // DEPRECATED - send canon result id instead of index (no use)

                        }
                        else{
                            // compare new results to all existing results in a new WU. When validating this WU, choose the first canonical result that is returned:
                            // - first  d_val_WU returned is validated, the following WUs are only used to provide a validation result for the missing results/hosts
                            /// TODO - all results will be used as input
                            ///      - However, tell function to use the IP addresses of all hosts currently running previous d_val_WUs
                            /// Problem: need to identify which results each d_val_wu is validating (only those result outputs are available)
                            ///      - insert bool previous_wu as input OR
                            ///      - function create_dist_val_wu(), when adding IP addresses of hosts, checks if there are already other d_val WU
                            ///        underway, and automatically inserts the addresses of those validator hosts for the corresponding results
                            // retval = create_dist_val_wu(dv_app->get_re_template(), dv_app->get_wu_template(), dv_app->get_d_val_app_name(), wu_item.alt_name,
                            //                            dv_app->get_num_wu_outputs(), dv_app->get_min_quorum(), nsuccess, last_res_id, wu_item.id,
                            //                            wu_item.canonical_resultid, previous_wu);
                            //                            canonical_result_index, previous_wu); // DEPRECATED - send canon result id instead of index (no use)
                        }
                    }

                    int validator_wuid = -1;


                    //int create_dist_val_wu(char* result_template_file, char* wu_template_file, char* app_name, char* wu_alt_name, int num_outputs, int min_quorum, int num_hosts,
                       //                    int last_result_id, int wuid_validating, int& validator_wuid){
                    retval = create_dist_val_wu(dv_app->get_re_template(), dv_app->get_wu_template(), dv_app->get_d_val_app_name(), wu_item.alt_name,
                                                //dv_app->get_num_wu_outputs(), dv_app->get_min_quorum(), nsuccess, last_res_id, wu_item.id);
                                                dv_app->get_num_wu_outputs(), dv_app->get_min_quorum(), nsuccess, last_res_id, wu_item.id, validator_wuid,
                                                resultids, dv_app->is_hashed(), dv_app->get_max_nbytes());

                    // if the WU was created succesffuly, update result validate state for all results (more than one may have been returned)
                    if (!retval){
                        // use temp_res_item vector (created earlier)
                        for (int k = (d_val_res_items.size() - 1); k>=0; k--){
                            //temp_res_item = items[k];
                            d_val_res_items[k].validator_wuid = validator_wuid;
                            d_val_res_items[k].res_validate_state = VALIDATE_STATE_UNDER_DIST_VAL;

                            TRANSITIONER_ITEM& d_val_res_item = d_val_res_items[k];

                            // Error: despite having a new result to validate, no result waiting for validation was found
                            log_messages.printf(MSG_DEBUG,
                                "Updating result [ID#%d] with new validate_state: %d. Retval: %d\n",
                                d_val_res_item.res_id, d_val_res_item.res_validate_state, retval
                            );

                            retval = transitioner.update_result(d_val_res_item);
                        }
                    }
                    else{
                        // Error: despite having a new result to validate, no result waiting for validation was found
                        log_messages.printf(MSG_CRITICAL,
                            "Error. Transitioner could not create distributed validation for wu: %s. Retval: %d\n",
                            wu_item.name, retval
                        );

                    }
                }
            }
            else {
                // Error: despite having a new result to validate, no result waiting for validation was found
                log_messages.printf(MSG_CRITICAL,
                    "Error. Transitioner discovered new result to validate in WU #%d, but could not find result with validate state init [%d].\n",
                    wu_item.id, VALIDATE_STATE_INIT
                );
            }
        }
        else{
            // Error: this should not be empty
            log_messages.printf(MSG_CRITICAL,
                "Error. Transitioner received list of results for WU #%d that was empty.\n", wu_item.id
            );
        }
    }

    // check for WU error conditions
    // NOTE: check on max # of success results is done in validater
    //
    if (ncouldnt_send > 0) {
        wu_item.error_mask |= WU_ERROR_COULDNT_SEND_RESULT;
    }

    // if WU has results with errors and no success yet,
    // reset homogeneous redundancy class to give other platforms a try
    //
    if (nerrors & !(nsuccess || ninprogress)) {
        wu_item.hr_class = 0;
    }

    if (nerrors > wu_item.max_error_results) {
        log_messages.printf(MSG_NORMAL,
            "[WU#%d %s] WU has too many errors (%d errors for %d results)\n",
            wu_item.id, wu_item.name, nerrors, ntotal
        );
        wu_item.error_mask |= WU_ERROR_TOO_MANY_ERROR_RESULTS;
    }

    // see how many new results we need to make
    //
    int n_new_results_needed = wu_item.target_nresults - nunsent - ninprogress - nsuccess;
    if (n_new_results_needed < 0) n_new_results_needed = 0;
    int n_new_results_allowed = wu_item.max_total_results - ntotal;

    // if we're already at the limit and need more, error out the WU
    //
    bool too_many = false;
    if (n_new_results_allowed < 0) {
        too_many = true;
    } else if (n_new_results_allowed == 0) {
        if (n_new_results_needed > 0) {
            too_many = true;
        }
    } else {
        if (n_new_results_needed > n_new_results_allowed) {
            n_new_results_needed = n_new_results_allowed;
        }
    }
    if (too_many) {
        log_messages.printf(MSG_NORMAL,
            "[WU#%d %s] WU has too many total results (%d)\n",
            wu_item.id, wu_item.name, ntotal
        );
        wu_item.error_mask |= WU_ERROR_TOO_MANY_TOTAL_RESULTS;
    }

    // if this WU had an error, don't send any unsent results,
    // and trigger assimilation if needed
    //
    if (wu_item.error_mask) {
        for (i=0; i<items.size(); i++) {
            TRANSITIONER_ITEM& res_item = items[i];
            if (!res_item.res_id) continue;
            bool update_result = false;
            switch(res_item.res_server_state) {
            case RESULT_SERVER_STATE_UNSENT:
                log_messages.printf(MSG_NORMAL,
                    "[WU#%d %s] [RESULT#%d %s] server_state:UNSENT=>OVER; outcome:=>DIDNT_NEED\n",
                    wu_item.id, wu_item.name, res_item.res_id, res_item.res_name
                );
                res_item.res_server_state = RESULT_SERVER_STATE_OVER;
                res_item.res_outcome = RESULT_OUTCOME_DIDNT_NEED;
                update_result = true;
                break;
            case RESULT_SERVER_STATE_OVER:
                switch (res_item.res_outcome) {
                case RESULT_OUTCOME_SUCCESS:
                    switch(res_item.res_validate_state) {
                    case VALIDATE_STATE_INIT:
                    case VALIDATE_STATE_INCONCLUSIVE:
                        res_item.res_validate_state = VALIDATE_STATE_NO_CHECK;
                        update_result = true;
                        break;
                    }
                }
            }
            if (update_result) {
                retval = transitioner.update_result(res_item);
                if (retval) {
                    log_messages.printf(MSG_CRITICAL,
                        "[WU#%d %s] [RESULT#%d %s] result.update() == %d\n",
                        wu_item.id, wu_item.name, res_item.res_id, res_item.res_name, retval
                    );
                }
            }
        }
        if (wu_item.assimilate_state == ASSIMILATE_INIT) {
            wu_item.assimilate_state = ASSIMILATE_READY;
            log_messages.printf(MSG_NORMAL,
                "[WU#%d %s] error_mask:%d assimilate_state:INIT=>READY\n",
                wu_item.id, wu_item.name, wu_item.error_mask
            );
        }
    } else if (wu_item.canonical_resultid == 0) {
        // Here if no WU-level error.
        // Generate new results if needed.
        //
        std::string values;
        char value_buf[MAX_QUERY_LEN];
        if (n_new_results_needed > 0) {
            log_messages.printf(
                MSG_NORMAL,
                "[WU#%d %s] Generating %d more results (%d target - %d unsent - %d in progress - %d success)\n",
                wu_item.id, wu_item.name, n_new_results_needed,
                wu_item.target_nresults, nunsent, ninprogress, nsuccess
            );

            for (j=0; j<n_new_results_needed; j++) {
                sprintf(suffix, "%d", max_result_suffix+j+1);
                const char *rtfpath = config.project_path("%s", wu_item.result_template_file);
                int priority_increase = 0;
                if (nover && config.reliable_priority_on_over) {
                    priority_increase += config.reliable_priority_on_over;
                } else if (nover && !nerrors && config.reliable_priority_on_over_except_error) {
                    priority_increase += config.reliable_priority_on_over_except_error;
                }
                retval = create_result_ti(
                    wu_item, (char *)rtfpath, suffix, key, config, value_buf, priority_increase
                );
                if (retval) {
                    log_messages.printf(MSG_CRITICAL,
                        "[WU#%d %s] create_result_ti() %d\n",
                        wu_item.id, wu_item.name, retval
                    );
                    return retval;
                }
                if (j==0) {
                    values = value_buf;
                } else {
                    values += ",";
                    values += value_buf;
                }
            }

            // D_VAL
            // modify template of validator WUs (validate other WUs), to include xml_signature for all input files. This is necessary
            // since they may be requested by server (which will not be able to accept its upload without a signature)
            if (!config.dont_generate_upload_certificates){
                //retval = dval_modify_validator_wu_template(key, config, wu_item);
                retval = dval_modify_validator_wu_template(key, wu_item);
                if (retval){
                    log_messages.printf(MSG_CRITICAL,
                            "[WU#%d %s] dval_modify_validator_wu_template() %d\n",
                            wu_item.id, wu_item.name, retval
                        );
                        return retval;
                }
            }

            DB_RESULT r;
            retval = r.insert_batch(values);
            if (retval) {
                log_messages.printf(MSG_CRITICAL,
                    "[WU#%d %s] insert_batch() %d\n",
                    wu_item.id, wu_item.name, retval
                );
                return retval;
            }
        }
    }

    // scan results:
    //  - see if all over and validated
    //
    all_over_and_validated = true;
    bool all_over_and_ready_to_assimilate = true; // used for the defer assmilation
    int most_recently_returned = 0;
    for (i=0; i<items.size(); i++) {
        TRANSITIONER_ITEM& res_item = items[i];
        if (!res_item.res_id) continue;
        if (res_item.res_server_state == RESULT_SERVER_STATE_OVER) {
            if ( res_item.res_received_time > most_recently_returned ) {
                most_recently_returned = res_item.res_received_time;
            }
            if (res_item.res_outcome == RESULT_OUTCOME_SUCCESS) {
                if (res_item.res_validate_state == VALIDATE_STATE_INIT) {
                    all_over_and_validated = false;
                    all_over_and_ready_to_assimilate = false;
                }
            } else if ( res_item.res_outcome == RESULT_OUTCOME_NO_REPLY ) {
                if ( ( res_item.res_report_deadline + config.grace_period_hours*60*60 ) > now ) {
                    all_over_and_validated = false;
                }
            }
        } else {
            all_over_and_validated = false;
            all_over_and_ready_to_assimilate = false;
        }
    }

    // If we are defering assimilation until all results are over
    // and validated then when that happens we need to make sure
    // that it gets advanced to assimilate ready
    // the items.size is a kludge
    //
    if (all_over_and_ready_to_assimilate == true && wu_item.assimilate_state == ASSIMILATE_INIT && items.size() > 0 && wu_item.canonical_resultid > 0
    ) {
        wu_item.assimilate_state = ASSIMILATE_READY;
        log_messages.printf(MSG_NORMAL,
            "[WU#%d %s] Deferred assimililation now set to ASSIMILATE_STATE_READY\n",
            wu_item.id, wu_item.name
        );
    }

    // D_VAL
    // check if this wu has been assimilated, is valid (canon_result > 0), and is a validator WU (was used to validate another WU)
    if (wu_item.assimilate_state == ASSIMILATE_DONE && wu_item.canonical_resultid > 0 && canonical_result_index >= 0 && wu_item.validating_wuid >= 0){

        // D_VAL DEBUG
        log_messages.printf(MSG_CRITICAL,
            "handle_wu: Workunit [WU#%d %s] has been assimilated and may be ready for requesting files. Calling d_val_request_output_files. validating_wuid: %d.\n",
            wu_item.id, wu_item.name, wu_item.validating_wuid
        );

        // D_VAL
        // since this workunit is ready for validation, check if its results must be returned to the server

            retval = d_val_request_output_files(wu_item.validating_wuid);
            if (retval) {
                log_messages.printf(MSG_CRITICAL,
                    "handle_wu: d_val_request_output_files([WU#%d %s]) failed: %d\n",
                    wu_item.validating_wuid, wu_item.name, retval
                );
            }
    }

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "assim_state: %d | file_del_state: %d | canon_result: %d | canon_res_index: %d | canon_files_deleted: %d | mr_app_type: %d | validator_wuid: %d\n",
        wu_item.assimilate_state, wu_item.file_delete_state, wu_item.canonical_resultid,
        canonical_result_index, canonical_result_files_deleted, wu_item.mr_app_type,
        wu_item.validator_wuid
    );
    log_messages.printf(MSG_DEBUG,
        "[E] assim_state: %d | file_del_state: %d | canon_result: >0 | canon_res_index: >=0 | canon_files_deleted: 0 | mr_app_type: %d | validator_wuid: >=0\n",
        ASSIMILATE_DONE, FILE_DELETE_INIT, MR_APP_TYPE_MAP
    );

    // BOINC-MR

    // If this WU is a Map task, AND returns its output's hashes instead of the file itself [<hashed> tag],
    // open the output and save the hash and the original file size (saved inside returned output) in the Reduce WU template
    // Insert:
    // - <md5_cksum>...</md5_cksum>
    // - <nbytes>...</nbytes>
    //
    // If the file itself is returned, rename MAP output files and copy them to input directory to for Reduce workunit creation
    // input file must be created and moved after validating Map WU
    //
    // Check if
    // - WU is assimilated and has not been deleted yet
    // - there is a canonical result (WU was a success) and its files have not been deleted
    // - it is a Map task
    //
    if (wu_item.assimilate_state == ASSIMILATE_DONE && wu_item.file_delete_state == FILE_DELETE_INIT &&
        wu_item.canonical_resultid > 0 && canonical_result_index >= 0 && !canonical_result_files_deleted &&
        wu_item.mr_app_type == MR_APP_TYPE_MAP){

        MAPREDUCE_JOB* mrj;
        // get corresponding job
        mrj = mapreduce_jobs.mr_get_job(wu_item.mr_job_id);
        if (!mrj){
            log_messages.printf(MSG_CRITICAL, "BOINC-MR handle_wu() - could not find MapReduce job"
                                "with ID %d", wu_item.mr_job_id);
        }

        // BOINC-MR DEBUG
        log_messages.printf(MSG_CRITICAL, "BOINC-MR handle_wu() - MR job #%d status: %d\n",
                                wu_item.mr_job_id, mrj->get_job_status());

        // Cannot take for granted that Result's output file names will be of the type $RESULTNAME_$INDEX,
        // so get xml_doc_in from Result table in DB
        char h_buf[256], h_clause[256];
        strcpy(h_clause, "");
        DB_RESULT h_result;
        retval = 0;
        ///DB_RESULT file_avail_result;

        if (do_mod){
            sprintf(h_clause, " and id %% %d = %d ", mod_n, mod_i );
        }

        sprintf(h_buf,"where id=%d %s", wu_item.canonical_resultid, h_clause);
        retval = h_result.enumerate(h_buf);
        if (retval) {
            if (retval != ERR_DB_NOT_FOUND) {
                log_messages.printf(MSG_DEBUG, "BOINC-MR : handle_wu() - DB connection lost, exiting\n");
                exit(0);
            }
            log_messages.printf(MSG_DEBUG, "BOINC-MR : handle_wu() - Could not find Result with [ID#%d]. Query: %s\n", wu_item.canonical_resultid, h_buf);
        }

        else if(mrj->get_job_status() == MR_JOB_DONE){
            // there was an error before reaching this step (e.g.: was unable to save hash info from Map output)
            // which makes it impossible to continue this MR job
            log_messages.printf(MSG_CRITICAL, "BOINC-MR handle_wu() - MapReduce job with id %d is over [status: %d].\n"
                                "An error prevented it from starting the Reduce stage.\n",wu_item.mr_job_id, mrj->get_job_status());

        }

        // found job successfully and its Map task return hashed result instead of output itself
        else if (mrj->mr_hashed && !mrj->mr_is_input_available(wu_item.id)){
            /// BOINC-MR TODO: check if we haven't already added this info to hashed_outputs - default value?
            /// Use the same value - mr_is_input_available [set in check_create_red_wus() by mr_add_input_task]


            /// BOINC-MR TODO: scenario - normal boinc client requests work, gets hashed map task (returns the output file, not just hash)
            /// uncomment following lines;
            /// transitioner crashes after moving map output to input dir; investigate and solve problem
            // if a client is not able to host output file, it returned the file itself, not just the hash
            // if the file is available and valid, move it to input dir
            //char h_buf[256], h_clause[256];
            /*strcpy(h_clause, "");
            retval = 0;
            if (do_mod){
                sprintf(h_clause, " and id %% %d = %d ", mod_n, mod_i );
            }


            // result from same workunit, valid, and without serving map outputs (user_hosting=0)
            /*sprintf(h_buf,"where workunitid=%d and validate_state=1 and user_hosting=0 %s", wu_item.id, h_clause);
            ///retval = file_avail_result.enumerate(h_buf);
            // found a result
            if (!retval){
                // alternative to mr_save_hashed_outputs
                retval = mr_copy_map_outputs(file_avail_result, wu_item.name, mrj, false);
                // No errors so far
                if (!retval){
                    // add this wu id to list of available inputs
                    mrj->mr_add_input_task(wu_item.id);
                    // add this wu id to list of map output files available on the server
                    mrj->mr_add_output_avail(wu_item.id);
                    // check if all Map inputs are available. If so, create red WU
                    check_create_red_wus(mrj, wu_item.id);
                }
                else{
                    log_messages.printf(MSG_CRITICAL, "BOINC-MR : handle_wu() - Error copying Map output to input directory\n");
                    // BOINC-MR DEBUG
                    fflush(stderr);
                }
            }
            // there was no such result, proceed as normal (all map outputs are being hosted and uploaded by clients)
            else{
                if (retval != ERR_DB_NOT_FOUND) {
                    log_messages.printf(MSG_DEBUG, "BOINC-MR : handle_wu() - DB connection lost, exiting\n");
                    exit(0);
                }
                log_messages.printf(MSG_DEBUG, "BOINC-MR : handle_wu() - Could not find Result with [WU_ID#%d], valid and not hosting map outputs. Query: %s\n", wu_item.id, h_buf);
                */



                // Do not call this function if this map workunit was validated by clients (D_VAL) - hashes were not returned
                if (wu_item.validator_wuid >= 0){
                    // add this Map WU name to mr_map_wu_names. Used in mr_create_reduce_work()
                    std::string str = std::string(wu_item.name);
                    mrj->mr_add_map_wu_name(str);

                    // add this wu id to list of available inputs
                    mrj->mr_add_input_task(wu_item.id);
                    // check if all Map inputs are available. If so, create red WU
                    check_create_red_wus(mrj, wu_item.id);
                }
                else {
                    // parse xml_doc_in from Result and save hash and files from Map outputs in mrj->hashed_outputs array
                    retval = mr_save_hashed_outputs(h_result, wu_item.name, mrj);

                    if (retval){
                        log_messages.printf(MSG_CRITICAL, "BOINC-MR handle_wu() - could not set hash and file size for Reduce inputs "
                                    "for MR job with ID %d\n", wu_item.mr_job_id);

                        mrj->set_job_status(MR_JOB_DONE);
                        /// BOINC-MR TODO : add error_output/value to MR job - set in this case since MR job could not be completed
                        ///mrj->set_error_value(MR_JOB_HASH_ERROR);
                    }
                    else{
                        // add this Map WU name to mr_map_wu_names. Used in mr_create_reduce_work()
                        std::string str = std::string(wu_item.name);
                        mrj->mr_add_map_wu_name(str);

                        // add this wu id to list of available inputs
                        mrj->mr_add_input_task(wu_item.id);
                        // check if all Map inputs are available. If so, create red WU
                        check_create_red_wus(mrj, wu_item.id);
                    }
                }
            ///}
        }

        // Returned output itself (not just hash), so copy it to reduce wu staging area
        // check in MapReduce jobs list if this wu is already set as available (already copied files)
        else if (!mrj->mr_hashed && !mrj->mr_is_input_available(wu_item.id)){
            log_messages.printf(MSG_DEBUG, "BOINC-MR handle_wu() - MapReduce job with ID %d does not return"
                                "hashed outputs in Map stage.", wu_item.mr_job_id);

            // alternative to mr_save_hashed_outputs
            retval = mr_copy_map_outputs(h_result, wu_item.name, mrj, false);
            // No errors so far
            if (!retval){
                // add this wu id to list of available inputs
                mrj->mr_add_input_task(wu_item.id);
                // add this wu id to list of map output files available on the server
                ///mrj->mr_add_output_avail(wu_item.id);    /// scenario presented above
                // check if all Map inputs are available. If so, create red WU
                check_create_red_wus(mrj, wu_item.id);
            }
            else{
                log_messages.printf(MSG_CRITICAL, "BOINC-MR : handle_wu() - Error copying Map output to input directory\n");
                // BOINC-MR DEBUG
                fflush(stderr);
            }
        }
        // in case this MR job is hashed, but there is a new result available from a client that returned the
        /*else if(mrj->mr_hashed && !mrj->mr_is_output_file_available(wu_item.id)){
            DB_RESULT file_avail_result;
            retval = 0;
            if (do_mod){
                sprintf(h_clause, " and id %% %d = %d ", mod_n, mod_i );
            }

            // result from same workunit, valid, and without serving map outputs (user_hosting=0)
            sprintf(h_buf,"where workunitid=%d and validate_state=1 and user_hosting=0 %s", wu_item.id, h_clause);
            retval = file_avail_result.enumerate(h_buf);
            // found a result whose file has not been moved and saved as reduce input
            if (!retval){
                retval = mr_copy_map_outputs(h_result, wu_item.name, mrj, true);
                // No errors so far
                if (!retval){
                    // add this wu id to list of map output files available on the server
                    mrj->mr_add_output_avail(wu_item.id);
                }
                else{
                    log_messages.printf(MSG_CRITICAL, "BOINC-MR : handle_wu() - Error copying Map output to input directory\n");
                    // BOINC-MR DEBUG
                    fflush(stderr);

                    /// BOINC-MR TODO: do not try again? act as if it worked so it won't try again / try other result?
                    // add this wu id to list of map output files available on the server
                    // mrj->mr_add_output_avail(wu_item.id);
                }
            }
            // there was no such result, proceed as normal (all map outputs are being hosted and uploaded by clients)
            else{
                if (retval != ERR_DB_NOT_FOUND) {
                    log_messages.printf(MSG_DEBUG, "BOINC-MR : handle_wu() - DB connection lost, exiting\n");
                    exit(0);
                }
                log_messages.printf(MSG_DEBUG, "BOINC-MR : handle_wu() - Could not find Result with [WU_ID#%d], valid and not hosting map outputs. Query: %s\n", wu_item.id, h_buf);
            }
        }*/
    }


    // Move canonical Map result output to input directory for Reduce workunit creation
    // input file must be created and moved after validating Map WU
    // Check if
    // - WU is assimilated and has not been deleted yet
    // - there is a canonical result (WU was a success) and its files have not been deleted
    // - it is a Map task
    // If so, rename MAP output files and copy them to input directory
    //
    /*if (wu_item.assimilate_state == ASSIMILATE_DONE && wu_item.file_delete_state == FILE_DELETE_INIT &&
        wu_item.canonical_resultid > 0 && canonical_result_index >= 0 && !canonical_result_files_deleted &&
        wu_item.mr_app_type == MR_APP_TYPE_MAP){

        MAPREDUCE_JOB* mrj;
        // get corresponding job
        mrj = mapreduce_jobs.mr_get_job(wu_item.mr_job_id);
        if (!mrj){
            log_messages.printf(MSG_CRITICAL, "BOINC-MR handle_wu() - could not find MapReduce job int mapreduce_jobs"
                                "with ID %d", wu_item.mr_job_id);
        }
        else if(mrj->get_job_status() == MR_JOB_DONE){
            // there was an error before reaching this step (e.g.: was unable to save hash info from Map output)
            // which makes it impossible to continue this MR job
            log_messages.printf(MSG_CRITICAL, "BOINC-MR handle_wu() - MapReduce job with id %d is over. An error prevented"
                                " it from starting the Reduce stage.", wu_item.mr_job_id);

        }
        else{
            // check in MapReduce jobs list if this wu is already set as available (already copied files)
            if (!mrj->mr_is_input_available(wu_item.id)){
                char buf[256], clause[256];
                strcpy(clause, "");
                DB_RESULT result;
                retval = 0;

                // Cannot take for granted that Result's output file names will be of the type $RESULTNAME_$INDEX,
                // so get xml_doc_in from Result table in DB
                if (do_mod){
                    sprintf(clause, " and id %% %d = %d ", mod_n, mod_i );
                }

                sprintf(buf,"where id=%d %s", wu_item.canonical_resultid, clause);
                retval = result.enumerate(buf);
                if (retval) {
                    if (retval != ERR_DB_NOT_FOUND) {
                        log_messages.printf(MSG_DEBUG, "BOINC-MR : handle_wu() - DB connection lost, exiting\n");
                        exit(0);
                    }
                    log_messages.printf(MSG_DEBUG, "BOINC-MR : handle_wu() - Could not find Result with [ID#%d]. Query: %s\n", wu_item.res_id, buf);
                }
                else {
                    //std::string str_name(wu_item.name);
                    // parse xml_doc_in from Result and copy output
                    retval = mr_copy_map_outputs(result, wu_item.name, mrj);
                    //retval = mr_copy_map_outputs(result, str_name, mrj);
                }

                // No errors so far
                if (!retval){
                    create_red_wus(mrj, wu_item.id);
                }
                else{
                    log_messages.printf(MSG_CRITICAL, "BOINC-MR : handle_wu() - Error copying Map output to input directory\n");
                    // BOINC-MR DEBUG
                    fflush(stderr);
                }
            }
        }
    }
    // BOINC-MR DEBUG
    // WU is not a Map task or still is not ready/done so output not moved to input directory
    else{
        log_messages.printf(MSG_DEBUG, "BOINC-MR | handle_wu() | [WU#%d] not done OR not Map task - MAP outputs not moved [EXPECTED]. assim_state: "
                            "%d [%d] | file_del_state: %d [%d]\n"
                            "canon_resultid: %d [>0] | canon_result_index: %d | canon_files_del?: %d [0] | mr_app_type=%d [%d]\n",
                            wu_item.id, wu_item.assimilate_state, ASSIMILATE_DONE, wu_item.file_delete_state, FILE_DELETE_INIT,
                            wu_item.canonical_resultid, canonical_result_index,
                            canonical_result_files_deleted, wu_item.mr_app_type, MR_APP_TYPE_MAP
        );
    }*/


    // if WU is assimilated, trigger file deletion
    //
    if (wu_item.assimilate_state == ASSIMILATE_DONE && ((most_recently_returned + config.delete_delay_hours*60*60) < now)) {
        // can delete input files if all results OVER
        //
        if (all_over_and_validated && wu_item.file_delete_state == FILE_DELETE_INIT) {
            wu_item.file_delete_state = FILE_DELETE_READY;
            log_messages.printf(MSG_DEBUG,
                "[WU#%d %s] ASSIMILATE_DONE: file_delete_state:=>READY\n",
                wu_item.id, wu_item.name
            );
        }

        // output of error results can be deleted immediately;
        // output of success results can be deleted if validated
        //
        for (i=0; i<items.size(); i++) {
            TRANSITIONER_ITEM& res_item = items[i];

            // can delete canonical result outputs only if all successful
            // results have been validated
            //
            if (((int)i == canonical_result_index) && !all_over_and_validated) {
                continue;
            }

            if (res_item.res_id) {
                do_delete = false;
                switch(res_item.res_outcome) {
                case RESULT_OUTCOME_CLIENT_ERROR:
                    do_delete = true;
                    break;
                case RESULT_OUTCOME_SUCCESS:
                    do_delete = (res_item.res_validate_state != VALIDATE_STATE_INIT);
                    break;
                }
                if (do_delete && res_item.res_file_delete_state == FILE_DELETE_INIT) {
                    log_messages.printf(MSG_NORMAL,
                        "[WU#%d %s] [RESULT#%d %s] file_delete_state:=>READY\n",
                        wu_item.id, wu_item.name, res_item.res_id, res_item.res_name
                    );
                    res_item.res_file_delete_state = FILE_DELETE_READY;

                    retval = transitioner.update_result(res_item);
                    if (retval) {
                        log_messages.printf(MSG_CRITICAL,
                            "[WU#%d %s] [RESULT#%d %s] result.update() == %d\n",
                            wu_item.id, wu_item.name, res_item.res_id, res_item.res_name, retval
                        );
                    }
                }
            }
        }
    } else if ( wu_item.assimilate_state == ASSIMILATE_DONE ) {
        log_messages.printf(MSG_DEBUG,
            "[WU#%d %s] not checking for items to be ready for delete because the deferred delete time has not expired.  That will occur in %d seconds\n",
            wu_item.id,
            wu_item.name,
            most_recently_returned + config.delete_delay_hours*60*60-(int)now
        );
    }

    // compute next transition time = minimum timeout of in-progress results
    //
    if (wu_item.canonical_resultid) {
        wu_item.transition_time = INT_MAX;
    } else {
        // If there is no canonical result,
        // make sure that the transitioner will 'see' this WU again.
        // In principle this is NOT needed, but it is one way to make
        // the BOINC back-end more robust.
        //
        const int ten_days = 10*86400;
        int long_delay = (int)(1.5*wu_item.delay_bound);
        wu_item.transition_time = (long_delay > ten_days) ? long_delay : ten_days;
        wu_item.transition_time += time(0);
    }
    int max_grace_or_delay_time = 0;
    for (i=0; i<items.size(); i++) {
        TRANSITIONER_ITEM& res_item = items[i];
        if (res_item.res_id) {
            if (res_item.res_server_state == RESULT_SERVER_STATE_IN_PROGRESS) {
                // In cases where a result has been RESENT to a host, the
                // report deadline time may be EARLIER than
                // sent_time + delay_bound
                // because the sent_time has been updated with the later
                // "resend" time.
                //
                // x = res_item.res_sent_time + wu_item.delay_bound;
                x = res_item.res_report_deadline;
                if (x < wu_item.transition_time) {
                    wu_item.transition_time = x;
                }
            } else if ( res_item.res_server_state == RESULT_SERVER_STATE_OVER  ) {
                if ( res_item.res_outcome == RESULT_OUTCOME_NO_REPLY ) {
                    // Transition again after the grace period has expired
                    //
                    if ((res_item.res_report_deadline + config.grace_period_hours*60*60) > now) {
                        x = res_item.res_report_deadline + config.grace_period_hours*60*60;
                        if (x > max_grace_or_delay_time) {
                            max_grace_or_delay_time = x;
                        }
                    }
                } else if (res_item.res_outcome == RESULT_OUTCOME_SUCCESS || res_item.res_outcome == RESULT_OUTCOME_CLIENT_ERROR || res_item.res_outcome == RESULT_OUTCOME_VALIDATE_ERROR) {
                    // Transition again after deferred delete period has experied
                    //
                    if ((res_item.res_received_time + config.delete_delay_hours*60*60) > now) {
                        x = res_item.res_received_time + config.delete_delay_hours*60*60;
                        if (x > max_grace_or_delay_time && res_item.res_received_time > 0) {
                            max_grace_or_delay_time = x;
                        }
                    }
                }
            }
        }
    }

    // If either of the grace period or delete delay is less than
    // the next transition time then use that value
    //
    if (max_grace_or_delay_time < wu_item.transition_time && max_grace_or_delay_time > now && ninprogress == 0) {
        wu_item.transition_time = max_grace_or_delay_time;
        log_messages.printf(MSG_NORMAL,
            "[WU#%d %s] Delaying transition due to grace period or delete day.  New transition time = %d sec\n",
            wu_item.id, wu_item.name, wu_item.transition_time
        );
    }

    // If transition time is in the past,
    // the system is bogged down and behind schedule.
    // Delay processing of the WU by an amount DOUBLE the amount we are behind,
    // but not less than 60 secs or more than one day.
    //
    if (wu_item.transition_time < now) {
        int extra_delay = 2*(now - wu_item.transition_time);
        if (extra_delay < 60) extra_delay = 60;
        if (extra_delay > 86400) extra_delay = 86400;
        log_messages.printf(MSG_DEBUG,
            "[WU#%d %s] transition time in past: adding extra delay %d sec\n",
            wu_item.id, wu_item.name, extra_delay
        );
        wu_item.transition_time = now + extra_delay;
    }

    log_messages.printf(MSG_DEBUG,
        "[WU#%d %s] setting transition_time to %d\n",
        wu_item.id, wu_item.name, wu_item.transition_time
    );

    retval = transitioner.update_workunit(wu_item, wu_item_original);
    if (retval) {
        log_messages.printf(MSG_CRITICAL,
            "[WU#%d %s] workunit.update() == %d\n",
            wu_item.id, wu_item.name, retval
        );
        return retval;
    }
    return 0;
}

bool do_pass() {
    int retval;
    DB_TRANSITIONER_ITEM_SET transitioner;
    std::vector<TRANSITIONER_ITEM> items;
    bool did_something = false;

    if (!one_pass) check_stop_daemons();

    // loop over entries that are due to be checked
    //
    while (1) {
        retval = transitioner.enumerate(
            (int)time(0), SELECT_LIMIT, mod_n, mod_i, items
        );
        if (retval) {
            if (retval != ERR_DB_NOT_FOUND) {
                log_messages.printf(MSG_CRITICAL,
                    "WU enum error%d; exiting\n", retval
                );
                exit(1);
            }
            break;
        }
        did_something = true;
        TRANSITIONER_ITEM& wu_item = items[0];
        retval = handle_wu(transitioner, items);
        if (retval) {
            log_messages.printf(MSG_CRITICAL,
                "[WU#%d %s] handle_wu: %d; quitting\n",
                wu_item.id, wu_item.name, retval
            );
            exit(1);
        }

        if (!one_pass) check_stop_daemons();
    }
    return did_something;
}

void main_loop() {
    int retval;

    retval = boinc_db.open(config.db_name, config.db_host, config.db_user, config.db_passwd);
    if (retval) {
        log_messages.printf(MSG_CRITICAL, "boinc_db.open: %d\n", retval);
        exit(1);
    }

    while (1) {
        log_messages.printf(MSG_DEBUG, "doing a pass\n");
        if (!do_pass()) {
            if (one_pass) break;
#ifdef GCL_SIMULATOR
            continue_simulation("transitioner");
            signal(SIGUSR2, simulator_signal_handler);
            pause();
#else
            log_messages.printf(MSG_DEBUG, "sleeping %d\n", sleep_interval);
            sleep(sleep_interval);
#endif
        }
    }
}

void usage(char *name) {
    fprintf(stderr,
        "Handles transitions in the state of a WU\n"
        " - a result has become DONE (via timeout or client reply)\n"
        " - the WU error mask is set (e.g. by validater)\n"
        " - assimilation is finished\n\n"
        "Usage: %s [OPTION]...\n\n"
        "Options: \n"
        "  [ -one_pass ]                  do one pass, then exit\n"
        "  [ -d x ]                       debug level x\n"
        "  [ -mod n i ]                   process only WUs with (id mod n) == i\n"
        "  [ -sleep_interval x ]          sleep x seconds if nothing to do\n"
        "  [ -h | -help | --help ]        Show this help text.\n"
        "  [ -v | -version | --version ]  Shows version information.\n",
        name
    );
}

int main(int argc, char** argv) {
    int i, retval;
    char path[256];

    // BOINC-MR DEBUG
    //chdir("/new/working/directory");

    startup_time = time(0);
    for (i=1; i<argc; i++) {
        if (!strcmp(argv[i], "-one_pass")) {
            one_pass = true;
        } else if (!strcmp(argv[i], "-d")) {
            if(!argv[++i]) {
                log_messages.printf(MSG_CRITICAL, "%s requires an argument\n\n", argv[--i]);
                usage(argv[0]);
                exit(1);
            }
            log_messages.set_debug_level(atoi(argv[i]));
        } else if (!strcmp(argv[i], "-mod")) {
            if(!argv[i+1] || !argv[i+2]) {
                log_messages.printf(MSG_CRITICAL, "%s requires two arguments\n\n", argv[i]);
                usage(argv[0]);
                exit(1);
            }
            mod_n = atoi(argv[++i]);
            mod_i = atoi(argv[++i]);
            do_mod = true;
        } else if (!strcmp(argv[i], "-sleep_interval")) {
            if(!argv[++i]) {
                log_messages.printf(MSG_CRITICAL, "%s requires an argument\n\n", argv[--i]);
                usage(argv[0]);
                exit(1);
            }
            sleep_interval = atoi(argv[i]);
        } else if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "-help") || !strcmp(argv[i], "--help")) {
            usage(argv[0]);
            exit(0);
        } else if (!strcmp(argv[i], "-v") || !strcmp(argv[i], "-version") || !strcmp(argv[i], "--version")) {
            printf("%s\n", SVN_VERSION);
            exit(0);
        } else {
            log_messages.printf(MSG_CRITICAL, "unknown command line argument: %s\n\n", argv[i]);
            usage(argv[0]);
            exit(1);
        }
    }
    if (!one_pass) check_stop_daemons();

    retval = config.parse_file();
    if (retval) {
        log_messages.printf(MSG_CRITICAL, "Can't parse config.xml: %s\n", boincerror(retval));
        exit(1);
    }

    sprintf(path, "%s/upload_private", config.key_dir);
    retval = read_key_file(path, key);
    if (retval) {
        log_messages.printf(MSG_CRITICAL, "can't read key\n");
        exit(1);
    }

    // BOINC-MR
    // read MR jobs file - get info on existing MapReduce jobs, their status and template files

    // BOINC-MR DEBUG
    log_messages.printf(MSG_CRITICAL, "[BOINC-MR] Inside Transitioner-main(). Before mapreduce_jobs.init()\n");
    fflush(stderr);

    // BOINC-MR: parse mr_jobracker.xml file and initialize variables
    // This file only has general info on mr_jobs: existing jobs, their IDs, template files, number of Map and Reduce tasks.
    // No info on transient data: number of available input files; mappers that are serving files, etc...
    /// New file: "mrjobwu#ID.xml"
    mapreduce_jobs.init();

    // BOINC-MR DEBUG
    fprintf(stderr, "[BOINC-MR] After mapreduce_jobs.init()\n");
    fflush(stderr);

    // BOINC-MR DEBUG
    log_messages.printf(MSG_CRITICAL, "[BOINC-MR] Inside transitioner-main(). After init(). Before mapreduce_jobs.parse()\n");
    fflush(stderr);
    if (mapreduce_jobs.parse()){
        log_messages.printf(MSG_CRITICAL, "Error parsing MapReduce jobs file - during mapreduce_jobs.parse()\n");
    }
    log_messages.printf(MSG_CRITICAL, "[BOINC-MR] Inside sched_main-main(). After mapreduce_jobs.parse()\n");
    fflush(stderr);


    /// D_VAL TODO - parse dist_val.xml file and initialize variables for distributed validation
    // This file has information on which applications are validated by clients, and templates required when creating the validator WU that will be sent to
    // validator client

    // D_VAL DEBUG
    log_messages.printf(MSG_CRITICAL, "[D_VAL] Inside Transitioner-main(). Before dist_val_apps.init()\n");
    fflush(stderr);

    dist_val_apps.init();
    // D_VAL DEBUG
    log_messages.printf(MSG_CRITICAL, "[D_VAL] Inside transitioner-main(). After init(). Before dist_val_apps.parse()\n");
    fflush(stderr);
    if (dist_val_apps.parse()){
        log_messages.printf(MSG_CRITICAL, "Error parsing MapReduce jobs file - during dist_val_apps.parse()\n");
    }
    log_messages.printf(MSG_CRITICAL, "[D_VAL] Inside sched_main-main(). After dist_val_apps.parse()\n");
    fflush(stderr);


    log_messages.printf(MSG_NORMAL, "Starting\n");

    install_stop_signal_handler();

    main_loop();
}

const char *BOINC_RCSID_be98c91511 = "$Id: transitioner.cpp 19089 2009-09-18 15:59:40Z davea $";
