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

// Simple validator framework:
// Lets you create a custom validator by supplying three functions.
// See http://boinc.berkeley.edu/trac/wiki/ValidationSimple
//

#include "config.h"
#include <vector>
#include <cstdlib>
#include <string>

#include "boinc_db.h"
#include "error_numbers.h"

#include "sched_config.h"
#include "sched_msgs.h"

// D_VAL
#include <fstream>      // read canonical Result output file, and parse information
#include <sstream>      // istringstream
#include "distributed_validation.h"

#include "validator.h"
#include "validate_util.h"
#include "validate_util2.h"

using std::vector;

/// D_VAL TODO
// similar to is_valid(RESULT& result, WORKUNIT& wu)
// Here when a result has been validated and its granted_credit has been set.
// Grant credit to host, user and team, and update host error rate.
//
//int d_val_is_valid(RESULT& result, WORKUNIT& wu) {
//    DB_HOST host;
//    DB_CREDITED_JOB credited_job;
//    int retval;
//    char buf[256];
//
//    retval = host.lookup_id(result.hostid);
//    if (retval) {
//        log_messages.printf(MSG_CRITICAL,
//            "[RESULT#%d] lookup of host %d failed %d\n",
//            result.id, result.hostid, retval
//        );
//        return retval;
//    }
//
//    grant_credit(host, result.sent_time, result.cpu_time, result.granted_credit);
//
//    double turnaround = result.received_time - result.sent_time;
//    compute_avg_turnaround(host, turnaround);
//
//    double old_error_rate = host.error_rate;
//    if (!is_unreplicated(wu)) {
//        update_error_rate(host, true);
//    }
//    sprintf(
//        buf,
//        "avg_turnaround=%f, error_rate=%f",
//        host.avg_turnaround, host.error_rate
//    );
//    retval = host.update_field(buf);
//    if (retval) {
//        log_messages.printf(MSG_CRITICAL,
//            "[RESULT#%d] update of host %d failed %d\n",
//            result.id, result.hostid, retval
//        );
//    }
//    log_messages.printf(MSG_DEBUG,
//        "[HOST#%d] error rate %f->%f\n",
//        host.id, old_error_rate, host.error_rate
//    );
//
//    if (update_credited_job) {
//        credited_job.userid = host.userid;
//        credited_job.workunitid = long(wu.opaque);
//        retval = credited_job.insert();
//        if (retval) {
//            log_messages.printf(MSG_CRITICAL,
//                "[RESULT#%d] Warning: credited_job insert failed (userid: %d workunit: %f err: %d)\n",
//                result.id, host.userid, wu.opaque, retval
//            );
//        } else {
//            log_messages.printf(MSG_DEBUG,
//                "[RESULT#%d %s] added credited_job record [WU#%d OPAQUE#%f USER#%d]\n",
//                result.id, result.name, wu.id, wu.opaque, host.userid
//            );
//        }
//    }
//
//    return 0;
//}

/// D_VAL TODO
//
// similar to is_invalid()
//
//int d_val_is_invalid(WORKUNIT& wu, RESULT& result) {
//    char buf[256];
//    int retval;
//    DB_HOST host;
//
//    retval = host.lookup_id(result.hostid);
//    if (retval) {
//        log_messages.printf(MSG_CRITICAL,
//            "[RESULT#%d] lookup of host %d failed %d\n",
//            result.id, result.hostid, retval
//        );
//        return retval;
//    }
//    double old_error_rate = host.error_rate;
//    if (!is_unreplicated(wu)) {
//        update_error_rate(host, false);
//    }
//    sprintf(buf, "error_rate=%f", host.error_rate);
//    retval = host.update_field(buf);
//    if (retval) {
//        log_messages.printf(MSG_CRITICAL,
//            "[RESULT#%d] update of host %d failed %d\n",
//            result.id, result.hostid, retval
//        );
//        return retval;
//    }
//    log_messages.printf(MSG_DEBUG,
//        "[HOST#%d] invalid result; error rate %f->%f\n",
//        host.id, old_error_rate, host.error_rate
//    );
//    return 0;
//}

#define CREDIT_EPSILON .001


/// D_VAL TODO
//
// similar to median_mean_credit()
//
// If we have N correct results with nonzero claimed credit,
// compute a canonical credit as follows:
// - if N==0 (all claimed credits are infinitesmal), return CREDIT_EPSILON
// - if N==1, return that credit
// - if N==2, return min
// - if N>2, toss out min and max, return average of rest
//
//double d_val_median_mean_credit(vector<RESULT>& results) {
double d_val_median_mean_credit(std::vector<int> claimed_credits, std::vector<int> valid_states){
    int ilow=-1, ihigh=-1;
    double credit_low = 0, credit_high = 0;
    int nvalid = 0;
    unsigned int i;

    for (i=0; i<claimed_credits.size(); i++) {
        //RESULT& result = results[i];
        if (valid_states[i] != VALIDATE_STATE_VALID) continue;
        if (claimed_credits[i] < CREDIT_EPSILON) continue;
        if (ilow < 0) {
            ilow = ihigh = i;
            credit_low = credit_high = claimed_credits[i];
        } else {
            if (claimed_credits[i] < credit_low) {
                ilow = i;
                credit_low = claimed_credits[i];
            }
            if (claimed_credits[i] > credit_high) {
                ihigh = i;
                credit_high = claimed_credits[i];
            }
        }
        nvalid++;
    }

    switch(nvalid) {
    case 0:
        return CREDIT_EPSILON;
    case 1:
    case 2:
        return credit_low;
    default:
        double sum = 0;
        for (i=0; i<claimed_credits.size(); i++) {
            if (i == (unsigned int) ilow) continue;
            if (i == (unsigned int) ihigh) continue;
            //RESULT& result = results[i];
            if (valid_states[i] != VALIDATE_STATE_VALID) continue;

            sum += claimed_credits[i];
        }
        return sum/(nvalid-2);
    }
}


// There was an error when trying to update information on the results that were being validated by a validator WU
// Treat this as if corresponding results being validated were considered invalid (if their validate_state != VALID)
// Do not change their validate_state if it has already been set to valid.
// @ result: ID for the workunit responsible for validation
// @ validating_wuid: ID for the workunit being validated
int d_val_handle_parse_error_db(int result_wuid, int validating_wuid){

    char query[MAX_QUERY_LEN];
    int retval;

    sprintf(query,
        "update result set validate_state=%d where workunitid=%d and validator_wuid<=%d and validate_state!=%d",
        VALIDATE_STATE_INVALID, validating_wuid, result_wuid, VALIDATE_STATE_VALID
    );
    DB_CONN* db = &boinc_db;
    retval = db->do_query(query);
    if (retval) return retval;
    if (db->affected_rows() != 1) return ERR_DB_NOT_FOUND;

    return 0;
}

// D_VAL
// Parse result ID from DB query
int d_val_parse_res_id(MYSQL_ROW& r){
    int resid;
    resid = atoi(r[0]);
    return resid;
}

// D_VAL
// Parse claimed credit from DB query (RESULT)
int d_val_parse_claimed_cred(MYSQL_ROW& r){
    int claim_cred;
    claim_cred = atoi(r[1]);
    return claim_cred;
}

// D_VAL
// read output file from canonical result for validator workunit. Update vector val_st with values of validate_state found in file
// @ f_info: file to read
// @ num_results: number of results that have returned an output
// @ val_st: vector with validate_state values to update
//
// Example output file:
//    outcome: 6 validate_state: 2
//    outcome: 1 validate_state: 1
//    outcome: 1 validate_state: 1
//    ERROR
//    9761b591ea2bd92ba1e1143936f698fa
//    9761b591ea2bd92ba1e1143936f698fa

int d_val_read_output_file(FILE_INFO f_info, int num_results, std::vector<int>& val_st){
    //MFILE in_file;

    // C implementation
//    FILE* f = fopen(f_info->path.c_str(), "r");
//    if (!f) return ERR_OPEN;
//
//    char line [ 128 ]; /* or other suitable maximum line size */
//    /* read a line */
//    while ( fgets ( line, sizeof line, file ) != NULL ){
//        fputs ( line, stdout ); /* write the line */
//    }
//    fclose ( file );

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "d_val_read_output_file: About to read file: %s\n",
        f_info.path.c_str()
    );

    std::ifstream infile(f_info.path.c_str());
    if (infile.is_open()){

        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG, "d_val_read_output_file: File is_open: true\n");

        std::string line = "";
        std::string outcome_str, val_state_str;
        int outcome, val_state;
        int results_verified = 0;           // check if all results are mentioned/handled inside output file
        //std::istringstream iss (line);
        bool first_line = true;
        while (std::getline(infile, line)){

            // D_VAL DEBUG
            log_messages.printf(MSG_DEBUG, "Read line: %s\n", line.c_str());

            std::istringstream iss(line);

            // ignore first line (corresponds to explanation of file structure and application info)
            if (first_line){
                first_line = false;
                continue;
            }


            // go through all the lines that follow this organization
            if (!(iss >> outcome_str >> outcome >> val_state_str >> val_state)){
                break;
            }

            // process line (outcome: X validate_state: Y)
            // semantic check (outcome: / validate_state:)
            if (outcome_str != "outcome:" || val_state_str != "validate_state:"){
                log_messages.printf(MSG_CRITICAL,
                    "d_val_read_output_file: Semantic error while reading output file from validator Result: %s\nLine read:%s",
                    f_info.path.c_str(), line.c_str()
                );
            }

            // add this validate_state to vector
            val_st.push_back(val_state);

            // increment number of results found so far inside output file
            results_verified++;
        }

        // error if we did not go through all expected results
        if (results_verified < num_results){
            log_messages.printf(MSG_CRITICAL,
                "d_val_read_output_file: Output file from validator Result only refers to %d files, while there we expect a total of %d files.\n",
                results_verified, num_results
            );
            infile.close();
            return DVAL_ERR_MISMATCH_NUM_RESULTS;
        }

        infile.close();
    }
    // could not open file
    else{
        log_messages.printf(MSG_CRITICAL,
            "d_val_read_output_file: Could not open file: %s\n",
            f_info.path.c_str()
        );
        return ERR_OPENDIR;
    }

    return 0;
}


// similar to d_val_read_output_file(FILE_INFO f_info, int num_results, std::vector<int>& val_st)
//
// Read output file for result, and save hash information in hashes, and the total number of results in num_results
//
// @f_info: file to read
// @ num_results: variable to store number of results (number of lines in output file that contain hashes after "outcome: X validate_state: Y" lines)
// @ hashes: vector with hash values found in output file
//
// Example output file:
//    outcome: 6 validate_state: 2
//    outcome: 1 validate_state: 1
//    outcome: 1 validate_state: 1
//    ERROR
//    9761b591ea2bd92ba1e1143936f698fb
//    9761b591ea2bd92ba1e1143936f698fa
//
/// D_VAL TODO: complete this function
///
int d_val_read_hashes_out_file(FILE_INFO f_info, int& num_results, std::vector<std::string>& hashes){
    return -1;
}


// D_VAL
// Check if results have different results due to being sent different files (hashes).
// Compare hashes returned by the mismatched results and the canonical result (results[i])
//
// @ canon_res: canonical result for this workunit
// @ invalid_results: group of results that were considered invalid (different from canonical result)
//
int d_val_check_hashes(RESULT canon_res, std::vector<RESULT> invalid_results){
    vector<FILE_INFO> files;

    int retval = get_output_file_infos(canon_res, files);
    if (retval) {
        log_messages.printf(MSG_CRITICAL,
            "[RESULT#%d %s] d_val_check_hashes: can't get output filenames\n",
            canon_res.id, canon_res.name
        );
        return retval;
    }

    // read information on validate_state for canonical result
    std::vector<std::string> canon_hashes;         // vector with ordered list of validate_state found inside this result's output file
    int num_results = -1;

    // read output file, and update validate_state vector val_states
    retval = d_val_read_hashes_out_file(files[0], num_results, canon_hashes);
    // if there's an error, we are unable to compare this to invalid results - report it
    if (retval){
        log_messages.printf(MSG_CRITICAL,
            "d_val_check_hashes: Error while reading output file [%s] from canonical result: %s\n",
            files[0].path.c_str(), canon_res.name
        );
        return retval;
    } else if(num_results < 0){
        log_messages.printf(MSG_CRITICAL,
            "d_val_check_hashes: Error while reading hashes from output file [%s] of canonical result: %s\n",
            files[0].path.c_str(), canon_res.name
        );
        return retval;
    }

    // go through each result, read their hash information and compare it to canonical result.
    std::vector<std::string> cur_res_hashes;
    int j;
    for (int i=0; i<invalid_results.size(); i++){
        num_results = -1;
        cur_res_hashes.clear();
        retval = d_val_read_hashes_out_file(files[0], num_results, cur_res_hashes);
        if (retval){
            log_messages.printf(MSG_CRITICAL,
                "d_val_check_hashes: Error while reading output file [%s] from invalid result: %s\n",
                files[0].path.c_str(), invalid_results[i].name
            );
            continue;
        } else if(num_results < 0){
            log_messages.printf(MSG_CRITICAL,
                "d_val_check_hashes: Error while reading hashes from output file [%s] of invalid result: %s\n",
                files[0].path.c_str(), invalid_results[i].name
            );
            continue;
        }

        // sanity check - same number of hashes?
        if (canon_hashes.size() != cur_res_hashes.size()){
            log_messages.printf(MSG_CRITICAL,
                "d_val_check_hashes: Error: Invalid result [#%d] has different number of hashes from canonical result [#%d]. Invalid result: %zu | Canonical result: %zu\n",
                invalid_results[i].id, canon_res.id, cur_res_hashes.size(), canon_hashes.size()
            );
            continue;
        }

        // compare hashes
        for (j=0; j<canon_hashes.size(); j++){
            // if different, save information in correct folder, with filename = invalid result ID
            if (canon_hashes[j] != cur_res_hashes[j]){
                /// D_VAL TODO: save result output file in correct folder
                // save_incorrect_hash_file(canon_result, invalid_results[i], canon_hashes, cur_res_hashes);
                break;
            }
        }

    }




}

// D_VAL
// Update information on all the results that this validator WU has handled.
// check which results were considered valid, and update their validate_state in the DB
//
// @ result: canonical result (used to identify which results have been validated by it - through result.validator_wuid <= result_wuid)
// @ validating_wuid: ID of workunit that was validated/handled by this result (results returned must match this WU ID)
// @ res_wuid: Workunit ID for this canonical result
int d_val_parse_canon_result(RESULT result, int validating_wuid, int res_wuid){

    // D_VAL DEBUG
    log_messages.printf(MSG_CRITICAL,
        "[RESULT#%d %s] Entered d_val_parse_canon_result. Result.workunitid: %d | Validating_wuid: %d\n",
        result.id, result.name, res_wuid, validating_wuid
    );

    //std::vector<int> validate_states;
    //std::vector<std::string> hashes;

    vector<FILE_INFO> files;

    int retval = get_output_file_infos(result, files);
    if (retval) {
        log_messages.printf(MSG_CRITICAL,
            "[RESULT#%d %s] d_val_parse_canon_result: can't get output filenames\n",
            result.id, result.name
        );
        return retval;
    }


    // get ID of all results which were handled by this result's WU
    // important detail: order these results by their id (ascending order) => same order with which they were referred to in the workunit template (enumerate_ip_hosts)
    char query[MAX_QUERY_LEN];
    DB_CONN* db = &boinc_db;
    MYSQL_RES* rp;
    MYSQL_ROW row;
    int res_id;
    int claimed_cred;
    std::vector<int> res_ids;
    std::vector<bool>invalid_credit;        // list corresponding to each result - true if the result claimed more than max_claimed_credit
    std::vector<int>claimed_credits;        // array of claimed credit by each result (required to calculate mean = credit to assign all valid results)


    // we have to get info on all results that are being validated by this result, through the field validator_wuid (has to match or be lower than this result's wuid)
    // - lower validator_wuid are considered since there each result may be validated by more than one workunit - the value of validator_wuid is only set if it is 0
    // must also obtain the claimed_credit value, to check if it is over the max_claimed_credit value;
    sprintf(query,
        "SELECT "
        "   id, claimed_credit "
        "FROM "
        "   result "
        "WHERE "
        "   result.workunitid=%d and result.validator_wuid<=%d AND"
        " result.outcome=%d "                                                             // ignore results whose outcome was not successfull (e.g. - outcome = 3 [RESULT_OUTCOME_CLIENT_ERROR])
        "ORDER BY "
        "   result.id asc",
        validating_wuid, res_wuid, RESULT_OUTCOME_SUCCESS
    );

    retval = db->do_query(query);
    if (retval) return retval;

    // the following stores the entire result set in memory
    //
    rp = mysql_store_result(db->mysql);
    if (!rp) return mysql_errno(db->mysql);

    do {
        row = mysql_fetch_row(rp);
        if (!row) {
            mysql_free_result(rp);
        } else {
            // save address
            //strcpy(ip_address, mr_parse_ip_addr(row));
            res_id = d_val_parse_res_id(row);
            claimed_cred = d_val_parse_claimed_cred(row);
            res_ids.push_back(res_id);
            claimed_credits.push_back(claimed_cred);
            if (max_claimed_credit && claimed_cred > max_claimed_credit) {
                invalid_credit.push_back(true);
            }
            else
                invalid_credit.push_back(false);
        }
    } while (row);

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "d_val_parse_canon_result: Found [%zu] results from this workunit [#%d], that were validated by wu [#%d].\n",
        res_ids.size(), validating_wuid, res_wuid
    );



    // read information on validate_state for all results (number of results = IDs returned by db_query) validated
    int num_results = res_ids.size();
    std::vector<int> val_states;         // vector with ordered list of validate_state found inside this result's output file

    // read output file from canonical result, and update validate_state vector val_states
    retval = d_val_read_output_file(files[0], num_results, val_states);
    if (retval){
        log_messages.printf(MSG_CRITICAL,
            "d_val_parse_canon_result: Error while reading output file [%s] from result: %s\n",
            files[0].path.c_str(), result.name
        );
        return retval;
    }

    /// D_VAL TODO: update credit to assign RESULTs
    double credit = d_val_median_mean_credit(claimed_credits, val_states);

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "d_val_parse_canon_result: Median credit: %f\n",
        credit
    );


    /// D_VAL TODO: uncomment and finish credit assignment
    // check if it is under maximum defined value
//    if (max_granted_credit && credit>max_granted_credit) {
//        credit = max_granted_credit;
//    }
//
//    double granted_credit = 0;
//    // set canonical_result_id to ID of first result which is valid
//    int canonicalid = -1;
//    for (int k=0; k<res_ids.size(); k++){
//        if (val_states[k] == VALIDATE_STATE_VALID){
//            if (canonicalid == -1)
//                canonicalid = res_ids[k];
//
//            // grant credit for valid results
//            //
//            //update_result = true;
//            granted_credit = grant_claimed_credit ? claimed_credits[k] : credit;
//            if (max_granted_credit && granted_credit > max_granted_credit) {
//                granted_credit = max_granted_credit;
//            }
//
//            /// D_VAL TODO
//            retval = -1;
//            //retval = d_val_is_valid(result, wu);
//            if (retval) {
//                log_messages.printf(MSG_DEBUG,
//                    "[RESULT#%d] is_valid() failed: %d\n",
//                    res_ids[k], retval
//                );
//            }
//            log_messages.printf(MSG_NORMAL,
//                "[RESULT#%d] Valid; granted %f credit\n",
//                res_ids[k], granted_credit
//            );
//        } // valid results
//        else if(val_states[k] == VALIDATE_STATE_INVALID){
//            log_messages.printf(MSG_NORMAL,
//                "[RESULT#%d] Invalid\n",
//                res_ids[k]
//            );
//            /// D_VAL TODO
//            //d_val_is_invalid(wu, result);
//        }
//    }



    /// D_VAL TODO: also update RESULT.granted_credit
    // update different validate_state values in same query
    std::stringstream queryss;
    //queryss.str("UPDATE result SET validate_state = CASE id "); // if we use this, the following "<<" operation replaces the original string (points to beginning of stringstream)

    queryss << "UPDATE result SET validate_state = CASE id ";

    // modify query according to val_states' values
    int i;
    int canonicalid = -1;
    for (i=0; i<val_states.size(); i++){
        // update validate_state according to claimed credit values (invalid_credit == true?)
        if (invalid_credit[i])
            val_states[i] = VALIDATE_STATE_INVALID;

        // save canonicalid, if there are valid results
        if (val_states[i] == VALIDATE_STATE_VALID){
            if (canonicalid == -1)
                canonicalid = res_ids[i];
        }

        queryss << " WHEN " << res_ids[i] << " THEN " << val_states[i];

        //cur_file_info << "<file_info>\n  <number>" << fnumber <<"</number>\n"
    }

    // D_VAL
    // also add the user_hosting = 1, for all valid results (0 otherwise)
//    UPDATE tablename SET
//    col1 = CASE name WHEN 'name1' THEN 5 WHEN 'name2' THEN 3 ELSE 0 END,
//    col2 = CASE name WHEN 'name1' THEN '' WHEN 'name2' THEN 'whatever' ELSE '' END;
    queryss << " END, user_hosting = CASE id ";
    for (i=0; i<val_states.size(); i++){
        // update validate_state according to claimed credit values (invalid_credit == true?)
        if (invalid_credit[i])
            queryss << " WHEN " << res_ids[i] << " THEN 0";

        // save canonicalid, if there are valid results
        if (val_states[i] == VALIDATE_STATE_VALID)
            queryss << " WHEN " << res_ids[i] << " THEN 1";
    }

    // limit query to the required IDs separated by commas (e.g., "(1,2,5,6)")
    std::stringstream ids;
    //ids.str("("); // if we use this, the following "<<" operation replaces the original string (points to beginning of stringstream)

    ids << "(";

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "IDs (first) list: %s\n",
        ids.str().c_str()
    );

    for (i=0; i<res_ids.size(); i++){
        ids << res_ids[i];
        // add comma "," up until last value
        if (i != (res_ids.size()-1)){
            ids << ",";
        }

        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG,
            "IDs (current) list: %s\n",
            ids.str().c_str()
        );

    }
    ids << ")";
    queryss << " END WHERE id IN " << ids.str();

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "IDs (final) list: %s\n",
        ids.str().c_str()
    );

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG,
        "d_val_parse_canon_result: Updating d_val results' val_state through query: %s\n",
        queryss.str().c_str()
    );

    retval = db->do_query(queryss.str().c_str());
    if (retval){
        log_messages.printf(MSG_CRITICAL,
            "d_val_parse_canon_result: Error updating d_val results' val_state through query: %s\n",
            queryss.str().c_str()
        );
        return retval;
    }

    // update validated workunit's canonical_resultid, if it was found
    if (canonicalid >= 0){
        DB_WORKUNIT db_wu;
        char buf[256];

        db_wu.id = validating_wuid;
        sprintf(buf, "canonical_resultid=%d", canonicalid);

        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG,
            "d_val_parse_canon_result. About to update workunit [#%d] canonical_resultid to %d\n",
            validating_wuid, canonicalid
        );

        retval = db_wu.update_field(buf);
        if (retval) {
            log_messages.printf(MSG_CRITICAL,
                "[WORKTUNIT#%d] update of workunit's canonical_resultid to %d failed %d\n",
                validating_wuid, canonicalid, retval
            );
        }
    }


    //if (canonicalid) {
    /// D_VAL TODO: trigger assimilator for validated WU/Results
    /// save server_state - needed to check if there are any unsent results (server_state == RESULT_SERVER_STATE_UNSENT)
    // taken from validator.cpp [454]
    // since we found a canonical result,
    // trigger the assimilator, but do NOT trigger
    // the transitioner - doing so creates a race condition
    //
//    transition_time = NEVER;
//    log_messages.printf(MSG_DEBUG,
//        "[WU#%d] Found a canonical result: id=%d\n",
//        wuid, wu.name, canonicalid
//    );
//    wu.canonical_resultid = canonicalid;
//    wu.canonical_credit = credit;
//    wu.assimilate_state = ASSIMILATE_READY;
//
//    // If found a canonical result, don't send any unsent results
//    //
//    for (i=0; i<items.size(); i++) {
//        RESULT& result = items[i].res;
//
//        if (result.server_state != RESULT_SERVER_STATE_UNSENT) {
//            continue;
//        }
//
//        result.server_state = RESULT_SERVER_STATE_OVER;
//        result.outcome = RESULT_OUTCOME_DIDNT_NEED;
//        retval = validator.update_result(result);
//        if (retval) {
//            log_messages.printf(MSG_CRITICAL,
//                "[RESULT#%d %s] result.update() failed: %d\n",
//                result.id, result.name, retval
//            );
//        }
//    }




    return 0;
}

//// Since D_VAL WU with ID wuid has been successfully validated by clients, check if this D_VAL_APP wants its output files to be sent back to server
//// Since this is the validator, we must ask the scheduler to communicate with hosts. Therefore, change the req_out_file_state field in the DB to
//// D_VAL_REQUEST_FILE.
////
//// @ wuid: ID of workunit to request files from (which WORKUNIT to change in DB)
////
//int d_val_request_output_files(int wuid){
//    char query[MAX_QUERY_LEN];
//    DB_CONN* db = &boinc_db;
//    int retval;
//    char app_name[256];
//
////    char buf2[256];
////    DB_WORKUNIT wu;
////    wu.id = result.workunitid;
////    sprintf(buf2, "req_out_file_state=%d", D_VAL_REQUEST_FILE);
////    wu.update_field(buf2);
//
//    // look in DB for app corresponding to this wuid
//    sprintf(query,
//            "SELECT app.name from workunit, app "
//            "WHERE workunit.id=%d AND "
//            "workunit.appid = app.id",
//            D_VAL_REQUEST_FILE, wuid);
//
//    // D_VAL DEBUG
//    log_messages.printf(MSG_DEBUG,
//        "d_val_request_output_files: Looking for app that corresponds to WU [#%d] with query: %s\n",
//        wuid, query
//    );
//
//    retval = db->do_query(query);
//    if (retval){
//        log_messages.printf(MSG_CRITICAL,
//            "d_val_request_output_files: Error updating Workunit's req_out_file_state through query: %s\n",
//            query
//        );
//        return retval;
//    }
//
//    // the following stores the entire result set in memory
//    //
//    MYSQL_RES* rp;
//    MYSQL_ROW row;
//    rp = mysql_store_result(db->mysql);
//    if (!rp) return mysql_errno(db->mysql);
//
//    do {
//        row = mysql_fetch_row(rp);
//        if (!row) {
//            mysql_free_result(rp);
//        } else {
//            // save app_name
//            strncpy(app_name, row[0], sizeof(app_name));
//        }
//    } while (row);
//
//
//
//    // go through list of D_VAL_APPs and see if this one has requested that its results be returned to server after validation (must_return_results())
//    D_VAL_APP* dval_temp;
//    bool return_result = false;
//    int i;
//    for (i=0; i<dist_val_apps.dval_apps.size(); i++){
//        dval_temp = dist_val_apps.dval_apps[i];
//        // check
//        if (strcmp(dval_temp->get_d_val_app_name(), app_name) == 0){
//            return_result = dval_temp->must_return_result();
//            break;
//        }
//    }
//
//    if (!return_result){
//        // D_VAL DEBUG
//        log_messages.printf(MSG_DEBUG,
//            "d_val_request_output_files: Application [%s] does not want its results to be returned to server.\n",
//            app_name
//        );
//        return 0;
//    }
//
//    // results need to be returned, so update DB, in order to let scheduler know that hosts must be asked to return files
//    sprintf(query, "UPDATE workunit SET req_out_file_state = %d WHERE id=%d",
//             D_VAL_REQUEST_FILE, wuid);
//
//    // D_VAL DEBUG
//    log_messages.printf(MSG_DEBUG,
//        "d_val_request_output_files: Updating d_val Workunit's req_out_file_state through query: %s\n",
//        query
//    );
//
//    retval = db->do_query(query);
//    if (retval){
//        log_messages.printf(MSG_CRITICAL,
//            "d_val_request_output_files: Error updating Workunit's req_out_file_state through query: %s\n",
//            query
//        );
//        return retval;
//    }
//}


// Given a set of results, check for a canonical result,
// i.e. a set of at least min_quorum/2+1 results for which
// that are equivalent according to check_pair().
//
// invariants:
// results.size() >= wu.min_quorum
// for each result:
//   result.outcome == SUCCESS
//   result.validate_state == INIT
//
int check_set(
    vector<RESULT>& results, WORKUNIT& wu,
    int& canonicalid, double& credit, bool& retry
) {
    vector<void*> data;
    vector<bool> had_error;
    int i, j, neq = 0, n, retval;
    int min_valid = wu.min_quorum/2+1;

    retry = false;
    n = results.size();
    data.resize(n);
    had_error.resize(n);

    // Initialize results

    for (i=0; i<n; i++) {
        data[i] = NULL;
        had_error[i] = false;
    }
    int good_results = 0;
    for (i=0; i<n; i++) {
        retval = init_result(results[i], data[i]);
        if (retval == ERR_OPENDIR) {
            log_messages.printf(MSG_CRITICAL,
                "check_set: init_result([RESULT#%d %s]) transient failure\n",
                results[i].id, results[i].name
            );
            had_error[i] = true;
        } else if (retval) {
            log_messages.printf(MSG_CRITICAL,
                "check_set: init_result([RESULT#%d %s]) failed: %d\n",
                results[i].id, results[i].name, retval
            );
            results[i].outcome = RESULT_OUTCOME_VALIDATE_ERROR;
            results[i].validate_state = VALIDATE_STATE_INVALID;
            had_error[i] = true;
        } else  {
            good_results++;
        }
    }
    if (good_results < wu.min_quorum) goto cleanup;

    // Compare results

    for (i=0; i<n; i++) {
        if (had_error[i]) continue;
        vector<bool> matches;
        matches.resize(n);
        neq = 0;
        for (j=0; j!=n; j++) {
            if (had_error[j]) continue;
            bool match = false;
            if (i == j) {
                ++neq;
                matches[j] = true;
            } else if (compare_results(results[i], data[i], results[j], data[j], match)) {
                log_messages.printf(MSG_CRITICAL,
                    "generic_check_set: check_pair_with_data([RESULT#%d %s], [RESULT#%d %s]) failed\n",
                    results[i].id, results[i].name, results[j].id, results[j].name
                );
            } else if (match) {
                ++neq;
                matches[j] = true;
            }
        }
        if (neq >= min_valid) {

            // set validate state for each result
            //
            // D_VAL: group all results that do not match canonical result, and check if they have different hashes
            std::vector<RESULT> mismatch_results;
            for (j=0; j!=n; j++) {
                if (had_error[j]) continue;
                if (max_claimed_credit && results[j].claimed_credit > max_claimed_credit) {
                    results[j].validate_state = VALIDATE_STATE_INVALID;
                } else {
                    results[j].validate_state = matches[j] ? VALIDATE_STATE_VALID : VALIDATE_STATE_INVALID;
                }
                // D_VAL: check if all validator clients received the same input files (compare hashes)
                if (!matches[j] && wu.validating_wuid >= 0){
                    mismatch_results.push_back(results[i]);
                }
            }

            // D_VAL: check the hashes of mistmatched results
            /// D_VAL TODO: if this result does not match canonical result, check if it received different files:
            if (mismatch_results.size() > 0 && wu.validating_wuid >= 0){
                // D_VAL DEBUG
                log_messages.printf(MSG_DEBUG,
                    "check_set: About to check hashes of %zu mismatching results.\n",
                    mismatch_results.size()
                );
                d_val_check_hashes(results[i], mismatch_results);
            }


            // D_VAL
            // If this is a validator workunit (validates output of previous WU), check which results were considered valid
            if (wu.validating_wuid >= 0){

                // D_VAL DEBUG
                log_messages.printf(MSG_DEBUG,
                    "check_set: Workunit [#%d %s] has validating_wuid >= 0 : %d\n",
                    wu.id, wu.name, wu.validating_wuid
                );

                //retval = 0;
                // check which results were considered valid, and update their validate_state in the DB
                //ŕetval = d_val_parse_canon_result(results[i], wu.validating_wuid);
                d_val_parse_canon_result(results[i], wu.validating_wuid, wu.id);

                if (retval) {
                    log_messages.printf(MSG_CRITICAL,
                        "check_set: d_val_parse_canon_result([RESULT#%d %s]) failed: %d\n",
                        results[i].id, results[i].name, retval
                    );

                    // if there was an error, treat this as if corresponding results being validated were considered invalid (if their validate_state != VALID)
                    retval = d_val_handle_parse_error_db(results[i].workunitid, wu.validating_wuid);
                    if (retval) {
                        log_messages.printf(MSG_CRITICAL,
                            "check_set: d_val_parse_canon_result([RESULT#%d %s]) failed: %d\n",
                            results[i].id, results[i].name, retval
                        );
                    }
                }
                /// DEPRECATED - this is done by transitioner now (only one with access to dist_val_apps)
//                else{
//                    // If there were no errors, request output files to be sent back to server (if this D_VAL_APP has requested it)
//                    // If so, change req_out_file_state to D_VAL_REQUEST_FILE;
//                    retval = d_val_request_output_files(wu.validating_wuid);
//                    if (retval) {
//                        log_messages.printf(MSG_CRITICAL,
//                            "check_set: d_val_request_output_files([RESULT#%d %s]) failed: %d\n",
//                            results[i].id, results[i].name, retval
//                        );
//                    }
//                }

            }

            canonicalid = results[i].id;
            credit = compute_granted_credit(wu, results);

            // update transition_time (set to now) to make transitioner handle the WU that was just validated
            char buf[256];
            DB_WORKUNIT validating_wu;
            validating_wu.id = wu.validating_wuid;
            sprintf(
                buf, "transition_time=%d",
                (int)time(0)
            );
            retval = validating_wu.update_field(buf);
            if (retval) {
                log_messages.printf(MSG_CRITICAL,
                    "[WU #%d] update failed: %d\n", validating_wu.id, retval
                );
            }
            break;
        }
    }

cleanup:

    for (i=0; i<n; i++) {
        cleanup_result(results[i], data[i]);
    }
    return 0;
}

// r1 is the new result; r2 is canonical result
//
void check_pair(RESULT& r1, RESULT& r2, bool& retry) {
    void* data1;
    void* data2;
    int retval;
    bool match;

    retry = false;
    retval = init_result(r1, data1);
    if (retval == ERR_OPENDIR) {
        log_messages.printf(MSG_CRITICAL,
            "check_pair: init_result([RESULT#%d %s]) transient failure 1\n",
            r1.id, r1.name
        );
        retry = true;
        return;
    } else if (retval) {
        log_messages.printf(MSG_CRITICAL,
            "check_pair: init_result([RESULT#%d %s]) perm failure 1\n",
            r1.id, r1.name
        );
        r1.outcome = RESULT_OUTCOME_VALIDATE_ERROR;
        r1.validate_state = VALIDATE_STATE_INVALID;
        return;
    }

    retval = init_result(r2, data2);
    if (retval == ERR_OPENDIR) {
        log_messages.printf(MSG_CRITICAL,
            "check_pair: init_result([RESULT#%d %s]) transient failure 2\n",
            r2.id, r2.name
        );
        cleanup_result(r1, data1);
        retry = true;
        return;
    } else if (retval) {
        log_messages.printf(MSG_CRITICAL,
            "check_pair: init_result([RESULT#%d %s]) perm failure2\n",
            r2.id, r2.name
        );
        cleanup_result(r1, data1);
        r1.outcome = RESULT_OUTCOME_VALIDATE_ERROR;
        r1.validate_state = VALIDATE_STATE_INVALID;
        return;
    }

    retval = compare_results(r1, data1, r2, data2, match);
    if (max_claimed_credit && r1.claimed_credit > max_claimed_credit) {
        r1.validate_state = VALIDATE_STATE_INVALID;
    } else {
        r1.validate_state = match?VALIDATE_STATE_VALID:VALIDATE_STATE_INVALID;
    }
    cleanup_result(r1, data1);
    cleanup_result(r2, data2);
}
