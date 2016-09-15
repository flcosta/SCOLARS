/**
* Distributed Validation class - server side
* Used by scheduler to handle distributed validation workunits, create unique alternative output file names
*
* Version info:
* -
*
* v 0.1 - Sep 2013 - Fernando Costa
*/



#include <string>
#include <stdio.h>              // strncpy
#include <sstream>              // sstream
//#include <string.h>
#include "miofile.h"        // parse()

#include "error_numbers.h"      // ERR_XML_PARSE
#include "str_util.h"
#include "boinc_db.h"           // BLOB SIZE
#include "sched_msgs.h"     // log_messages
#include "sched_config.h"   // SCHED_CONFIG config (e.g. used to copy get project path)
#include "backend_lib.h"    // read_filename
#include "parse.h"          // match_tag()
#include "str_replace.h"    // strcpy2() / strlcpy

#include "distributed_validation.h"

//using std::stringstream;

// Initialize values
void D_VAL_SET::init(char* fname){
    if (fname == NULL){
        strcpy(d_val_fname, D_VAL_FILENAME);
    }
    else{
        strcpy(d_val_fname, fname);
    }
}


// return application (D_VAL_APP), based on workunit's name [e.g.: app_name_WU_ID - read up to before last '_' OR app_name_WUID - read up to last '_']
// @wu_name: name of workunit for required application
D_VAL_APP* D_VAL_SET::get_dv_app_wu(const char* wu_name){
    const char* end_name;
    char app_name[256], alt_app_name[256];      // account for one or two '_' chars separating app name

    end_name = strrchr(wu_name, '_');
    if (!end_name){
        log_messages.printf(MSG_CRITICAL, "get_dv_app_wu. Error: Workunit name was not standard (did not contain '_' character): %s\n", wu_name);
        return NULL;
    }
    //end_name --;

    // D_VAL DEBUG
    log_messages.printf(MSG_NORMAL,
        "[Inside get_dv_app_wu] After finding end_name: %s\n", end_name
    );

    size_t length = end_name - wu_name;
    strncpy(app_name, wu_name, length);
    app_name[length] = '\0';

    // D_VAL DEBUG
    log_messages.printf(MSG_NORMAL,
        "[Inside get_dv_app_wu] App name 1st try (up to last '_'): %s | length (size): %zu VS. length (variable): %zu\n", app_name, strlen(app_name), length
    );

    end_name = strrchr(app_name, '_');
    if (!end_name){
        log_messages.printf(MSG_CRITICAL, "get_dv_app_wu. Workunit name only contained one '_' character: %s\n", wu_name);
        //return NULL;
        strcpy(alt_app_name, " ");
    }
    else{
        length = end_name - app_name;
        strncpy(alt_app_name, app_name, length);
        alt_app_name[length] = '\0';
        //end_name = '\0';
    }

    // D_VAL DEBUG
    log_messages.printf(MSG_NORMAL,
        "[Inside get_dv_app_wu] Calculated alternative app name (up to 2nd '_' char): %s\n", alt_app_name
    );

    // D_VAL DEBUG
    log_messages.printf(MSG_NORMAL, "get_dv_app_wu: Original WU name: %s | Processed app name: %s\n", wu_name, app_name);

    D_VAL_APP* dvapp;
    for (int i=0; i<dval_apps.size(); i++){
        dvapp = dval_apps[i];
        if (strcmp(dvapp->get_app_name_wu_to_validate(), app_name) == 0){

            // D_VAL DEBUG
            log_messages.printf(MSG_NORMAL, "get_dv_app_wu: Found app name in dist_val.xml list: %s\n", app_name);

            return dvapp;
        }
        else if (strcmp(dvapp->get_app_name_wu_to_validate(), alt_app_name) == 0){

            // D_VAL DEBUG
            log_messages.printf(MSG_NORMAL, "get_dv_app_wu: Found app name in dist_val.xml list: %s\n", alt_app_name);

            return dvapp;
        }
    }

    log_messages.printf(MSG_CRITICAL, "get_dv_app_wu. Error: Did not find this app in the dist_val.xml file: %s\n", app_name);
    return NULL;

}



// return application (D_VAL_APP), based on application's name
// @app_name: name of required application
D_VAL_APP* D_VAL_SET::get_dv_app_name(const char* app_name){

    // D_VAL DEBUG
    log_messages.printf(MSG_NORMAL, "get_dv_app_name: App name: %s\n", app_name);

    D_VAL_APP* dvapp;
    for (int i=0; i<dval_apps.size(); i++){
        dvapp = dval_apps[i];
        if (strcmp(dvapp->get_app_name_wu_to_validate(), app_name) == 0){

            // D_VAL DEBUG
            log_messages.printf(MSG_NORMAL, "get_dv_app_name: Found app name in dist_val.xml list: %s\n", app_name);

            return dvapp;
        }
    }

    log_messages.printf(MSG_CRITICAL, "get_dv_app_name. Error: Did not find this app in the dist_val.xml file: %s\n", app_name);
    return NULL;

}

D_VAL_APP::D_VAL_APP(){
    app_name_wu_to_validate[0] = '\0';
    d_val_app_name[0] = '\0';
    d_val_re_template[0] = '\0';
    d_val_wu_template[0] = '\0';
    min_quorum = 0;
    num_wu_outputs = 0;
    hashed = false;
    return_result = false;
    dval_max_nbytes = 0;
}


/// D_VAL TODO
// parse initial data from distributed validation apps info file (default: dist_val.xml) - similar to MAPREDUCE_JOB_SET::parse()
// Default file name in distributed_validation.h
int D_VAL_SET::parse(){
    const char *proj_dir;

    // D_VAL DEBUG
    fprintf(stderr, "Inside D_VAL_SET::parse()\n");
    fflush(stderr);

    proj_dir = config.project_path(d_val_fname);

    // D_VAL DEBUG
    fprintf(stderr, "Project Path for file %s: %s\n", d_val_fname, proj_dir);
    fflush(stderr);

    FILE* f = fopen(proj_dir, "r");
    // If file does not exist (not necessary for scheduler execution - can be created at new MR jobs execution
    if (!f){
        log_messages.printf(MSG_DEBUG, "D_VAL_SET::parse(): Distributed validation file does not exist in project directory: %s \n", d_val_fname);
        return ERR_FOPEN;
    }

    MIOFILE mf;
    mf.init_file(f);
    D_VAL_APP* dval_app;
    char buf[256];
    int retval;
    while (fgets(buf, 256, f)) {
        if (match_tag(buf, "<distributed_validation_set>")) continue;
        if (match_tag(buf, "</distributed_validation_set>")){
            fclose(f);
            return 0;
        }
        if (match_tag(buf, "<dist_val_app>")){
            dval_app = new D_VAL_APP;
            retval = dval_app->parse(mf);

            // D_VAL DEBUG
            log_messages.printf(MSG_DEBUG,
                "d_val_set::parse. Adding a new D_VAL_APP to dval_apps [validating app: %s | validator app: %s]. Retval: %d\n",
                dval_app->get_app_name_wu_to_validate(), dval_app->get_d_val_app_name(), retval
            );

            if (!retval) dval_apps.push_back(dval_app);
            else delete dval_app;
            continue;
        }

        log_messages.printf(MSG_DEBUG, "[unparsed_xml] D_VAL_JOB_SET::parse(): unrecognized %s\n", buf);

    }
    return ERR_XML_PARSE;
}


int D_VAL_APP::parse(MIOFILE &in){
    //std::string tempstr;
    char buf[256];
    //int retval;

    while (in.fgets(buf, 256)) {
        if (match_tag(buf, "</dist_val_app>")){
            // check if all the required fields have been filled
            // d_val_wu_template; d_val_re_template; d_val_app_name; app_name_wu_to_validate
            if (d_val_wu_template == NULL || d_val_wu_template[0] == '\0' || d_val_app_name == NULL || d_val_app_name[0] == '\0' ||
                d_val_re_template == NULL || d_val_re_template[0] == '\0' || app_name_wu_to_validate == NULL || app_name_wu_to_validate[0] == '\0'){
                log_messages.printf(MSG_CRITICAL,
                    "D_VAL_APP::parse() : Incorrect or missing value in dist_val.xml [e.g.: missing Workunit template file location].");
                return ERR_XML_PARSE;
            }
            else if (num_wu_outputs <= 0){
                log_messages.printf(MSG_CRITICAL,
                    "D_VAL_APP::parse() : Incorrect or missing <num_wu_outputs> in dist_val.xml [number of outputs <= 0].");
                return ERR_XML_PARSE;
            }
            else if (min_quorum <= 0){
                log_messages.printf(MSG_CRITICAL,
                    "D_VAL_APP::parse() : Incorrect or missing <min_quorum> in dist_val.xml [minimum quorum <= 0].");
                return ERR_XML_PARSE;
            }
            else if(dval_max_nbytes < 0){
                log_messages.printf(MSG_CRITICAL,
                    "D_VAL_APP::parse() : Incorrect <dval_max_nbytes> in dist_val.xml. Setting it to 0.");
                dval_max_nbytes = 0;
            }

            // D_VAL DEBUG
            // print all found values
            log_messages.printf(MSG_NORMAL,
                    "D_VAL_APP::parse(): Found values. Wu template: %s | Re template: %s | App name: %s | App to validate: %s.",
                    d_val_wu_template, d_val_re_template, d_val_app_name, app_name_wu_to_validate);

            return 0;
        }
        if(parse_str(buf, "<wu_template>", d_val_wu_template, sizeof(d_val_wu_template))) continue;
        if(parse_str(buf, "<re_template>", d_val_re_template, sizeof(d_val_re_template))) continue;
        if(parse_str(buf, "<wu_app_to_validate>", app_name_wu_to_validate, sizeof(app_name_wu_to_validate))) continue;
        if(parse_str(buf, "<dist_val_app_name>", d_val_app_name, sizeof(d_val_app_name))) continue;
        if(parse_int(buf, "<num_wu_outputs>", num_wu_outputs)) continue;
        if(parse_int(buf, "<min_quorum>", min_quorum)) continue;
        if(parse_bool(buf, "hashed", hashed)) continue;
        // if <return_result/> is set, the output files from this application are asked to be returned after its validation
        if (parse_bool(buf, "return_result", return_result)) continue;
        if (match_tag(buf, "<return_result/>")){
            return_result = true;
            continue;
        }

        if (parse_double(buf, "<dval_max_nbytes>", dval_max_nbytes)){

            // D_VAL DEBUG
            log_messages.printf(MSG_NORMAL,
                    "D_VAL_APP::parse. Found dval_max_nbytes: %f.", dval_max_nbytes);

            continue;
        }




        log_messages.printf(MSG_DEBUG, "[unparsed_xml] D_VAL_SET::parse(): unrecognized %s\n", buf);

    }
	return ERR_XML_PARSE;
}


/// D_VAL TODO: Change this to match returning rows to given result IDs (vector sent through create_dist_val_wu)
//same as enumerate_ip_mappers
// get IP address for all hosts that have returned a valid result for this WU (part of WU that has new successful result and VALIDATE_STATE_INIT)
// @ wuid: ID of workunit to check for
// @ val_states: validate_state for each result returned
int enumerate_ip_hosts(std::vector<std::string>& ip_hosts, std::vector<int>& val_states, std::vector<int> res_ids, int wuid){
//int enumerate_ip_hosts(std::vector<std::string>& ip_hosts, std::vector<int>& val_states, int wuid){
    char query[MAX_QUERY_LEN];
    std::string ip_address;   // string with host's IP address
    int retval;
    int val_state;          // validate_state value returned from DB
    //int resid;              // result ID for each row returned from DB [DEPRECATED]
    unsigned int i;
    MYSQL_RES* rp;
    MYSQL_ROW row;
    //SCHED_RESULT_ITEM ri;
    DB_CONN* db = &boinc_db;

    // fill string with all result ids (res_ids). E.g.: (X, Y, Z)
    std::stringstream ss_resids;
    ss_resids << "(";
    for (i=0; i<res_ids.size(); i++){
        ss_resids << res_ids[i];
        // add comma "," up until last value
        if (i != (res_ids.size()-1)){
            ss_resids << ",";
        }

        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG,
            "Res_IDs (current) list: %s\n",
            ss_resids.str().c_str()
        );

    }
    ss_resids << ")";

    sprintf(query,
        "SELECT "
        //"   external_ip_addr, validate_state, result.id "
        "   external_ip_addr, validate_state "
        "FROM "
        "   result, host "
        "WHERE "
        "   host.id=result.hostid and result.workunitid=%d "
        "   and result.id IN %s"
        "   and result.outcome=%d "
        //"   and mr_app_type=1 and user_hosting=1 and mr_job_id=%d "
        "ORDER BY"
        "   result.id asc",
        wuid, ss_resids.str().c_str(), RESULT_OUTCOME_SUCCESS
    );

    // D_VAL DEBUG
    fprintf(stderr, "New query to find Hosts's IP: %s\n", query);
    fflush(stderr);

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
            //retval = d_val_parse_ip_addr_val_st_res_id(row, ip_address, val_state, resid);
            retval = d_val_parse_ip_addr_val_st(row, ip_address, val_state);
            if (retval) return retval;

            ip_hosts.push_back(ip_address);
            val_states.push_back(val_state);
            //res_ids.push_back(resid);
        }
    } while (row);

    return 0;
}

// D_VAL
// Parse IP address, and validate_state from DB query
//int d_val_parse_ip_addr_val_st_res_id(MYSQL_ROW& r, std::string& ext_ip_addr, int& val_state, int& res_id){
int d_val_parse_ip_addr_val_st(MYSQL_ROW& r, std::string& ext_ip_addr, int& val_state){
    //char ext_ip_addr[256];
    char ip_addr[256];
    char valid_state[256];
    char resid[256];

    strcpy2(ip_addr, r[0]);     // save IP address (first column in SELECT query)

    fprintf(stderr, "Read IP addr (row[0]) from mysql: %s\n", ip_addr);
    fflush(stderr);

    ext_ip_addr = std::string(ip_addr);

    strcpy2(valid_state, r[1]);
    val_state = atoi(valid_state);

    // D_VAL DEBUG
    fprintf(stderr, "Read validate_state (row[1]) from mysql: %s\n", valid_state);
    fflush(stderr);

    //std::string result_id;
//    strcpy2(resid, r[2]);
//    res_id = atoi(resid);
//
//    // D_VAL DEBUG
//    fprintf(stderr, "Read result.id (row[2]) from mysql: %d\n", res_id);
//    fflush(stderr);

    //char *ptok;
    //int val_state;

    // Tokenize string (char array)
    //ptok = strtok(ext_ip_val_state," ");
    // check for error
//    if (ptok == NULL){
//        fprintf(stderr, "Error in d_val_parse_ip_addr [D_VAL]. Could not parse IP address and validate state from MySQL row: %s\n", ext_ip_val_state);
//        fflush(stderr);
//        return -1;
//    }
    // save first token (IP address)
    //ext_ip_addr = std::string(ip_addr);

    // save second token (validate_state)
    //ptok = strtok(NULL," ");
    // check for error
//    if (ptok == NULL){
//        fprintf(stderr, "Error in d_val_parse_ip_addr [D_VAL]. Could not parse IP address and validate state from MySQL row: %s\n", ext_ip_val_state);
//        fflush(stderr);
//        return -1;
//    }
    //val_state = atoi(ptok);

    //delete[] ext_ip_val_state;
    return 0;
}

/// D_VAL TODO - check for previous d_val WUs case (get IP address of previous hosts - identify which result/input they are hosting)
//    select distinct r1.id "R1 being validated", h1.external_ip_addr "Host R1", r2.id "R2 validator ID", r2.validating_wuid "Validatin WUID",
//    h2.external_ip_addr "Host for R2" from result r1, result r2, host h1, host h2 where r1.hostid=h1.id and r2.validating_wuid=r1.workunitid and
//    r2.hostid=h2.id order by r1.id, r2.id;
// @ val_hosts: array to be filled with IP addresses of hosts that have been assigned validator WUs for this workunit (wuid)
// @ resultids: vector with ID of all results that are about to be validated (to check against validators' validated results)
// @ wuid: ID of workunit that is being validated
int enumerate_validator_hosts(std::vector<std::string>& val_hosts, std::vector<int> resultids, int wuid){

    // D_VAL DEBUG
    fprintf(stderr, "Entered enumerate_validator_hosts.\n");
    fflush(stderr);

    // resize val_hosts to match the number of Result IDs
    val_hosts.resize(resultids.size());

    // D_VAL DEBUG
//    fprintf(stderr, "Just applied resize. new size: %zu\n", val_hosts.size());
//    fflush(stderr);

    // initialize vector with empty strings
//    for (int k=0; k<val_hosts.size(); k++){
//        val_hosts.insert(val_hosts.begin() + k, "");
//    }

    // D_VAL DEBUG
    fprintf(stderr, "enumerate_validator_hosts: finished resetting val_hosts.\n");
    fflush(stderr);

    char query[MAX_QUERY_LEN];
    std::string ip_address;   // string with host's IP address
    int retval;
    //int val_state;          // validate_state value returned from DB
    int resid;              // result ID for each row returned from DB
    unsigned int i;
    MYSQL_RES* rp;
    MYSQL_ROW row;
    //SCHED_RESULT_ITEM ri;
    DB_CONN* db = &boinc_db;

    sprintf(query,
        "SELECT DISTINCT "
        "   external_ip_addr, re2.id "
        "FROM "
        "   result re1, result re2, host, workunit wu1, workunit wu2 "
        "WHERE "
        "   host.id=re1.hostid and re1.workunitid = wu1.id and wu1.validating_wuid=%d and wu1.validating_wuid=wu2.id and "
        "   wu2.id=re2.workunitid and re2.validator_wuid=wu1.id and re1.outcome=%d "
        //"   and mr_app_type=1 and user_hosting=1 and mr_job_id=%d "
        "ORDER BY "
        "   re2.id asc",
        wuid, RESULT_OUTCOME_SUCCESS
    );

    // D_VAL DEBUG
    fprintf(stderr, "Submitting query to find previous validators addresses: %s\n", query);
    fflush(stderr);

    retval = db->do_query(query);
    if (retval) return retval;

    // the following stores the entire result set in memory
    //
    rp = mysql_store_result(db->mysql);
    if (!rp) return mysql_errno(db->mysql);

    //std::string address_list;
    std::stringstream ss_address_list;
    ss_address_list.str("");
    int cur_result=-1, resultids_index=-1;
    int j=0;
    do {
        row = mysql_fetch_row(rp);
        if (!row) {
            mysql_free_result(rp);
        } else {

            log_messages.printf(MSG_DEBUG,
                                "Found validator hosts+result.ID: %s %s\n", row[0], row[1]);

            // save address and result ID
            // returned row: "IP_addr Result.ID" - previous validator addr + ID of result validated
            retval = d_val_parse_ip_addr_res_id(row, ip_address, resid);
            if (retval) return retval;

            log_messages.printf(MSG_DEBUG,
                                "Set ip_address: %s | resid: %d\n", ip_address.c_str(), resid);

            // query rows are ordered by result ID, therefore whenever there is a new Result ID, save previous addresses and reset ss_address_list value
            if (cur_result != resid){

                // changed ID so before resetting ss_address_list, add it to the val_hosts vector (if it is not empty)
                if (ss_address_list.str() != "" && !ss_address_list.str().empty() && resultids_index>=0 && resultids_index<resultids.size()){
                    val_hosts.insert(val_hosts.begin() + resultids_index, ss_address_list.str());

                    // D_VAL DEBUG
                    log_messages.printf(MSG_DEBUG,
                                        "Inserted a host IP addr [%s] to val_hosts in index: %d [resid: %d]. Current value: %s\n",
                                        ss_address_list.str().c_str(), resultids_index, resid, val_hosts[resultids_index].c_str());
                }

                // reset previous values (new ss_address_list for new result)
                resultids_index = -1;
                cur_result = resid;
                ss_address_list.clear();
                ss_address_list.str("");

                // go through vector of result IDs to find ID that matches resid
                for (j=0; j<resultids.size(); j++){
                    if (resultids[j] == resid){
                        resultids_index = j;
                        break;
                    }
                }

                // store current
                if (resultids_index>=0 && resultids_index<resultids.size() && !ip_address.empty()){
                    // add this address to the list of validator hosts
                    ss_address_list << ip_address << " ";

                    // D_VAL DEBUG
                    log_messages.printf(MSG_DEBUG,
                                        "Adding address [%s] to ss_address_list (different Result ID [%d]. Index: %d). Current value [ss_adr_list]: %s\n",
                                        ip_address.c_str(), resid, resultids_index, ss_address_list.str().c_str());

                }
                else{
                    // ERROR with result ID index
                    log_messages.printf(MSG_CRITICAL,
                        "enumerate_validator_hosts. Could not find result with ID: %d. Result index out of bounds: %d\n", resid, resultids_index
                    );
                    continue;
                }
            }
            // same result ID of previous row (just add this address to string. e.g.: previous string - "XXX "; add new address YYY - "XXX YYY "
            else {

                if (resultids_index>=0 && resultids_index<resultids.size() && !ip_address.empty()){
                    ss_address_list << ip_address << " ";

                    // D_VAL DEBUG
                    log_messages.printf(MSG_DEBUG,
                                        "Adding address [%s] to ss_address_list (same Result ID. Index: %d). Current value [ss_adr_list]: %s\n",
                                        ip_address.c_str(), resultids_index, ss_address_list.str().c_str());

                }
                // incorrect or uninitiated resultids_index. Try to find corresponding result in resultids vector.
                else{
                    for (j=0; j<resultids.size(); j++){
                        if (resultids[j] == resid){
                            resultids_index = j;
                            break ;
                        }
                    }
                    if (resultids_index<0 || resultids_index>=resultids.size()){
                        // ERROR with result ID index
                        log_messages.printf(MSG_CRITICAL,
                            "enumerate_validator_hosts. Result index out of bounds: %d\n", resultids_index
                        );
                        continue;
                    }

                    if (ip_address.empty()){
                        // ERROR with result ID index
                        log_messages.printf(MSG_CRITICAL,
                            "enumerate_validator_hosts. Error retrieving IP address from DB (empty string).\n", resultids_index
                        );
                        continue;
                    }

                    ss_address_list << ip_address << " ";
                }
            }
        }
    } while (row);

    log_messages.printf(MSG_DEBUG, "Finished handling previous validator hosts\n");

    // 1 result ID still not inserted into val_hosts
    if (ss_address_list.str() != "" && resultids_index>=0 && resultids_index<resultids.size()){
        val_hosts.insert(val_hosts.begin() + resultids_index, ss_address_list.str());

        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG,
                            "Inserted a host IP addr [%s] to val_hosts. Current value: %s\n", ss_address_list.str().c_str(), val_hosts[resultids_index].c_str());
    }

    return 0;

}


// D_VAL
// Parse IP address and result ID from DB query
int d_val_parse_ip_addr_res_id(MYSQL_ROW& r, std::string& ext_ip_addr, int& res_id){
    char ip_addr[256];
    char resid[256];
    //std::string result_id;

    strcpy2(ip_addr, r[0]);     // save IP address (first column in SELECT query)

    // D_VAL DEBUG
    fprintf(stderr, "Read IP addr (row[0]) from mysql: %s\n", ip_addr);
    fflush(stderr);

    ext_ip_addr = std::string(ip_addr);


    strcpy2(resid, r[1]);
    res_id = atoi(resid);

    // D_VAL DEBUG
    fprintf(stderr, "Read Result ID (row[1]) from mysql: %d\n", res_id);
    fflush(stderr);

    return 0;
}


/*  similar to add_ips_to_xml_doc(sched_send) [eventually replace]
// add information on number of host and wu outputs to wu's <command_line> in wu template
// e.g.: --num_wu_outputs X --num_hosts Y
// add the correct number of input files to WU template
// add host addresses to wu template
//
// @ wu_template: char array with wu template contents
// @ num_outputs: number of outputs of wu that this wu is validating
// @ num_hosts: number of hosts that have returned results from wu that is being validated
// @ wuid: ID of workunit that this WU will be validating
// @ res_ids: list of IDs of Results to be validated
// @ dval_max_nbytes: value of <max_nbytes> to add to this WU's file_info (necessary to allow server to request these input files when D_VAL wu is validated)
*/
int modify_wu_template(char* wu_template, int num_outputs, int num_hosts, int wuid_validated, std::vector<int> res_ids, double dval_max_nbytes){

    int num_inputs = num_outputs * num_hosts;

    // input file protocol
    // <file_info>
    //   <number>0</number>
    // </file_info>
    // introduce the correct number of input files (num_outputs * num_hosts)
    std::string finfo_number_tag = "<number>";
    std::string end_file_info_tag = "</file_info>\n";
    std::string workunit_tag = "<workunit>";

    // file_ref after <workunit> tag
    // <file_ref>
    //  <file_number>0</file_number>
    //  <open_name>in0</open_name>
    //</file_ref>
    std::string file_ref_tag = "<file_ref>";
    std::string end_file_ref_tag = "</file_ref>\n";
    std::string min_quorum_tag = "<min_quorum>";

    std::stringstream cur_file_info;        // string with information on current file info <file_info> to insert into wu template
    std::stringstream cur_file_ref;        // string with information on current file ref <file_ref> to insert into wu template
    char *p, *q;

    // size is the same as wu_template[SIZE] in create_dist_val
    char temp_wu_template[MEDIUM_BLOB_SIZE];          // make changes in this temporary buffer first. If there are no errors, copy to wu_template
    strncpy(temp_wu_template, wu_template, sizeof(temp_wu_template));

    int counter_inputs = 0;

    //same as enumerate_ip_mappers
    std::vector<std::string> ip_hosts;
    std::vector<int> validate_states;           // needed to identify which results were already under distributed validation (previous D_VAL WUs)
    //std::vector<int> results_id;                /// Deprecated (enumerate_ip_hosts returns canon_result index) - results' ID - necessary to identify the canonical result

    // if there are previous D_VAL wu already running, save the addresses of those hosts to use as <url> for this WU's input file
    std::vector<std::string> ip_hosts_previous_d_val_wu;

    /// D_VAL TODO - check for case in which there are previous d_val WUs
//    select distinct r1.id "R1 being validated", h1.external_ip_addr "Host R1", r2.id "R2 validator ID", r2.validating_wuid "Validatin WUID",
//    h2.external_ip_addr "Host for R2" from result r1, result r2, host h1, host h2 where r1.hostid=h1.id and r2.validating_wuid=r1.workunitid and
//    r2.hostid=h2.id order by r1.id, r2.id;

    //int canon_index = -1;
    // look for result with canon_index, and only return the addresses for that host (at first place in the ip_hosts array)
    int retval;
//    if (canon_index != -1){
//        retval = enumerate_ip_hosts_canonical(ip_hosts);
//        if (retval) {
//            log_messages.printf(MSG_CRITICAL,
//                "modify_wu_template: DB access error. Can't find IP address of host that returned canonical result for workunit with ID: %d\n", wuid_validated
//            );
//            return retval;
//        }
//    }


    //std::vector<int> res_ids;
    retval = enumerate_ip_hosts(ip_hosts, validate_states, res_ids, wuid_validated);
    //retval = enumerate_ip_hosts(ip_hosts, validate_states, wuid_validated);
    if (retval) {
        log_messages.printf(MSG_CRITICAL,
            "modify_wu_template: can't find IP addresses of hosts that returned successful results for workunit with ID: %d\n", wuid_validated
        );
        return retval;
    }

    // sanity check - all hosts must have made their output available
    if (num_hosts > ip_hosts.size()){
        log_messages.printf(MSG_CRITICAL,
            "modify_wu_template: Number of hosts that hold/serve outputs (%zu) does not match the number of results (%d)\n", ip_hosts.size(), num_hosts
        );
        return -1;
    }
    else if (num_hosts < ip_hosts.size()){
        log_messages.printf(MSG_CRITICAL,
            "modify_wu_template: Another result was validated during D_VAL wu creation. Number of hosts in DB (%zu) is larger than the number of results "
            "given (%d)\n", ip_hosts.size(), num_hosts
        );
        return -1;
    }

    // D_VAL DEBUG
    fprintf(stderr, "After enumerate_ip_hosts. Before enumerate_validator_hosts. Res_ids size: %zu\n", res_ids.size());
    fflush(stderr);


    // D_VAL DEBUG
    //sleep(180);


    // get addresses of hosts that have already started validating this workunit
    std::vector<std::string> validator_hosts;
    retval = enumerate_validator_hosts(validator_hosts, res_ids, wuid_validated);



    /// DEPRECATED - unnecessary (we have to insert nbytes, md5_cksum and url tags for every <file_info>)
    //p = temp_wu_template;
//    while (p = strstr(p, finfo_number_tag.c_str())){
//        q = p;          // save last valid position for <number> tag (this function returns when p == NULL)
//        counter_inputs++;
//        // stop counting if we have already found the required number of inputs (ignore the remaining
//        if (counter_inputs >= num_inputs)
//            continue;
//    }

    // D_VAL DEBUG
    //log_messages.printf(MSG_DEBUG, "Original WU template has (at least) %d input files.", counter_inputs);

    // if there are already the required number of inputs, it means that p is already pointing to the final <number> tag.
    // look for the </file_info> tag and ignore remaining files (go directly to <workunit> tag)
    ///if (counter_inputs >= num_inputs){
    /*if (0){
        // q has the last position of <number>. Find </file_info> from there.
        p = strstr(q, end_file_info_tag.c_str());
        // move p to the end of </file_info> tag
        p += strlen(end_file_info_tag.c_str());

        // look for <workunit> tag, to append to p
        if(!(q = strstr(p, workunit_tag.c_str()))){
            log_messages.printf(MSG_CRITICAL, "Error modifying WU template for dist_val application. Did not find <workunit> tag.");
            return ERR_XML_PARSE;
        }
        // "move" all content from q (starting from <workunit>) to p
        p = q;

        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG, "Modified WU template. Updated <file_info>. Current content:\n%s", temp_wu_template);

        // update information on <file_ref>
        counter_inputs = 0;
        //q = p;    // p = q; just before
        while (p = strstr(p, file_ref_tag.c_str())){
            q = p;  // save last valid position of <file_ref> tag (this function returns when p == NULL)
            counter_inputs++;
            // stop counting if we have already found the required number of inputs (ignore the remaining
            if (counter_inputs >= num_inputs)
                continue;
        }
        // q has the last position of <file_ref>. Find </file_ref> from there.
        p = strstr(q, end_file_ref_tag.c_str());
        // move to end of </file_ref>\n tag
        p += strlen(end_file_ref_tag.c_str());

        // look for <min_quorum> tag, to append to p
        if(!(q = strstr(p, min_quorum_tag.c_str()))){
            log_messages.printf(MSG_CRITICAL, "Error modifying WU template for dist_val application. Did not find <min_quorum> tag.");
            return ERR_XML_PARSE;
        }
        // "move" all content from q (starting from <min_quorum>) to p
        p = q;

        // D_VAL DEBUG
        log_messages.printf(MSG_DEBUG, "Modified WU template. Updated <file_ref> Current content:\n%s", temp_wu_template);
    ///}*/
    // if there is a lower number of inputs than required (typical use case)
    ///else if(counter_inputs < num_inputs){

    //int missing_inputs = num_inputs - counter_inputs;

    // D_VAL DEBUG
    //log_messages.printf(MSG_DEBUG, "modify_wu_template(). Found %d input files. Still missing: %d", counter_inputs, missing_inputs);

    // if there are no input files (not usual, but still valid - add all files; point p to beggining of file instead of to the next </file_info> tag)
    /*if(counter_inputs == 0)
        p = temp_wu_template;
    else {
        // q has the position of the last <number> tag found. Find </file_info> from here.
        p = strstr(q, end_file_info_tag.c_str());
        // move p to the end of </file_info> tag
        p += strlen(end_file_info_tag.c_str());
    }*/

    // consider that there are no inputs
    p = temp_wu_template;

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG, "After setting p to temp_wu_template. p current value:\n%s\n", p);
    fflush(stderr);


    // save information after <workunit> before adding input files to text
    if(!(q = strstr(p, workunit_tag.c_str()))){
        log_messages.printf(MSG_CRITICAL, "Error modifying WU template for dist_val application. Did not find <workunit> tag.");
        return ERR_XML_PARSE;
    }
    char temp[MEDIUM_BLOB_SIZE];
    strncpy(temp, q, sizeof(temp));


    // save information after <min_quorum> before adding input files to text
    //char *after_file_info_min_quorum;
    if(!(q = strstr(p, min_quorum_tag.c_str()))){
        log_messages.printf(MSG_CRITICAL, "Error modifying WU template for dist_val application. Did not find <min_quorum> tag.");
        return ERR_XML_PARSE;
    }

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG, "Found <min_quorum>. After tag (saving into temp_min_quorum):\n%s", q);
    fflush(stderr);

    char temp_min_quorum[MEDIUM_BLOB_SIZE];
    strncpy(temp_min_quorum, q, sizeof(temp_min_quorum));

    //char url[];

    // write first comment line
    strncpy(p, "<!-- workunit template for the BOINC Validating Nas Ep program (distributed app that validates NAS EP output) -->\n\n",
            strlen("<!-- workunit template for the BOINC Validating Nas Ep program (distributed app that validates NAS EP output) -->\n\n"));

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG, "After adding <!---> to p. p current value:\n%s\n", p);
    fflush(stderr);

    // move to following line
    p += strlen("<!-- workunit template for the BOINC Validating Nas Ep program (distributed app that validates NAS EP output) -->\n\n");

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG, "After moving p to after <!--->. p current value:\n%s\n", p);
    fflush(stderr);

    // format string to insert for each input file, and add missing input files
    // add nbytes, md5_cksum and url tags
    // go through the different outputs for each host
    int fnumber = 0;
    for (int i=0; i<num_hosts; i++){

        log_messages.printf(MSG_DEBUG, "Adding all [%d] output files for host with IP: %s\n", num_outputs, ip_hosts[i].c_str());

        for (int j=0; j<num_outputs; j++){
            fnumber = i * num_outputs + j;

            log_messages.printf(MSG_DEBUG, "Adding output file #%d | file number: %d.\n", j, fnumber);

            //introduce num_bytes, md5_cksum, url;

            cur_file_info.str("");
            cur_file_info.clear(); // Clear state flags.
            cur_file_info << "<file_info>\n  <number>" << fnumber <<"</number>\n"
                             "  <sticky/>\n"               // set as sticky, otherwise it will be deleted as soon as application is executed
                             "  <c2c_up/>\n";          // let this file be uploaded to other validators (in future validating workunits)


            // If this input can be uploaded (dval_max_nbytes is set in "dist_val.xml"), then add the value to file_info
            if (dval_max_nbytes > 0)
                cur_file_info << "  <max_nbytes>" << dval_max_nbytes << "</max_nbytes>\n";
                             //"  <nbytes>0</nbytes>\n  <md5_cksum>zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz</md5_cksum>\n  <url>" << ip_hosts[i] << "</url>\n</file_info>\n"; // << endline;
                             //"  <nbytes>0</nbytes>\n  <md5_cksum>zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz</md5_cksum>\n  <c2c>" << ip_hosts[i] << "</c2c>\n</file_info>\n"; // << endline;
                             // no checksum or nbytes provided - check if client is able to download anyway
            if (!validator_hosts[i].empty() || validator_hosts[i] != ""){
                cur_file_info << "  <c2c>" << validator_hosts[i] << ip_hosts[i] << "</c2c>\n</file_info>\n"; // << endline;
            }
            // if there are no previous validator hosts
            else {
                cur_file_info << "  <c2c>" << ip_hosts[i] << "</c2c>\n</file_info>\n"; // << endline;
            }

            // D_VAL DEBUG
            log_messages.printf(MSG_DEBUG, "Adding file_info <number>%d</number> to template (with previous val_hosts):\n%s", fnumber, cur_file_info.str().c_str());

//            sprintf(tag, "  <nbytes>%f</nbytes>\n", h_out.fsize);
//
//            //strncat(wu_template, tag, sizeof(tag));
//            strncat(wu_template_temp, tag, strlen(tag));
//            sprintf(tag, "  <md5_cksum>%s</md5_cksum>\n", h_out.hash_md5);
//            //strncat(wu_template, tag, sizeof(tag));
//            strncat(wu_template_temp, tag, strlen(tag));
//
//            //strncat(wu_template, "<url>P2P</url>\n", sizeof("<url>P2P</url>\n"));
//            strncat(wu_template_temp, p2p, strlen(p2p));


        // sanity check - see if by adding these files, we go over the maximum template size [MEDIUM_BLOB_SIZE]
        //*p = '\0';

            // D_VAL DEBUG
            log_messages.printf(MSG_DEBUG, "Before ending char array p with '\\0'.\n");
            fflush(stderr);
            log_messages.printf(MSG_DEBUG, "p current value:\n%s\n", p);
            fflush(stderr);

            //strcpy(p, '\0');
            *p = '\0';

            log_messages.printf(MSG_DEBUG, "After ending char array p with '\\0'\n");
            fflush(stderr);

            if ((strlen(temp_wu_template) + strlen(cur_file_info.str().c_str())) > MEDIUM_BLOB_SIZE){
                log_messages.printf(MSG_CRITICAL, "Error modifying WU template for dist_val application. Adding input files <file_info> would exceed max WU template size.");
                fflush(stderr);
                return ERR_BUFFER_OVERFLOW;
            }
            strcpy(p, cur_file_info.str().c_str());
            p += strlen(cur_file_info.str().c_str());
        }
    }

    // finished adding input files' <file_info>. Now, add the remaining string (stored in temp)
    // sanity check
    *p = '\0';
    if ((strlen(temp_wu_template) + strlen(temp)) > MEDIUM_BLOB_SIZE){
        log_messages.printf(MSG_CRITICAL, "Error modifying WU template for dist_val application. Adding input files <file_info> would exceed max WU template size.");
        return ERR_BUFFER_OVERFLOW;
    }

    strcpy(p, temp);

    // D_VAL DEBUG
//    log_messages.printf(MSG_DEBUG, "Modified WU template. Updated <file_info>. Current content:\n%s\nEND\n", temp_wu_template);
//    fflush(stderr);

    // update information on <file_ref>
    counter_inputs = 0;
    q = p;

    // D_VAL DEBUG
//    log_messages.printf(MSG_DEBUG, "Set q=p and counter_inputs=0. Current p:\n%s\n", p);
//    fflush(stderr);
//
//    while (p = strstr(p, file_ref_tag.c_str())){
//
//        // D_VAL DEBUG
//        log_messages.printf(MSG_DEBUG, "Found file_ref tag. Current p:\n%s\n", p);
//        fflush(stderr);
//
//        p+= strlen(file_ref_tag.c_str());
//
//        q = p;  // save last valid position of <file_ref> tag (this function returns when p == NULL)
//        counter_inputs++;
//        // stop counting if we have already found the required number of inputs (ignore the remaining
//        if (counter_inputs >= num_inputs)
//            continue;
//    }
//
//    // D_VAL DEBUG
//    log_messages.printf(MSG_DEBUG, "Found %d file_ref tags.\n", counter_inputs);
//    fflush(stderr);

    // if there are no input files (not usual, but still valid - add all files; point p to beggining of file instead of to the next </file_info> tag)
    if(counter_inputs == 0){
        p = q;
        strncpy(p, "\n<workunit>\n", sizeof("\n<workunit>\n"));
        p += strlen("\n<workunit>\n");
    }
    else {
        // q has the last position of <file_ref>. Find </file_ref> from there.
        p = strstr(q, end_file_ref_tag.c_str());
        // move to end of </file_ref>\n tag
        p += strlen(end_file_ref_tag.c_str());
    }

    // D_VAL DEBUG
    log_messages.printf(MSG_DEBUG, "Moved past the file_ref tags. current p:\n%s\n", p);
    fflush(stderr);

//    // save information after <min_quorum> before adding input files to text
//    if(!(q = strstr(p, min_quorum_tag.c_str()))){
//        log_messages.printf(MSG_CRITICAL, "Error modifying WU template for dist_val application. Did not find <min_quorum> tag.");
//        return ERR_XML_PARSE;
//    }

    // D_VAL DEBUG
//    log_messages.printf(MSG_DEBUG, "Found <min_quorum>. After tag (q):\n%s", q);
//    fflush(stderr);

    //char temp[MEDIUM_BLOB_SIZE];
    // everything after <min_quorum> was copied to temp_min_quorum
    //strncpy(temp_min_quorum, after_file_info_min_quorum, sizeof(after_file_info_min_quorum));

    // format string to insert for each file_ref, and add missing input files
    for (int i=counter_inputs; i<num_inputs; i++){
        cur_file_ref.str("");
        cur_file_ref.clear(); // Clear state flags.
        cur_file_ref << "  <file_ref>\n    <file_number>" << i << "</file_number>\n    <open_name>in" << i << "</open_name>\n  </file_ref>\n"; // << endline;

        // sanity check - see if by adding these files, we go over the maximum template size [MEDIUM_BLOB_SIZE]
        *p = '\0';
        if ((strlen(temp_wu_template) + strlen(cur_file_ref.str().c_str())) > MEDIUM_BLOB_SIZE){
            log_messages.printf(MSG_CRITICAL, "Error modifying WU template for dist_val application. Adding input files <file_ref> would exceed max WU template size.");
            return ERR_BUFFER_OVERFLOW;
        }
        strcpy(p, cur_file_ref.str().c_str());
        p += strlen(cur_file_ref.str().c_str());
    }

    // finished adding input files' <file_ref> tags. Now, add the remaining string (stored in temp_min_quorum)
    // sanity check
    *p = '\0';
    if ((strlen(temp_wu_template) + strlen(temp_min_quorum)) > MEDIUM_BLOB_SIZE){
        log_messages.printf(MSG_CRITICAL, "Error modifying WU template for dist_val application. Adding input files <file_ref> would exceed max WU template size.");
        return ERR_BUFFER_OVERFLOW;
    }

    strcpy(p, temp_min_quorum);

    // D_VAL DEBUG
//    log_messages.printf(MSG_DEBUG, "Modified WU template. Updated <file_ref>. Current content:\n%s", temp_wu_template);
//    fflush(stderr);
    //}

    strncpy(wu_template, temp_wu_template, sizeof(temp_wu_template));

    return 0;


}


// similar to mr_create_reduce_work, in mr_jobtracker.cpp
//
/*
Create work example
http://boinc.berkeley.edu/trac/wiki/WorkGeneration
*/
//
/// D_VAL TODO - * In cases where prev valid is running, add validators to list of peers that have the necessary inputs for validating wu.
//
// Create dist_val Workunit: clients download the output of WU results, and validate it. Number of inputs for validating wu = num_hosts * num_outputs
//
// Arguments
// @ result_template_file: Result template filename (must be read inside function)
// @ wu_template_file: WU template file name (must be read inside function)
// @ app_name: name of application for this WU (e.g., validates inputs by comparison)
// @ num_outputs: number of output files for Wu that is being validated
// @ min_quorum: minimum quorum for validation purposes
// @ num_hosts: number of hosts that have finished executing the WU we're validating (= number of successful results returned so far)
// @ wu_alt_name: alternative name for wu to be validated (used to name input files)
// @ last_result_id: ID of one of the results that is going to be validated distributedly. Required to differentiate between diff dist val WU validating the
//                   same WU (diff no. of results), when choosing a name. [DB - all results currently being validated by another client have validate_state = VALIDATE_STATE_UNDER_DIST_VAL]
// @ wuid_validating: ID of Workunit that is going to be validated by clients
// @ validator_wuid: ID of WU that has been created (to fill validator_wuid field in DB for corresponding results being validated/handled)
// @ result_ids: list of IDs of RESULTs currently being considered for validation (necessary to avoid race condition with validator/transitioner
// @ is_hashed: true if this WU is going to be validated by comparison of hashes of its output files (instead of using output files themselves)
// @ dval_max_nbytes: value of <max_nbytes> to set on WU's input file_info
//
int create_dist_val_wu(char* result_template_file, char* wu_template_file, char* app_name, char* wu_alt_name, int num_outputs, int min_quorum, int num_hosts,
                       int last_result_id, int wuid_validating, int& validator_wuid, std::vector<int> result_ids, bool is_hashed, double dval_max_nbytes){
//    (char* result_template_file, char* wu_template_file, char* app_name,
//     int num_inputs, int num_reducers, std::vector<std::string> wu_name, int reduce_wu_index, MAPREDUCE_JOB* mrj)
    validator_wuid = -1;            // default value
    int x;
    DB_APP app;
    DB_WORKUNIT wu;
    int retval = 0;

    char wu_template[MEDIUM_BLOB_SIZE];
    char result_template_path[256];
    //const char result_template_path[256];
    char buf[256];

    int num_inputs = num_outputs * num_hosts;   // number of inputs for validating WU depends on the number of outputs of WU that is being validated AND
                                                // number of hosts that have returned a result

    const char* infiles[num_inputs];
    //char* infiles[num_inputs];

    /// Unnecessary?
    //strncpy(app.name, app_name, (sizeof(app.name)-1));
    //app.name[sizeof(app.name)-1]='\0';

    // D_VAL DEBUG
    // sleep(90);

    // D_VAL
    log_messages.printf(MSG_CRITICAL, "Inside create_dist_val_wu. App name: %s\n", app_name);
    fflush(stderr);

    sprintf(buf, "where name='%s'", app_name);
    retval = app.lookup(buf);
    if (retval) {
        log_messages.printf(MSG_CRITICAL, "Error creating Distributed Validation WU: app not found: %s\n", app.name);
        return 1;
    }

    // template_file already has templates/ dir included
    retval = read_filename(config.project_path(wu_template_file), wu_template, sizeof(wu_template));
    if (retval) {
        fprintf(stderr, "create_work: can't open WU template: %d\n", retval);
        return 1;
        //exit(1);
    }

    wu.clear();
    // defaults (in case they're not in WU template)
    wu.id = 0;
    wu.appid = app.id;
    wu.min_quorum = 2;
    wu.target_nresults = 2;
    wu.max_error_results = 3;
    wu.max_total_results = 10;
    wu.max_success_results = 6;
    wu.rsc_fpops_est = 3600e9;
    wu.rsc_fpops_bound =  86400e9;
    wu.rsc_memory_bound = 5e8;
    wu.rsc_disk_bound = 1e9;
    wu.rsc_bandwidth_bound = 0.0;
    wu.delay_bound = 7*86400;

    //char d_val_wu_name[256];
    // Create base WU name - app name and the ID of the last result received: for each new available result, a new D_VAL WU is created. Therefore, the last available
    // result ID is always different
    //sprintf(d_val_wu_name, "%s_%d", wu_alt_name, last_result_id);
    sprintf(wu.name, "%s_%d", wu_alt_name, last_result_id);
    //sprintf(wu.name, "%s_%d", partial_reduce_wu_name, i);

    wu.validating_wuid = wuid_validating;

    char *input_name = new char[256];
    // create unique input names. Format: altname_clientID_outputID
    // different clients produce ouptuts with the same name, therefore the clientID is required to differentiate between them. E.g.: client A and B produce output_0;
    // name: output_0_0 (client A) and output_1_0 (client B) - if results are valid, they both files are equal
    /// input files ordered by client, then output index. E.g.: in0, in1 (client1); in2, in3 (client2); etc...
    int input_file_index = 0;
    for (int j=0; j<num_hosts; j++){

        for (int k=0; k<num_outputs; k++){
            input_file_index = j*num_outputs + k;

            if (!is_hashed){
                // D_VAL DEBUG
                log_messages.printf(MSG_DEBUG, "Normal (non-hashed) validation! Wu_name: %s | client_index: %d | output_index: %d | input_index: %d | Total num_inputs: %d\n",
                                    wu.name, j, k, input_file_index, num_inputs);
                fflush(stderr);

                sprintf(input_name, "%s_%d_%d", wu_alt_name, j, k);
            }
            // distributed validation through hashes
            else{
                // D_VAL DEBUG
                log_messages.printf(MSG_DEBUG, "Hashed validation! Wu_name: %s | client_index: %d | output_index: %d | input_index: %d | Total num_inputs: %d\n",
                                    wu.name, j, (k+num_outputs), input_file_index, num_inputs);
                fflush(stderr);

                sprintf(input_name, "%s_%d_%d", wu_alt_name, j, (k+num_outputs));
            }

            // D_VAL DEBUG
            log_messages.printf(MSG_CRITICAL, "Inside create_dist_val. Created input name: %s. Now copying it to infiles array [%d].\n", input_name, input_file_index);
            fflush(stderr);

            infiles[input_file_index] = input_name;

            // D_VAL DEBUG
            if (!is_hashed)
                log_messages.printf(MSG_CRITICAL, "Inside create_dist_val. Output #%d, client #%d | "
                                " Infile[%d]: %s\n", k, j, input_file_index, infiles[input_file_index]);
            else
                log_messages.printf(MSG_CRITICAL, "Inside create_dist_val. Output #%d, client #%d | "
                                " Infile[%d]: %s\n", (k+num_outputs), j, input_file_index, infiles[input_file_index]);

            fflush(stderr);
            input_name = new char[256];
        }
    }

    // D_VAL DEBUG
    for (int m=0; m<num_inputs; m++){
        log_messages.printf(MSG_NORMAL, "[dist_val] Infile[%d]: %s\n", (m), infiles[m]);

    }

    /// D_VAL TODO: add hosts to wu template?
    // similar to add_ips_to_xml_doc(sched_send) [eventually replace]
    // add information on number of host and wu outputs to wu's <command_line> in wu template
    // e.g.: --num_wu_outputs X --num_hosts Y
    // add the correct number of input files to WU template
    // add host addresses to wu template
    if (modify_wu_template(wu_template, num_outputs, num_hosts, wuid_validating, result_ids, dval_max_nbytes)){
        log_messages.printf(MSG_CRITICAL, "Error in create_dist_val(). Could not modify Workunit template for app: %s", app_name);
        return 1;
    }

    // save information on num_outputs, num_hosts and min_quorum in command line
    std::stringstream cmd_line;
    cmd_line << "--num_outputs " << num_outputs << " --num_hosts " << num_hosts << " --min_quorum " << min_quorum;

    // result template path
    strncpy(result_template_path, config.project_path(result_template_file), (sizeof(result_template_path)-1));
    result_template_path[sizeof(result_template_path)-1] = '\0';

    log_messages.printf(MSG_CRITICAL, "Inside mr_create_reduce_work. Calling create_work()\n"
                            "result_template_file: %s || result_template_path: %s | infiles size/num_inputs: %d\n",
                            result_template_file, result_template_path, num_inputs);

    // copy original wu, in case there's an error
        //wu2 = wu3;
        //retval = mr_insert_mappers_addr(wu3, wu.mr_job_id);
        //if (retval) {
          //  log_messages.printf(MSG_CRITICAL,
          //      "add_wu_to_reply: can't insert BOINC-MR Mappers address tag: %d\n", retval
            //);
            // send saved WU
            //wu3 = wu2;
        //}

    // update information on whether this validator Workunit will use hashes for the validation or not
    if (is_hashed){
        wu.validation_hashed = true;
    }
    else{
        wu.validation_hashed = false;
    }

    retval = create_work(
            wu,
            wu_template,            // contents, not path
            result_template_file,   // relative to project root - "templates/TEMPLATE_RE.xml"
            result_template_path,   // "./templates/TEMPLATE_RE.xml"
            //infiles,
            const_cast<const char **>(infiles),
            //ninfiles,
            num_inputs,
            config,                  // SCHED_CONFIG&
            //,
            cmd_line.str().c_str()           // optional (= NULL)
            //additional_xml          // optional (= NULL)
            );

    /// D_VAL TODO: check if this is updated inside create_work
    validator_wuid = wu.id;

    // D_VAL DEBUG
    fprintf(stderr, "create_dist_val_wu() - Setting validator_wuid to wu.id: %d\n", validator_wuid);
    fflush(stderr);

    if (retval) {
        log_messages.printf(MSG_CRITICAL,
            "create_dist_val_wu() - can't create Dist Val Work Unit. Retval: %d\n", retval
        );

        // BOINC-MR
        fflush(stderr);

        //exit(retval);
        return retval;
    }

    log_messages.printf(MSG_DEBUG, "[distributed_validation.cpp] Leaving create_dist_val_wu(). Returning 0...\n");
    fflush(stderr);

    return 0;

//    return "TODO";
}



/// D_VAL - DEPRECATED
///
///
/* */
// similar to add_ips_to_xml_doc, in sched_send.cpp
//
// Create unique alternative names for this workunit's output files, based on pre-defined base name, this WU's id and a counter
//  e.g.: <alt_name> in template: outfile; unique alt_name created: FILE1 - <alt_name> outfile_WUID_0 </...>  FILE2 - <alt_name> outfile_WUID_1 </...>
// @ result_template: char array with result template contents - max size: BLOB_SIZE [char _result_template[BLOB_SIZE]]
// @ wu_id: ID of workunit whose result template we are changing
//
//int create_unique_alt_names(char* res_template, WORKUNIT& wu){
/*int create_unique_alt_names(char* res_template, int wu_id){
    std::string alt_name_tag = "<alt_name>";
    std::string end_alt_name_tag = "</alt_name>\n";
    std::stringstream unique_alt_name;        // string with final version of alternative name (including ending and \n): "fubar</alt_name>\n"
    char base_name[256];                    // base name defined by user in result template [append wu_id and counter to this name to create the final version]
    char* p, *q, buf[BLOB_SIZE];
    int counter = 0, length;
    //stringstream ss;
    //char res_template[BLOB_SIZE];


    char temp_res_template[BLOB_SIZE];          // make changes in this temporary buffer first. If there are no errors, copy to res_template
    strncpy(temp_res_template, res_template, sizeof(temp_res_template));

    p = strstr(temp_res_template, alt_name_tag.c_str());
    if (!p) {
        log_messages.printf(MSG_CRITICAL,
            "create_unique_alt_names: %s not found in result template for WU [#%d]: %s\n", alt_name_tag.c_str(), wu_id, res_template
        );
        return ERR_NULL;
    }
    while (p){

        // BOINC-MR DEBUG
        log_messages.printf(MSG_DEBUG, "create_unique_alt_names: found the string: %s\n", alt_name_tag.c_str());
        fflush(stderr);

        p = p + strlen(alt_name_tag.c_str());       // move pointer to after <alt_name> tag
        q = strstr(p, end_alt_name_tag.c_str());    // pointer to start of </alt_name>

        // sanity check
        if (!q){
            log_messages.printf(MSG_CRITICAL,
            "create_unique_alt_names: Closing tag for <alt_name> not found in result template for WU [#%d]\n", wu_id);
            return ERR_XML_PARSE;
        }

        // BOINC-MR DEBUG
        log_messages.printf(MSG_DEBUG, "create_unique_alt_names: found the string: %s\n", end_alt_name_tag.c_str());
        fflush(stderr);

        length = q-p;
        // read base name - remove whitespaces first
        strncpy(base_name, p, length);
        base_name[length] = '\0';

        // BOINC-MR DEBUG
        log_messages.printf(MSG_DEBUG, "create_unique_alt_names: base_name [b4 whitespaces]: %s\n", base_name);
        fflush(stderr);

        // remove whitespaces
        strip_whitespace(base_name);

        // BOINC-MR DEBUG
        log_messages.printf(MSG_DEBUG, "create_unique_alt_names: base_name [after whitespaces]: %s\n", base_name);
        fflush(stderr);

        // append wu_id and counter to create unique name
        //unique_alt_name = base_name;

        unique_alt_name.str("");
        unique_alt_name.clear(); // Clear state flags.
        unique_alt_name << base_name << "_" << wu_id << "_" << counter << "</alt_name>\n";
        // unique_alt_name += (wu_id)+"_"+counter+"</alt_name>\n";
        counter++;

        // BOINC-MR DEBUG
        log_messages.printf(MSG_DEBUG, "create_unique_alt_names: Unique alt name: %s\n", unique_alt_name.str().c_str());
        fflush(stderr);

        // set q to after "</alt_name>\n", and update p (which is pointing to just after "<alt_name>"
        q += strlen(end_alt_name_tag.c_str());
        strcpy(buf, q);

        // BOINC-MR DEBUG
        log_messages.printf(MSG_DEBUG, "create_unique_alt_names: copied q to buf: %s\n", buf);
        fflush(stderr);

        //strcpy(p, unique_alt_name.c_str());
        strcpy(p, unique_alt_name.str().c_str());

        // BOINC-MR DEBUG
        log_messages.printf(MSG_DEBUG, "create_unique_alt_names: copied unique to p: %s\n", p);
        fflush(stderr);

        strcat(p, buf);

        // BOINC-MR DEBUG
        log_messages.printf(MSG_DEBUG, "create_unique_alt_names: copied buf to p: %s\n", p);
        fflush(stderr);

        p = strstr(p, alt_name_tag.c_str());
    }

    if (strlen(temp_res_template) > BLOB_SIZE-1) {
        log_messages.printf(MSG_CRITICAL,
            "create_unique_alt_names: Result template size overflow: %d\n", strlen(temp_res_template));
        return ERR_BUFFER_OVERFLOW;
    }
    else
        strncpy(res_template, temp_res_template, sizeof(temp_res_template));

    return 0;
}
*/



