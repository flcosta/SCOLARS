/**
* MapReduce Job Tracker class - server side
* Used by scheduler to handle MapReduce jobs, create Reduce work units, and register completed Map results
*
* Version info:
* => TODO - Creating support for return of hashed Map outputs
* - Working with centralized version - creates Reduce wu after all Map wus have been returned and validated
*
* v 1.2 - Jan 2012 - Fernando Costa
*/

#include <vector>
#include <string>

#include "boinc_db.h"
#include "backend_lib.h"    // read_filename
#include "util.h"           // read_file_malloc

#include "miofile.h"        // parse()
#include "parse.h"          // match_tag()
#include "sched_msgs.h"     // log_messages
#include "error_numbers.h"  // ERR_XML_PARSE
#include "sched_config.h"   // SCHED_CONFIG config (used to copy Map output to input, and create Reduce WU)
#include "filesys.h"        // boinc_copy()
#include "sched_util.h"     // dir_hier_path()
#include "str_replace.h"    // strcpy2() / strlcpy

#include "mr_jobtracker.h"


// Initialize values
void MAPREDUCE_JOB_SET::init(char* fname){
    if (fname == NULL){
        strcpy(mr_jobs_fname, MR_JOBS_FILENAME);
    }
    else{
        strcpy(mr_jobs_fname, fname);
    }
}


// parse existing MapReduce jobs from file "mr_filename"
// Default file name in mr_jobtracker.h
int MAPREDUCE_JOB_SET::parse() {
    const char *proj_dir;

    // BOINC-MR DEBUG
    fprintf(stderr, "Inside MAPREDUCE_JOB_SET::parse()\n");
    fflush(stderr);

    //proj_dir = config.project_path("%s",mr_jobs_fname);
    proj_dir = config.project_path(mr_jobs_fname);

    // BOINC-MR DEBUG
    fprintf(stderr, "Project Path for file %s: %s\n", mr_jobs_fname, proj_dir);
    fflush(stderr);

    FILE* f = fopen(proj_dir, "r");
    // If file does not exist (not necessary for scheduler execution - can be created at new MR jobs execution
    if (!f){
        log_messages.printf(MSG_DEBUG, "MAPREDUCE_JOB_SET::parse(): MapReduce jobs file does not exist in project directory: %s \n", mr_jobs_fname);
        return ERR_FOPEN;
    }

    MIOFILE mf;
    mf.init_file(f);
    MAPREDUCE_JOB* mr_job;
    char buf[256];
    int retval;
    while (fgets(buf, 256, f)) {
        if (match_tag(buf, "<mapreduce_job_set>")) continue;
        if (match_tag(buf, "</mapreduce_job_set>")){
            fclose(f);
            return 0;
        }
        if (match_tag(buf, "<mr_job>")){
            mr_job = new MAPREDUCE_JOB;
            retval = mr_job->parse(mf);

            /// BOINC-MR TODO - additional checks required?
            /// validate mr_job values [num maps/reduces; template location...]
            /// Probe DB to see if the workunits still exist

            if (!retval) mr_jobs.push_back(mr_job);
            else delete mr_job;
            continue;
        }


        /// BOINC-MR TODO - define log file (in MR_JOB_SET?
        log_messages.printf(MSG_DEBUG, "[unparsed_xml] MAPREDUCE_JOB_SET::parse(): unrecognized %s\n", buf);

    }
    return ERR_XML_PARSE;
}

MAPREDUCE_JOB::MAPREDUCE_JOB(){
    mr_job_id = 0;
    mr_num_map_tasks = 0;
    mr_num_reduce_tasks = 0;
    mr_job_status = MR_JOB_INIT;
    //mr_inputs_available = false;
    num_available_inputs = 0;
    mr_validated = false;
    reduce_wu_name_index = 0;
    mr_hashed = false;
}

MAPREDUCE_JOB::~MAPREDUCE_JOB(){
    mr_available_inputs.clear();
    mr_hashed_output_files.clear();
    //clear_available_inputs();
}


/// BOINC-MR TODO -deprecated
// Not necessary if vector of available inputs is made of INTs instead of RESULT* or WORKUNIT*
/*int MAPREDUCE_JOB::clear_available_inputs(){
    for(int i=0 ; i<mr_available_inputs.size() ; i++)
        delete mr_available_inputs[i];
    mr_available_inputs.clear();
}*/


/// BOINC-MR TODO - create MAPREDUCE_JOB::write(out)
/// write mr jobs metadata to file to keep info in case of server error

// Parse information on MapReduce job from file
// Called from:
// - MAPREDUCE_JOB_SET::parse()
//
int MAPREDUCE_JOB::parse(MIOFILE &in){
    std::string tempstr;
    char buf[256];
    int retval;

    while (in.fgets(buf, 256)) {
        if (match_tag(buf, "</mr_job>")){
            if (mr_num_map_tasks <= 0 || mr_num_reduce_tasks <= 0 || mr_job_id <=0){
                log_messages.printf(MSG_CRITICAL,
                    "MAPREDUCE_JOB::parse() : Incorrect or missing value in mr_jobtracker.xml [e.g.: mr_job_id less than 0].\n");
                return ERR_XML_PARSE;
            }
            // initialize hashed_outputs vector with number of mappers (hashed_outputs[mappers][reducers])
            // each mapper will have its reduce output information added on mr_save_hashed_outputs()
            for (int i=0; i<mr_num_map_tasks; i++){
                hashed_outputs.push_back(std::vector<hash_out>());
            }
            return 0;
        }
        if (parse_int(buf, "<mr_job_id>", mr_job_id)) continue;
        if (parse_int(buf, "<num_maps>", mr_num_map_tasks)) continue;

        if (parse_int(buf, "<num_reduces>", mr_num_reduce_tasks)) continue;
        //if (match_tag(buf, "<num_reduces>")) continue;

        // parse status
        if (match_tag(buf, "<map_running/>")) {
            // BOINC-MR
            log_messages.printf(MSG_DEBUG,
                "BOINC-MR DEBUG - Insider MAPREDUCE_JOB::parse() found <map_running/>\n");
            mr_job_status = MR_JOB_MAP_RUNNING;
            continue;
        }
        if (match_tag(buf, "<reduce_running/>")) {
            // BOINC-MR
            log_messages.printf(MSG_DEBUG,
                "BOINC-MR DEBUG - Insider MAPREDUCE_JOB::parse() found <reduce_running/>\n");
            mr_job_status = MR_JOB_RED_RUNNING;
            continue;
        }
        if (match_tag(buf, "<done/>")) {
            // BOINC-MR
            log_messages.printf(MSG_DEBUG,
                "BOINC-MR DEBUG - Insider MAPREDUCE_JOB::parse() found <done/>\n");
            mr_job_status = MR_JOB_DONE;
            continue;
        }

        if (match_tag(buf, "<hashed/>")) {
            // BOINC-MR
            log_messages.printf(MSG_DEBUG,
                "BOINC-MR DEBUG - Insider MAPREDUCE_JOB::parse() found <hashed/>\n");
            mr_hashed = true;
            continue;
        }

        /// BOINC-MR TODO - leave list of reduce inputs as sequence of integers separated by " " space,
        /// or create a tag for each input (<reduce_input_wu_id>)? - too many inputs?
        if (match_tag(buf, "<available_reduce_inputs>")) {
            copy_element_contents(in, "</available_reduce_inputs>", tempstr);
            // Space-separated list of available reduce inputs stored in tempstr
            parse_available_input(tempstr);
            continue;
        }
        /// BOINC-MR TODO - leave list of map output files (stored in input dir) as sequence of integers separated by " " space,
        /// or create a tag for each input (<map_output_wu_id>)? - too many inputs?
        /*if (match_tag(buf, "<available_map_outputs>")) {
            copy_element_contents(in, "</available_map_outputs>", tempstr);
            // Space-separated list of available reduce inputs stored in tempstr
            parse_available_map_output(tempstr);
            continue;
        }*/
        // parse map-specific info
        if (match_tag(buf, "<map>")){
            retval = parse_map_info(in);

            if (retval) return ERR_XML_PARSE;
            else continue;
        }
        // parse reduce-specific info
        if (match_tag(buf, "<reduce>")){
            retval = parse_reduce_info(in);

            if (retval) return ERR_XML_PARSE;
            else continue;
        }

        if (match_tag(buf, "<allow_inc_inputs/>")) {
            // BOINC-MR
            log_messages.printf(MSG_DEBUG,
                "BOINC-MR DEBUG - Insider MAPREDUCE_JOB::parse() found <allow_inc_inputs/>\n");
            mr_allow_inc_inputs = true;;
            continue;
        }

        // make sure that each Reduce WU have unique names - each time a new batch is created, this value is incremented and saved on file (in case server fails)
        if (parse_int(buf, "<reduce_wu_name_index>", reduce_wu_name_index)) continue;

        log_messages.printf(MSG_DEBUG, "[unparsed_xml] MAPREDUCE_JOB::parse(): unrecognized %s\n", buf);
    }
    return ERR_XML_PARSE;
}

/// BOINC-MR TODO : What information on Map tasks does the Scheduler need? Only Transitioner needs it?
// Parse information inside <map> tags in mr_jobs file
int MAPREDUCE_JOB::parse_map_info(MIOFILE& in){
    char buf[256];
    /// temporary - replace with variable in MR_JOB class? Unnecessary for MAP
    char mr_map_wu_template[256], mr_map_re_template[256], mr_map_app_name[256];

    while (in.fgets(buf, 256)) {
        if (match_tag(buf, "</map>")) return 0;

        // Info required to create work unit (by transitioner)
        if(parse_str(buf, "<wu_template>", mr_map_wu_template, sizeof(mr_map_wu_template))) continue;
        if(parse_str(buf, "<re_template>", mr_map_re_template, sizeof(mr_map_re_template))) continue;
        if(parse_str(buf, "<app_name>", mr_map_app_name, sizeof(mr_map_app_name))) continue;

        if(parse_str(buf, "<wu_template>", mr_map_wu_template, sizeof(mr_map_wu_template))) continue;
        if(parse_str(buf, "<re_template>", mr_map_re_template, sizeof(mr_map_re_template))) continue;
        if(parse_str(buf, "<app_name>", mr_map_app_name, sizeof(mr_map_app_name))) continue;

    }
	return ERR_XML_PARSE;
}

// Parse information inside <reduce> tags in mr_jobs file
int MAPREDUCE_JOB::parse_reduce_info(MIOFILE& in){
    char buf[256];

    while (in.fgets(buf, 256)) {
        if (match_tag(buf, "</reduce>")) return 0;

        // Info required to create work unit (by transitioner)
        if(parse_str(buf, "<wu_template>", mr_reduce_wu_template, sizeof(mr_reduce_wu_template))) continue;
        if(parse_str(buf, "<re_template>", mr_reduce_re_template, sizeof(mr_reduce_re_template))) continue;
        if(parse_str(buf, "<app_name>", mr_reduce_app_name, sizeof(mr_reduce_app_name))) continue;

        log_messages.printf(MSG_DEBUG, "[unparsed_xml] MAPREDUCE_JOB::parse_reduce_info(): unrecognized %s\n", buf);

    }
	return ERR_XML_PARSE;
}


/// BOINC-MR TODO
/// ==> str_tokenizer()
/// - parse_str()
/// - strip_whitespace()
// Take each token, parse into INT and place it
int MAPREDUCE_JOB::parse_available_input(std::string& str){
    char *cstr, *ptok;
    int wuid;

    cstr = new char [str.size()+1];
    strcpy (cstr, str.c_str());

    // Tokenize string (char array)
    ptok = strtok (cstr," ");
    while (ptok!=NULL){
        // convert from char* to int and insert into mr_available_inputs vector
        wuid = atoi (ptok);
        if (wuid != 0) mr_available_inputs.push_back(wuid);
        else {
            log_messages.printf(MSG_DEBUG, "MAPREDUCE_JOB::parse_available_input() - ERROR parsing available inputs from xml. Could not convert to int: %s\n", ptok);
        }

        printf ("%s\n",ptok);
        ptok = strtok(NULL," ");
    }

    delete[] cstr;
    return 0;
}


// Take each token, parse into INT and place it
// check for map output files that are available on the server as reduce input (not just stored in clients)
int MAPREDUCE_JOB::parse_available_map_output(std::string& str){
    char *cstr, *ptok;
    int wuid;

    cstr = new char [str.size()+1];
    strcpy (cstr, str.c_str());

    // Tokenize string (char array)
    ptok = strtok (cstr," ");
    while (ptok!=NULL){
        // convert from char* to int and insert into mr_hashed_output_files vector
        wuid = atoi (ptok);
        if (wuid != 0) mr_hashed_output_files.push_back(wuid);
        else {
            log_messages.printf(MSG_DEBUG, "MAPREDUCE_JOB::parse_available_map_output() - ERROR parsing available inputs from xml. Could not convert to int: %s\n", ptok);
        }

        printf ("%s\n",ptok);
        ptok = strtok(NULL," ");
    }

    delete[] cstr;
    return 0;
}


/// DEPRECATED - not used...
// Remove Workunit ID wu_id from list of available inputs for Reduce tasks
bool MAPREDUCE_JOB::mr_remove_input_task(int wu_id){
    bool success = false;
    vector<int>::iterator wuid_iter;

    wuid_iter = mr_available_inputs.begin();
    while (wuid_iter != mr_available_inputs.end()) {

        // BOINC-MR DEBUG
        printf("[MAPREDUCE_JOB::mr_remove_input_task] Going through workunit list. Current WU ID: %d | Comparing with: %d\n", *wuid_iter, wu_id);

        if (*wuid_iter == wu_id){
            wuid_iter = mr_available_inputs.erase(wuid_iter);
            success = true;

            // BOINC-MR DEBUG
            printf("BOINC-MR DEBUG [MR_jobtracker] Inside mr_remove_input_task() - successfully removed WU ID #%d from upload list\n", wu_id);
        }
        else
            wuid_iter++;
    }

    num_available_inputs--;
}


// Add Workunit ID wu_id to list of available inputs for Reduce tasks
bool MAPREDUCE_JOB::mr_add_input_task(int wu_id){
    vector<int>::iterator wuid_iter;

    // First,check if the workunit ID isn't already there
    wuid_iter = mr_available_inputs.begin();
    while (wuid_iter != mr_available_inputs.end()) {

        // BOINC-MR DEBUG
        fprintf(stderr, "[MAPREDUCE_JOB::mr_add_input_task()] Going through available WU IDs available as Reduce inputs. Current WU ID: %d | Comparing with: %d\n", (*wuid_iter), wu_id);
        fflush(stderr);

        if (*wuid_iter == wu_id){

            // BOINC-MR DEBUG
            fprintf(stderr, "[MR_jobtracker] ERROR: Work unit #%d already in list of available Reduce inputs.\n", wu_id);
            fflush(stderr);

            return false;
        }
        else{
            wuid_iter++;
        }
    }

    // BOINC-MR DEBUG
    fprintf(stderr, "[MAPREDUCE_JOB::mr_add_input_task()] Work unit #%d not present in list of available Reduce inputs; Adding it.\n", wu_id);
    fflush(stderr);

    mr_available_inputs.push_back(wu_id);
    num_available_inputs++;
    return true;
}


// Add Workunit ID wu_id to list of map outputs available for download on the server
bool MAPREDUCE_JOB::mr_add_output_avail(int wu_id){
    vector<int>::iterator wuid_iter;

    // First,check if the workunit ID isn't already there
    wuid_iter = mr_hashed_output_files.begin();
    while (wuid_iter != mr_hashed_output_files.end()) {

        // BOINC-MR DEBUG
        fprintf(stderr, "[MAPREDUCE_JOB::mr_add_output_avail()] Going through available WU IDs available as Reduce inputs. Current WU ID: %d | Comparing with: %d\n", (*wuid_iter), wu_id);
        fflush(stderr);

        if (*wuid_iter == wu_id){

            // BOINC-MR DEBUG
            fprintf(stderr, "[MR_jobtracker] ERROR: Work unit #%d already in list of available Reduce inputs.\n", wu_id);
            fflush(stderr);

            return false;
        }
        else{
            wuid_iter++;
        }
    }

    // BOINC-MR DEBUG
    fprintf(stderr, "[MAPREDUCE_JOB::mr_add_output_avail()] Work unit #%d not present in list of available Reduce inputs; Adding it.\n", wu_id);
    fflush(stderr);

    mr_hashed_output_files.push_back(wu_id);
    return true;
}


// Add Host with ID "host_id" to list of users serving output files for MapReduce job with ID "job_id"
int MAPREDUCE_JOB_SET::mr_add_host_job(int host_id, int job_id){
    MAPREDUCE_JOB* mr_j;
    bool found = false;
    std::vector<MAPREDUCE_JOB*>::iterator iter;

    log_messages.printf(MSG_DEBUG, "[BOINC-MR] Inside mr_add_host_job() Adding Host [#%d] to list of file servers for MR JOB #%d\n", host_id, job_id);

    // Lookup job with this ID. If not found, report error
    for (iter = mr_jobs.begin(); iter != mr_jobs.end(); iter++){
        if ((*iter)->mr_job_id == job_id){
            mr_j = (*iter);
            found = true;
            break;
        }
    }

    if (!found){
        log_messages.printf(MSG_DEBUG, "[BOINC-MR] MAPREDUCE_JOB_SET::mr_add_host_job - ERROR: Did not find a MapReduce job with id: %d\n", job_id);
        return 1;
    }

    return mr_j->mr_add_host(host_id);
}

// Add host with ID "host_id" to list of users serving Map outputs on this MR job
//int MAPREDUCE_JOB::mr_add_host(DB_HOST* h){
int MAPREDUCE_JOB::mr_add_host(int host_id){
    int i;
    std::vector<int>::iterator iter;

    log_messages.printf(MSG_DEBUG, "[BOINC-MR] Inside MR_JOB::mr_add_host()\n");

    // Check if this host is not already in the list
    for (iter = mr_mappers_serving.begin(); iter != mr_mappers_serving.end();){

        // Already here, no need to add it again
        if ((*iter) == host_id){
            // BOINC-MR DEBUG
            log_messages.printf(MSG_DEBUG, "[BOINC-MR] MAPREDUCE_JOB::mr_add_host() - [HOST #%d] already in list of serving Mappers\n", host_id);
            return 0;
        }
        else{
             ++iter;
        }
    }
    mr_mappers_serving.push_back(host_id);
    // BOINC-MR DEBUG
    log_messages.printf(MSG_DEBUG, "[BOINC-MR] MAPREDUCE_JOB::mr_add_host() - Added [HOST #%d] to list of serving Mappers\n", host_id);
    return 0;
}

/// BOINC-MR TODO : user must send within <Result> the output port

// Receive xml_doc_out as input and check if host is serving files
/// BOINC-MR TODO : check which files are being uploaded, and add them accordingly to the correspoding
/// MAPREDUCE_JOB
int MAPREDUCE_JOB_SET::mr_check_files_served(char* buf, int job_id, int host_id){
    char *begin_tag, *end_tag, *p, *q;
    //std::vector<char*> files_served;
    // change to string instead of char pointer
    std::vector<std::string> files_served;
    int f_jobid;    // FILE_INFO's MR job id
    char mapreduce_info[1024], f_name[256];
    char *mr_info_unescaped;
    int user_output_port, len, num_file_infos = 0;
    MAPREDUCE_JOB *mr_j;
    // int destlen = 1024;
    //char mapreduce_info[destlen];

    begin_tag = buf;
    while (begin_tag = strstr(begin_tag, "<file_info>")){
        if (end_tag = strstr(begin_tag, "</file_info>")){
            num_file_infos++;
            // Inside a FILE_INFO tag. Parse Mapreduce elements
            p = strstr(begin_tag, "<mapreduce>");
            // All files for this result are part of a MapReduce job (although uploading them is optional)
            if (!p) return ERR_XML_PARSE;
            p = strchr(p, '>');
            p++;
            // after <mapreduce> tag
            // get pointer at the end of mapreduce tag
            q = strstr(p, "</mapreduce>");
            // If there is no end tag, or tag is outside of this file_info, return error
            if (!q || atoi(q) > atoi(end_tag)) return 1;
            len = (int)(q-p);
            if (len >= sizeof(mapreduce_info)) len = sizeof(mapreduce_info)-1;
            memcpy(mapreduce_info, p, len);
            mapreduce_info[len] = 0;
            xml_unescape(mapreduce_info, mr_info_unescaped, sizeof(mapreduce_info));
            // mr_info_unescaped contains all the data inside <mapreduce> tag

            // parse job_id - if not found, return error
            // f_jobid contains id of this file's MR job id - important to know to which MAPREDUCE_JOB to associate the files with
            if (!parse_int(mr_info_unescaped, "<job_id>", f_jobid)){
                log_messages.printf(MSG_NORMAL, "[MAPREDUCE_JOB_SET::mr_check_files_served] Error: "
                                    "File_info has <mapreduce> tag but no job id <job_id>: %s\n", mr_info_unescaped);
                return ERR_XML_PARSE;
            }
            // if not the same as Result's id, return error
            if (f_jobid != job_id){
                log_messages.printf(MSG_NORMAL, "[MAPREDUCE_JOB_SET::mr_check_files_served] Error: "
                                    "File_info's job id [%d] does not match Result's job id: [%d] | Info: %s\n", f_jobid, job_id, mr_info_unescaped);
                return ERR_XML_PARSE;
            }

            // Obtain this file_info's name
            //if (!parse_str(p, "<name>", f_name, sizeof(f_name))){     - wrong / deprecated
            if (!parse_str(mr_info_unescaped, "<name>", f_name, sizeof(f_name))){
                log_messages.printf(MSG_NORMAL, "[MAPREDUCE_JOB_SET::mr_check_files_served] Error: "
                                    "No name tag found for FILE_INFO: %s\n", buf);
                return ERR_XML_PARSE;
            }

            /// BOINC-MR TODO : Error? Or just assume there is a default?
            if (!parse_int(mr_info_unescaped, "<output_port>", user_output_port)){
                log_messages.printf(MSG_NORMAL, "[MAPREDUCE_JOB_SET::mr_check_files_served] Error: "
                                    "No output port defined for FILE_INFO %s: %s\n", f_name, mr_info_unescaped);
                return ERR_XML_PARSE;
            }

            // check if this file is being uploaded
            if (!match_tag(buf, "<mr_file_uploading/>")){
                // BOINC-MR DEBUG
                log_messages.printf(MSG_DEBUG, "[MAPREDUCE_JOB_SET::mr_check_files_served] "
                                    "FILE_INFO %s not being uploaded by host ID#%d: %s\n", f_name, host_id, mr_info_unescaped);

                // move string - search only from after end_tag
                // needed for next loop cycle (start searching after last file info closing tag </file_info>
                begin_tag = strchr(end_tag, '>');
                begin_tag++;
                continue;
            }

            // go through MR jobs, to find the one identified by job_id and add this file and output port for this user
            mr_j = mr_get_job(job_id);
            // could not find object with this ID - error
            if (!mr_j){
                log_messages.printf(MSG_NORMAL, "[MAPREDUCE_JOB_SET::mr_check_files_served] ERROR: "
                                    "No MAPREDUCE_JOB object found in mr_jobs with ID#%d\n", job_id);
                return 1;
            }
            files_served.push_back(f_name);
            /// BOINC-MR TODO : only add to MR JOB after confirming that all files are being served?
            //mr_j->mr_add_file_served_by_user(f_name, host_id, user_output_port);

            // move string - search only from after end_tag
            // needed for next loop cycle (start searching after last file info closing tag </file_info>
            begin_tag = strchr(end_tag, '>');
            begin_tag++;

            // BOINC-MR DEBUG
            log_messages.printf(MSG_DEBUG, "MAPREDUCE_JOB_SET::mr_check_file_serving() "
                                "Successfully parsed file_info tag for file %s from MR job ID#%d. File being served on port %d\n", f_name, f_jobid, user_output_port);

        }
        // Parse error
        else{
            log_messages.printf(MSG_NORMAL, "MAPREDUCE_JOB_SET::mr_check_file_serving() ERROR parsing XML. Unclosed FILE_INFO tag.\n");
            return ERR_XML_PARSE;
        }
    }

    // Add files served to MR JOB
    if (num_file_infos != files_served.size()){
        log_messages.printf(MSG_NORMAL, "MAPREDUCE_JOB_SET::mr_check_file_serving() ERROR: User not serving all Map outputs: %s.\n", buf);
        return 1;
    }
    // Add this host to list of serving users
    else{
        mr_j->mr_add_host(host_id);
        return 0;
    }
    ///
    // mr_j->mr_add_file_served_by_user(f_name, h, user_output_port);

    //return 0;
}

// return MAPREDUCE_JOB with given id
MAPREDUCE_JOB* MAPREDUCE_JOB_SET::mr_get_job(int id){
    vector<int>::size_type sz = mr_jobs.size();
    //MAPREDUCE_JOB* temp;
    int i;

    for (i=0; i<sz; i++){
        //temp = mr_jobs[i];
        //if (temp->mr_job_id == id)
        if (mr_jobs[i]->mr_job_id == id)
            return mr_jobs[i];
    }
    return NULL;
}

// Check if output for WU with ID "wuid" from MR job "jobid" is available for Reduce phase
bool MAPREDUCE_JOB_SET::mr_check_input_job(int wuid, int jobid){
    vector<int>::size_type sz = mr_jobs.size();
    //MAPREDUCE_JOB* temp;
    int i;

    for (i=0; i<sz; i++){
        //temp = mr_jobs[i];
        if (mr_jobs[i]->mr_job_id == jobid)
            return mr_jobs[i]->mr_is_input_available(wuid);
    }

    return false;
}

// return true if this WU is already available for input
// Called by Transitioner to check whether Map output has already been copied to input directory
bool MAPREDUCE_JOB::mr_is_input_available(int wuid){
    bool found = false;
    std::vector<int>::iterator iter;

    for (iter = mr_available_inputs.begin(); iter != mr_available_inputs.end();){
        if ((*iter) == wuid){
            found = true;
            break;
        }
        else{
             ++iter;
        }
    }

    return found;
}


// ** Only needed for hashed wu (mr_hashed = true), to save information on results sent to normal BOINC clients, that are unable to upload map outputs (return the file itself) **
// return true if this WU already has an output file available at server
// Called by Transitioner to check whether this wu already moved the output file to the input dir, or if it only saved hashes, for example
bool MAPREDUCE_JOB::mr_is_output_file_available(int wuid){
    bool found = false;
    std::vector<int>::iterator iter;

    for (iter = mr_hashed_output_files.begin(); iter != mr_hashed_output_files.end();){
        if ((*iter) == wuid){
            found = true;
            break;
        }
        else{
             ++iter;
        }
    }

    return found;
}


/// BOINC-MR TODO : create 3 column table where each row corresponds to a user (user_id | output_port | list_served_files)
/// OR hashtable with file_name as key => (file_name | list_serving_users) AND list of (user - output_port) binaries
/// UNNECESSARY - INCORRECT/INVALID TO ONLY SERVE PART OF MAP OUTPUTS
int MAPREDUCE_JOB::mr_add_file_served_by_user(char* f_name, int host_id, int user_output_port){


}


/*
Create work example
http://boinc.berkeley.edu/trac/wiki/WorkGeneration
*/

// Arguments
// - wu_name: list of names from Map Work Units of this MR job [mr_map_wu_names]
int mr_create_reduce_work(char* result_template_file, char* wu_template_file, char* app_name
                    ,int num_inputs, int num_reducers, std::vector<std::string> wu_name, int reduce_wu_index, MAPREDUCE_JOB* mrj, bool red_dist_val){

    /* initialize random seed: */
    srand (time(NULL));

    // BOINC-MR DEBUG
    //log_messages.printf(MSG_DEBUG, "[mr_jobtracker.cpp] Entered mr_create_reduce_work()...\n");
    //fflush(stderr);
    DB_APP app;
    DB_WORKUNIT wu;
    int retval = 0;
    /// BOINC-MR TODO : cleanup - hack to allow wu.xml_doc to be larger
    ///char wu_template[BLOB_SIZE];
    char wu_template[MEDIUM_BLOB_SIZE];
    char result_template_path[256];
    //const char result_template_path[256];
    char buf[256];

    const char* infiles[num_inputs];
    //char* infiles[num_inputs];

    /// Unnecessary?
    strncpy(app.name, app_name, (sizeof(app.name)-1));
    app.name[sizeof(app.name)-1]='\0';

    // BOINC-MR
    //log_messages.printf(MSG_CRITICAL, "Inside mr_create_reduce_work. App name: %s\n", app.name);
    //fflush(stderr);

    /// Already done by Transitioner

    sprintf(buf, "where name='%s'", app.name);
    retval = app.lookup(buf);
    if (retval) {
        log_messages.printf(MSG_CRITICAL, "Error creating Reduce WU: app not found: %s\n", app.name);
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

    // D_VAL
    wu.validating_wuid = -1;

    char partial_reduce_wu_name[256];
    // Create base WU name - app name and this MR job's index (MISSING: index of current reducer - each Reducer has 1 unique Work Unit)
    sprintf(partial_reduce_wu_name, "%s_%d", app_name, reduce_wu_index);

    //sprintf(wu.name, "%s_%d", app_name, reduce_wu_index);

    //config.project_path(result_template_file);

    //const char *temp_path = config.project_path("%s", result_template_file);
    //strcpy(result_template_path, config.project_path(result_template_file));
    //strcat(result_template_path, result_template_file);

    /// Error here: config.project_path returns a constant char* - cannot change it
    /// Possible solution: char *result_template_path = config.project_path();
    strncpy(result_template_path, config.project_path(result_template_file), (sizeof(result_template_path)-1));
    result_template_path[sizeof(result_template_path)-1] = '\0';

    for (int i=0; i<num_reducers; i++) {
        // create WU full name - add current reducer's index
        sprintf(wu.name, "%s_%d", partial_reduce_wu_name, i);

        // reset ID (otherwise, it would only update wu created in previous iteration
        wu.id = 0;
        // Create "num_inputs" input files, with each name composed of WU_i
        //
        // Naming convention: MAPWUNAME_i (index within total number of reduces)
        // E.g: "mapred_map_3-min_wu_1_[0/1/2]" (map workunit name: mapred_map_3-min_wu_1)
        // Inputs for same MR job:
        // Reduce task #1 - [mapred_map_3-min_wu_1]_1, [mapred_map_3-min_wu_3_1], etc. ;
        // Reduce task #2 - [mapred_map_3-min_wu_1]_2, [mapred_map_3-min_wu_3_2], etc.
        //

        // BOINC-MR DEBUG
        log_messages.printf(MSG_CRITICAL, "Inside mr_create_reduce_work. Creating WU for Reducer #%d [num_red: %d]\n", i, num_reducers);
        fflush(stderr);
        //log_messages.printf(MSG_DEBUG, "Number of names in vector wu_name: %d\n", wu_name.size());
        //fflush(stderr);

        char *input_name = new char[256];
        for (int j=0; j<num_inputs; j++){

            log_messages.printf(MSG_DEBUG, "Wu_name[j]: %s | j: %d | Num_inputs: %d\n", wu_name[j].c_str(), j, num_inputs);
            fflush(stderr);

            sprintf(input_name, "%s_%d", wu_name[j].c_str(), i);

            // BOINC-MR DEBUG
            log_messages.printf(MSG_CRITICAL, "Inside mr_create_reduce_work. Created input name: %s. Now copying it to infiles array.\n", input_name);
            fflush(stderr);

            infiles[j] = input_name;
            //strcpy(infiles[j], input_name);

            // BOINC-MR
            log_messages.printf(MSG_CRITICAL, "Inside mr_create_reduce_work. Reading input #%d: \"%s\" for Reduce WU #%d."
                                " Infile[%d]: %s\n", j, input_name, i, j, infiles[j]);
            fflush(stderr);
            input_name = new char[256];
        }

        // BOINC-MR DEBUG
        for (int m=0; m<num_inputs; m++){
            log_messages.printf(MSG_NORMAL, "Infile[%d]: %s\n", (m), infiles[m]);

        }
        ///log_messages.printf(MSG_CRITICAL, "Inside mr_create_reduce_work. Calling create_work()\n"
        ///                    "result_template_file: %s || result_template_path: %s | infiles size/num_inputs: %d\n",
        ///                    result_template_file, result_template_path, num_inputs);
        ///fflush(stderr);
        //log_messages.printf(MSG_DEBUG, "WU Template before calling create_work()\n", wu_template);
        //log_messages.printf(MSG_DEBUG, "%s\n", wu_template);
        //fflush(stderr);

        // If this is a hashed job edit reduce wu template before creating each workunit - i = index of reduce wu
        if (mrj->mr_hashed){
            retval = mr_edit_reduce_template(i, mrj, wu_template);
            if (retval){
                log_messages.printf(MSG_CRITICAL,
                    "mr_create_reduce_work() - Error: unable to edit Reduce WU template: %d\n", retval
                );

                // BOINC-MR
                fflush(stderr);

                return retval;
            }
        }

        // if this reduce work unit is supposed to be valdiated by clients (dist_val is true), then create a unique, random alternative name for each reduce workunit
        if (red_dist_val){
            create_unique_alt_name(wu.alt_name, OBFUSC_ALT_NAME_NUM_CHARS);

            log_messages.printf(MSG_DEBUG,
                "mr_create_reduce_work() - created unique alt name: %s\n", wu.alt_name
            );

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
                config                  // SCHED_CONFIG&
                //,
                //command_line,           // optional (= NULL)
                //additional_xml          // optional (= NULL)
                );

        delete [] input_name;
        input_name = NULL;

        if (retval) {
            log_messages.printf(MSG_CRITICAL,
                "mr_create_reduce_work() - can't create Reduce Work Unit: %d\n", retval
            );

            // BOINC-MR
            fflush(stderr);

            //exit(retval);
            return retval;
        }

        /// BOINC-MR DEBUG
        log_messages.printf(MSG_DEBUG, "mr_create_reduce_work() - returned from create_work(): WU ID #%d", wu.id);
    }

    log_messages.printf(MSG_DEBUG, "[mr_jobtracker.cpp] Leaving mr_create_reduce_work(). Returning 0...\n");
    fflush(stderr);

    return 0;
}

// insert "text" right after "after" in the given buffer
//
/*int insert_after(char* buffer, const char* after, const char* text) {
    char* p;
    char temp[BLOB_SIZE];

    if (strlen(buffer) + strlen(text) > BLOB_SIZE-1) {
        log_messages.printf(MSG_CRITICAL,
            "insert_after: overflow: %d %d\n", strlen(buffer), strlen(text)
        );
        return ERR_BUFFER_OVERFLOW;
    }
    p = strstr(buffer, after);
    if (!p) {
        log_messages.printf(MSG_CRITICAL,
            "insert_after: %s not found in %s\n", after, buffer
        );
        return ERR_NULL;
    }
    p += strlen(after);
    strcpy(temp, p);
    strcpy(p, text);
    strcat(p, temp);
    return 0;
}*/


void replace_file_name(char* xml_doc, char* filename, char* new_filename) {
    /// BOINC-MR DEBUG : cleanup - hack for larger wu.xml_doc
    ///char buf[BLOB_SIZE], temp[256];
    char buf[MEDIUM_BLOB_SIZE], temp[256];
    char * p;

    strcpy(buf, xml_doc);
    p = strtok(buf,"\n");
    while (p) {

    }
}


/// BOINC-MR TODO : check for out of bounds exception when writing in wu_template_temp
/// - use variable to count size of each string added to array
// @red_ind : reduce index - to find corresponding hash and file size info in hashed_outputs [mapper][red_ind]
// @ wu_template : char array with reduce wu template contents
int mr_edit_reduce_template(int red_ind, MAPREDUCE_JOB* mrj, char* wu_template){
    int retval;
    //char wu_template_file[256];
    char wu_template_temp[MEDIUM_BLOB_SIZE];
    ///char buf[256];
    char tag[256];
    int fnumber;
    hash_out h_out;
    char p2p[64];
    char* buf;     // token that holds each line of wu template contents
    sprintf(p2p, "  <url>P2P</url>\n");
    int mapper_index;       // map output index (out of total number of mappers - starts at 0)
    char* temp_index;
    // get list of map output workunit names in order to identify each output's mapper index
    std::vector<std::string> tmp_str = mrj->get_mr_map_wu_names();


    // BOINC-MR DEBUG
    log_messages.printf(MSG_DEBUG, "EXISTING template DATA [should be NULL] [length %zu]:\n %s\n",
                        strlen(wu_template_temp), wu_template_temp);

    memset(wu_template_temp, 0, sizeof(wu_template_temp));

    log_messages.printf(MSG_DEBUG, "After memset [length %zu]:\n %s\n",
                        strlen(wu_template_temp), wu_template_temp);


    // BOINC-MR DEBUG
    double bytes_written = 0;

    // go through template and save new content in wu_template_temp
    // read it line by line until we find the <number> tag
    buf = strtok (wu_template,"\n");
    while (buf!=NULL){

        // BOINC-MR DEBUG
        //fprintf(stderr,"Read [buf]: %s\n", buf);
        //fflush(stderr);


    ///while (fgets(buf, sizeof(buf), f)){

        if (parse_int(buf, "<number>", fnumber)){
            strncat(wu_template_temp, buf, strlen(buf));
            strncat(wu_template_temp, "\n", sizeof("\n"));

            // check if number of inputs is correct [sanity check - should never reach this stage with this error]
            if (fnumber >= tmp_str.size()){
                log_messages.printf(MSG_CRITICAL,
                    "mr_edit_reduce_template() - ERROR: File number in reduce wu template [<number>%d</number>] "
                    "larger than number of map outputs: %zu. Possible error in mr_jobtracker.xml\n",
                    fnumber, tmp_str.size()
                );
                return -1;
            }

            // IMPORTANT: <number> may not correspond to index of map output - get index from map wu names:
            //      - mr_map_$APP_wu_$INDEX
            //      - e.g.: mr_map_word_counth_wu_6       [map output number 6 (7th mapper)]
            // order of map wu names is the same as <number> tags - first map wu output on the list appears first in the reduce wu template
            temp_index = strrchr(strdup(tmp_str[fnumber].c_str()), '_');
            temp_index++;
            mapper_index = atoi(temp_index);

            /// BOINC-MR DEPRECATED
            //h_out = mrj->hashed_outputs[fnumber][red_ind];

            // D_VAL
            // check if the hashes were returned - if the reduce WU was chosen for distributed validation, they were not returned
            if (mrj->hashed_outputs[mapper_index].size() > 0){

                h_out = mrj->hashed_outputs[mapper_index][red_ind];
                sprintf(tag, "  <nbytes>%f</nbytes>\n", h_out.fsize);

                log_messages.printf(MSG_DEBUG,
                    "mr_edit_reduce_template() - file number: %d | red_ind: %d | Wrote nbytes: %f\n",
                    //fnumber, red_ind, h_out.fsize
                    mapper_index, red_ind, h_out.fsize
                );

                //strncat(wu_template, tag, sizeof(tag));
                strncat(wu_template_temp, tag, strlen(tag));
                sprintf(tag, "  <md5_cksum>%s</md5_cksum>\n", h_out.hash_md5);
                //strncat(wu_template, tag, sizeof(tag));
                strncat(wu_template_temp, tag, strlen(tag));

                log_messages.printf(MSG_DEBUG,
                    "mr_edit_reduce_template() - Wrote md5 hash for file #%d [mr_red_word_counth_0_%d]: %s\n",
                    //fnumber, red_ind, h_out.hash_md5
                    mapper_index, red_ind, h_out.hash_md5
                );

                // all elementes must be present (or none): URL/nbytes/md5_cksum
                //strncat(wu_template, "<url>P2P</url>\n", sizeof("<url>P2P</url>\n"));
                strncat(wu_template_temp, p2p, strlen(p2p));
            }
            // D_VAL DEBUG
            else{
                // distributed validation reduce workunit (no hashes available)
                log_messages.printf(MSG_DEBUG,
                    "mr_edit_reduce_template() - mrj->hashed_outputs[%d] (mapper_index) is empty. D_VAL Reduce workunit.\n",
                    //fnumber, red_ind, h_out.hash_md5
                    mapper_index
                );

            }

            buf = strtok(NULL,"\n");
            continue;
        }
        // ignore possible existing values for <nbytes> and <md5_cksum>
        if (match_tag(buf, "<nbytes>")){
            buf = strtok(NULL,"\n");
            continue;
        }
        if (match_tag(buf, "<md5_cksum>")){
            buf = strtok(NULL,"\n");
            continue;
        }
        if (match_tag(buf, "</md5_cksum>")){
            buf = strtok(NULL,"\n");
            continue;
        }
        // url may have been set on purpose [ADICS, for e.g.] -  leave it unless it has exactly P2P as content
        if (parse_str(buf, "<url>", tag, sizeof(tag))){

            // BOINC-MR DEBUG
            log_messages.printf(MSG_DEBUG, "Found <url> tag in reduce template: %s \n", tag);

            // if URL is not "P2P", keep it
            if (strcmp(tag, "P2P")){

                // BOINC-MR DEBUG
                log_messages.printf(MSG_DEBUG,
                    "Different from P2P: %s \n", tag);

                buf = strtok(NULL,"\n");
                continue;

                // BOINC-MR DEBUG
                bytes_written += strlen(buf) + sizeof("\n");

                strncat(wu_template_temp, buf, strlen(buf));
                strncat(wu_template_temp, "\n", sizeof("\n"));
            }
            buf = strtok(NULL,"\n");
            continue;
        }

        // BOINC-MR DEBUG
        bytes_written += strlen(buf) + sizeof("\n");

        // save all the rest
        strncat(wu_template_temp, buf, strlen(buf));
        strncat(wu_template_temp, "\n", sizeof("\n"));
        buf = strtok(NULL,"\n");
    }
    // close file and open for writing (to overwrite file)
    ///fclose(f);
    ///f = fopen(config.project_path(wu_template_file), "w");
    ///if (!f) return ERR_FOPEN;

    // write everything back (from wu_template) into Reduce template file
    /*size_t s_len = strlen(wu_template);
    size_t n = fwrite(wu_template, strlen(wu_template), 1, f);

    if (n != 1){
        log_messages.printf(MSG_CRITICAL,
                "mr_edit_reduce_template() - Error [fwrite returned !=1] writing to Reduce template file: %s. \n",
                wu_template_file
            );
        fclose(f);
        return ERR_FWRITE;
    }
    fclose(f);*/

    // BOINC-MR DEBUG
    log_messages.printf(MSG_DEBUG,
            "Size of wu_template: %zu || strlen: %zu || strlen (temp): %zu || sizeof (temp): %zu\n",
            //Edited template:\n %s\n",
            sizeof(wu_template), strlen(wu_template), strlen(wu_template_temp), sizeof(wu_template_temp)
            //, wu_template_temp
    );

    // do not check if template is correct - leave that for create_work()
    // copy contents back into correct char array
    strncpy(wu_template, wu_template_temp, strlen(wu_template_temp));
    strcpy(wu_template_temp, "");

    // BOINC-MR DEBUG
    log_messages.printf(MSG_DEBUG,
            "Size of wu_template AFTER strncpy: %lu || strlen: %zu\n", sizeof(wu_template), strlen(wu_template));

    return 0;
}



// open Map output files containing hash [output itself kept in client, only its hash and file fsize returned to server inside single file]
// copy file size (first line) and corresponding hash (remaining lines) to hash_output array
// Number of outputs = 2 * Num reducers:
// - first half corresponds to outputs that stayed in client;
// - second half corresponds to output containing file size and hash
//
// @result : result obtained from database
// @ wu_name : required to get mapper #
// @ mrj : MR job

/// TODO: add option to support both MR and normal clients who have returned the actual file (not just the hash)

int mr_save_hashed_outputs(DB_RESULT result, char* wu_name, MAPREDUCE_JOB* mrj){
    char* p;
    char filename[256], buf[BLOB_SIZE];
    std::vector<const char*> output_filepaths;        // to store each parsed filename's path
    int i;              // iterator to check when we have passed the halfway line (to the returned outputs)
    char pathname[512];
    bool error = false;
    int retval;

    // parse xml_doc_in (similar to mr_copy_map_outputs)
    strcpy(buf, result.xml_doc_in);
    p = strtok(buf,"\n");
    i = 0;
    while (p) {
        if (parse_str(p, "<name>", filename, sizeof(filename))) {
        } else if (match_tag(p, "<file_info>")) {

            strcpy(filename, "");
            strcpy(pathname, "");
            strdup("");
        } else if (match_tag(p, "</file_info>")) {

            // only check for file if this is on the later half of results (returned to server)
            if (i < mrj->get_num_reduce_tasks()){


                /// TODO: check if this file was returned by client
                /// - compare hash returned to file's checksum;
                /// - validate against other wu?

                /// BOINC-MR DEBUG
                log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: mr_save_hashed_outputs() - File [%s] part of outputs not returned to server [index: %d]\n", filename, i);
                i++;
                p = strtok(0, "\n");
                continue;
            }

            // BOINC-MR DEBUG
            fprintf(stderr, "mr_save_hashed_outputs() i:%d >= %d [number of red tasks]. File: %s\n", i, mrj->get_num_reduce_tasks(),
                    pathname);

            // save file's path in pathname
            dir_hier_path(filename, config.upload_dir, config.uldl_dir_fanout, pathname, true);
            if (boinc_file_exists(pathname)) {
                retval = 0;
            }
            else{
                // file was not found
                error = true;
                // check if dir the file is supposed to be in exists
                char* p = strrchr(pathname, '/');
                *p = 0;
                if (boinc_file_exists(pathname)) {
                    retval = ERR_NOT_FOUND;
                }
                else
                    retval = ERR_OPENDIR;
            }
            if (retval == ERR_OPENDIR) {
                log_messages.printf(MSG_CRITICAL,
                    "BOINC-MR: mr_save_hashed_outputs() - [RESULT#%d] missing dir for %s\n",
                    result.id, pathname
                );
            } else if (retval) {
                // result not found
                log_messages.printf(MSG_CRITICAL,
                    "BOINC-MR: mr_save_hashed_outputs() - [RESULT#%d] outcome=%d client_state=%d No Map output file %s to copy to input dir\n",
                    result.id, result.outcome, result.client_state, filename
                );
            } else {
                // add to vector
                output_filepaths.push_back(strdup(pathname));
                /// BOINC-MR DEBUG
                log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: mr_save_hashed_outputs() - Added %s to output filepaths.\n", pathname);
            }
        }       // end of match_tag ("</file_info>")
        p = strtok(0, "\n");
    }

    // If there was an error while parsing
    /// BOINC-MR TODO : try different output if there was an error finding canonical result?
    if (error) return 1;

    // Check if number of files parsed corresponds to the number of Reduce tasks.
    if (output_filepaths.size() != mrj->get_num_reduce_tasks()){
        log_messages.printf(MSG_CRITICAL, "BOINC-MR Error: number of files parsed from [Result #%d] does"
                            " not match number of Reduce tasks for job #%d. Num files: %zu | Num Reducers: %d\n",
                            result.id, mrj->mr_job_id, output_filepaths.size(), mrj->get_num_reduce_tasks());
        return 1;
    }

    // at this point, only hash output files are stored in output_filepaths. Go through each one, read its content and
    // save the information in the corresponding position in the hashed_outputs array
    //char hashed_output[256];        // 256 bits is enough - hash (max 64 bits) + file size (number)
    char hash_buf[256];
    FILE* f;
    int index;
    hash_out h_out;
    char* ind;
    for (int j=0; j<output_filepaths.size(); j++){
        //h_out = new hash_out;     // unnecessary - like int/double, only need to set new values. not a pointer - does not overwrite previous ones
        f = fopen(output_filepaths[j], "r");
        if (!f){
            perror("Unable to open file");
            log_messages.printf(MSG_CRITICAL,
                    "BOINC-MR: mr_save_hashed_outputs() - Errno: %d. Unable to open file: %s\n",
                    errno, output_filepaths[j]
            );
            return ERR_FOPEN;
        }

        // read first line to get file size
        if (fgets(hash_buf, 256, f) != NULL) {
            h_out.fsize = atof(hash_buf);
            //fsize = atoi(hash_buf);      // unnecessary - write to hash_out struct immediately
            // BOINC-MR DEBUG
            log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: mr_save_hashed_outputs() - Read file size [%f] for file: %s\n",
                    h_out.fsize, output_filepaths[j]
            );
        }
        else{
            log_messages.printf(MSG_CRITICAL,
                    "BOINC-MR: mr_save_hashed_outputs() - Unable to read file [Map hashed output]: %s\n",
                    output_filepaths[j]
            );
            fclose(f);
            return ERR_FREAD;
        }
        // read second line to get hash
        if (fgets(hash_buf, 256, f) != NULL) {
            strncpy(h_out.hash_md5, hash_buf, sizeof(h_out.hash_md5));
            //strncpy(hashed_output, hash_buf, 256);      // unnecessary - write to hash_out struct immediately
            // BOINC-MR DEBUG
            log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: mr_save_hashed_outputs() - Read hash [%s] for file: %s\n",
                    h_out.hash_md5, output_filepaths[j]
            );
        }
        else{
            log_messages.printf(MSG_CRITICAL,
                    "BOINC-MR: mr_save_hashed_outputs() - Unable to read hash in file [Map hashed output]: %s\n",
                    output_filepaths[j]
            );
            fclose(f);
            return ERR_FREAD;
        }

        // save info on correct position in hashed_outputs vector
        // hashed_outputs [mapper#][reducer#]
        // get map index (from wu name) - number after last '_' character. E.g.: mr_map_word_count_wu_99 => index=99
        ind = strrchr(wu_name, '_');
        ind++;
        index = atoi(ind);
        mrj->hashed_outputs[index].push_back(h_out);

        /// BOINC-MR DEBUG
        hash_out temp_h_out;
        h_out = mrj->hashed_outputs[index][j];
        log_messages.printf(MSG_DEBUG,
            "mr_save_hashed_outputs() - index[mapper#%d][red#%d] | Wrote nbytes: %f | hash: %s\n",
            //fnumber, red_ind, h_out.fsize
            index, j, h_out.fsize, h_out.hash_md5
        );

        fclose(f);
    }

    //fclose(f);

    return 0;
}

/// BOINC-MR TODO
/// check for output file location - done?

// Naming convention: #MAPWUNAME_#OUTPUTINDEX - e.g. mapred_map-3min_X_0/1/2
// Put it at the right place in the download dir hierarchy
// Number of reduce tasks = number of Map outputs / partitions

// parse xml_doc_in from Result to get filenames of Map Outputs and
// copy these files to input directory
/*
* @copy_only - true if this wu was already added to list of reduce inputs, as hashes, but the map output files were not copied to input directory
*/
int mr_copy_map_outputs(DB_RESULT result, char* wu_name, MAPREDUCE_JOB* mrj, bool copy_only){
//int mr_copy_map_outputs(DB_RESULT result, std::string , MAPREDUCE_JOB* mrj){
    char* p;
    char filename[256], buf[BLOB_SIZE];
    std::vector<const char*> output_filepaths;        // to store each parsed filename's path
    //std::vector<std::string> output_filepaths;        // to store each parsed filename's path
    bool error = false;
    int retval;
    char pathname[256];

    bool file_is_hash = false;      // needed to avoid adding hashes to list of output files

    // parse xml_doc_in (similar to result_delete_files() from file_deleter.cpp
    strcpy(buf, result.xml_doc_in);
    p = strtok(buf,"\n");

    /// BOINC-MR DEBUG
    log_messages.printf(MSG_DEBUG,
        "BOINC-MR: mr_copy_map_outputs() - Parsing xml_doc_in:\n%s",
        buf
    );

    while (p) {

        /// BOINC-MR DEBUG
        fprintf(stderr, "%s\n", p);
        fflush(stderr);

        if (parse_str(p, "<name>", filename, sizeof(filename))) {
        } else if (match_tag(p, "<file_info>")) {
            file_is_hash = false;
            strcpy(filename, "");
            strcpy(pathname, "");
        // check if this is an output file or rather its hash - identified by tag <mr_hash/>
        } else if (match_tag(p, "<mr_hash/>")) {
            file_is_hash = true;
        } else if (match_tag(p, "</file_info>")) {
            // ignore if this is hash
            if (file_is_hash){

                /// BOINC-MR DEBUG
                fprintf(stderr, "File is hash - continue;\n", p);
                fflush(stderr);

                //file_is_hash = false;
                p = strtok(0, "\n");
                continue;
            }

            /// BOINC-MR DEBUG
            fprintf(stderr, "Continue?\n", p);
            fflush(stderr);

            // save file's path in pathname
            dir_hier_path(filename, config.upload_dir, config.uldl_dir_fanout, pathname, true);
            if (boinc_file_exists(pathname)) {
                retval = 0;
            }
            else{

                /// BOINC-MR DEBUG
                log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: mr_copy_map_outputs() - Could not find file in %s\n",
                    pathname
                );

                error = true;
                char* p = strrchr(pathname, '/');
                *p = 0;
                if (boinc_file_exists(pathname)) {
                    retval = ERR_NOT_FOUND;
                }
                else
                    retval = ERR_OPENDIR;
            }
            if (retval == ERR_OPENDIR) {
                //error = true;
                log_messages.printf(MSG_CRITICAL,
                    "BOINC-MR: mr_copy_map_outputs() - [RESULT#%d] missing dir for %s\n",
                    result.id, pathname
                );
            } else if (retval) {
                // result not found
                log_messages.printf(MSG_CRITICAL,
                    "BOINC-MR: mr_copy_map_outputs() - [RESULT#%d] outcome=%d client_state=%d No Map output file %s to copy to input dir\n",
                    result.id, result.outcome, result.client_state, filename
                );
            } else {
                // add to vector
                output_filepaths.push_back(strdup(pathname));
                /// BOINC-MR DEBUG
                log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: mr_copy_map_outputs() - Added %s to output filepaths.\n", pathname);

                /// BOINC-MR TODO - check if adding pathname to vector and changing afterwards also changes previous values
            }

        }
        p = strtok(0, "\n");
    }

    // BOINC-MR
    log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: mr_copy_map_outputs() - After parsing Result's xml_doc_in. Num output files: %zu\n",output_filepaths.size());
    fflush(stderr);
    log_messages.printf(MSG_DEBUG,
                    "BOINC-MR: mr_copy_map_outputs() - After parsing Result's xml_doc_in. Num reduce tasks: %d | Error: %d\n",mrj->get_num_reduce_tasks(), error);
    fflush(stderr);

    // If there was an error while parsing
    /// BOINC-MR TODO : try different output if there was an error finding canonical result?
    if (error) return 1;


    // Check if number of files parsed corresponds to the number of Reduce tasks.
    // If not, there is an error
    if (output_filepaths.size() != mrj->get_num_reduce_tasks()){
        log_messages.printf(MSG_CRITICAL, "mr_copy_map_outputs() BOINC-MR Error: number of files parsed from [Result #%d] does"
                            " not match number of Reduce tasks for job #%d. Num files: %zu | Num Reducers: %d\n",
                            result.id, mrj->mr_job_id, output_filepaths.size(), mrj->get_num_reduce_tasks());
        return 1;
    }

    char in_name[256], path[512];
    int mthd_retval = 0;
    // copy each file (saved in output_filepaths) to new input directory
    for (int k = 0; k < output_filepaths.size(); k++){
        // set name for this Reduce input
        sprintf(in_name, "%s_%d", wu_name, k);

        // BOINC-MR
        log_messages.printf(MSG_DEBUG,
                        "BOINC-MR: mr_copy_map_outputs() - Copying output file. Input file name: %s\n", in_name);
        fflush(stderr);

        // get new input's file path - stored in path (calls dir_hier_path())
        retval = config.download_path(in_name, path);
        if (retval){
            log_messages.printf(MSG_CRITICAL,
                    "BOINC-MR: mr_copy_map_outputs() - Error: Could not create file %s as Reduce input\n",path
                );
            mthd_retval = retval;
            break;
        }

        retval = boinc_copy(output_filepaths[k], path);
        if (retval){
            log_messages.printf(MSG_NORMAL, "BOINC-MR : mr_copy_map_outputs() - Error: copying Map output file %s to input dir: %s", output_filepaths[k], path);
            mthd_retval = retval;
            break;
        }
        // BOINC-MR DEBUG
        else{
            log_messages.printf(MSG_NORMAL, "BOINC-MR : mr_copy_map_outputs() - SUCCESS - copied Map output file %s to input dir: %s\n", output_filepaths[k], path);
        }
    }

    // if there was an error
    if (mthd_retval){
        // BOINC-MR
        log_messages.printf(MSG_CRITICAL,
                        "BOINC-MR: mr_copy_map_outputs() - After moving output files, Error: %d.\n", mthd_retval);
        fflush(stderr);

        return retval;
    }

    // stop here, files were copied and
    if (copy_only){
        log_messages.printf(MSG_DEBUG, "*** copy_out *** After moving output files, no errors. Copy only, WU %s was already added to MR job\n", wu_name);
        return 0;
    }


    // BOINC-MR
    log_messages.printf(MSG_CRITICAL,
                    "BOINC-MR: mr_copy_map_outputs() - After moving output files, no errors. Adding WU %s to MR Job\n", wu_name);
    fflush(stderr);

    // BOINC-MR DEBUG
    log_messages.printf(MSG_DEBUG, "*** copy_out *** Before adding wu_name(char) [%s] to mr_map_wu_names. Size: %zu\n", wu_name, mrj->get_mr_map_wu_names().size());
    fflush(stderr);
    std::vector<std::string> tmp = mrj->get_mr_map_wu_names();
    for (int l=0; l<tmp.size();l++){
        log_messages.printf(MSG_DEBUG,
                    "*** BOINC-MR: copy_out *** [MR JOB #%d] Wu_name[%d]: %s.\n", mrj->mr_job_id, l, tmp[l].c_str());
                    fflush(stderr);
    }

    // add this WU's name to list - to be used when creating Reduce WU
    //std::string str = std::string(wu_name);
    std::string str = std::string(wu_name);
    //std::string str1 = "Test string ";   // c-string


        // BOINC-MR DEBUG
    log_messages.printf(MSG_DEBUG, "*** Created string str\n");
    fflush(stderr);
    /*log_messages.printf(MSG_DEBUG, "*** Size of wu_name: %d\n", sizeof(wu_name));
    fflush(stderr);
    log_messages.printf(MSG_DEBUG, "*** Last character of wu_name: %d\n", wu_name[sizeof(wu_name)]);
    fflush(stderr);
    //wu_name[sizeof(wu_name)-1] = '\0';
    //log_messages.printf(MSG_DEBUG, "*** Last character of wu_name (should be '\0'): %d\n", wu_name[sizeof(wu_name)]);
    //fflush(stderr);*/
    //log_messages.printf(MSG_DEBUG, "*** Created string str1: %s\n", str1.c_str());
    //fflush(stderr);
    log_messages.printf(MSG_DEBUG, "*** String [NO Variable - conversion]: %s\n", std::string(wu_name).c_str());
    fflush(stderr);
    log_messages.printf(MSG_DEBUG, "*** String: %s\n", str.c_str());
    fflush(stderr);

    mrj->mr_add_map_wu_name(str);
    //mrj->mr_add_map_wu_name(wu_name);

    // BOINC-MR DEBUG
    log_messages.printf(MSG_DEBUG, "*** copy_out *** After adding wu_name(string) [%s] to mr_map_wu_names. Size: %zu.\n", str.c_str(), mrj->get_mr_map_wu_names().size());
    fflush(stderr);


    return 0;

}

//void MAPREDUCE_JOB::mr_add_map_wu_name(char* wuname){
void MAPREDUCE_JOB::mr_add_map_wu_name(std::string wuname){
    /*char name[256], name2[256];
    strcpy(name, wuname, (sizeof(name)-1));
    name[sizeof(name)-1] = '\0';
    sprintf(name2, "%s", wuname);*/

    log_messages.printf(MSG_NORMAL,
                    "BOINC-MR: Inside mr_add_map_wu_name()\n");
    fflush(stderr);

    log_messages.printf(MSG_NORMAL,
                    "BOINC-MR: String wuname: %s\n", wuname.c_str());
    fflush(stderr);

    mr_map_wu_names.push_back(wuname);
}


// BOINC-MR
// xml_doc_in added to attribute fetched from DB
// return IP address of all hosts that have executed a valid Map workunit for this job and corresponding wu name
/// TODO: DEPRECATED - return list of addresses for specific reduce input (clients that executed a map workunit from this job)
/// DEPRECATED - result is required in query to connect host to workunit
/// mapreduce_jobs.mr_get_job(jobid);
/// DEPRECATED - return ALL hosts
// => return hosts that are hosting Map outputs
//      - user_hosting = 1;
//int DEPRECATED_enumerate_ip_mappers(std::vector<std::string>& ip_addrs, int jobid){
int enumerate_ip_mappers(std::vector<std::string>& ip_addrs, std::vector<std::string>& wu_names, int jobid){
    char query[MAX_QUERY_LEN];
    std::string ip_address, wu_name;
    int retval;
    unsigned int i;
    MYSQL_RES* rp;
    MYSQL_ROW row;
    //SCHED_RESULT_ITEM ri;
    DB_CONN* db = &boinc_db;

    sprintf(query,
        "SELECT DISTINCT"
        "   external_ip_addr, workunit.name "
        "FROM "
        "   result, workunit, host "
        "WHERE "
        "   host.id=result.hostid and workunit.id=result.workunitid "
        "   and mr_app_type=1 and user_hosting=1 and mr_job_id=%d "
        "ORDER BY"
        "   workunit.name asc",
        jobid
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
            ip_address = mr_parse_ip_addr(row);
            wu_name = mr_parse_wu_name(row);
            ip_addrs.push_back(ip_address);
            wu_names.push_back(wu_name);
        }
    } while (row);

    return 0;
}

// Parse IP address from DB query
///char external_ip_addr[256]; // IP address seen by scheduler
std::string mr_parse_ip_addr(MYSQL_ROW& r){
    char ext_ip_addr[256];
    strcpy2(ext_ip_addr, r[0]);
    return std::string(ext_ip_addr);
}

// parse result name from this query result row
std::string mr_parse_wu_name(MYSQL_ROW& r){
    //char ext_ip_addr[256];
    char wu_name[256];
    strcpy2(wu_name, r[1]);
    return std::string(wu_name);
}

