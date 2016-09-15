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


#ifndef MR_JOBTRACKER_H_INCLUDED
#define MR_JOBTRACKER_H_INCLUDED

#ifndef _WIN32
#include <string>
#include <vector>
#endif

using std::string;
using std::vector;

/// BOINC-MR TODO : directory necessary? Where to store mr_jobs file?
#define MR_JOBS_FILENAME    "mr_jobtracker.xml"
#define MR_JOB_TEMPLATES    "mr_job_templates.xml"
//#define MR_DIRECTORY        "mapreduce/"
    /// Unnecessary

#define MR_LOG_FILE         "mr_jobtracker.log"

// Value for MapReduce job status
#define MR_JOB_INIT             0
#define MR_JOB_MAP_RUNNING      1
#define MR_JOB_RED_RUNNING      2
#define MR_JOB_DONE             3

// Value for type of application - Map or Reduce
#define MR_APP_TYPE_MAP         1
#define MR_APP_TYPE_REDUCE      2


// structure that saves file size and hash of the outputs of Map tasks
struct hash_out {
	double fsize;          // file size
	char hash_md5[64];  // Map output md5 hash
} ;


/*
MapReduce Job class
- MapReduce jobs file: text/XML file with list of result IDs of tasks that are already available,
as well as the total number of files required for Reduce input; Also contains number of Reduce tasks planned for the next step;
Already defined previously to let Mappers know how to divide their output.
*/
class MAPREDUCE_JOB {
private:

    std::vector<int> mr_available_inputs;        // list of Workunit IDs of Map outputs already available for input for Reduce tasks

    std::vector <int> mr_hashed_output_files;   // list of WU IDs of Map outputs that are stored in server/available for download as Reduce inputs (used for hashed WUs when results are sent to clients that are unable to host map output files)

    int mr_num_map_tasks;
        // total number of inputs for this job (=number of map tasks)
    int mr_num_reduce_tasks;
    int mr_job_status;
        // if it is running Map tasks, uninitialized, on the Reduce phase or done
    int num_available_inputs;
        // Transitioner keeps track of new Map Results that have finished for this Map task


    // Reduce Phase
    char mr_reduce_wu_template[256];
    char mr_reduce_re_template[256];
        // Result and WU templates for Reduce tasks
    char mr_reduce_app_name[256];
    /// BOINC-MR TODO : update this value whenever Reduce task is finished
    int num_validate_outputs;
        // Number of Reduce outputs validated (once >= mr_num_reduce_tasks, Map outputs can be deleted [and assimilated])


    /// What to save here? Host id (in DB), external IP address, or User ID?
    /// At this point, we should have in g_reply - DB_HOST and DB_USER
    /// DB_HOST - holds ip addr, and userid of owner
    //std::vector<DB_HOST*> mr_mappers_serving; // Transitioner does not have g_reply

    std::vector<int> mr_mappers_serving; // hostid - IP address obtained only when sched is submitting result OR when transitioner creates Results from WU
        // unnecessary? Already in DB: Result -> user_hosting
        // set of IDs from hosts that have finished Map tasks and are serving output files

        /// BOINC-MR TODO: necessary for transitioner? Scheduler will have to search DB for specific map jobs that have
        /// user_hosting = 1 and are of MAP type, and wu.MR_JOB_ID = X
        //  OR
        /// Transitioner saves this info and inserts it into new Reduce WU (xml_doc_in?) or each Result's WU


    ///std::vector<int> mr_reduce_input_wu_ids;
        // list of Workunit IDs for all the inputs required by Reduce phase
        // to be filled by scheduler? Unnecessary?

    //std::vector<char*> mr_map_wu_names;
    std::vector<std::string> mr_map_wu_names;
        // list of names of Map WUs, necessary when creating Reduce Workunit - Reduce inputs are named after
        // each Map WU's name and index e.g.: "mapred_map_3-min_0" + "_INDEX"

    int reduce_wu_name_index;
        // Index to create Reduce Work Unit names: APPNAME_INDEX - (e.g.: mapred_reduce_3-min_0 / 1 / 2 ...)

public:

    //hash_out hashed_outputs[][];
    std::vector< std::vector<hash_out> > hashed_outputs;
        // bi-dimensional array of hash_out objects. Each position corresponds to a single output from one Map task
        // hashed_outputs [mapper#][reducer#] - e.g.: hashed_outputs[1][2] - hash output from mapper #1, for reducer #2

    //std::vector<char*> get_mr_map_wu_names(){
    std::vector<std::string> get_mr_map_wu_names(){
        return mr_map_wu_names;
    };

    bool mr_hashed;
        // true if this MR job's output is only stored in client, with hashes being returned and validated at server

    int mr_job_id;
    bool mr_validated;  /// BOINC-MR TODO - unnecessary?
        // when 1st Reduce task is chosen by scheduler to send to user, check if all hosts currently set in mr_mappers_serving
        // have reported successful results. mr_validated = true after this check

    bool mr_allow_inc_inputs;
        // true if this job allows Reduce tasks to be created before all Map tasks have finished (upload Map output sooner)

    MAPREDUCE_JOB();
    ~MAPREDUCE_JOB();

    // add/remove task from list of available inputs
    ///bool mr_add_input_task(RESULT*);
    bool mr_add_input_task(int);

    bool mr_add_output_avail(int);
        // add wu (input: wu id) to list of map outputs available for download from server (used in mr_hashed jobs)

    ///bool mr_remove_input_task(RESULT*);
    bool mr_remove_input_task(int);
    bool mr_all_inputs_available(){
        return num_available_inputs >= mr_num_map_tasks? true: false;
    };
        // return true if all map tasks have finished (reduce work units can be created)
    int parse(MIOFILE&);
        // get information from MapReduce jobs file
    bool write();
    bool delete_available_inputs();
        // clear and delete vector of available inputs
    int parse_available_input(std::string&);
    int parse_available_map_output(std::string&);

    int mr_add_host(int);
    int mr_remove_host(int);

    int parse_map_info(MIOFILE&);
    int parse_reduce_info(MIOFILE&);

    int mr_add_file_served_by_user(char*, int, int);

    bool mr_is_input_available(int);

    bool mr_is_output_file_available(int);

    int get_num_reduce_tasks(){
        return mr_num_reduce_tasks;
    };

    int get_num_available_inputs(){
        return num_available_inputs;
    }

    int get_num_map_tasks(){
        return mr_num_map_tasks;
    };

    int get_reduce_name_index(){
        return reduce_wu_name_index;
    };

    void increment_reduce_name_index(){
        ++reduce_wu_name_index;
    };
    void decrement_reduce_name_index(){
        --reduce_wu_name_index;
    };

    void set_job_status(int s){
        mr_job_status = s;
    };
    int get_job_status(){
        return mr_job_status;
    };


    // Info required for creating Reduce workunits
    //void mr_add_map_wu_name(char*);
    void mr_add_map_wu_name(std::string);
        // Add WU name for Transitioner to know the names of output files [$WUNAME_$INDEX - e.g. wu1_0; wu1_1;]

    char* get_reduce_re_template(){
        return mr_reduce_re_template;
    };
    char* get_reduce_wu_template(){
        return mr_reduce_wu_template;
    };
    char* get_reduce_app_name(){
        return mr_reduce_app_name;
    };


    //int clear_available_inputs();
        /// BOINC-MR TODO : Deprecated - only necessary if available inputs vector is made of pointers instead of int
        // remove available inputs
};

// List of existing MapReduce jobs
//
class MAPREDUCE_JOB_SET{
private:
    /// BOINC-MR TODO - use hashtable instead of vector
    /// more accesses and updates to jobs
    /// OR small number of jobs, and only inside each job use
    /// hashtables to look for map and reduce tasks
    std::vector<MAPREDUCE_JOB*> mr_jobs;

public:
    char mr_jobs_fname[256];
        // file name of MapReduce jobs file

//    int insert_job(MAPREDUCE_JOB*);       // DEPRECATED
//    int remove_job(MAPREDUCE_JOB*);       // DEPRECATED
    bool mr_add_job(MAPREDUCE_JOB*);

    void init(char* = NULL);
        // set initial values for variables (mr_filename as default)
    int parse();
        // parse initial data from MapReduce job info file (default: mr_jobtracker.xml)

    int mr_check_files_served(char*, int, int);
        // parse xml_doc_out and check which files are being served
        /// not being used
    int mr_add_host_job(int, int);
        // add DB_HOST to MapReduce job with specified ID
        /// not being used
    int mr_remove_host_job(int, int);
        /// not being used
    bool mr_check_input_job(int, int);
        // check whether WU is already available as input for Reduce phase for specified MR job
        /// not being used

    MAPREDUCE_JOB* mr_get_job(int);
        // return MAPREDUCE_JOB corresponding to given job id
};


int mr_edit_reduce_template(int, MAPREDUCE_JOB*, char*);
        // called by transitioner to edit reduce WU template before create_work() is called
        // - values of <md5_cksum> and <nbytes> saved in hashed_outputs [num_mappers][num_reducers] is copied to red wu template
        //   for each unique work unit (called *num_reducers* times)
        // only called when Map taks has <hashed/> tag - only hash output is returned, not file itself


int mr_save_hashed_outputs(DB_RESULT, char*, MAPREDUCE_JOB*);
        // called by transitioner to save md5 hash and file size of output that is being stored by the client
        // - saved in MR job's hashed_outputs [num_mappers][num_reducers] array
        // only called when Map taks has <hashed/> tag - only hash output is returned, not file itself


int mr_copy_map_outputs(DB_RESULT, char*, MAPREDUCE_JOB*, bool copy_only=false);
//int mr_copy_map_outputs(DB_RESULT, std::string, MAPREDUCE_JOB*);
    // called by Transitioner to copy Map outputs to input directory to be used as input for Reduce tasks
    // copy_only is set to true only when using this function exclusively for copying map outputs to input dir (previously added as input to MR job)

//int mr_create_reduce_work(char*, char*, char*, int, int, std::vector<char*>);
int mr_create_reduce_work(char*, char*, char*, int, int, std::vector<std::string>, int, MAPREDUCE_JOB*, bool);
    // called by Transitioner to create Reduce workunits to be sent to users, after Map phase is complete

int enumerate_ip_mappers(std::vector<std::string>&, std::vector<std::string>&, int);
std::string mr_parse_ip_addr(MYSQL_ROW&);
std::string mr_parse_wu_name(MYSQL_ROW&);

#endif // MR_JOBTRACKER_H_INCLUDED
