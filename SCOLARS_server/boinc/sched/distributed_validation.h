// This file is part of SCOLARS distributed validation (distributed validation extension for BOINC)
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

#ifndef _WIN32
#include <string>
//#include <vector>
#endif

#define D_VAL_FILENAME    "dist_val.xml"    // information on distributed validation apps/workunits

#include "miofile.h"        // parse()


// Values for req_out_file_state [DB]
// This allows for Validator of D_VAL WU to set D_VAL_REQUEST_FILE, which will make the scheduler ask the corresponding hosts to send the files
#define D_VAL_REQUEST_INIT -1
    // initial value, when Workunit is created and added to DB
#define D_VAL_REQUEST_FILE 1
    // D_VAL successfully validated, wait for contact from corresponding host, to ask for output files
#define D_VAL_REQUEST_SENT 2
    // request for output file sent to host
#define D_VAL_REQUEST_FILE_RECEIVED 3
    // output files successfully sent by host and received by server


/*
Application to be validated by client - information on its templates and name.
This information is required by transitioner in order to dynamically create validator WUs.
- Distributed validation file: text/XML file with list of apps that are meant to be validated by clients
*/
class D_VAL_APP {
private:

    char d_val_wu_template[256];
    char d_val_re_template[256];
        // Result and WU templates for Reduce tasks
    char d_val_app_name[256];
        // app name for this distributed validation application
    char app_name_wu_to_validate[256];
        // name of app to be validated by client instead of server
    int num_wu_outputs;
        // number of outputs from wu to validate
    int min_quorum;
        // minimum number of identical results required to form a quorum (and consider WU valid)
    bool hashed;
        // true if this distributed validation is performed on the hashes of the output files (instead of the output files themselves)
    bool return_result;
        // true if this distributed validation's output files must be returned back to the server

    double dval_max_nbytes;
        // value for <max_nbytes> of validator WU's input files (file_info)

public:

   int parse(MIOFILE&);
        // get information from Distributed Validation file (e.g. dist_val.xml)

   char* get_wu_template(){
       return d_val_wu_template;
   };

   char* get_re_template(){
       return d_val_re_template;
   };

   char* get_app_name_wu_to_validate(){
       return app_name_wu_to_validate;
   };

   char* get_d_val_app_name(){
       return d_val_app_name;
   };

   int get_num_wu_outputs(){
       return num_wu_outputs;
   }

   int get_min_quorum(){
       return min_quorum;
   }

   bool is_hashed(){
       return hashed;
   }

   bool must_return_result(){
       return return_result;
   }

   double get_max_nbytes(){
       return dval_max_nbytes;
   }

   D_VAL_APP();


};

// List of applications that are meant to be validated by clients instead of server (distributed validation)
//
class D_VAL_SET {
private:
    /// BOINC-MR TODO - use hashtable instead of vector
    /// more accesses and updates to jobs
    /// OR small number of jobs, and only inside each job use
    /// hashtables to look for map and reduce tasks
    //std::vector<D_VAL_APP*> dval_apps;

public:
    std::vector<D_VAL_APP*> dval_apps;

    char d_val_fname[256];
        // file name of file with information on distributed validation apps

    bool d_val_add_app(D_VAL_APP*);

    void init(char* = NULL);
        // set initial values for variables (dist_val.xml as default)
    int parse();
        // parse initial data from distributed validation apps info file (default: dist_val.xml)

    D_VAL_APP* d_val_get_app(std::string);
        // return D_VAL that is responsible for validating app with this name
        /// DEPRECATED? transitioner's wu_item does not have access to app name, only to its ID - use get_dv_app(int)

    D_VAL_APP* get_dv_app_wu(const char*);
        // return D_VAL that is responsible for validating app with this workunit name

    D_VAL_APP* get_dv_app_name(const char*);
        // return D_VAL that is responsible for validating app with this application name

};


// deprecated: create_dist_val_wu(dv_app->get_re_template(), dv_app->get_wu_template(), dv_app->get_d_val_app_name(), nsuccess, last_res_id);
//create_dist_val_wu(char* result_template_file, char* wu_template_file, char* app_name, char* wu_alt_name, int num_outputs, int min_quorum, int num_hosts, int last_result_id, int wuid_validating, int& validator_wuid, bool is_hashed){
//create_dist_val_wu(char* result_template_file, char* wu_template_file, char* app_name, char* wu_alt_name, int num_outputs, int min_quorum, int num_hosts, int last_result_id, int wuid_validating, int& validator_wuid, std::vector<int> result_ids, bool is_hashed, double dval_max_nbytes)
int create_dist_val_wu(char*, char*, char*, char*, int, int, int, int, int, int&, std::vector<int>, bool, double);
    // called by Transitioner to create Dist Val workunits to be sent to users, after enough results from WU being validated are returned

int enumerate_ip_hosts(std::vector<std::string>&, std::vector<int>&, int);
    // get list of IP addresses of hosts that have successfully completed this workunit

int d_val_parse_ip_addr_val_st(MYSQL_ROW& r, std::string&, int&);
    // parse ip address, validate_state and result ID from db query

int d_val_parse_ip_addr_res_id(MYSQL_ROW& r, std::string&, int&);
    // parse IP address and result ID from db query


//extern D_VAL_SET dist_val_apps;
    // initialized by transitioner, but used by scheduler
    /// D_VAL TODO: check if this works correctly
