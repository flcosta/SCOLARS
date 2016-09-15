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

#include "config.h"
#ifdef _USING_FCGI_
#include "boinc_fcgi.h"
#else
#include <cstdio>
#endif
#include <cstdlib>
#include <cstring>
#include <string>
#include <ctime>
#include <cassert>
#include <unistd.h>
#include <cmath>
#include <sys/types.h>
#include <sys/stat.h>

/// D_VAL DEBUG - delete
#include <iostream>

#include "boinc_db.h"
#include "crypt.h"
#include "error_numbers.h"
#include "md5_file.h"
#include "parse.h"
#include "str_util.h"
#include "str_replace.h"
#include "common_defs.h"
#include "filesys.h"
#include "sched_util.h"
#include "util.h"

#include "backend_lib.h"

// BOINC-MR
#include "mr_jobtracker.h"      // values of MapReduce application type (Map or Reduce) [int]

// Distributed validation
#include "distributed_validation.h"

using std::string;

static struct random_init {
    random_init() {
    srand48(getpid() + time(0));
                        }
} random_init;

int read_file(FILE* f, char* buf, int len) {
    int n = fread(buf, 1, len, f);
    buf[n] = 0;
    return 0;
}

int read_filename(const char* path, char* buf, int len) {
    int retval;
#ifndef _USING_FCGI_
    FILE* f = fopen(path, "r");
#else
    FCGI_FILE *f=FCGI::fopen(path, "r");
#endif
    if (!f) return -1;
    retval = read_file(f, buf, len);
    fclose(f);
    return retval;
}

// see checkin notes Dec 30 2004
//
static bool got_md5_info(
    const char *path,
    char *md5data,
    double *nbytes
) {
    bool retval=false;
    // look for file named FILENAME.md5 containing md5sum and length.
    // If found, and newer mod time than file, read md5 sum and file
    // length from it.

    char md5name[512];
    struct stat md5stat, filestat;
    char endline='\0';

    sprintf(md5name, "%s.md5", path);

    // get mod times for file
    if (stat(path, &filestat))
        return retval;

    // get mod time for md5 cache file
    if (stat(md5name, &md5stat))
        return retval;

    // if cached md5 newer, then open it
#ifndef _USING_FCGI_
    FILE *fp=fopen(md5name, "r");
#else
    FCGI_FILE *fp=FCGI::fopen(md5name, "r");
#endif
    if (!fp)
        return retval;

    // read two quantities: md5 sum and length.  If we can't read
    // these, or there is MORE stuff in the file' it's not an md5
    // cache file
    if (3 == fscanf(fp, "%s %lf%c", md5data, nbytes, &endline) &&
        endline=='\n' &&
        EOF==fgetc(fp)
       )
        retval=true;
    fclose(fp);

    // if this is one of our cached md5 files, but it's OLDER than the
    // data file which it supposedly corresponds to, delete it.
    if (retval && md5stat.st_mtime<filestat.st_mtime) {
        unlink(md5name);
        retval=false;
    }
    return retval;
}

// see checkin notes Dec 30 2004
//
static void write_md5_info(
    const char *path,
    const char *md5,
    double nbytes
) {
    // Write file FILENAME.md5 containing md5sum and length
    //
    char md5name[512];
    struct stat statbuf;
    int retval;

    // if file already exists with this name, don't touch it.
    //
    sprintf(md5name, "%s.md5", path);
    if (!stat(md5name, &statbuf)) {
        return;
    }

#ifndef _USING_FCGI_
    FILE *fp=fopen(md5name, "w");
#else
    FCGI_FILE *fp=FCGI::fopen(md5name, "w");
#endif

    // if can't open the file, give up
    //
    if (!fp) {
        return;
    }

    retval = fprintf(fp,"%s %.15e\n", md5, nbytes);
    fclose(fp);

    // if we didn't write properly to the file, delete it.
    //
    if (retval<0) {
        unlink(md5name);
    }

    return;
}


//
// Create unique alternative name for this workunit. Create a random alphanumeric string as alt_name.
// returns: alternative name string
//
// @ num_chars: number of characters of alternative name [max: 256 - DB: varchar (256)]
// @ s: char array to modify [wu.alt_name]

//std::string create_unique_alt_name(int num_chars){
int create_unique_alt_name(char* s, int num_chars){

    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    //char s[num_chars];

    /// D_VAL DEBUG
    fprintf(stderr, "Inside create_unique_alt_name(). alphanum: %s\n", alphanum);
    fflush(stderr);

    /* initialize random seed: */
    //srand (time(NULL));

    for (int i = 0; i < num_chars; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[num_chars] = '\0';

    /// D_VAL DEBUG
    fprintf(stderr, "Inside create_unique_alt_name(). char s[]: %s\n", s);
    fflush(stderr);

    //std::string str = s;

    /// D_VAL DEBUG
    //fprintf(stderr, "Inside create_unique_alt_name(). Copied s to srt. str: %s\n", str.c_str());
    //fflush(stderr);

    // D_VAL DEBUG
    //log_messages.printf(MSG_DEBUG, "create_unique_alt_name: created random string: %s\n", str.c_str());
    //fflush(stderr);

    return 0;
}

// fill in the workunit's XML document (wu.xml_doc)
// by scanning the WU template, macro-substituting the input files,
// and putting in the command line element and additional XML
//
// BOINC-MR
// parse MR info from wu template (MR job ID) [application type - map/reduce]
// updated to allow remote inputs - not supported in this version
//
// D_VAL
// looks for <dist_val> and <obfusc> tags. If found, create unique, random alternative name for this workunit, to be stored in DB [alt_name]
//
//
// Distributed validation
// check for dist_val tag: if set, this workunit is going to be validated by another client, not the server
static bool input_file_found[1024];

static int process_wu_template(
    WORKUNIT& wu,
    char* tmplate,
    const char** infiles,
    int ninfiles,
    SCHED_CONFIG& config_loc,
    const char* command_line,
    const char* additional_xml
) {
    // BOINC-MR : update to latest version of BOINC server (allows remote input files - inserts url/hash/nbytes)
    vector<string> urls;
    string md5str, urlstr, tmpstr;
    string c2cstr;      // string with list of client IP addresses (input file locations)
    bool c2c_transfer = false;      // true if this workunit has files which are hosted in other clients (distributed validation)
    bool d_val_hash_reduce = false;      // D_VAL: true if this is a reduce workunit that was validated by clients (D_VAL), and no hashes were returned to server
    double nbytesdef = -1;
    for (int i=0; i<1024; i++) {
        input_file_found[i] = false;
    }

    char* p;
    char buf[BLOB_SIZE], md5[33], path[256], url[256], top_download_path[256];
    string out, cmdline;
    int retval, file_number;
    double nbytes;
    char open_name[256];
    bool found=false;
    int nfiles_parsed = 0;
    // C2C
    bool c2c_upload = false;

    // D_VAL
    double dval_max_nbytes;

    //BOINC-MR DEBUG
    fprintf(stderr, "Printing the WU Template to stderr\n");
    fprintf(stderr, "%s", tmplate);
    fflush(stderr);
    out = "";
    for (p=strtok(tmplate, "\n"); p; p=strtok(0, "\n")) {
        if (match_tag(p, "<file_info>")) {
            dval_max_nbytes = 0;
            c2c_upload = false;
            c2c_transfer = false;
            c2cstr = "";
            urls.clear();
            bool generated_locally = false;
            file_number = -1;
            out += "<file_info>\n";
            while (1) {
                p = strtok(0, "\n");
                if (!p) break;
                if (parse_int(p, "<number>", file_number)) {
                    continue;
                } else if (parse_bool(p, "generated_locally", generated_locally)) {
                    continue;
                } else if (parse_str(p, "url", urlstr)) {
                    urls.push_back(urlstr);
                    continue;
                } else if (parse_str(p, "md5_cksum", md5str)) {
                    continue;
                } else if (parse_double(p, "<nbytes>", nbytesdef)) {
                    continue;
                // D_VAL
                // parse <c2c> tag
                } else if (parse_str(p, "c2c", c2cstr)) {
                    //c2c_ips.push_back(c2cstr);

                    // D_VAL DEBUG
                    fprintf(stderr, "Found c2c:%s\n", c2cstr.c_str());
                    fflush(stderr);

                    c2c_transfer = true;
                    continue;
                // C2C : check if this file should be available for upload (inter-client transfer)
                } else if(parse_bool(p, "c2c_up", c2c_upload)){

                    // C2C DEBUG
                    fprintf(stderr, "Found c2c_up!\n");
                    fflush(stderr);

                    out += "    <c2c_up/>\n";
                    continue;
                }else if(parse_bool(p, "d_val_red", d_val_hash_reduce)){

                    // D_VAL
                    fprintf(stderr, "Found d_val_red!\n");
                    fflush(stderr);

                    //d_val_hash_reduce = true;
                    continue;
                }else if(parse_double(p, "<max_nbytes>", dval_max_nbytes)){

                    // C2C DEBUG
                    fprintf(stderr, "Found max_nbytes!\n");
                    fflush(stderr);
                    continue;

                } else if (match_tag(p, "</file_info>")) {
                    if (nbytesdef != -1 || md5str != "" || urlstr != "") {
                        if (nbytesdef == -1 || md5str == "" || urlstr == "") {
                            fprintf(stderr, "All file properties must be defined "
                                "if at least one is defined (url, md5_cksum, nbytes)!\n"
                            );
                            return ERR_XML_PARSE;
                        }
                    }
                    if (file_number < 0) {
                        fprintf(stderr, "No file number found\n");
                        return ERR_XML_PARSE;
                    }
                    if (file_number >= ninfiles) {
                        fprintf(stderr,
                            "Too few input files given; need at least %d\n",
                            file_number+1
                        );
                        return ERR_XML_PARSE;
                    }
                    nfiles_parsed++;
                    if (input_file_found[file_number]) {
                        fprintf(stderr,
                            "Input file %d listed twice\n", file_number
                        );
                        return ERR_XML_PARSE;
                    }
                    input_file_found[file_number] = true;

                    // D_VAL
                    // if this is validating wuid, which means that the c2c_up tag was found, then allow max_nbytes to be added to WU template
                    // - it means that this input may be requested by server when the distributed validation is successful
                    if (c2c_upload && dval_max_nbytes > 0){
                        sprintf(buf,
                            "    <max_nbytes>%f</max_nbytes>\n",
                            dval_max_nbytes
                        );
                        out += buf;
                    }

                    if (generated_locally) {
                        sprintf(buf,
                            "    <name>%s</name>\n"
                            "    <generated_locally/>\n"
                            "</file_info>\n",
                            infiles[file_number]
                        );
                    // D_VAL
                    // if nbytes, md5 and url were not defined, but <c2c> was found, then this is a distributed validation workunit
                    // do not attempt to locate file, send this information as is to client
                    } else if(nbytesdef == -1 && c2c_transfer){
//                        c2cstr = "";
//                        for (unsigned int i=0; i<c2c_ips.size(); i++) {
//                            c2cstr += "    <c2c>" + c2c_ips.at(i) + "</url>\n";
//                        }
                        sprintf(buf,
                            "    <name>%s</name>\n"
                            "    <c2c>%s</c2c>\n"
                            "</file_info>\n",
                            infiles[file_number],
                            c2cstr.c_str()
                        );
                    }
                    // D_VAL
                    // if nbytes, md5 and url were not defined, but <d_val_hash_reduce/> was found, then this is a distributed validation reduce workunit
                    // This means that not even the hash was returned back to the server.
                    // do not attempt to locate file, send this information as is to client
                    else if(nbytesdef == -1 && d_val_hash_reduce){
                        sprintf(buf,
                            "    <name>%s</name>\n"
                            "</file_info>\n",
                            infiles[file_number]
                        );
                    }
                    else if (nbytesdef == -1) {
                        // here if nybtes was not supplied; stage the file
                        //
                        dir_hier_path(
                            infiles[file_number], config_loc.download_dir,
                            config_loc.uldl_dir_fanout, path, true
                        );

                        // if file isn't found in hierarchy,
                        // look for it at top level and copy
                        //
                        if (!boinc_file_exists(path)) {
                            sprintf(top_download_path,
                                "%s/%s",config_loc.download_dir,
                                infiles[file_number]
                            );
                            boinc_copy(top_download_path, path);
                        }

                        if (!config_loc.cache_md5_info || !got_md5_info(path, md5, &nbytes)) {
                            retval = md5_file(path, md5, nbytes);
                            if (retval) {
                                fprintf(stderr, "process_wu_template: md5_file %d\n", retval);
                                return retval;
                            }
                            else if (config_loc.cache_md5_info) {
                                write_md5_info(path, md5, nbytes);
                            }
                        }

                        dir_hier_url(
                            infiles[file_number], config_loc.download_url,
                            config_loc.uldl_dir_fanout, url
                        );
                        sprintf(buf,
                            "    <name>%s</name>\n"
                            "    <url>%s</url>\n"
                            "    <md5_cksum>%s</md5_cksum>\n"
                            "    <nbytes>%.0f</nbytes>\n"
                            "</file_info>\n",
                            infiles[file_number],
                            url,
                            md5,
                            nbytes
                        );
                    }  else {
                        // here if nbytes etc. was supplied,
                        // i.e the file is already staged, possibly remotely
                        //
                        urlstr = "";
                        for (unsigned int i=0; i<urls.size(); i++) {
                            urlstr += "    <url>" + urls.at(i) + "</url>\n";
                        }
                        sprintf(buf,
                            "    <name>%s</name>\n"
                            "%s"
                            "    <md5_cksum>%s</md5_cksum>\n"
                            "    <nbytes>%.0f</nbytes>\n"
                            "</file_info>\n",
                            infiles[file_number],
                            urlstr.c_str(),
                            md5str.c_str(),
                            nbytesdef
                        );

                        //BOINC-MR DEBUG
                        fprintf(stderr,
                            "Nbytes was defined [nbytesdef %f] [nbytes: %f]. Inserting into wu:\n%s\n",
                            nbytesdef, nbytes, buf
                        );

                    }
                    out += buf;
                    break;
                } else {
                    out += p;
                    out += "\n";
                }
            }
        } else if (match_tag(p, "<workunit>")) {
            found = true;
            out += "<workunit>\n";
            if (command_line) {
                out += "<command_line>\n";
                out += command_line;
                out += "\n</command_line>\n";
            }
        } else if (match_tag(p, "</workunit>")) {
            if (additional_xml && strlen(additional_xml)) {
                out += additional_xml;
                out += "\n";
            }
            out += "</workunit>\n";
        } else if (match_tag(p, "<file_ref>")) {
            out += "<file_ref>\n";
            bool found_file_number = false, found_open_name = false;
            while (1) {
                p = strtok(0, "\n");
                if (!p) break;
                if (parse_int(p, "<file_number>", file_number)) {
                    sprintf(buf, "    <file_name>%s</file_name>\n",
                        infiles[file_number]
                    );
                    out += buf;
                    found_file_number = true;
                    continue;
                } else if (parse_str(p, "<open_name>", open_name, sizeof(open_name))) {
                    sprintf(buf, "    <open_name>%s</open_name>\n", open_name);
                    out += buf;
                    found_open_name = true;
                    continue;
                } else if (match_tag(p, "</file_ref>")) {
                    if (!found_file_number) {
                        fprintf(stderr, "No file number found\n");
                        return ERR_XML_PARSE;
                    }
                    if (!found_open_name) {
                        fprintf(stderr, "No open name found\n");
                        return ERR_XML_PARSE;
                    }
                    out += "</file_ref>\n";
                    break;
                } else {
                    sprintf(buf, "%s\n", p);
                    out += buf;
                }
            }
        } else if (parse_str(p, "<command_line>", cmdline)) {
            if (command_line) {
                fprintf(stderr, "Can't specify command line twice");
                return ERR_XML_PARSE;
            }
            out += "<command_line>\n";
            out += cmdline;
            out += "\n</command_line>\n";
        } else if (parse_double(p, "<rsc_fpops_est>", wu.rsc_fpops_est)) {
            continue;
        } else if (parse_double(p, "<rsc_fpops_bound>", wu.rsc_fpops_bound)) {
            continue;
        } else if (parse_double(p, "<rsc_memory_bound>", wu.rsc_memory_bound)) {
            continue;
        } else if (parse_double(p, "<rsc_bandwidth_bound>", wu.rsc_bandwidth_bound)) {
            continue;
        } else if (parse_double(p, "<rsc_disk_bound>", wu.rsc_disk_bound)) {
            continue;
        } else if (parse_int(p, "<batch>", wu.batch)) {
            continue;
        } else if (parse_int(p, "<delay_bound>", wu.delay_bound)) {
            continue;
        } else if (parse_int(p, "<min_quorum>", wu.min_quorum)) {
            continue;
        } else if (parse_int(p, "<target_nresults>", wu.target_nresults)) {
            continue;
        } else if (parse_int(p, "<max_error_results>", wu.max_error_results)) {
            continue;
        } else if (parse_int(p, "<max_total_results>", wu.max_total_results)) {
            continue;
        } else if (parse_int(p, "<max_success_results>", wu.max_success_results)) {
            continue;

        // BOINC-MR
        }else if (match_tag(p, "<mapreduce>")) {
            out += "<mapreduce>\n";
            while (1) {
                p = strtok(0, "\n");
                if (!p) break;
                if (parse_int(p, "<mr_job_id>", wu.mr_job_id)) {
                    sprintf(buf, "    <mr_job_id>%d</mr_job_id>\n",
                        wu.mr_job_id
                    );
                    out += buf;
                    continue;
                } else if (match_tag(p, "<map/>")) {
                    wu.mr_app_type = MR_APP_TYPE_MAP;
                    out += "<map/>\n";
                    continue;
                } else if (match_tag(p, "<reduce/>")) {
                    wu.mr_app_type = MR_APP_TYPE_REDUCE;
                    out += "<reduce/>\n";
                    continue;
                } else if (match_tag(p, "</mapreduce>")) {
                    if (wu.mr_job_id <= 0){
                        fprintf(stderr, "No MapReduce job ID found\n");
                        return ERR_XML_PARSE;
                    }
                    if (wu.mr_app_type == 0){
                        fprintf(stderr, "No MapReduce Application Type (Map or Reduce) tag found\n");
                        return ERR_XML_PARSE;
                    }
                    out += "</mapreduce>\n";
                    break;

                } else {
                    sprintf(buf, "%s\n", p);
                    out += buf;
                }
            }

        }else if (match_tag(p, "<dist_val/>")) {

            /// D_VAL DEBUG
            fprintf(stderr, "Found dist_val on Workunit template!\n");
            fflush(stderr);

            out += "<dist_val/>\n";
            wu.dist_val = 1;

            /// D_VAL DEBUG
//            fprintf(stderr, "setting wu.alt_namec\n");
//            fflush(stderr);
//
//            strcpy(wu.alt_name, "altname");
//
//            fprintf(stderr, "printing wu.alt_namec\n");
//            fflush(stderr);
//            fprintf(stderr, "wu.alt_namec: ***%s***\n", wu.alt_name);
//            fflush(stderr);

            //fprintf(stderr, "setting wu.alt_name\n");
            //fflush(stderr);

            //wu.alt_name = "app";
            //wu.set_alt_name("app");

            /// D_VAL DEBUG
            fprintf(stderr, "printing wu.alt_name\n");
            fflush(stderr);

            /// D_VAL DEBUG
            fprintf(stderr, "wu.alt_name: ***%s***\n", wu.alt_name);
            fflush(stderr);

            // if the alternative name hasn't been created yet (e.g., by dist_val)
            if (!wu.alt_name || wu.alt_name == NULL || strlen(wu.alt_name) == 0){

                /// D_VAL DEBUG
                fprintf(stderr, "wu.alt_name is empty(). Calling create_unique_alt_name()\n");
                fflush(stderr);

                //std::string temp;
                //wu.alt_name = create_unique_alt_name(OBFUSC_ALT_NAME_NUM_CHARS);
                create_unique_alt_name(wu.alt_name, OBFUSC_ALT_NAME_NUM_CHARS);

                /// D_VAL DEBUG
                fprintf(stderr, "Exited create_unique_alt_name(). Alt name: %s\n", wu.alt_name);
                fflush(stderr);
                //out += "<alt_name>" + wu.alt_name + "</alt_name>\n";
            }

            // add this info to wu_template (to be sent to client)
            sprintf(buf, "    <alt_name>%s</alt_name>\n", wu.alt_name);
            out += buf;

        // in case obfuscation is used withtout distributed validation (i.e., it is required and implicitily used for distributed validation,
        // but for workflows it may be used between stages)
        }else if (match_tag(p, "<obfusc/>")) {

            /// OBFUSC DEBUG
            fprintf(stderr, "Found obfusc on Workunit template!\n");
            fflush(stderr);

            // not sent to client, only stored/saved by server
            //out += "<dist_val/>\n";
            wu.obfusc = 1;

            // if the alternative name hasn't been created yet (e.g., by dist_val)
            //if (wu.alt_name.empty()){
            if (!wu.alt_name || wu.alt_name == NULL || strlen(wu.alt_name)){
                //std::string temp;

                //wu.alt_name = create_unique_alt_name(OBFUSC_ALT_NAME_NUM_CHARS);
                create_unique_alt_name(wu.alt_name, OBFUSC_ALT_NAME_NUM_CHARS);
            }
            // add this info to wu_template (to be sent to client)
            //out += "<alt_name>" + wu.alt_name + "</alt_name>\n";
            sprintf(buf, "    <alt_name>%s</alt_name>\n", wu.alt_name);
            out += buf;

        } else {
            out += p;
            out += "\n";
        }
    }
    if (!found) {
        fprintf(stderr, "process_wu_template: bad WU template - no <workunit>\n");
        return -1;
    }
    if (nfiles_parsed != ninfiles) {
        fprintf(stderr,
            "process_wu_template: %d input files listed, but template has %d\n",
            ninfiles, nfiles_parsed
        );
        return -1;
    }
    if (out.size() > sizeof(wu.xml_doc)-1) {
        fprintf(stderr,
            "create_work: WU XML field is too long (%zu bytes; max is %lud)\n",
            out.size(), sizeof(wu.xml_doc)-1
        );
        return ERR_BUFFER_OVERFLOW;
    }

    strcpy(wu.xml_doc, out.c_str());
    //BOINC-MR DEBUG
    fprintf(stderr, "process_wu_template() - Returning 0\n");
    fflush(stderr);
    return 0;
}

// initialize an about-to-be-created result, given its WU
//
static void initialize_result(DB_RESULT& result, WORKUNIT& wu) {
    result.id = 0;
    result.create_time = time(0);
    result.workunitid = wu.id;
    result.server_state = RESULT_SERVER_STATE_UNSENT;
    result.hostid = 0;
    result.report_deadline = 0;
    result.sent_time = 0;
    result.received_time = 0;
    result.client_state = 0;
    result.cpu_time = 0;
    strcpy(result.xml_doc_out, "");
    strcpy(result.stderr_out, "");
    result.outcome = RESULT_OUTCOME_INIT;
    result.file_delete_state = ASSIMILATE_INIT;
    result.validate_state = VALIDATE_STATE_INIT;
    result.claimed_credit = 0;
    result.granted_credit = 0;
    result.appid = wu.appid;
    result.priority = wu.priority;
    result.batch = wu.batch;

    // BOINC-MR
    /// BOINC-MR TODO - necessary ? Only keep changes that will be used in DB
    result.is_mapreduce = wu.mr_job_id > 0? true: false;
    result.mr_deadline = 0;
    result.mr_job_id = wu.mr_job_id;
}


/// D_VAL
// dist_val value is needed to identify WU that are meant to be validated by clients
int create_result_ti(
    TRANSITIONER_ITEM& ti,
    char* result_template_filename,
    char* result_name_suffix,
    R_RSA_PRIVATE_KEY& key,
    SCHED_CONFIG& config_loc,
    char* query_string,
        // if nonzero, write value list here; else do insert
    int priority_increase
) {
    WORKUNIT wu;

    // copy relevant fields from TRANSITIONER_ITEM to WORKUNIT
    //
    strcpy(wu.name, ti.name);
    wu.id = ti.id;
    wu.appid = ti.appid;
    wu.priority = ti.priority;
    wu.batch = ti.batch;
    // D_VAL
    wu.dist_val = ti.dist_val;
    // D_VAL DEBUG
    //fprintf(stderr, "TI dist_val: %d\n", ti.dist_val);
    return create_result(
        wu,
        result_template_filename,
        result_name_suffix,
        key,
        config_loc,
        query_string,
        priority_increase
    );
}

/// Distributed Validation - D_VAL
//
// Create a new result for the given WU.
// This is called ONLY from the transitioner
//
int create_result(
    WORKUNIT& wu,
    char* result_template_filename,
    char* result_name_suffix,
    R_RSA_PRIVATE_KEY& key,
    SCHED_CONFIG& config_loc,
    char* query_string,
        // if nonzero, write value list here; else do insert
    int priority_increase
) {
    DB_RESULT result;
    char base_outfile_name[256];
    char result_template[BLOB_SIZE];
    int retval;

    result.clear();
    initialize_result(result, wu);
    result.priority = result.priority + priority_increase;
    sprintf(result.name, "%s_%s", wu.name, result_name_suffix);
    sprintf(base_outfile_name, "%s_", result.name);
    retval = read_filename(
        result_template_filename, result_template, sizeof(result_template)
    );
    if (retval) {
        fprintf(stderr,
            "Failed to read result template file '%s': %d\n",
            result_template_filename, retval
        );
        return retval;
    }

    retval = process_result_template(
        result_template, key, base_outfile_name, config_loc
    );
    if (retval) {
        fprintf(stderr, "process_result_template() error: %d\n", retval);
    }
    if (strlen(result_template) > sizeof(result.xml_doc_in)-1) {
        fprintf(stderr,
            "result XML doc is too long: %zu bytes, max is %lu\n",
            strlen(result_template), sizeof(result.xml_doc_in)-1
        );
        return ERR_BUFFER_OVERFLOW;
    }

    strlcpy(result.xml_doc_in, result_template, sizeof(result.xml_doc_in));

    result.random = lrand48();

//    // D_VAL
//    if (!config_loc.dont_generate_upload_certificates) {
//        char wu_template[MEDIUM_BLOB_SIZE];
//        // The WORKUNIT wu does not have the xml_doc field stored, therefore we must fetch it from the DB in order to change it
//        DB_WORKUNIT dbwu;
//        dbwu.id = wu.id;
//        /// D_VAL TODO: optimize these two DB queries into ("SELECT validating_wuid, xml_doc" in single query)
//        // check if this workunit is a validator WU (only then must we insert a xml_signature for all inputs (in case they are required for upload later)
//        retval = dbwu.get_field_int("validating_wuid", dbwu.validating_wuid);
//        if (!retval && dbwu.validating_wuid >= 0) {
//
//            // D_VAL DEBUG
//            fprintf(stderr,
//                "create_result: [%s] This is a Validator Workunit.\n", wu.name
//            );
//            fflush(stderr);
//
//            retval = dbwu.get_field_str("xml_doc", dbwu.xml_doc, sizeof(dbwu.xml_doc));
//
//            // D_VAL DEBUG
//            fprintf(stderr,
//                "create_result: After getting xml_doc:\n%s\n", dbwu.xml_doc
//            );
//            fflush(stderr);
//
//            if (!retval) {
//                retval = add_signatures(dbwu.xml_doc, key);
//                if (retval) {
//                    fprintf(stderr,
//                        "create_result: [%s] WU Error trying to add xml_signature [add_signatures()] to xml_doc: %d\n", wu.name, retval
//                    );
//                    return retval;
//                }
//
//                // D_VAL DEBUG
//                fprintf(stderr,
//                    "create_result: After inserting xml_signature in xml_doc:\n%s\n", dbwu.xml_doc
//                );
//                fflush(stderr);
//
//                // update DB with new xml_doc value
//                char buf2[MEDIUM_BLOB_SIZE+256];
//                sprintf(buf2, "xml_doc=\"%s\"", dbwu.xml_doc);
//                // update this value on the WU that was validated by wu
//                retval = dbwu.update_field(buf2);
//                if (retval) {
//                    fprintf(stderr,
//                        "create_result: [%s] WU update failed (inserting xml_signature): %d\n", wu.name, retval
//                    );
//                    return retval;
//                }
//            }
//            // error fetching xml_doc field
//            else{
//                fprintf(stderr,
//                    "create_result: [%s] Error trying to fetch WU's xml_doc from DB: %d\n", wu.name, retval
//                );
//                return retval;
//            }
//        }
//        else if (retval){
//            fprintf(stderr,
//                "create_result: [%s] Error trying to fetch WU's validating_wuid from DB: %d\n", wu.name, retval
//            );
//            return retval;
//        }
//    }

    if (query_string) {
        result.db_print_values(query_string);
    } else {
        retval = result.insert();
        if (retval) {
            fprintf(stderr, "result.insert(): %d\n", retval);
            return retval;
        }
    }

    // if using locality scheduling, advertise data file
    // associated with this newly-created result
    //
    if (config_loc.locality_scheduling) {
        const char *datafilename;
        char *last=strstr(result.name, "__");
        if (result.name<last && last<(result.name+255)) {
            datafilename = config.project_path("locality_scheduling/working_set_removal/%s", result.name);
            unlink(datafilename);
            datafilename = config.project_path("locality_scheduling/work_available/%s", result.name);
            boinc_touch_file(datafilename);
        }
    }
    return 0;
}

// make sure a WU's input files are actually there
//
int check_files(char** infiles, int ninfiles, SCHED_CONFIG& config_loc) {
    int i;
    char path[256];

    for (i=0; i<ninfiles; i++) {
        dir_hier_path(
            infiles[i], config_loc.download_dir, config_loc.uldl_dir_fanout, path
        );
		if (!boinc_file_exists(path)) {
			return 1;
		}

    }
    return 0;
}

int create_work(
    DB_WORKUNIT& wu,
    const char* _wu_template,
    const char* result_template_filename,
    const char* result_template_filepath,
    const char** infiles,
    int ninfiles,
    SCHED_CONFIG& config_loc,
    const char* command_line,
    const char* additional_xml
) {
    int retval;
    char _result_template[BLOB_SIZE];
    char wu_template[BLOB_SIZE];

#if 0
    retval = check_files(infiles, ninfiles, config_loc);
    if (retval) {
        fprintf(stderr, "Missing input file: %s\n", infiles[0]);
        return -1;
    }
#endif

    strcpy(wu_template, _wu_template);
    wu.create_time = time(0);
    retval = process_wu_template(
        wu, wu_template, infiles, ninfiles, config_loc, command_line, additional_xml
    );
    if (retval) {
        fprintf(stderr, "process_wu_template: %d\n", retval);
        return retval;
    }

    // BOINC-MR DEBUG
    fprintf(stderr, "create_work() - Before read_filename()\n");
    fflush(stderr);

    retval = read_filename(
        result_template_filepath, _result_template, sizeof(_result_template)
    );
    if (retval) {
        fprintf(stderr, "create_work: can't read result template file %s\n", result_template_filepath);
        return retval;
    }

    if (strlen(result_template_filename) > sizeof(wu.result_template_file)-1) {
        fprintf(stderr, "result template filename is too big: %zu bytes, max is %lu\n",
            strlen(result_template_filename), sizeof(wu.result_template_file)-1
        );
        return ERR_BUFFER_OVERFLOW;
    }


//    /// Distributed validation
//    // change the result template, to create unique alternative names for each of the output files (<alt_name>)
//    // do this by adding the wu_id and a counter to the tag. E.g.: <alt_name>file1_1010_0</> <alt_name>file2_1010_1</>
//    if (wu.is_dist_val()){
//        if (create_unique_alt_names(_result_template))
//            // handle errors - do not create this WU
//            return ERR_UNIQUE_ALT_NAMES;
//        // check if changes did not create buffer overflow
//        else if (strlen(result_template_filename) > sizeof(wu.result_template_file)-1) {
//            fprintf(stderr, "After adding unique alternative names: result template filename became too big: %d bytes, max is %d\n",
//                strlen(result_template_filename), sizeof(wu.result_template_file)-1
//            );
//            return ERR_BUFFER_OVERFLOW;
//        }
//    }

    // BOINC-MR DEBUG
    fprintf(stderr, "create_work() - After read_filename(), before strlcpy result template to WU\n");
    fflush(stderr);

    strlcpy(wu.result_template_file, result_template_filename, sizeof(wu.result_template_file));

    if (wu.rsc_fpops_est == 0) {
        fprintf(stderr, "no rsc_fpops_est given; can't create job\n");
        return ERR_NO_OPTION;
    }
    if (wu.rsc_fpops_bound == 0) {
        fprintf(stderr, "no rsc_fpops_bound given; can't create job\n");
        return ERR_NO_OPTION;
    }
    if (wu.rsc_disk_bound == 0) {
        fprintf(stderr, "no rsc_disk_bound given; can't create job\n");
        return ERR_NO_OPTION;
    }
    if (wu.target_nresults == 0) {
        fprintf(stderr, "no target_nresults given; can't create job\n");
        return ERR_NO_OPTION;
    }
    if (wu.max_error_results == 0) {
        fprintf(stderr, "no max_error_results given; can't create job\n");
        return ERR_NO_OPTION;
    }
    if (wu.max_total_results == 0) {
        fprintf(stderr, "no max_total_results given; can't create job\n");
        return ERR_NO_OPTION;
    }
    if (wu.max_success_results == 0) {
        fprintf(stderr, "no max_success_results given; can't create job\n");
        return ERR_NO_OPTION;
    }
    if (wu.max_success_results > wu.max_total_results) {
        fprintf(stderr, "max_success_results > max_total_results; can't create job\n");
        return ERR_INVALID_PARAM;
    }
    if (wu.max_error_results > wu.max_total_results) {
        fprintf(stderr, "max_error_results > max_total_results; can't create job\n");
        return ERR_INVALID_PARAM;
    }
    if (wu.target_nresults > wu.max_success_results) {
        fprintf(stderr, "target_nresults > max_success_results; can't create job\n");
        return ERR_INVALID_PARAM;
    }
    if (strstr(wu.name, ASSIGNED_WU_STR)) {
        wu.transition_time = INT_MAX;
    } else {
        wu.transition_time = time(0);
    }
    if (wu.id) {

        /// BOINC-MR DEBUG
        fprintf(stderr, "create_work() ERROR: should not have a defined ID (#%d)! Should be 0...\n", wu.id);
        fflush(stderr);

        retval = wu.update();
        if (retval) {
            fprintf(stderr, "create_work: workunit.update() %d\n", retval);
            return retval;
        }
    } else {
        retval = wu.insert();
        if (retval) {
            fprintf(stderr, "create_work: workunit.insert() %d\n", retval);
            return retval;
        }
        wu.id = boinc_db.insert_id();
    }

    // BOINC-MR DEBUG
    fprintf(stderr, "create_work() - Inserted WU [#%d] in DB, returning 0\n", wu.id);
    fflush(stderr);

    return 0;
}

const char *BOINC_RCSID_b5f8b10eb5 = "$Id: backend_lib.cpp 19054 2009-09-16 03:10:22Z davea $";
