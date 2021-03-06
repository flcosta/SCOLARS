// This file is part of BOINC.
// http://boinc.berkeley.edu
// Copyright (C) 2009 University of California
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

/*  AddRemoveUser.cpp */

#include <Carbon/Carbon.h>

#include <unistd.h>	// getlogin
#include <sys/types.h>	// getpwname, getpwuid, getuid
#include <pwd.h>	// getpwname, getpwuid, getuid
#include <grp.h>        // getgrnam

#include "LoginItemAPI.h"  //please take a look at LoginItemAPI.h for an explanation of the routines available to you.

void printUsage(void);
void SetLoginItem(Boolean addLogInItem);
static char * PersistentFGets(char *buf, size_t buflen, FILE *f);


int main(int argc, char *argv[])
{
    Boolean             AddUsers = false;
    Boolean             SetSavers = false;
    Boolean             isGroupMember;
    Boolean             saverIsSet = false;
    passwd              *pw;
    uid_t               saved_uid;
    long                OSVersion;
    group               grpBOINC_master, *grpBOINC_masterPtr;
    char                bmBuf[32768];
    short               index, i;
    char                *p;
    char                s[256];
    FILE                *f;
    OSStatus            err;
    
#ifndef _DEBUG
    if (getuid() != 0) {
        printf("This program must be run as root\n");
        printUsage();
        return 0;
    }
#endif

    if (argc < 3) {
        printUsage();
        return 0;
    }
    
    if (strcmp(argv[1], "-a") == 0) {
        AddUsers = true;
    } else if (strcmp(argv[1], "-s") == 0) {
        AddUsers = true;
        SetSavers = true;
    } else if (strcmp(argv[1], "-r") != 0) {
        printUsage();
        return 0;
    }
    
    err = Gestalt(gestaltSystemVersion, &OSVersion);
    if (err != noErr)
        return err;

    err = getgrnam_r("boinc_master", &grpBOINC_master, bmBuf, sizeof(bmBuf), &grpBOINC_masterPtr);
    if (err) {          // Should never happen unless buffer too small
        puts("getgrnam(\"boinc_master\") failed\n");
        return -1;
    }

    for (index=2; index<argc; index++) {
        isGroupMember = false;
        i = 0;
        while ((p = grpBOINC_master.gr_mem[i]) != NULL) {  // Step through all users in group boinc_master
            if (strcmp(p, argv[index]) == 0) {
                // User is a member of group boinc_master
                isGroupMember = true;
                break;
            }
            ++i;
        }

        if ((!isGroupMember) && AddUsers) {
            sprintf(s, "dscl . -merge /groups/boinc_master users %s", argv[index]);
            system(s);
        }
        
        if (isGroupMember && (!AddUsers)) {
            sprintf(s, "dscl . -delete /Groups/boinc_master GroupMembership %s", argv[index]);
            system(s);
        }

        pw = getpwnam(argv[index]);
        if (pw == NULL) {
            printf("User %s not found.\n", argv[index]);
            continue;
        }

        saved_uid = geteuid();
        seteuid(pw->pw_uid);                        // Temporarily set effective uid to this user

        SetLoginItem(AddUsers);                     // Set or remove login item for this user

        if (OSVersion < 0x1060) {
            sprintf(s, "sudo -u %s defaults -currentHost read com.apple.screensaver moduleName", 
                    argv[index]); 
        } else {
            sprintf(s, "sudo -u %s defaults -currentHost read com.apple.screensaver moduleDict -dict", 
                    argv[index]); 
        }
        f = popen(s, "r");
        
        if (f) {
            saverIsSet = false;
            while (PersistentFGets(s, sizeof(s), f)) {
                if (strstr(s, "BOINCSaver")) {
                    saverIsSet = true;
                    break;
                }
            }
            pclose(f);
        }

        if ((!saverIsSet) && SetSavers) {
            if (OSVersion < 0x1060) {
                sprintf(s, "sudo -u %s defaults -currentHost write com.apple.screensaver moduleName BOINCSaver", 
                    argv[index]); 
                system(s);
                sprintf(s, "sudo -u %s defaults -currentHost write com.apple.screensaver modulePath \"/Library/Screen Savers/BOINCSaver.saver\"", 
                    argv[index]); 
                system(s);
            } else {
                sprintf(s, "sudo -u %s defaults -currentHost write com.apple.screensaver moduleDict -dict moduleName BOINCSaver path \"/Library/Screen Savers/BOINCSaver.saver\"", 
                        argv[index]);
                system(s);
            }
        }
        
        if (saverIsSet && (!AddUsers)) {
            if (OSVersion < 0x1060) {
                sprintf(s, "sudo -u %s defaults -currentHost write com.apple.screensaver moduleName Flurry", 
                    argv[index]); 
                system(s);
                sprintf(s, "sudo -u %s defaults -currentHost write com.apple.screensaver modulePath \"/System/Library/Screen Savers/Flurry.saver\"", 
                    argv[index]); 
                system(s);
            } else {
                sprintf(s, "sudo -u %s defaults -currentHost write com.apple.screensaver moduleDict -dict moduleName Flurry path \"/System/Library/Screen Savers/Flurry.saver\"", 
                        argv[index]);
                system(s);
            }
        }

        seteuid(saved_uid);                         // Set effective uid back to privileged user
    }
    
    return 0;
}

void printUsage() {
    printf("Usage: sudo AddRemoveUser [-a | -s | -r] [user1 [user2 [user3...]]]\n");
    printf("   -a: add users to those with permission to run BOINC Manager.\n");
    printf("   -s: same as -a plus set users' screensaver to BOINC.\n");
    printf("   -r: remove users' permission to run BOINC Manager, and \n");
    printf("      if their screensaver was set to BOINC change it to Flurry.\n");
    printf("\n");
}

void SetLoginItem(Boolean addLogInItem){
    Boolean                 Success;
    int                     NumberOfLoginItems, Counter;
    char                    *p, *q;

    Success = false;
    
    NumberOfLoginItems = GetCountOfLoginItems(kCurrentUser);
    
    // Search existing login items in reverse order, deleting any duplicates of ours
    for (Counter = NumberOfLoginItems ; Counter > 0 ; Counter--)
    {
        p = ReturnLoginItemPropertyAtIndex(kCurrentUser, kApplicationNameInfo, Counter-1);
        q = p;
        while (*q)
        {
            // It is OK to modify the returned string because we "own" it
            *q = toupper(*q);	// Make it case-insensitive
            q++;
        }
    
        if (strcmp(p, "BOINCMANAGER.APP") == 0) {
            Success = RemoveLoginItemAtIndex(kCurrentUser, Counter-1);
        }
    }

    if (addLogInItem) {
        Success = AddLoginItemWithPropertiesToUser(kCurrentUser, "/Applications/BOINCManager.app", kHideOnLaunch);
    }
}

static char * PersistentFGets(char *buf, size_t buflen, FILE *f) {
    char *p = buf;
    size_t len = buflen;
    size_t datalen = 0;

    *buf = '\0';
    while (datalen < (buflen - 1)) {
        fgets(p, len, f);
        if (feof(f)) break;
        if (ferror(f) && (errno != EINTR)) break;
        if (strchr(buf, '\n')) break;
        datalen = strlen(buf);
        p = buf + datalen;
        len -= datalen;
    }
    return (buf[0] ? buf : NULL);
}
