/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include <windows.h>
#include <odbcinst.h>

#define NAME "h2odbc"

bool isInstalled() {
    const int max=10240;
    char drivers[max];
    WORD len;
    if(SQLGetInstalledDrivers(drivers,max,&len)==FALSE) {
        return false;
    }
    char* search=drivers;
    while(*search) {
        if(!strncmp(search, NAME, strlen(NAME))) {
            return true;
        }
        search += strlen(search)+1;
    }
    return false;
}


bool uninstall() {
    BOOL removeDataSources=FALSE;
    BOOL removeAll=FALSE;
    DWORD usageCount;
    BOOL successfull;
    bool result=false;
    do {
        successfull=SQLRemoveDriver(NAME, removeDataSources, &usageCount);
        if(successfull==TRUE) {
            result=true;
        }
    } while(removeAll==TRUE && usageCount>0);
    return result;
}


int APIENTRY WinMain(HINSTANCE hInstance,
                     HINSTANCE hPrevInstance,
                     LPSTR     lpCmdLine,
                     int       nCmdShow)
{

    if(!isInstalled()) {
        MessageBox(0,
            "The ODBC driver is not installed.\n"
            "There is nothing to un-install.",
            "Uninstall",MB_OK);
    } else {
        int result=MessageBox(0,
            "Un-Install the ODBC driver now?",
            "Uninstall",MB_YESNO);
        if(result==IDYES) {
            if(uninstall()) {
                MessageBox(0,
                    "The driver has been uninstalled successfully.",
                    "Uninstall",MB_OK);
            } else {
                MessageBox(0,
                    "There was an error while un-installing.",
                    "Uninstall",MB_OK);
            }
        } else {
            MessageBox(0,
                "The driver remains installed.",
                "Uninstall",MB_OK);
        }
    }
    return 0;
}
