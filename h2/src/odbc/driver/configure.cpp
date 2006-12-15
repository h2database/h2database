/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

typedef struct {
    char name[MAX_STRING_LEN+1];
    char url[MAX_STRING_LEN+1];
    char user[MAX_STRING_LEN+1];
    char password[MAX_STRING_LEN+1];
} DNSConfiguration;

BOOL INSTAPI ConfigDriver(HWND hwndParent, 
             WORD fRequest, LPCSTR lpszDriver,
             LPCSTR lpszArgs, LPSTR lpszMsg,
             WORD cbMsgMax, WORD *pcbMsgOut) {
    trace("ConfigDriver");
    switch(fRequest) {
    case ODBC_INSTALL_DRIVER:
    case ODBC_CONFIG_DRIVER:
    case ODBC_REMOVE_DRIVER:
        return TRUE;
    }
    return FALSE;
}

LONG CALLBACK ConfigDlgProc(HWND hdlg,WORD wMsg,WPARAM wParam,LPARAM lParam) {
    DNSConfiguration* config;
    switch (wMsg) {
    case WM_INITDIALOG:
        config=(DNSConfiguration*)lParam;
        SetDlgItemText(hdlg,IDC_NAME,config->name);
        SetDlgItemText(hdlg,IDC_URL,config->url);
        SetDlgItemText(hdlg,IDC_USER,config->user);
        SetDlgItemText(hdlg,IDC_PASSWORD,config->password);
        SetWindowLong(hdlg, GWL_USERDATA, (LONG) lParam );
        return TRUE;
    case WM_COMMAND:
        switch (LOWORD(wParam)) {
        case IDOK:
            config=(DNSConfiguration*)GetWindowLong(hdlg,GWL_USERDATA);
            GetDlgItemText(hdlg,IDC_NAME,config->name,MAX_STRING_LEN);
            GetDlgItemText(hdlg,IDC_URL,config->url,MAX_STRING_LEN);
            GetDlgItemText(hdlg,IDC_USER,config->user,MAX_STRING_LEN);
            GetDlgItemText(hdlg,IDC_PASSWORD,config->password,MAX_STRING_LEN);
            EndDialog(hdlg, wParam);
            return TRUE;
        case IDCANCEL:
            EndDialog(hdlg, wParam);
            return TRUE;
        }
        break;
    }
    return FALSE;
}

BOOL INSTAPI ConfigDSN(HWND    hwndParent,
             WORD fRequest, LPCSTR    lpszDriver,
             LPCSTR lpszAttributes) {
    trace("ConfigDSN");
    char* begin=strstr(lpszAttributes,"DSN=");
    int len=0;
    char name[MAX_STRING_LEN+1];
    name[0]=0;
    DNSConfiguration configuration;
    configuration.name[0]=0;
    configuration.url[0]=0;
    configuration.user[0]=0;
    configuration.password[0]=0;
    if(begin) {
        trace(" begin");              
        begin+=4; 
        char* end=strstr(begin,";");
        int len;
        if(end==NULL) {
            len=strlen(begin);
        } else {
            len=end-begin;
        }
        if(len>MAX_STRING_LEN) {
            len=MAX_STRING_LEN;
        }
        strncpy(name, begin, len+1);
        strncpy(configuration.name, begin, len+1);
    }
    if(fRequest==ODBC_REMOVE_DSN) {
        trace(" ODBC_REMOVE_DSN");              
        return SQLRemoveDSNFromIni(name);
    }
    if(fRequest==ODBC_CONFIG_DSN) {
        trace(" ODBC_CONFIG_DSN");  
        SQLGetPrivateProfileString(
            name, 
            "URL", "", configuration.url, MAX_STRING_LEN,
            "ODBC.INI");
        SQLGetPrivateProfileString(
            name, 
            "User", "", configuration.user, MAX_STRING_LEN,
            "ODBC.INI");
        SQLGetPrivateProfileString(
            name, 
            "Password", "", configuration.password, MAX_STRING_LEN,
            "ODBC.INI");
    } else if(fRequest==ODBC_ADD_DSN) {
        // ok
        trace(" ODBC_ADD_DSN");  
    } else {
        // error
        trace(" ?");  
        return FALSE;
    }
    HMODULE module = GetModuleHandle("h2odbc");
    if(hwndParent) {
        int result=DialogBoxParam(module, 
                MAKEINTRESOURCE(IDD_CONFIG), 
                hwndParent, 
                (DLGPROC)ConfigDlgProc, 
                (LPARAM)&configuration);
        if(result != IDOK) {
            trace(" result != IDOK, %d lastError=%d", result, GetLastError());
            return TRUE;
        }
    }
    
    trace(" SQLRemoveDSNFromIni...");
    SQLRemoveDSNFromIni(name);
    SQLWriteDSNToIni(configuration.name,lpszDriver);
    SQLWritePrivateProfileString(
        configuration.name, "URL", configuration.url,
        "ODBC.INI");
    SQLWritePrivateProfileString(
        configuration.name, "User", configuration.user,
        "ODBC.INI");
    SQLWritePrivateProfileString(
        configuration.name, "Password", configuration.password,
        "ODBC.INI");
    

    // TODO: temp hack (because m_dll is 0?)
    // SQLRemoveDSNFromIni(name);
    // SQLWriteDSNToIni("Test",lpszDriver);
    // SQLWritePrivateProfileString(
    //     configuration.name, "URL", "jdbc:h2:localhost:9082",
    //     "ODBC.INI");
    // SQLWritePrivateProfileString(
    //     configuration.name, "User", "sa",
    //     "ODBC.INI");
    // SQLWritePrivateProfileString(
    //     configuration.name, "Password", "123",
    //     "ODBC.INI");

    trace(" return TRUE");
    return TRUE;
}
