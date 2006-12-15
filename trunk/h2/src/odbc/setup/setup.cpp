/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include <windows.h>
#include <odbcinst.h>

#define NAME "h2odbc"

const char* DRIVER =      
    NAME "\0"
    "Driver=h2odbc.dll" "\0"
    "Setup=h2odbc.dll" "\0"
    "APILevel=0" "\0"
    "ConnectFunctions=YYN" "\0"
    "FileUsage=0\0"
    "DriverODBCVer=03.00" "\0" 
    "SQLLevel=0" "\0"
    "\0";

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

bool install(char* path) {
    // path=0 means systemdir
    DWORD usageCount=0;
    char pathout[512];
    BOOL result=SQLInstallDriverEx(
        DRIVER,
        path,
        pathout,sizeof(pathout),0,
        ODBC_INSTALL_COMPLETE,
        &usageCount);
    if(result==FALSE) {
        DWORD pfErrorCode;
        char lpszErrorMsg[512];

        SQLInstallerError(1,
            &pfErrorCode,
            lpszErrorMsg,
            sizeof(lpszErrorMsg),
            0);
        MessageBox(0,lpszErrorMsg,"Error",MB_OK);
    }
    return result==TRUE;
}

LRESULT CALLBACK WndProc(HWND hWnd, UINT message, WPARAM wParam, LPARAM lParam) {
    switch (message) {
    case WM_COMMAND:
    case WM_PAINT:
        break;
    case WM_DESTROY:
        PostQuitMessage(0);
        break;
    default:
        return DefWindowProc(hWnd, message, wParam, lParam);
   }
   return 0;
}

int APIENTRY WinMain(HINSTANCE hInstance, HINSTANCE hPrevInstance,
    LPSTR lpCmdLine, int nCmdShow) {
    HWND hWnd;
    WNDCLASSEX wcex;
    wcex.cbSize = sizeof(WNDCLASSEX); 
    wcex.style            = CS_HREDRAW | CS_VREDRAW;
    wcex.lpfnWndProc    = (WNDPROC)WndProc;
    wcex.cbClsExtra        = 0;
    wcex.cbWndExtra        = 0;
    wcex.hInstance        = hInstance;
    wcex.hIcon            = 0; //LoadIcon(hInstance, (LPCTSTR)IDI_H2ODBC_ADMIN);
    wcex.hCursor        = LoadCursor(NULL, IDC_ARROW);
    wcex.hbrBackground    = (HBRUSH)(COLOR_WINDOW+1);
    wcex.lpszMenuName    = 0; // (LPCSTR)IDC_H2ODBC_ADMIN;
    wcex.lpszClassName    = "h2odbc_setup";
    wcex.hIconSm        = 0; //LoadIcon(wcex.hInstance, (LPCTSTR)IDI_SMALL);

    RegisterClassEx(&wcex);

    hWnd = CreateWindow("h2odbc_setup", "H2ODBC", WS_OVERLAPPEDWINDOW,
        CW_USEDEFAULT, 0, CW_USEDEFAULT, 0, NULL, NULL, hInstance, NULL);
    if(!hWnd) {
        return FALSE;
    }
    
       int result;
    if(!isInstalled()) {
        result=MessageBox(0,
            "The driver is not yet installed.\n"
            "Install the driver now? If you like to install, \n"
            "you will need to locate the driver dll.",
            "ODBC Installation",MB_OKCANCEL);
        if(result!=IDOK) {
            MessageBox(0,
                "Installation canceled.",
                "ODBC Installation",MB_OK);
            return 0;
        }
        char directory[512];
        directory[0]=0;
        OPENFILENAME file;
        file.lStructSize=sizeof(file);
        file.hwndOwner=hWnd;
        file.hInstance=0;
        file.lpstrFilter="H2ODBC Driver (h2odbc.dll)" "\0" "h2odbc.dll" "\0" "\0";
        file.lpstrCustomFilter=0;
        file.nMaxCustFilter=0;
        file.nFilterIndex=0;
        file.lpstrFile=directory;
        file.nMaxFile=sizeof(directory);
        file.lpstrFileTitle=0;
        file.nMaxFileTitle=0;
        file.lpstrInitialDir=0;
        file.lpstrTitle="ODBC Installation";
        file.Flags=OFN_FILEMUSTEXIST | OFN_HIDEREADONLY;
        file.nFileOffset=0;
        file.nFileExtension=0;
        file.lpstrDefExt=0;
        file.lCustData=0;
        file.lpfnHook=0;
        file.lpTemplateName=0;
        result=GetOpenFileName(&file);
        if(result==FALSE) {
            MessageBox(0,
                "Installation canceled.",
                "ODBC Installation",MB_OK);
            return 0;
        } else {
            directory[file.nFileOffset]=0;
            if(!install(directory)) {
                MessageBox(0,
                    "There was an error while installing.",
                    "ODBC Installation",MB_OK);
                return 0;
            }
        }
        MessageBox(0,
            "Installation completed successfully.\n"
            "You can now add a new datasource to your ODBC configuration.\n"
            "The ODBC Data Source Administator dialog will appear now,\n"
            "and whenever you start this application again.",
            "ODBC Installation",MB_OK);
    }
    SQLManageDataSources(hWnd);
    return 0;
}

