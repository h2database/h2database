/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include <cstdlib>
#include <iostream>
#include <winsock.h>
#include <windows.h>
#include <odbcinst.h>
#include <sqlext.h>

using namespace std;

void test();

int main(int argc, char *argv[]) {
    test();
    system("PAUSE");
    return EXIT_SUCCESS;
}

SQLHENV     henv;
SQLHDBC     hdbc;
SQLHSTMT    hstmt;
SQLRETURN   retcode;

#define NAME_LEN 20

void showError() {
    SQLCHAR sqlState[6], msg[100];
    SQLINTEGER nativeError;
    SQLSMALLINT i, msgLen;
    SQLRETURN retcode = SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlState, &nativeError, msg, sizeof(msg), &msgLen);
    if(retcode == SQL_NO_DATA) {
        printf("Error: no data\n");
    } else {
        printf("Error: state=%s msg=%s native=%d\n", sqlState, msg, nativeError);
    }  
}

void testBindParameter() {
    /* Prepare the SQL statement with parameter markers. */
    retcode = SQLPrepare(hstmt,
        (SQLCHAR*)"INSERT INTO TEST(ID, NAME) VALUES (?, ?)", SQL_NTS);
    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
        /* Specify data types and buffers for OrderID, CustID, OpenDate, SalesPerson, */
        /* Status parameter data. */
        SQLINTEGER id;
        SQLCHAR name[NAME_LEN];
        SQLINTEGER cbId = 0, cbName = SQL_NTS; 
        SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_SSHORT,
            SQL_INTEGER, 0, 0, &id, 0, &cbId);
        SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
            SQL_CHAR, NAME_LEN, 0, name, 0, &cbName); 
        /* Specify first row of parameter data. */
        id = 1002;   
        strcpy((char*)name, "Galaxy2");
        /* Execute statement with first row. */
        retcode = SQLExecute(hstmt);   
        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
            printf("inserted\n");
        } else {
            showError();
        }
    } else {
        showError();
    }
}

void testTables() {
    SQLCHAR name[100];
    SQLCHAR remark[100];
    SQLINTEGER cbName = SQL_NTS, cbRemark = SQL_NTS;
     
    printf("catalogs\n");
    retcode = SQLTables(hstmt, (SQLCHAR*)SQL_ALL_CATALOGS, SQL_NTS, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS); 
    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
        SQLBindCol(hstmt, 1, SQL_C_CHAR, name, sizeof(name), &cbName);
        SQLBindCol(hstmt, 5, SQL_C_CHAR, remark, sizeof(remark), &cbRemark);
        while (TRUE) {
            retcode = SQLFetch(hstmt);
            if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
                showError();
            } else if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
                printf("catalog: %s remark: %s\n", name, remark);
            } else {
                break;
            }
        }        
    } else {
        showError();
    }

    printf("schemas\n");
    retcode = SQLTables(hstmt, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)SQL_ALL_SCHEMAS, SQL_NTS, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS); 
    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
        SQLBindCol(hstmt, 2, SQL_C_CHAR, name, sizeof(name), &cbName);
        SQLBindCol(hstmt, 5, SQL_C_CHAR, remark, sizeof(remark), &cbRemark);
        while (TRUE) {
            retcode = SQLFetch(hstmt);
            if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
                showError();
            } else if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
                printf("schema: %s remark: %s\n", name, remark);
            } else {
                break;
            }
        }        
    } else {
        showError();
    }

    printf("tableTypes\n");
    retcode = SQLTables(hstmt, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)SQL_ALL_TABLE_TYPES, SQL_NTS); 
    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
        SQLBindCol(hstmt, 4, SQL_C_CHAR, name, sizeof(name), &cbName);
        SQLBindCol(hstmt, 5, SQL_C_CHAR, remark, sizeof(remark), &cbRemark);
        while (TRUE) {
            retcode = SQLFetch(hstmt);
            if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
                showError();
            } else if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
                printf("tableType: %s remark: %s\n", name, remark);
            } else {
                break;
            }
        }        
    } else {
        showError();
    }

    printf("tables\n");
    retcode = SQLTables(hstmt, (SQLCHAR*)"%", SQL_NTS, (SQLCHAR*)"%", SQL_NTS, (SQLCHAR*)"%", SQL_NTS, (SQLCHAR*)"%", SQL_NTS); 
    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
        SQLBindCol(hstmt, 3, SQL_C_CHAR, name, sizeof(name), &cbName);
        SQLBindCol(hstmt, 5, SQL_C_CHAR, remark, sizeof(remark), &cbRemark);
        while (TRUE) {
            retcode = SQLFetch(hstmt);
            if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
                showError();
            } else if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
                printf("table: %s remark: %s\n", name, remark);
            } else {
                break;
            }
        }        
    } else {
        showError();
    }
    
    printf("done\n");
}

void testBindCol() {
    SQLINTEGER id;
    SQLCHAR name[NAME_LEN];
    SQLINTEGER cbId, cbName;
    SQLRETURN retcode = SQLExecDirect(hstmt, (SQLCHAR*)"SELECT ID, NAME FROM TEST", SQL_NTS);
    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
        /* Bind columns 1, 2 */
        SQLBindCol(hstmt, 1, SQL_C_ULONG, &id, 0, &cbId);
        SQLBindCol(hstmt, 2, SQL_C_CHAR, name, NAME_LEN, &cbName);
        /* Fetch and print each row of data. On */
        /* an error, display a message and exit. */
        while (TRUE) {
            retcode = SQLFetch(hstmt);
            if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
                showError();
            } else if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
                printf("id=%-5d %-*s\n", id, NAME_LEN-1, name);
            } else {
                break;
            }
        }
    } else {
       showError();
    }
}

void test() {
    /*Allocate environment handle */
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
        /* Set the ODBC version environment attribute */
        retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0); 
        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
        /* Allocate connection handle */
            retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc); 
            if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
            /* Set login timeout to 5 seconds. */
                SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (void*)5, 0);
                /* Connect to data source */
                retcode = SQLConnect(hdbc, (SQLCHAR*) "Test", SQL_NTS,
                  (SQLCHAR*) "sa", SQL_NTS,
                  (SQLCHAR*) "", SQL_NTS);
                if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO){
                    /* Allocate statement handle */
                    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt); 
                    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
                        /* Process data */
                        //testBindCol();
                        //testBindParameter();
                        testTables();
                        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
                    }
                    SQLDisconnect(hdbc);
                }
                SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
            }
        }
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
    }
}       
