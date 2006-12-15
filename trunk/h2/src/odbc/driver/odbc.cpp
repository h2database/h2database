/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

HINSTANCE m_dll = 0;

/*
static FILE* m_logfile=NULL;
void trace(char* message,...) {    
    if(m_logfile==NULL) {
        m_logfile=fopen("C:\\h2odbc.log","a+");
    }
    va_list valist;
    va_start(valist,message);
    vfprintf(m_logfile,message, valist);
    fprintf(m_logfile,"\r\n");
    va_end(valist);
    fflush(m_logfile);
}
*/

bool initialized = false;
bool traceOn = false;
#define BUFFER_SIZE 1024
char tracePath[BUFFER_SIZE+1];

void initTrace() {
    if(initialized) {
        return;
    }
    char* key = "Software\\H2\\ODBC"; 
    HKEY hk; 
    DWORD dwDisp;     
    if (RegCreateKeyEx(HKEY_CURRENT_USER, key, 
          0, NULL, REG_OPTION_NON_VOLATILE,
          KEY_WRITE, NULL, &hk, &dwDisp) != ERROR_SUCCESS) {
        return;
    }
    if (RegOpenKeyEx(HKEY_CURRENT_USER, key, 0, KEY_QUERY_VALUE, &hk) != ERROR_SUCCESS) {
        initialized = true;
        return;
    }
    DWORD bufferSize = BUFFER_SIZE;
    if(RegQueryValueEx(hk, "LogFile", NULL, NULL, (BYTE*)tracePath, &bufferSize) != ERROR_SUCCESS) {
        RegCloseKey(hk);
        initialized = true;
        return;
    }
    tracePath[bufferSize] = 0;
    RegCloseKey(hk);
    traceOn = bufferSize > 0; 
    initialized = true;
}

void trace(char* message,...) {    
    initTrace();
    if(!traceOn) {
        return;
    }
    FILE* log = fopen(tracePath,"a+");
    va_list valist;
    va_start(valist,message);
    vfprintf(log,message, valist);
    fprintf(log,"\r\n");
    va_end(valist);
    fflush(log);
    fclose(log);
}

int WINAPI DllMain(HINSTANCE hInst,DWORD fdwReason,PVOID pvReserved) {
    if(fdwReason==DLL_PROCESS_ATTACH) {
        m_dll=hInst;
    }
    return 1;
}

int _export FAR PASCAL libmain(HANDLE hModule,short cbHeapSize,
         SQLCHAR FAR *lszCmdLine) {
    trace("libmain");
    m_dll = (HINSTANCE) hModule;
    return TRUE;
}

void Connection::appendStatement() {
    Statement* s=new Statement(this);
    m_stats.push_back(s);
}

void Connection::removeStatement(int i) {
    delete m_stats[i];
    m_stats.erase(m_stats.begin()+i);
}

void Connection::open(string name, string user, string password) {
    trace("Connection::open");
    m_name=name;
    m_user=user;
    m_password=password;
    trace("url=%s user=%s", (char*)m_name.data(), (char*)m_user.data());
    string prefix = "jdbc:h2:odbc://";
    if(strncmp((char*)name.data(), (char*)prefix.data(), prefix.length()) != 0) {
        trace("url does not start with prefix");
        m_state == C_CLOSED;
        return;
    }
    string server = name.substr(prefix.length());
    trace("server %s", (char*)server.data());
    int dbnameIdx = server.find('/');
    if(dbnameIdx <= 0) {
        trace("url does not contain '/'");
        setError("Wrong URL format");
        m_state == C_CLOSED;
        return;
    }
    string dbname = server.substr(dbnameIdx+1);
    trace("dbname %s", dbname.data());
    server = server.substr(0, dbnameIdx);
    int portIdx = server.find(':');
    trace("portIdx %d", portIdx);
    int port = 9082;
    if(portIdx != 0) {
        string portString = server.substr(portIdx+1);
        trace("portString %s", (char*)portString.data());
        port = atoi((char*)portString.data());
        trace("port %d", port);
        server = server.substr(0, portIdx);
        trace("server %s", server.data());
    }
    m_socket = new Socket(server.data(), port);
    if(m_socket->isClosed()) {
        setError(E_08001); // unable to establish
        m_state == C_CLOSED;
        return;
    }
    m_socket->writeByte('C');
    m_socket->writeString(dbname.data())->writeString(m_user.data())->writeString(m_password.data());
    int result = m_socket->readByte();
    if(result=='O') {
        trace("ok!");
        m_state == C_OPEN;
    } else {
        trace("error");
        setError(E_08004); // rejected
        m_state == C_CLOSED;
    }
}

void Connection::setAutoCommit(bool autocommit) {
    if(autocommit != m_autoCommit) {
        m_socket->writeByte('A');
        m_socket->writeByte(autocommit ? '1' : '0');
        m_autoCommit=autocommit;
    }
}

void Connection::commit() {
    m_socket->writeByte('A');
    m_socket->writeByte('C');
}

void Connection::rollback() {
    m_socket->writeByte('A');
    m_socket->writeByte('R');
}

void Connection::close() {
    if(m_state == C_OPEN) {
        m_socket->close();
        m_state=C_CLOSED;
    }
}

string Connection::getNativeSQL(const char* sql) {
    Socket* s = m_socket;
    s->writeByte('M');
    s->writeByte('N');
    s->writeString(sql);
    string native = s->readString();
    return native;
}   

Environment* Environment::cast(void* pointer) {
    if(pointer==0) {
        return 0;
    }
    Environment* env=(Environment*)pointer;
    if(env->m_magic!=MAGIC_ENVIRONMENT) {
        return 0;
    }
    return env;
}

Connection* Connection::cast(void* pointer) {
    if(pointer==0) {
        return 0;
    }
    Connection* conn=(Connection*)pointer;
    if(conn->m_magic!=MAGIC_CONNECTION) {
        return 0;
    }
    return conn;
}

Connection::Connection(Environment* e) {
    m_magic = MAGIC_CONNECTION;
    m_environment = e;
    m_state = C_INIT;
    m_error = 0;
}

Connection::~Connection() {
    if(m_magic==MAGIC_CONNECTION) {
        m_magic=0;
    } else {
        trace("~Connection %d",m_magic);
        return;
    }
}

Environment::Environment() {
    m_magic=MAGIC_ENVIRONMENT;
}

Environment::~Environment() {
    if(m_magic==MAGIC_ENVIRONMENT) {
        m_magic=0;
    } else {
        trace("~Environment %d",m_magic);
        return;
    }    
}

Connection* Environment::createConnection() {
    m_openConnections++;
    return new Connection(this);
}

void Environment::closeConnection(Connection* conn) {
    delete conn;
    m_openConnections--;
}



