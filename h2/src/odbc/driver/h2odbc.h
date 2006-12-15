/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */
 
#include <winsock.h>
#include <windows.h>
#include <odbcinst.h>
#include <sqlext.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <vector>

#include "resource.h"

using namespace std;

const int MAX_STRING_LEN=511;

void initSockets();
void trace(char* message,...);
void setString(char* dest, int dest_len, SQLCHAR *source, SQLSMALLINT source_len);
void returnString(SQLCHAR* dest, SQLSMALLINT dest_len, SQLSMALLINT* dest_pt, const char* source);
void returnString(SQLPOINTER dest, SQLINTEGER dest_len, SQLINTEGER* dest_pt, const char* source);
void returnInt(SQLPOINTER InfoValue, SQLINTEGER* LengthPtr, SQLUINTEGER value);
void returnInt(SQLPOINTER NumericPtr, int value);
void returnInt(SQLPOINTER InfoValue, SQLSMALLINT* LengthPtr, SQLUINTEGER value);
void returnSmall(SQLPOINTER InfoValue, SQLSMALLINT* LengthPtr, SQLUSMALLINT value);
void returnSmall(SQLPOINTER InfoValue, SQLINTEGER* LengthPtr, SQLUSMALLINT value);
void returnPointer(SQLPOINTER pointer, void* value);
int getDefaultCType(int sqlType);

extern HINSTANCE m_dll;
extern bool m_socket_init;

class Socket {
    SOCKET m_socket;
private:    
    void setError(char* where);
    void read(char* buffer, int len);
    void write(const char* buffer, int len);
public:
    Socket(const char* host,int port);
    void close();
    int readByte();
    int readInt();
    bool readBool();
    string readString();
    Socket* writeByte(int byte);
    Socket* writeBool(bool x);
    Socket* writeInt(int x);
    Socket* writeString(const char* string);
    bool isClosed() {
        return m_socket != 0;
    }
};

class Environment;
class Connection;
class Statement;
class Descriptor;
class DescriptorRecord;

enum DescriptorState {D_ACTIVE,D_FREE};    
enum DescriptorType {DT_DEFAULT,DT_SHARED};
enum ConnectionState {C_INIT,C_OPEN,C_CLOSED};
enum StatementState {S_PREPARED,S_EXECUTED,S_POSITIONED,S_CLOSED};

enum Send {S_EXECUTE};
const int MAGIC_ENVIRONMENT=0xABC0;
const int MAGIC_CONNECTION=0xABC1;
const int MAGIC_STATEMENT=0xABC2;
const int MAGIC_DESCRIPTOR=0xABC3;
const int MAGIC_DESCRIPTOR_RECORD=0xABC4;

class Descriptor {
    int m_magic;
    Connection* m_connection;
    int m_id;
    int m_type;
    int m_arraySize;
    int m_count;
    SQLUSMALLINT* m_statusPointer;
    SQLUINTEGER* m_rowsProcessedPointer;
    
    DescriptorState m_state;
    vector<Statement*> m_bound;
    vector<DescriptorRecord*> m_records;
    char* m_error;
    bool m_rowWiseBinding;
    int m_rowSize;
public:
    static Descriptor* cast(void* pointer);
    // create a shared descriptor
    Descriptor(Connection* c);
    // create a default descriptor (not shared)
    Descriptor();
    ~Descriptor();
    void init();
    bool isFreed() {
        return m_state==D_FREE;
    }
    void clearRecords() {
        m_records.clear();
    }
    void addRecord(DescriptorRecord* rec) {
        m_records.push_back(rec);
    }
    void readData(int i, Socket* s);
    DescriptorRecord* getRecord(int i) {
        return m_records[i];
    }
    int getRecordCount() {
        return m_records.size();
    }
    void setStatusPointer(SQLUSMALLINT* newPointer) {
        m_statusPointer = newPointer;
    }
    SQLUSMALLINT* getStatusPointer() {
        return m_statusPointer;
    }
    void setRowsProcessedPointer(SQLUINTEGER* rowsProcessedPointer) {
        m_rowsProcessedPointer = rowsProcessedPointer;
    }
    void setRowsProcessed(int rows) {
        if(m_rowsProcessedPointer != 0) {
            *m_rowsProcessedPointer = rows;
        }
    }
    void setStatus(SQLUSMALLINT value) {
        if(m_statusPointer != 0) {
            *m_statusPointer = value;
        }
    }   
    void setError(char* error) {
        m_error = error;
    }
    char* getError() {
        return m_error;
    }
    void setBindingType(bool rowWiseBinding, int rowSize);
};

/*
Header:
    SQL_DESC_ALLOC_TYPE                 (readonly)
    SQL_DESC_BIND_TYPE                  (read/write)
    SQL_DESC_ARRAY_SIZE                 (read/write)
    SQL_DESC_COUNT                      (readonly)
    SQL_DESC_ARRAY_STATUS_PTR           (read/write)
    SQL_DESC_ROWS_PROCESSED_PTR         (read/write)
    SQL_DESC_BIND_OFFSET_PTR            (read/write)
    
Record:
    SQL_DESC_AUTO_UNIQUE_VALUE     
    SQL_DESC_LOCAL_TYPE_NAME            SQLCHAR* TypeName  
    SQL_DESC_BASE_COLUMN_NAME             SQLCHAR* TableName         
    SQL_DESC_NAME                       SQLCHAR* ColumnName / DisplayName
    SQL_DESC_BASE_TABLE_NAME            SQLCHAR* TableName         
    SQL_DESC_NULLABLE
    SQL_DESC_CASE_SENSITIVE     
    SQL_DESC_OCTET_LENGTH
    SQL_DESC_CATALOG_NAME               SQLCHAR* CatalogName         
    SQL_DESC_OCTET_LENGTH_PTR
    SQL_DESC_CONCISE_TYPE                 SQLSMALLINT DataType
    SQL_DESC_PARAMETER_TYPE
    SQL_DESC_DATA_PTR     
    SQL_DESC_PRECISION
    SQL_DESC_DATETIME_INTERVAL_CODE     
    SQL_DESC_SCALE
    SQL_DESC_DATETIME_INTERVAL_PRECISION     
    SQL_DESC_SCHEMA_NAME                SQLCHAR* SchemaName         
    SQL_DESC_DISPLAY_SIZE     
    SQL_DESC_SEARCHABLE
    SQL_DESC_FIXED_PREC_SCALE     
    SQL_DESC_TABLE_NAME                 SQLCHAR* TableName         
    SQL_DESC_INDICATOR_PTR     
    SQL_DESC_TYPE
    SQL_DESC_LABEL                      SQLCHAR*       
    SQL_DESC_TYPE_NAME                  SQLCHAR*     
    SQL_DESC_LENGTH     
    SQL_DESC_UNNAMED
    SQL_DESC_LITERAL_PREFIX             SQLCHAR*         
    SQL_DESC_UNSIGNED
    SQL_DESC_LITERAL_SUFFIX             SQLCHAR*         
    SQL_DESC_UPDATABLE
*/

class DescriptorRecord {
    int m_magic;
    Descriptor* m_descriptor;
    int m_sqlDataType;
    int m_cDataType;
    string m_name;
    string m_columnName;
    string m_tableName;
    SQLPOINTER m_pointer;
    SQLINTEGER* m_statusPointer;
    int m_targetBufferLength;
    int m_dataInt;
    string m_dataString;
    int m_precision;
    int m_scale;
    int m_displaySize;
    bool m_wasNull;
public:
    static DescriptorRecord* cast(void* pointer);
    DescriptorRecord(Descriptor* d) {
        m_magic=MAGIC_DESCRIPTOR_RECORD;
        m_descriptor=d;
        m_sqlDataType = 0;
        m_cDataType = 0;
        m_pointer = 0;
        m_statusPointer = 0;
        m_targetBufferLength = 0;
        m_dataInt = 0;
        m_precision = 0;
        m_scale = 0;
        m_displaySize = 0;
        m_wasNull = false;
    }
    ~DescriptorRecord() {
        if(m_magic==MAGIC_DESCRIPTOR_RECORD) {
            m_magic=0;
        } else {
            trace("~DescriptorRecord %d",m_magic);
            return;
        }
    }
    void setSqlDataType(int sqlDataType) {
        m_sqlDataType = sqlDataType;
    }
    void setCDataType(int cDataType) {
        m_cDataType = cDataType;
    }
    void setTargetBufferLength(int len) {
         m_targetBufferLength = len;
    }
    void setTargetPointer(SQLPOINTER pointer) {
        m_pointer = pointer;
    }
    void setTargetStatusPointer(SQLINTEGER* statusPointer) {
        m_statusPointer = statusPointer;
    }
    int getSqlDataType() {
        return m_sqlDataType;
    }
    bool hasFixedPrecisionScale();
    int getDisplaySize() {
        return m_displaySize;
    }
    int getPrecision() {
        return m_precision;
    }
    int getScale() {
        return m_scale;
    }
    string getColumnName() {
        return m_columnName;
    }
    string getTableName() {
        return m_tableName;
    }
    char* getString();
    int getInt();
    void readMeta(Socket* s);
    void sendParameterValue(Socket* s);
    void readData(Socket* s);
    void copyData(DescriptorRecord* ar);
    bool wasNull() {
        return m_wasNull;
    }
    void setNull();
    int getLength();
    const char* getPrefix();
    const char* getSuffix();
};

class Environment {
    int m_magic;
    int m_id;
    int m_openConnections;
    int m_behavior;
    char* m_error;    
public:
    static Environment* cast(void* pointer);
    Environment();
    ~Environment();
    Connection* createConnection();
    void closeConnection(Connection* conn);
    int getOpenConnectionCount() {
        return m_openConnections;
    }
    void setBehavior(int behavior) {
        m_behavior=behavior;
    }
    int getBehavior() {
        return m_behavior;
    }
    void setError(char* error) {
        m_error = error;
    }
    char* getError() {
        return m_error;
    }    
};

class Connection {
    int m_magic;
    friend class Environment;
    Environment* m_environment;
    ConnectionState m_state;
    int m_id;
    string m_name;
    string m_user;
    string m_password;
    bool m_readOnly;
    bool m_autoCommit;
    vector<Statement*> m_stats;
    Socket* m_socket;
    char* m_error;
    string m_dataSourceName;
private:
    Connection(Environment* e);
    void appendStatement();
    void removeStatement(int i);
    ~Connection();
    
public:
    static Connection* cast(void* pointer);
    Environment* getEnvironment() {
        return m_environment;
    }
    void open(string name,string user,string password);
    bool isClosed() {
        return m_state==C_CLOSED;
    }
    void close();
    void setReadOnly(bool readonly) {
        m_readOnly=readonly;
    }
    bool getReadOnly() {
        return m_readOnly;
    }
    void setAutoCommit(bool autocommit);
    void commit();
    void rollback();
    bool getAutoCommit() {
        return m_autoCommit;
    }
    Socket* getSocket() {
        return m_socket;
    }
    void setError(char* error) {
        m_error = error;
    }
    char* getError() {
        return m_error;
    }
    string getNativeSQL(const char* sql);    
    void setDataSourceName(string dataSourceName) {
        m_dataSourceName = dataSourceName;
    }
    string getDataSourceName() {
        return m_dataSourceName;
    }
};

class Statement {
    int m_magic;
    Connection* m_connection;
    int m_id;
    Descriptor m_appRowDefault;
    Descriptor m_impRowDefault;
    Descriptor m_appParamDefault;
    Descriptor m_impParamDefault;
    Descriptor* m_appRow;
    Descriptor* m_impRow;
    Descriptor* m_appParam;
    Descriptor* m_impParam;
    StatementState m_state;
    string m_sql;
    int m_columnCount;
    int m_updateCount;
    int m_resultSetId;
    int m_preparedId;
    int m_rowId;
    bool m_hasResultSet;
    int m_parameterCount;
    bool m_useBookmarks;
    char* m_error;
private:
    void processResultSet(Socket* s);
public:
    static Statement* cast(void* pointer);
    Statement(Connection* c);
    ~Statement();
    bool prepare(char* sql);
    bool execute(char* sql);
    bool executePrepared();
    void getMetaTables(char* catalog, char* schema, char* table, char* tabletypes);
    void getMetaColumns(char* catalog, char* schema, char* table, char* column);
    void getMetaVersionColumns(char* catalog, char* schema, char* table);
    void getMetaBestRowIdentifier(char* catalog, char* schema, char* table, int scope, bool nullable);
    void getMetaIndexInfo(char* catalog, char* schema, char* table, bool unique, bool approximate);
    void getMetaTypeInfoAll();
    void getMetaTypeInfo(int sqlType);
    Descriptor* getAppRowDesc();
    Descriptor* getImpRowDesc();
    Descriptor* getAppParamDesc();
    Descriptor* getImpParamDesc();
    int getColumnCount() {
        return m_columnCount;
    }
    bool next();
    int getParametersCount() {
        return m_parameterCount;
    }
    int getUpdateCount() {
        return m_updateCount;
    }
    const char* getSQL();
    void closeCursor();
    void setUseBookmarks(bool bookmarks) {
        m_useBookmarks = bookmarks;
    }
    bool getUseBookmarks() {
        return m_useBookmarks;
    }
    int getRowId() {
        return m_rowId;
    }
    void setError(char* error) {
        m_error = error;
    }
    char* getError() {
        return m_error;
    }    
    void addParameter();
};

#define E_01000 "01000 General warning"
#define E_01001 "01001 Cursor operation conflict"
#define E_01002 "01002 Disconnect error"
#define E_01003 "01003 NULL value eliminated in set function"
#define E_01004 "01004 String data, right-truncated"
#define E_01006 "01006 Privilege not revoked"
#define E_01007 "01007 Privilege not granted"
#define E_01S00 "01S00 Invalid connection string attribute"
#define E_01S01 "01S01 Error in row"
#define E_01S02 "01S02 Option value changed"
#define E_01S06 "01S06 Attempt to fetch before the result set returned the first rowset"
#define E_01S07 "01S07 Fractional truncation"
#define E_01S08 "01S08 Error saving File DSN"
#define E_01S09 "01S09 Invalid keyword"
#define E_07001 "07001 Wrong number of parameters"
#define E_07002 "07002 COUNT field incorrect"
#define E_07005 "07005 Prepared statement not a cursor-specification"
#define E_07006 "07006 Restricted data type attribute violation"
#define E_07009 "07009 Invalid descriptor index"
#define E_07S01 "07S01 Invalid use of default parameter"
#define E_08001 "08001 Client unable to establish connection"
#define E_08002 "08002 Connection name in use"
#define E_08003 "08003 Connection does not exist"
#define E_08004 "08004 Server rejected the connection"
#define E_08007 "08007 Connection failure during transaction"
#define E_08S01 "08S01 Communication link failure"
#define E_21S01 "21S01 Insert value list does not match column list"
#define E_21S02 "21S02 Degree of derived table does not match column list"
#define E_22001 "22001 String data, right-truncated"
#define E_22002 "22002 Indicator variable required but not supplied"
#define E_22003 "22003 Numeric value out of range"
#define E_22007 "22007 Invalid datetime format"
#define E_22008 "22008 Datetime field overflow"
#define E_22012 "22012 Division by zero"
#define E_22015 "22015 Interval field overflow"
#define E_22018 "22018 Invalid character value for cast specification"
#define E_22019 "22019 Invalid escape character"
#define E_22025 "22025 Invalid escape sequence"
#define E_22026 "22026 String data, length mismatch"
#define E_23000 "23000 Integrity constraint violation"
#define E_24000 "24000 Invalid cursor state"
#define E_25000 "25000 Invalid transaction state"
#define E_25S01 "25S01 Transaction state"
#define E_25S02 "25S02 Transaction is still active"
#define E_25S03 "25S03 Transaction is rolled back"
#define E_28000 "28000 Invalid authorization specification"
#define E_34000 "34000 Invalid cursor name"
#define E_3C000 "3C000 Duplicate cursor name"
#define E_3D000 "3D000 Invalid catalog name"
#define E_3F000 "3F000 Invalid schema name"
#define E_40001 "40001 Serialization failure"
#define E_40002 "40002 Integrity constraint violation"
#define E_40003 "40003 Statement completion unknown"
#define E_42000 "42000 Syntax error or access violation"
#define E_42S01 "42S01 Base table or view already exists"
#define E_42S02 "42S02 Base table or view not found"
#define E_42S11 "42S11 Index already exists"
#define E_42S12 "42S12 Index not found"
#define E_42S21 "42S21 Column already exists"
#define E_42S22 "42S22 Column not found"
#define E_44000 "44000 WITH CHECK OPTION violation"
#define E_HY000 "HY000 General error"
#define E_HY001 "HY001 Memory allocation error"
#define E_HY003 "HY003 Invalid application buffer type"
#define E_HY004 "HY004 Invalid SQL data type"
#define E_HY007 "HY007 Associated statement is not prepared"
#define E_HY008 "HY008 Operation canceled"
#define E_HY009 "HY009 Invalid use of null pointer"
#define E_HY010 "HY010 Function sequence error"
#define E_HY011 "HY011 Attribute cannot be set now"
#define E_HY012 "HY012 Invalid transaction operation code"
#define E_HY013 "HY013 Memory management error"
#define E_HY014 "HY014 Limit on the number of handles exceeded"
#define E_HY015 "HY015 No cursor name available"
#define E_HY016 "HY016 Cannot modify an implementation row descriptor"
#define E_HY017 "HY017 Invalid use of an automatically allocated descriptor handle"
#define E_HY018 "HY018 Server declined cancel request"
#define E_HY019 "HY019 Non-character and non-binary data sent in pieces"
#define E_HY020 "HY020 Attempt to concatenate a null value"
#define E_HY021 "HY021 Inconsistent descriptor information"
#define E_HY024 "HY024 Invalid attribute value"
#define E_HY090 "HY090 Invalid string or buffer length"
#define E_HY091 "HY091 Invalid descriptor field identifier"
#define E_HY092 "HY092 Invalid attribute/option identifier"
#define E_HY095 "HY095 Function type out of range"
#define E_HY096 "HY096 Invalid information type"
#define E_HY097 "HY097 Column type out of range"
#define E_HY098 "HY098 Scope type out of range"
#define E_HY099 "HY099 Nullable type out of range"
#define E_HY100 "HY100 Uniqueness option type out of range"
#define E_HY101 "HY101 Accuracy option type out of range"
#define E_HY103 "HY103 Invalid retrieval code"
#define E_HY104 "HY104 Invalid precision or scale value"
#define E_HY105 "HY105 Invalid parameter type"
#define E_HY106 "HY106 Fetch type out of range"
#define E_HY107 "HY107 Row value out of range"
#define E_HY109 "HY109 Invalid cursor position"
#define E_HY110 "HY110 Invalid driver completion"
#define E_HY111 "HY111 Invalid bookmark value"
#define E_HYC00 "HYC00 Optional feature not implemented"
#define E_HYT00 "HYT00 Timeout expired"
#define E_HYT01 "HYT01 Connection timeout expired"
#define E_IM001 "IM001 Driver does not support this function"
#define E_IM002 "IM002 Data source name not found and no default driver specified"
#define E_IM003 "IM003 Specified driver could not be loaded"
#define E_IM004 "IM004 Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"
#define E_IM005 "IM005 Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"
#define E_IM006 "IM006 Driver's SQLSetConnectAttr failed"
#define E_IM007 "IM007 No data source or driver specified; dialog prohibited"
#define E_IM008 "IM008 Dialog failed"
#define E_IM009 "IM009 Unable to load translation DLL"
#define E_IM010 "IM010 Data source name too long"
#define E_IM011 "IM011 Driver name too long"
#define E_IM012 "IM012 DRIVER keyword syntax error"
#define E_IM013 "IM013 Trace file error"
#define E_IM014 "IM014 Invalid name of File DSN"
#define E_IM015 "IM015 Corrupt file data source"


