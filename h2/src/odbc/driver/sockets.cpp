/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

bool m_socket_init = false;

Socket::Socket(const char* host,int port) {
    if(!m_socket_init) {
        initSockets();
        m_socket_init = true;
    }
    m_socket=socket(PF_INET,SOCK_STREAM,IPPROTO_TCP);
    if(m_socket==INVALID_SOCKET) {
        setError("socket");
        return;
    }
    // check if it's an IP number (because that's simpler)
    // could be also IPv6 or something, if Winsocks supports that
    // also supported is 0x4.0x3.0x2.0x10
    SOCKADDR_IN sin;
    memset((void*)&sin,0,sizeof(sin));
    sin.sin_family=AF_INET;
    sin.sin_port=htons(port);
    long addr=inet_addr(host);
    if(addr=!INADDR_NONE) {
        sin.sin_addr.s_addr=addr;
    } else {
        // ok then it's hopefully a host name
        HOSTENT* hostent;
        hostent=gethostbyname(host);
        if(hostent==NULL) {
            setError("gethostbyname");
            return;
        }
        // take the first ip address
        memcpy(
            &sin.sin_addr,
            hostent->h_addr_list[0],
            hostent->h_length);
    }
    int result=connect(m_socket,(sockaddr*)&sin,sizeof(sin));
    if(result==SOCKET_ERROR) {
        setError("connect");
        return;
    }
}

void Socket::close() {
    if(m_socket==0) {
        return;
    }
    closesocket(m_socket);
    m_socket=0;
}

int Socket::readByte() {
    if(m_socket==0) {
        return -1;
    }
    char buffer[1];
    read(buffer,1);
    // trace("  readByte=%c", buffer[0]);
    return buffer[0];
}

int Socket::readInt() {
    if(m_socket==0) {
        return -1;
    }
    char buffer[4];
    read(buffer, 4);
    int x = ((buffer[0] & 0xff) << 24) | ((buffer[1] & 0xff) << 16) | 
            ((buffer[2] & 0xff) << 8) | (buffer[3] & 0xff);
    return x;
}

void Socket::read(char* buffer, int len) {
    if(m_socket==0) {
        return;
    }
    do {
        int result=recv(m_socket,buffer,len,0);
        if(result==0) {
            // closed?
            setError("recv");    
            return;
        } else if(result==SOCKET_ERROR) {
            setError("recv");    
            return;
        } 
        len-=result;
        buffer+=result;
    } while(len>0);
}

Socket* Socket::writeString(const char* string) {
    // trace("  writeString %s", string);
    int len = strlen(string);
    writeInt(len);
    write(string, len);
    return this;
}

string Socket::readString() {
    if(m_socket==0) {
        return "";
    }
    int len = readInt();
    if(len<0) {
        len = 0;
    }
    char buffer[len+1];
    if(len==0) {
        buffer[0]=0;
    } else {
        read(buffer, len);
        buffer[len] = 0;
    }
    // trace("  readString len=%d s=%s", len, buffer);        
    return buffer;
}

Socket* Socket::writeByte(int byte) {
    // trace("  writeByte %c", byte);
    char buffer[1];
    buffer[0]=byte;
    write(buffer, 1);
    return this;    
}

Socket* Socket::writeBool(bool x) {
    // trace("  writeBoox %d", x ? 1 : 0);
    writeInt(x ? 1 : 0);
    return this;    
}

bool Socket::readBool() {
    return readInt() == 1;
}

Socket* Socket::writeInt(int x) {
    // trace("  writeInt=%d", x);
    char buffer[4];
    buffer[0] = (x >> 24) & 0xff;
    buffer[1] = (x >> 16) & 0xff;
    buffer[2] = (x >> 8) & 0xff;
    buffer[3] = x & 0xff;
    write(buffer, 4);
    return this;
}

void Socket::write(const char* buffer, int len) {
    if(m_socket==0) {
        return;
    }
    do {
        int result=send(m_socket,buffer,len,0);
        if(result==SOCKET_ERROR) {
            setError("send");
            return;
        }
        len-=result;
        buffer+=result;
    } while(len>0);
}

void Socket::setError(char* where) {
    m_socket=0;
    int error=WSAGetLastError();
    trace("Socket error %d in %s", error, where);
}

void initSockets() {
    trace("initSockets");
    WORD wVersionRequested;
    WSADATA wsaData;
    wVersionRequested = MAKEWORD( 1, 1 );
    int err = WSAStartup( wVersionRequested, &wsaData );
    if ( err != 0 ) {
        trace("No Winsock.dll");
        return;
    }
    if ( LOBYTE( wsaData.wVersion ) != 1 ||
             HIBYTE( wsaData.wVersion ) != 1 ) {
        trace("No Winsock.dll v1.1");
        WSACleanup( );
        return;   
    }
}
