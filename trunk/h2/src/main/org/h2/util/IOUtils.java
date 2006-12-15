/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

public class IOUtils {
    
    private static final int BUFFER_BLOCK_SIZE = 4 * 1024;
    
    public static void closeSilently(OutputStream out) {
        if(out != null) {
            try {
                out.close();
            } catch(IOException e) {
                // ignore
            }
        }
    }
    
    public static void skipFully(InputStream in, long skip) throws IOException {
        while(skip > 0) {
            skip -= in.skip(skip);
        }
    }

    public static void skipFully(Reader reader, long skip) throws IOException {
        while(skip > 0) {
            skip -= reader.skip(skip);
        }
    }
    
    public static long copyAndClose(InputStream in, OutputStream out) throws IOException {
        try {
            return copyAndCloseInput(in, out);
        } finally {
            out.close();
        }
    }

    public static long copyAndCloseInput(InputStream in, OutputStream out) throws IOException {
        int written = 0;
        try {
            byte[] buffer = new byte[4 * 1024];
            while(true) {
                int len = in.read(buffer);
                if(len < 0) {
                    break;
                }
                out.write(buffer, 0, len);
                written += len;                
            }
        } finally {
            in.close();
        }
        return written;
    }
    
    public static long copyAndCloseInput(Reader in, Writer out) throws IOException {
        long written = 0;
        try {
            char[] buffer = new char[4 * 1024];
            while(true) {
                int len = in.read(buffer);
                if(len < 0) {
                    break;
                }
                out.write(buffer, 0, len);
                written += len;
            }
        } finally {
            in.close();
        }
        return written;
    }    

    public static void closeSilently(InputStream in) {
        if(in != null) {
            try {
                in.close();
            } catch(IOException e) {
                // ignore
            }
        }
    }
    
    public static void closeSilently(Reader reader) {
        if(reader != null) {
            try {
                reader.close();
            } catch(IOException e) {
                // ignore
            }
        }
    }

    public static void closeSilently(Writer writer) {
        if(writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch(IOException e) {
                // ignore
            }
        }
    }

    public static byte[] readBytesAndClose(InputStream in, int length) throws IOException {
        try {
            if(length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(BUFFER_BLOCK_SIZE, length);
            ByteArrayOutputStream out=new ByteArrayOutputStream(block);
            byte[] buff=new byte[block];
            while(length > 0) {
                int len = Math.min(block, length);
                len = in.read(buff, 0, len);
                if(len < 0) {
                    break;
                }
                out.write(buff, 0, len);
                length -= len;
            }
            return out.toByteArray();
        } finally {
            in.close();
        }
    }
    
    public static String readStringAndClose(Reader in, int length) throws IOException {
        if(length <= 0) {
            length = Integer.MAX_VALUE;
        }
        int block = Math.min(BUFFER_BLOCK_SIZE, length);
        StringWriter out=new StringWriter(length == Integer.MAX_VALUE ? block : length);
        char[] buff=new char[block];
        while(length > 0) {
            int len = Math.min(block, length);
            len = in.read(buff, 0, len);
            if(len < 0) {
                break;
            }
            out.write(buff, 0, len);
            length -= len;
        }
        in.close();
        return out.toString();
    }

    public static int readFully(InputStream in, byte[] buffer, int max) throws IOException {
        int off = 0, len = Math.min(max, buffer.length);
        if(len == 0) {
            return 0;
        }        
        while (true) {
            int l = len - off;
            if (l <= 0) {
                break;
            }
            l = in.read(buffer, off, l);
            if (l < 0) {
                break;
            }
            off += l;
        }
        return off < 0 ? -1 : off;
    }

    public static int readFully(Reader in, char[] buffer, int max) throws IOException {
        int off = 0, len = Math.min(max, buffer.length);
        if(len == 0) {
            return 0;
        }        
        while (true) {
            int l = len - off;
            if (l <= 0) {
                break;
            }
            l = in.read(buffer, off, l);
            if (l < 0) {
                break;
            }
            off += l;
        }
        return off < 0 ? -1 : off;
    }
}
