package org.h2.util;

public class ObjectUtils {
    
    public static Integer getInteger(int x) {
//#ifdef JDK16
/*
        if(true)
            return Integer.valueOf(x);
*/
//#endif
//#ifdef JDK14
        return new Integer(x);
//#endif
    }

    public static Character getCharacter(char x) {
//#ifdef JDK16
/*
        if(true)
            return Character.valueOf(x);
*/
//#endif
//#ifdef JDK14
        return new Character(x);
//#endif
    }
    
    public static Long getLong(long x) {
//#ifdef JDK16
/*
        if(true)
            return Long.valueOf(x);
*/
//#endif
//#ifdef JDK14
        return new Long(x);
//#endif
    }    
    
    public static Short getShort(short x) {
//#ifdef JDK16
/*
        if(true)
            return Short.valueOf(x);
*/
//#endif
//#ifdef JDK14
        return new Short(x);
//#endif
    }        
    
    public static Byte getByte(byte x) {
//#ifdef JDK16
/*
        if(true)
            return Byte.valueOf(x);
*/
//#endif
//#ifdef JDK14
        return new Byte(x);
//#endif
    }  

    public static Float getFloat(float x) {
//#ifdef JDK16
/*
        if(true)
            return Float.valueOf(x);
*/
//#endif
//#ifdef JDK14
        return new Float(x);
//#endif
    }    
    
    public static Double getDouble(double x) {
//#ifdef JDK16
/*
        if(true)
            return Double.valueOf(x);
*/
//#endif
//#ifdef JDK14
        return new Double(x);
//#endif
    }       

}
