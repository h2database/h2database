/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.lang;

import org.h2.java.Ignore;

/* c:

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <wchar.h>
#include <stdint.h>

#define jvoid void
#define jboolean int8_t
#define jbyte int8_t
#define jchar wchar_t
#define jint int32_t
#define jlong int64_t
#define jfloat float
#define jdouble double
#define ujint uint32_t
#define ujlong uint64_t
#define true 1
#define false 0
#define null 0

#define LENGTH(a) (*(((jint*)(a))-3))
#define CLASS_ID(a) (*(((jint*)(a))-2))
#define NEW_ARRAY(size, length) new_array(0, size, length)
#define NEW_OBJ_ARRAY(length) new_array(0, sizeof(void*), length)
#define NEW_OBJ(typeId, typeName) new_object(typeId, sizeof(struct typeName))
#define SET(variable, p) set_object(variable, p)
#define STRING(s) ((java_lang_String*) string(s))

void* new_array(jint object, jint size, jint length);
void* new_object(jint type, jint size);
void* reference(void* o);
void release(void* o);
void* set(void* o, void* n);
void* string(char* s);

*/

/*
 * Object layout:
 * m-2: data type
 * m-1: number of references
 * m: object data
 *
 * Array layout:
 * m-3: length (number of elements)
 * m-2: 0 (array marker)
 * m-1: number of references
 * m: first element
 */

/**
 * A java.lang.String implementation.
 */
public class String {

/* c:

void* new_array_with_count(jint object, jint size, jint length, jint refCount) {
    jint count = sizeof(jint) * 3 + size * length;
    jint* m = (jint*) calloc(1, count);
    *m = length;
    *(m + 2) = refCount;
    return m + 3;
}

void* new_array(jint object, jint size, jint length) {
    return new_array_with_count(object, size, length, 1);
}

void* new_static_array(jint object, jint size, jint length) {
    return new_array_with_count(object, size, length, 0);
}

void* new_object_with_count(jint type, jint size, jint refCount) {
    jint count = sizeof(jint) * 2 + size;
    jint* m = (jint*) calloc(1, count);
    *m = type;
    *(m + 1) = refCount;
    return m + 2;
}

void* new_object(jint type, jint size) {
    return new_object_with_count(type, size, 1);
}

void* new_static_object(jint type, jint size) {
    return new_object_with_count(type, size, 0);
}

void* reference(void* o) {
    if (o != 0) {
        jint* m = (jint*) o;
        if (*(m - 1) > 0) {
            (*(m - 1))++;
        }
    }
    return o;
}

void release(void* o) {
    if (o == 0) {
        return;
    }
    jint* m = (jint*) o;
    if (*(m - 1) <= 1) {
        if (*(m - 1) == 0) {
            return;
        }
        if (*(m - 2) == 0) {
            free(m - 3);
        } else {
            free(m - 2);
        }
    } else {
        (*(m - 1))--;
    }
}

void* set(void* o, void* n) {
    release(o);
    return reference(n);
}

void* string(char* s) {
    jint len = strlen(s);
    jchar* chars = NEW_ARRAY(sizeof(jchar), len);
    for (int i = 0; i < len; i++) {
        chars[i] = s[i];
    }
    return java_lang_String_init_obj(chars);
}

*/

    /**
     * The character array.
     */
    char[] chars;

    private int hashCode;

    public String(char[] chars) {
        this.chars = new char[chars.length];
        System.arraycopy(chars, 0, this.chars, 0, chars.length);
    }

    public String(char[] chars, int offset, int count) {
        this.chars = new char[count];
        System.arraycopy(chars, offset, this.chars, 0, count);
    }

    public int hashCode() {
        if (hashCode == 0) {
            if (chars.length == 0) {
                return 0;
            }
            int h = 0;
            for (char c : chars) {
                h = h * 31 + c;
            }
            hashCode = h;
            return h;
        }
        return hashCode;
    }

    /**
     * Get the length of the string.
     *
     * @return the length
     */
    public int length() {
        return chars.length;
    }

    @Ignore
    public java.lang.String asString() {
        return new java.lang.String(chars);
    }

    @Ignore
    public static String wrap(java.lang.String x) {
        return new String(x.toCharArray());
    }

}
