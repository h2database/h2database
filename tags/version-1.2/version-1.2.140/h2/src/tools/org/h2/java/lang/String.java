/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.lang;

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
void* set_object(void** target, void* o);
void* string(char* s);

*/

/*
 * Object layout:
 * m-3: arrays: length; otherwise not allocated
 * m-2: arrays: 0; otherwise type
 * m-1: number of references
 */

/**
 * A java.lang.String implementation.
 */
public class String {

/* c:

void* new_array(jint object, jint size, jint length) {
    int count = sizeof(jint) * 3 + size * length;
    int* m = (jint*) calloc(1, count);
    *m = length;
    *(m + 2) = 1;
    return m + 3;
}

void* new_object(jint type, jint size) {
    int count = sizeof(jint) * 2 + size;
    int* m = (jint*) calloc(1, count);
    *m = type;
    *(m + 1) = 1;
    return m + 2;
}

void* set_object(void** target, void* o) {
    int* m = (jint*) target;
    if (*(m - 1) == 1) {
        if (*(m - 2) == 0) {
            free(m - 3);
        } else {
            free(m - 2);
        }
    } else {
        (*(m - 1))--;
    }
    *target = o;
    m = (jint*) target;
    if (o != 0) {
        (*(m - 1))++;
    }
    return m;
}

void* string(char* s) {
    int len = strlen(s);
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

}
