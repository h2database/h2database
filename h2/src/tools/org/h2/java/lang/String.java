package org.h2.java.lang;

/* c:

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define jvoid void
#define jboolean int8_t
#define jbyte int8_t
#define jchar int16_t
#define jint int32_t
#define jlong int64_t
#define jfloat float
#define jdouble double
#define true 1
#define false 0
#define null 0

#define LENGTH(a) (*(((jint*)(a))-1))
#define NEW_ARRAY(size, length) new_array(0, size, length)
#define NEW_OBJ_ARRAY(length) new_array(1, sizeof(void*), length)
#define NEW_OBJ(typeId, typeName) new_object(typeId, sizeof(struct typeName))
#define SET(variable, p) set_object(variable, p)
#define STRING(s) ((java_lang_String*) string(s))

void* new_array(jint object, jint size, jint length);
void* new_object(jint type, jint size);
void* set_object(void** target, void* o);
void* string(char* s);

*/

/**
 * A java.lang.String implementation.
 */
public class String {

/* c:

void* new_array(jint object, jint size, jint length) {
    int count = sizeof(jint) * 2 + size * length;
    int* m = (jint*) calloc(1, count);
    *m = object << 31 + length;
    *(m+1) = 1;
    return m + 2;
}

void* new_object(jint type, jint size) {
    int count = sizeof(jint) * 2 + size;
    int* m = (jint*) calloc(1, count);
    *m = type;
    *(m+1) = 1;
    return m + 2;
}

void* set_object(void** target, void* o) {
    int* m = (jint*) target;
    if (*(m - 2) == 1) {
        free(m - 1);
    } else {
         (*(m - 2))--;
    }
    *target = o;
    m = (jint*) target;
    if (o != 0) {
        (*(m - 2))++;
    }
}

void* string(char* s) {
    int len = strlen(s);
    jchar* chars = NEW_ARRAY(sizeof(char), 1 * LENGTH(chars));
    for (int i = 0; i < len; i++) {
        chars[i] = s[i];
    }
    return java_lang_String_init_obj(chars);
}

*/

    private char[] chars;
    private int hashCode;

    public String(char[] chars) {
        this.chars = new char[chars.length];
        System.arraycopy(chars, 0, this.chars, 0, chars.length);
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
