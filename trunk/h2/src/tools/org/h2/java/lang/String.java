package org.h2.java.lang;

/* c:

#define boolean int

#define LENGTH(a) (*(((int*)(a))-1))
#define NEW_ARRAY(size, length) new_array(0, size, length)
#define NEW_OBJ_ARRAY(length) new_array(1, sizeof(void*), length)
#define NEW_OBJ(type, size) new_object(type, sizeof(struct type))
#define SET(variable, p) set_object(variable, p)

void* new_array(int object, int size, int length);
void* new_object(int type, int size);
void* set_object(void** target, void* o);

*/

/**
 * A java.lang.String implementation.
 */
public class String {

/* c:

#include <stdlib.h>

void* new_array(int object, int size, int length) {
    int count = sizeof(int) * 2 + size * length;
    int* m = (int*) calloc(1, count);
    *m = object << 31 + length;
    *(m+1) = 1;
    return m + 2;
}

void* new_object(int type, int size) {
    int count = sizeof(int) * 2 + size;
    int* m = (int*) calloc(1, count);
    *m = type;
    *(m+1) = 1;
    return m + 2;
}

void* set_object(void** target, void* o) {
    int* m = (int*) target;
    if (*(m - 2) == 1) {
        free(m - 1);
    } else {
         (*(m - 2))--;
    }
    *target = o;
    m = (int*) target;
    if (o != 0) {
        (*(m - 2))++;
    }
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
