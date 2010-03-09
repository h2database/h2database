package org.h2.java.lang;

/**
 * A java.lang.String implementation.
 */
public class String {

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
