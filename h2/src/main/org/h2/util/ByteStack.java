/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * The stack of byte values. This class is not synchronized and should not be
 * used by multiple threads concurrently.
 */
public final class ByteStack {

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private int size;

    private byte[] array;

    /**
     * Creates a new empty instance.
     */
    public ByteStack() {
        array = Utils.EMPTY_BYTES;
    }

    /**
     * Pushes an item onto the top of this stack.
     *
     * @param item
     *            the item to push
     */
    public void push(byte item) {
        int index = size;
        int oldLength = array.length;
        if (index >= oldLength) {
            grow(oldLength);
        }
        array[index] = item;
        size = index + 1;
    }

    /**
     * Removes the item at the top of this stack and returns that item.
     *
     * @return the item at the top of this stack
     * @throws NoSuchElementException
     *             if stack is empty
     */
    public byte pop() {
        int index = size - 1;
        if (index < 0) {
            throw new NoSuchElementException();
        }
        size = index;
        return array[index];
    }

    /**
     * Removes the item at the top of this stack and returns that item.
     *
     * @param defaultValue
     *            value to return if stack is empty
     * @return the item at the top of this stack, or default value
     */
    public int poll(int defaultValue) {
        int index = size - 1;
        if (index < 0) {
            return defaultValue;
        }
        size = index;
        return array[index];
    }

    /**
     * Looks at the item at the top of this stack without removing it.
     *
     * @param defaultValue
     *            value to return if stack is empty
     * @return the item at the top of this stack, or default value
     */
    public int peek(int defaultValue) {
        int index = size - 1;
        if (index < 0) {
            return defaultValue;
        }
        return array[index];
    }

    /**
     * Returns {@code true} if this stack is empty.
     *
     * @return {@code true} if this stack is empty
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the number of items in this stack.
     *
     * @return the number of items in this stack
     */
    public int size() {
        return size;
    }

    private void grow(int length) {
        if (length == 0) {
            length = 0x10;
        } else if (length >= MAX_ARRAY_SIZE) {
            throw new OutOfMemoryError();
        } else if ((length <<= 1) < 0) {
            length = MAX_ARRAY_SIZE;
        }
        array = Arrays.copyOf(array, length);
    }

}
