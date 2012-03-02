/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store;

import java.nio.ByteBuffer;

/**
 * A left-leaning red black tree implementation.
 */
class Node {

    private static final int FLAG_BLACK = 1;
    // private static final int FLAG_BACK_REFERENCES = 2;

    private final TreeMapStore store;
    private long id;
    private long leftId, rightId;
    private long transaction;
    private Object key;
    private Object data;
    private Node left, right;
    private int flags;

    private Node(TreeMapStore store) {
        this.store = store;
    }

    static Node create(TreeMapStore store, Object key, Object data) {
        Node n = new Node(store);
        n.key = key;
        n.data = data;
        n.transaction = store.getTransaction();
        n.id = store.nextTempNodeId();
        return n;
    }

    static Node load(TreeMapStore store, long id, ByteBuffer buff) {
        Node n = new Node(store);
        n.id = id;
        n.load(buff);
        return n;
    }

    Node getLeft() {
        if (left == null && leftId != 0) {
            left = store.loadNode(leftId);
        }
        return left;
    }

    Node getRight() {
        if (right == null && rightId != 0) {
            right = store.loadNode(rightId);
        }
        return right;
    }

    long getLeftId() {
        return leftId;
    }

    void setLeftId(long leftId) {
        this.leftId = leftId;
        left = null;
    }

    long getRightId() {
        return rightId;
    }

    void setRightId(long rightId) {
        this.rightId = rightId;
        left = null;
    }

    private void setLeft(Node l) {
        this.left = l;
        this.leftId = l == null ? 0 : l.getId();
    }

    private void setRight(Node r) {
        this.right = r;
        this.rightId = r == null ? 0 : r.getId();
    }

    private Node copyOnWrite(long writeTransaction) {
        if (writeTransaction == transaction) {
            return this;
        }
        store.removeNode(id);
        Node n2 = create(store, key, data);
        n2.leftId = leftId;
        n2.left = left;
        n2.rightId = rightId;
        n2.right = right;
        n2.flags = flags;
        return n2;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(key);
        if (left != null || right != null || leftId != 0 || rightId != 0) {
            buff.append("{");
            if (left != null) {
                buff.append(left.toString());
            } else if (leftId != 0) {
                buff.append(leftId);
            }
            buff.append(",");
            if (right != null) {
                buff.append(right.toString());
            } else if (rightId != 0) {
                buff.append(rightId);
            }
            buff.append("}");
        }
        return buff.toString();
    }

    private void flipColor() {
        flags = flags ^ FLAG_BLACK;
        setLeft(getLeft().copyOnWrite(transaction));
        getLeft().flags = getLeft().flags ^ FLAG_BLACK;
        setRight(getRight().copyOnWrite(transaction));
        getRight().flags = getRight().flags ^ FLAG_BLACK;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Object getKey() {
        return key;
    }

    public Object getData() {
        return data;
    }

    private Node rotateLeft() {
        Node x = getRight().copyOnWrite(store.getTransaction());
        setRight(x.getLeft());
        x.setLeft(this);
        x.flags = flags;
        // make red
        flags = flags & ~FLAG_BLACK;
        return x;
    }

    private Node rotateRight() {
        Node x = getLeft().copyOnWrite(store.getTransaction());
        setLeft(x.getRight());
        x.setRight(this);
        x.flags = flags;
        // make red
        flags = flags & ~FLAG_BLACK;
        return x;
    }

    private Node moveRedLeft() {
        flipColor();
        if (isRed(getRight().getLeft())) {
            setRight(getRight().rotateRight());
            Node n = rotateLeft();
            n.flipColor();
            return n;
        }
        return this;
    }

    private Node moveRedRight() {
        flipColor();
        if (isRed(getLeft().getLeft())) {
            Node n = rotateRight();
            n.flipColor();
            return n;
        }
        return this;
    }

    private Node min() {
        Node n = this;
        while (n.getLeft() != null) {
            n = n.getLeft();
        }
        return n;
    }

    private Node deleteMin() {
        if (getLeft() == null) {
            store.removeNode(id);
            return null;
        }
        Node n = copyOnWrite(transaction);
        if (!isRed(n.getLeft()) && !isRed(n.getLeft().getLeft())) {
            n = n.moveRedLeft();
        }
        n.setLeft(n.getLeft().deleteMin());
        return n.fixUp();
    }

    static Node remove(Node n, Object key) {
        if (findNode(n, key) == null) {
            return n;
        }
        return n.delete(key);
    }

    private int compare(Object key) {
        return store.compare(key, this.key);
    }

    private Node delete(Object key) {
        Node n = copyOnWrite(transaction);
        if (store.compare(key, n) < 0) {
            if (!isRed(n.getLeft()) && !isRed(n.getLeft().getLeft())) {
                n = n.moveRedLeft();
            }
            n.setLeft(n.getLeft().delete(key));
        } else {
            if (isRed(n.getLeft())) {
                n = n.rotateRight();
            }
            if (n.compare(key) == 0 && n.getRight() == null) {
                store.removeNode(id);
                return null;
            }
            if (!isRed(n.getRight()) && !isRed(n.getRight().getLeft())) {
                n = n.moveRedRight();
            }
            if (n.compare(key) == 0) {
                Node min = n.getRight().min();
                n.key = min.key;
                n.data = min.data;
                n.setRight(n.getRight().deleteMin());
            } else {
                n.setRight(n.getRight().delete(key));
            }
        }
        return n.fixUp();
    }

    static Node findNode(Node n, Object key) {
        while (n != null) {
            int compare = n.compare(key);
            if (compare == 0) {
                return n;
            } else if (compare > 0) {
                n = n.getRight();
            } else {
                n = n.getLeft();
            }
        }
        return null;
    }

    static Node add(TreeMapStore store, Node n, Object key, Object data) {
        if (n == null) {
            n = Node.create(store, key, data);
            return n;
        }
        n = n.copyOnWrite(store.getTransaction());
        int compare = n.compare(key);
        if (compare == 0) {
            n.data = data;
        } else if (compare < 0) {
            n.setLeft(add(store, n.getLeft(), key, data));
        } else {
            n.setRight(add(store, n.getRight(), key, data));
        }
        return n.fixUp();
    }

    private Node fixUp() {
        Node n = this;
        if (isRed(getRight())) {
            n = rotateLeft();
        }
        if (isRed(n.getLeft()) && isRed(n.getLeft().getLeft())) {
            n = n.rotateRight();
        }
        if (isRed(n.getLeft()) && isRed(n.getRight())) {
            n.flipColor();
        }
        return n;
    }

    private boolean isRed(Node n) {
        return n != null && (n.flags & FLAG_BLACK) == 0;
    }

    private void load(ByteBuffer buff) {
        flags = buff.get();
        leftId = buff.getLong();
        rightId = buff.getLong();
        key = store.getKeyType().read(buff);
        data = store.getValueType().read(buff);
    }

    void store(ByteBuffer buff) {
        buff.put((byte) flags);
        buff.putLong(leftId);
        buff.putLong(rightId);
        store.getKeyType().write(buff, key);
        store.getValueType().write(buff, data);
    }

    int length() {
        return store.getKeyType().length(key) +
                store.getValueType().length(data) + 17;
    }

}
