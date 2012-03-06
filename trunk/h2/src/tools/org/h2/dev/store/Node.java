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

    private final StoredMap<?, ?> map;
    private long id;
    private long leftId, rightId;
    private long transaction;
    private Object key;
    private Object data;
    private Node left, right;
    private int flags;

    private Node(StoredMap<?, ?> map) {
        this.map = map;
    }

    static Node create(StoredMap<?, ?> map, Object key, Object data) {
        Node n = new Node(map);
        n.key = key;
        n.data = data;
        n.transaction = map.getTransaction();
        n.id = map.nextTempNodeId();
        return n;
    }

    static Node read(StoredMap<?, ?> map, long id, ByteBuffer buff) {
        Node n = new Node(map);
        n.id = id;
        n.read(buff);
        return n;
    }

    Node getLeft() {
        if (left == null && leftId != 0) {
            return map.readNode(leftId);
        }
        return left;
    }

    Node getRight() {
        if (right == null && rightId != 0) {
            return map.readNode(rightId);
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

    private Node copyOnWrite() {
        if (transaction == map.getTransaction()) {
            return this;
        }
        map.removeNode(id);
        Node n2 = create(map, key, data);
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
        setLeft(getLeft().copyOnWrite());
        getLeft().flags = getLeft().flags ^ FLAG_BLACK;
        setRight(getRight().copyOnWrite());
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
        Node x = getRight().copyOnWrite();
        setRight(x.getLeft());
        x.setLeft(this);
        x.flags = flags;
        // make red
        flags = flags & ~FLAG_BLACK;
        return x;
    }

    private Node rotateRight() {
        Node x = getLeft().copyOnWrite();
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

    private Node getMin() {
        Node n = this;
        while (n.getLeft() != null) {
            n = n.getLeft();
        }
        return n;
    }

    private Node removeMin() {
        if (getLeft() == null) {
            map.removeNode(id);
            return null;
        }
        Node n = copyOnWrite();
        if (!isRed(n.getLeft()) && !isRed(n.getLeft().getLeft())) {
            n = n.moveRedLeft();
        }
        n.setLeft(n.getLeft().removeMin());
        return n.fixUp();
    }

    static Node remove(Node n, Object key) {
        if (getNode(n, key) == null) {
            return n;
        }
        return n.remove(key);
    }

    int compare(Object key) {
        return map.compare(key, this.key);
    }

    private Node remove(Object key) {
        Node n = copyOnWrite();
        if (map.compare(key, n.key) < 0) {
            if (!isRed(n.getLeft()) && !isRed(n.getLeft().getLeft())) {
                n = n.moveRedLeft();
            }
            n.setLeft(n.getLeft().remove(key));
        } else {
            if (isRed(n.getLeft())) {
                n = n.rotateRight();
            }
            if (n.compare(key) == 0 && n.getRight() == null) {
                map.removeNode(id);
                return null;
            }
            if (!isRed(n.getRight()) && !isRed(n.getRight().getLeft())) {
                n = n.moveRedRight();
            }
            if (n.compare(key) == 0) {
                Node min = n.getRight().getMin();
                n.key = min.key;
                n.data = min.data;
                n.setRight(n.getRight().removeMin());
            } else {
                n.setRight(n.getRight().remove(key));
            }
        }
        return n.fixUp();
    }

    static Node getNode(Node n, Object key) {
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

    static Node put(StoredMap<?, ?> map, Node n, Object key, Object data) {
        if (n == null) {
            n = Node.create(map, key, data);
            return n;
        }
        n = n.copyOnWrite();
        int compare = n.compare(key);
        if (compare == 0) {
            n.data = data;
        } else if (compare < 0) {
            n.setLeft(put(map, n.getLeft(), key, data));
        } else {
            n.setRight(put(map, n.getRight(), key, data));
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

    private void read(ByteBuffer buff) {
        flags = buff.get();
        leftId = buff.getLong();
        rightId = buff.getLong();
        key = map.getKeyType().read(buff);
        data = map.getValueType().read(buff);
    }

    void write(ByteBuffer buff) {
        buff.put((byte) flags);
        buff.putLong(leftId);
        buff.putLong(rightId);
        map.getKeyType().write(buff, key);
        map.getValueType().write(buff, data);
    }

    int length() {
        return map.getKeyType().length(key) +
                map.getValueType().length(data) + 17;
    }

}
