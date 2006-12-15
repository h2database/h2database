/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.TableData;
import org.h2.value.Value;
import org.h2.value.ValueNull;


public class TreeIndex extends Index {

    private TreeNode root;
    private TableData tableData;

    public TreeIndex(TableData table, int id, String indexName, Column[] columns, IndexType indexType) {
        super(table, id, indexName, columns, indexType);
        tableData = table;
    }

    public void close(Session session) throws SQLException {
        root = null;
    }

    public void add(Session session, Row row) throws SQLException {
        TreeNode i = new TreeNode(row);
        TreeNode n = root, x = n;
        boolean  isleft = true;
        while (true) {
            if (n == null) {
                if (x == null) {
                    root = i;
                    rowCount++;
                    return;
                }
                set(x, isleft, i);
                break;
            }
            Row r = n.row;
            int compare = compareRows(row, r);
            if (compare == 0) {
                if(indexType.isUnique()) {
                    if(!isNull(row)) {
                        throw getDuplicateKeyException();
                    }
                }
                compare = compareKeys(row, r);
            }
            isleft = compare < 0;
            x = n;
            n = child(x, isleft);
        }
        balance(x, isleft);
        rowCount++;
    }

    private void balance(TreeNode x, boolean isleft) {
        while (true) {
            int sign = isleft ? 1 : -1;
            switch (x.balance * sign) {
            case 1 :
                x.balance = 0;
                return;
            case 0 :
                x.balance = -sign;
                break;
            case -1 :
                TreeNode l = child(x, isleft);
                if (l.balance == -sign) {
                    replace(x, l);
                    set(x, isleft, child(l, !isleft));
                    set(l, !isleft, x);
                    x.balance = 0;
                    l.balance = 0;
                } else {
                    TreeNode r = child(l, !isleft);
                    replace(x, r);
                    set(l, !isleft, child(r, isleft));
                    set(r, isleft, l);
                    set(x, isleft, child(r, !isleft));
                    set(r, !isleft, x);
                    int rb = r.balance;
                    x.balance = (rb == -sign) ? sign : 0;
                    l.balance = (rb == sign) ? -sign : 0;
                    r.balance = 0;
                }
                return;
            }
            if (x == root) {
                return;
            }
            isleft = x.isFromLeft();
            x = x.parent;
        }
    }

    private TreeNode child(TreeNode x, boolean isleft) {
        return isleft ? x.left : x.right;
    }

    private void replace(TreeNode x, TreeNode n) {
        if (x == root) {
            root = n;
            if (n != null) {
                n.parent = null;
            }
        } else {
            set(x.parent, x.isFromLeft(), n);
        }
    }

    private void set(TreeNode parent, boolean left, TreeNode n) {
        if (left) {
            parent.left = n;
        } else {
            parent.right = n;
        }
        if (n != null) {
            n.parent = parent;
        }
    }

    public void remove(Session session, Row row) throws SQLException {
        TreeNode x = findFirstNode(row, true);
        if (x == null) {
            throw Message.getInternalError("not found!");
        }
        TreeNode n;
        if (x.left == null) {
            n = x.right;
        } else if (x.right == null) {
            n = x.left;
        } else {
            TreeNode d = x;
            x = x.left;
            for (TreeNode temp = x; (temp = temp.right) != null;) {
                x = temp;
            }
            // x will be replaced with n later
            n = x.left;
            // swap d and x
            int b = x.balance;
            x.balance = d.balance;
            d.balance = b;

            // set x.parent
            TreeNode xp = x.parent;
            TreeNode dp = d.parent;
            if (d == root) {
                root = x;
            }
            x.parent = dp;
            if (dp != null) {
                if (dp.right == d) {
                    dp.right = x;
                } else {
                    dp.left = x;
                }
            }
            // TODO index / tree: link d.r = x(p?).r directly
            if (xp == d) {
                d.parent = x;
                if (d.left == x) {
                    x.left = d;
                    x.right = d.right;
                } else {
                    x.right = d;
                    x.left = d.left;
                }
            } else {
                d.parent = xp;
                xp.right = d;
                x.right = d.right;
                x.left = d.left;
            }

            if(Constants.CHECK && (x.right==null || x==null)) {
                throw Message.getInternalError("tree corrupted");
            }
            x.right.parent = x;
            x.left.parent = x;
            // set d.left, d.right
            d.left = n;
            if (n != null) {
                n.parent =d;
            }
            d.right =null;
            x = d;
        }
        rowCount--;
        
        boolean isleft = x.isFromLeft();
        replace(x, n);
        n = x.parent;
        while (n != null) {
            x = n;
            int sign = isleft ? 1 : -1;
            switch (x.balance * sign) {
            case -1 :
                x.balance = 0;
                break;
            case 0 :
                x.balance = sign;
                return;
            case 1 :
                TreeNode r = child(x, !isleft);
                int  b = r.balance;
                if (b * sign >= 0) {
                    replace(x, r);
                    set(x, !isleft, child(r, isleft));
                    set(r, isleft, x);
                    if (b == 0) {
                        x.balance = sign;
                        r.balance = -sign;
                        return;
                    }
                    x.balance = 0;
                    r.balance = 0;
                    x = r;
                } else {
                    TreeNode l = child(r, isleft);
                    replace(x, l);
                    b = l.balance;
                    set(r, isleft, child(l, !isleft));
                    set(l, !isleft, r);
                    set(x, !isleft, child(l, isleft));
                    set(l, isleft, x);
                    x.balance = (b == sign) ? -sign : 0;
                    r.balance = (b == -sign) ? sign : 0;
                    l.balance = 0;
                    x = l;
                }
            }
            isleft = x.isFromLeft();
            n = x.parent;
        }
    }

    private TreeNode findFirstNode(SearchRow row, boolean withKey) throws SQLException {
        TreeNode x = root, result = x;
        while (x != null) {
            result = x;
            int compare = compareRows(x.row, row);
            if (compare == 0 && withKey) {
                compare = compareKeys(x.row, row);
            }
            if (compare == 0) {
                if (withKey) {
                    return x;
                }
                x = x.left;
            } else if (compare > 0) {
                x = x.left;
            } else {
                x = x.right;
            }
        }
        return result;
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        if(first == null) {
            TreeNode x = root, n;
            while (x != null) {
                n = x.left;
                if (n == null) {
                    break;
                }
                x = n;
            }
            return new TreeCursor(this, x, first, last);
        } else {
            TreeNode x = findFirstNode(first, false);
            return new TreeCursor(this, x, first, last);
        }
    }
    
    public int getLookupCost(int rowCount) {
        for(int i=0, j = 1; ; i++) {
            j += j;
            if(j>=rowCount) {
                return i;
            }
        }
    }

    public int getCost(int[] masks) throws SQLException {
        return getCostRangeIndex(masks, tableData.getRowCount());
    }

    public void remove(Session session) throws SQLException {
        truncate(session);
    }

    public void truncate(Session session) throws SQLException {
        root = null;
        rowCount = 0;        
    }

    TreeNode next(TreeNode x) {
        if (x == null) {
            return null;
        }
        TreeNode r = x.right;
        if (r != null) {
            x = r;
            TreeNode l = x.left;
            while (l != null) {
                x = l;
                l = x.left;
            }
            return x;
        }
        TreeNode ch = x;
        x = x.parent;
        while (x != null && ch == x.right) {
            ch = x;
            x  = x.parent;
        }
        return x;
    }
    
    public void checkRename() throws SQLException {
    }

    public boolean needRebuild() {
        return true;
    }

    public boolean canGetFirstOrLast(boolean first) {
        return true;
    }

    public Value findFirstOrLast(Session session, boolean first) throws SQLException {
        if(first) {
            // TODO optimization: this loops through NULL values
            Cursor cursor = find(session, null, null);
            while(cursor.next()) {
                Value v = cursor.get().getValue(columnIndex[0]);
                if(v != ValueNull.INSTANCE) {
                    return v;
                }
            }
            return ValueNull.INSTANCE;
        } else {
            TreeNode x = root, n;
            while (x != null) {
                n = x.right;
                if (n == null) {
                    break;
                }
                x = n;
            }
            if(x != null) {
                Value v = x.row.getValue(columnIndex[0]);
                return v;
            }
            return ValueNull.INSTANCE;
        }
    }

}
