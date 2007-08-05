/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.value.Value;

public interface SearchRow {
    
    int getPos();
    Value getValue(int index);
    int getColumnCount();
    void setValue(int idx, Value v);
    void setPos(int pos);
    
}
