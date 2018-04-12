/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.BitSet;

/**
 * Class VersionedBitSet extends standard BitSet to add a version field.
 * This will allow bit set and version to be changed atomically.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
final class VersionedBitSet extends BitSet
{
    private long version;

    public VersionedBitSet() {}

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public VersionedBitSet cloneIt() {
        VersionedBitSet res = (VersionedBitSet) super.clone();
        res.version = version;
        return res;
    }

    @Override
    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public Object clone() {
        return cloneIt();
    }
}
