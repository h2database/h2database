/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.SQLException;

import org.h2.store.DataHandler;
import org.h2.store.DataPage;
import org.h2.store.FileStore;
import org.h2.test.TestBase;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Data page tests.
 */
public class TestDataPage extends TestBase implements DataHandler {

    boolean text;

    public void test() throws Exception {
        testAll();
        text = true;
        testAll();
    }

    private void testAll() throws Exception {
        DataPage page = DataPage.create(this, 128);

        char[] data = new char[0x10000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char) i;
        }
        String s = new String(data);
        page.writeString(s);
        int len = page.length();
        check(page.getStringLen(s), len);
        page.reset();
        check(s, page.readString());
        page.reset();

        page.writeString("H\u1111!");
        page.writeString("John\tBrack's \"how are you\" M\u1111ller");
        page.writeValue(ValueInt.get(10));
        page.writeValue(ValueString.get("test"));
        page.writeValue(ValueFloat.get(-2.25f));
        page.writeValue(ValueDouble.get(10.40));
        page.writeValue(ValueNull.INSTANCE);
        trace(new String(page.getBytes()));
        page.reset();

        trace(page.readString());
        trace(page.readString());
        trace(page.readValue().getInt());
        trace(page.readValue().getString());
        trace("" + page.readValue().getFloat());
        trace("" + page.readValue().getDouble());
        trace(page.readValue().toString());
        page.reset();

        page.writeInt(0);
        page.writeInt(Integer.MAX_VALUE);
        page.writeInt(Integer.MIN_VALUE);
        page.writeInt(1);
        page.writeInt(-1);
        page.writeInt(1234567890);
        page.writeInt(54321);
        trace(new String(page.getBytes()));
        page.reset();
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());

        page = null;
    }

    public boolean getTextStorage() {
        return text;
    }

    public String getDatabasePath() {
        return null;
    }

    public FileStore openFile(String name, String mode, boolean mustExist) throws SQLException {
        return null;
    }

    public int getChecksum(byte[] data, int start, int end) {
        return end - start;
    }

    public void checkPowerOff() throws SQLException {
    }

    public void checkWritingAllowed() throws SQLException {
    }

    public void freeUpDiskSpace() throws SQLException {
    }

    public void handleInvalidChecksum() throws SQLException {
        throw new SQLException();
    }

    public int compareTypeSave(Value a, Value b) throws SQLException {
        throw new SQLException();
    }

    public int getMaxLengthInplaceLob() {
        throw new Error();
    }

    public int allocateObjectId(boolean b, boolean c) {
        throw new Error();
    }

    public String createTempFile() throws SQLException {
        throw new SQLException();
    }

    public String getLobCompressionAlgorithm(int type) {
        throw new Error();
    }

    public Object getLobSyncObject() {
        return this;
    }

}
