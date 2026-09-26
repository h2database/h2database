/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.nio.ByteBuffer;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.db.ValueDataType;
import org.h2.mvstore.tx.VersionedValueType;
import org.h2.test.TestBase;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.VersionedValue;

/**
 * Tests compatibility of persisted transaction values.
 */
public class TestVersionedValueType extends TestBase {

    private static final long OPERATION_ID = 1L << 40;

    private final VersionedValueType<Value, Object> type = new VersionedValueType<>(new ValueDataType());

    /**
     * Run just this test.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() {
        testLegacyValues();
        testLegacyPages();
        testCurrentValues();
    }

    private void testLegacyValues() {
        // H2 2.4.240 encoding: operation id, optional flags, current / committed value.
        assertLegacyValue("0004e807", 0, 1000, 1000);
        assertLegacyValue("8080808080200304e80704e707", OPERATION_ID, 1000, 999);
        assertLegacyValue("ff80808080200104e807", OPERATION_ID + 127, 1000, null);
        assertLegacyValue("8081808080200204e707", OPERATION_ID + 128, null, 999);
        assertLegacyValue("81818080802000", OPERATION_ID + 129, null, null);

        // Every possible first byte of a persisted uncommitted operation id.
        for (int logId = 0; logId < 128; logId++) {
            WriteBuffer buff = new WriteBuffer();
            buff.putVarLong(OPERATION_ID + logId).put((byte) 3);
            ValueDataType valueType = new ValueDataType();
            valueType.write(buff, ValueInteger.get(1000));
            valueType.write(buff, ValueInteger.get(999));
            ByteBuffer data = buff.getBuffer().flip();
            assertValue(type.read(data), OPERATION_ID + logId, -1, 1000, 999);
            assertFalse(data.hasRemaining());
        }
    }

    private void assertLegacyValue(String hex, long operationId, Integer current, Integer committed) {
        // A value in an undo record is not necessarily at the end of its buffer.
        ByteBuffer buff = ByteBuffer.wrap(StringUtils.convertHexToBytes("7e" + hex + "7f"));
        assertEquals(0x7e, buff.get());
        assertValue(type.read(buff), operationId, -1, current, committed);
        assertEquals(0x7f, buff.get());
        assertFalse(buff.hasRemaining());
    }

    private void testLegacyPages() {
        VersionedValue<Value>[] values = type.createStorage(5);
        ByteBuffer buff = ByteBuffer.wrap(StringUtils.convertHexToBytes("01"
                + "0004e807"
                + "8080808080200304e80704e707"
                + "ff80808080200104e807"
                + "8081808080200204e707"
                + "81818080802000"));
        type.read(buff, values, values.length);
        assertValue(values[0], 0, -1, 1000, 1000);
        assertValue(values[1], OPERATION_ID, -1, 1000, 999);
        assertValue(values[2], OPERATION_ID + 127, -1, 1000, null);
        assertValue(values[3], OPERATION_ID + 128, -1, null, 999);
        assertValue(values[4], OPERATION_ID + 129, -1, null, null);
        assertFalse(buff.hasRemaining());

        // The fast-path page encoding did not change.
        values = type.createStorage(2);
        buff = ByteBuffer.wrap(StringUtils.convertHexToBytes("0004e80704e707"));
        type.read(buff, values, values.length);
        assertValue(values[0], 0, -1, 1000, 1000);
        assertValue(values[1], 0, -1, 999, 999);
        assertFalse(buff.hasRemaining());
    }

    private void testCurrentValues() {
        // Existing 2.5.x encodings, including entry id zero and committed tombstones.
        assertCurrentValue("0404e807", 0, -1, 1000, 1000);
        assertCurrentValue("060004e807", 0, 0, 1000, 1000);
        assertCurrentValue("0200", 0, 0, null, null);
        for (long entryId : new long[] { -1, 0 }) {
            String id = "808080808020" + (entryId == -1 ? "" : "00");
            assertCurrentValue((entryId == -1 ? "0d" : "0f") + id + "04e80704e707",
                    OPERATION_ID, entryId, 1000, 999);
            assertCurrentValue((entryId == -1 ? "05" : "07") + id + "04e807",
                    OPERATION_ID, entryId, 1000, null);
            assertCurrentValue((entryId == -1 ? "09" : "0b") + id + "04e707",
                    OPERATION_ID, entryId, null, 999);
            assertCurrentValue((entryId == -1 ? "01" : "03") + id,
                    OPERATION_ID, entryId, null, null);
        }
    }

    private void assertCurrentValue(String hex, long operationId, long entryId,
            Integer current, Integer committed) {
        byte[] encoded = StringUtils.convertHexToBytes(hex);
        ByteBuffer buff = ByteBuffer.wrap(encoded);
        VersionedValue<Value> value = type.read(buff);
        assertValue(value, operationId, entryId, current, committed);
        assertFalse(buff.hasRemaining());
        WriteBuffer written = new WriteBuffer();
        type.write(written, value);
        ByteBuffer data = written.getBuffer().flip();
        byte[] actual = new byte[data.remaining()];
        data.get(actual);
        assertEquals(encoded, actual);

        // The same scalar encoding is used in a slow-path page.
        buff = ByteBuffer.wrap(StringUtils.convertHexToBytes("01" + hex));
        VersionedValue<Value>[] values = type.createStorage(1);
        type.read(buff, values, 1);
        assertValue(values[0], operationId, entryId, current, committed);
        assertFalse(buff.hasRemaining());
    }

    private void assertValue(VersionedValue<Value> value, long operationId, long entryId,
            Integer current, Integer committed) {
        assertNotNull(value);
        assertEquals(operationId, value.getOperationId());
        assertEquals(entryId, value.getEntryId());
        assertEquals(operationId == 0, value.isCommitted());
        assertEquals(current == null ? null : ValueInteger.get(current), value.getCurrentValue());
        assertEquals(committed == null ? null : ValueInteger.get(committed), value.getCommittedValue());
    }
}
