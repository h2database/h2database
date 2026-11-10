/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import org.h2.mvstore.Chunk;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.SFChunk;
import org.h2.mvstore.SingleFileStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/** Reads a real chunk buffer against an unwritten file to expose mixed page-length sources. */
public class TestLargePageBuffer extends TestBase {
    private static final int OFFSET = 128;

    /** Uses a real file for length probes and counts unexpected reads of pending pages. */
    private static final class TestStore extends SingleFileStore {
        private final Path file;
        private int diskReads;

        /** Keeps a synthetic page isolated from business databases. */
        TestStore(Path file) {
            super(new HashMap<>());
            this.file = file;
        }

        /** Executes actual positioned file reads rather than returning fabricated page bytes. */
        @Override
        public ByteBuffer readFully(SFChunk ignored, long pos, int length) {
            diskReads++;
            ByteBuffer bytes = ByteBuffer.allocate(length);
            // Close the independent test channel after each actual file read.
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                DataUtils.readFully(channel, pos, bytes);
                return bytes;
            } catch (Exception error) {
                throw new IllegalStateException(error);
            }
        }
    }

    /** Runs the test with the native test framework. */
    public static void main(String... args) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    /** Checks pending and saved pages without requiring an asynchronous scheduling race. */
    @Override
    public void test() throws Exception {
        Path parent = Path.of(getBaseDir());
        Files.createDirectories(parent);
        Path root = Files.createTempDirectory(parent, "large-page-buffer-");
        // Remove only this test's newly generated fixture directory.
        try {
            checkPages(root);
        } finally {
            FileUtils.deleteRecursive(root.toString(), false);
        }
    }

    /** Calls the package-private reader without changing the production API for a test. */
    private void checkPages(Path root) throws Exception {
        Method reader = Chunk.class.getDeclaredMethod("readBufferForPage", FileStore.class, int.class, long.class);
        reader.setAccessible(true);
        Constructor<SFChunk> constructor = SFChunk.class.getDeclaredConstructor(int.class);
        constructor.setAccessible(true);
        Files.createDirectories(root);
        int failures = 0;
        // Cover each state independently so one failure cannot hide the remaining observations.
        for (String state : new String[]{"unallocated-large", "allocated-large", "allocated-small", "saved-large"}) {
            long start = System.nanoTime();
            Path file = root.resolve(state + ".bin");
            int length = state.endsWith("small") ? 64 : DataUtils.PAGE_LARGE + 4096;
            TestStore store = new TestStore(file);
            SFChunk chunk = constructor.newInstance(7);
            chunk.block = state.startsWith("unallocated") ? 0 : 2;
            chunk.len = (OFFSET + length + 4095) / 4096;
            ByteBuffer memory = ByteBuffer.allocate(chunk.len * 4096);
            memory.putInt(OFFSET, length - 4);
            chunk.buffer = memory;
            // The pending file contains zeros, exactly as space reserved before a write completes.
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(new byte[]{0}), (chunk.block + chunk.len) * 4096L - 1);
                // Written chunks must continue reading their length and contents from the file.
                if (state.startsWith("saved")) {
                    DataUtils.writeFully(channel, chunk.block * 4096L, memory.duplicate());
                    chunk.buffer = null;
                }
            }
            long pos = DataUtils.composePagePos(chunk.id, OFFSET, length, DataUtils.PAGE_TYPE_LEAF);
            ByteBuffer actual = (ByteBuffer) reader.invoke(chunk, store, OFFSET, pos);
            boolean pass = actual.remaining() == length
                    && actual.equals(memory.duplicate().position(OFFSET).limit(OFFSET + length))
                    && (state.startsWith("saved") ? store.diskReads == 2 : store.diskReads == 0);
            System.out.println("event=large_page_source stage=" + state + " outcome=" + (pass ? "pass" : "fail")
                    + " costMs=" + (System.nanoTime() - start) / 1_000_000
                    + " expectedBytes=" + length + " actualBytes=" + actual.remaining()
                    + " diskReads=" + store.diskReads);
            // Preserve every failure instead of reporting only the first broken pending state.
            if (!pass) {
                failures++;
            }
        }
        // A nonzero exit distinguishes a proven defect from a completed successful test.
        if (failures != 0) {
            throw new AssertionError("page source failures=" + failures);
        }
    }
}
