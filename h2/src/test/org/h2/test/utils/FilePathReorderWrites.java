/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Random;

import org.h2.store.fs.FileBase;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;
import org.h2.util.IOUtils;

/**
 * An unstable file system. It is used to simulate file system problems (for
 * example out of disk space).
 */
public class FilePathReorderWrites extends FilePathWrapper {

    /**
     * Whether trace output of all method calls is enabled.
     */
    static final boolean TRACE = false;

    private static final FilePathReorderWrites INSTANCE = new FilePathReorderWrites();

    private static final IOException POWER_FAILURE = new IOException("Power Failure");

    private static int powerFailureCountdown;

    private static boolean partialWrites;

    private static Random random = new Random(1);

    /**
     * Register the file system.
     *
     * @return the instance
     */
    public static FilePathReorderWrites register() {
        FilePath.register(INSTANCE);
        return INSTANCE;
    }

    /**
     * Set the number of write operations before a simulated power failure, and
     * the random seed (for partial writes).
     *
     * @param count the number of write operations (0 to never fail,
     *            Integer.MAX_VALUE to count the operations)
     * @param seed the new seed
     */
    public void setPowerOffCountdown(int count, int seed) {
        powerFailureCountdown = count;
        random.setSeed(seed);
    }

    public int getPowerOffCountdown() {
        return powerFailureCountdown;
    }

    /**
     * Whether partial writes are possible (writing only part of the data).
     *
     * @param partialWrites true to enable
     */
    public void setPartialWrites(boolean partialWrites) {
        FilePathReorderWrites.partialWrites = partialWrites;
    }

    boolean getPartialWrites() {
        return partialWrites;
    }

    /**
     * Get a buffer with a subset (the head) of the data of the source buffer.
     *
     * @param src the source buffer
     * @return a buffer with a subset of the data
     */
    ByteBuffer getRandomSubset(ByteBuffer src) {
        int len = src.remaining();
        len = Math.min(4096, Math.min(len, 1 + random.nextInt(len)));
        ByteBuffer temp = ByteBuffer.allocate(len);
        src.get(temp.array());
        return temp;
    }

    Random getRandom() {
        return random;
    }

    /**
     * Check if the simulated problem occurred.
     * This call will decrement the countdown.
     *
     * @throws IOException if the simulated power failure occurred
     */
    void checkError() throws IOException {
        if (powerFailureCountdown == 0) {
            return;
        }
        if (powerFailureCountdown < 0) {
            throw POWER_FAILURE;
        }
        powerFailureCountdown--;
        if (powerFailureCountdown == 0) {
            powerFailureCountdown--;
            throw POWER_FAILURE;
        }
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        InputStream in = newInputStream();
        FilePath copy = FilePath.get(getBase().toString() + ".copy");
        OutputStream out = copy.newOutputStream(false);
        IOUtils.copy(in, out);
        FileChannel base = getBase().open(mode);
        FileChannel readBase = copy.open(mode);
        return new FileReorderWrites(this, base, readBase);
    }

    @Override
    public String getScheme() {
        return "reorder";
    }

    public long getMaxAge() {
        // TODO implement, configurable
        return 45000;
    }

}

/**
 * An file that checks for errors before each write operation.
 */
class FileReorderWrites extends FileBase {

    private final FilePathReorderWrites file;
    /**
     * The base channel, where not all operations are immediately applied.
     */
    private final FileChannel base;

    /**
     * The base channel that is used for reading, where all operations are
     * immediately applied to get a consistent view before a power failure.
     */
    private final FileChannel readBase;

    private boolean closed;

    /**
     * The list of not yet applied to the base channel. It is sorted by time.
     */
    private ArrayList<FileOperation> notAppliedList = new ArrayList<FileOperation>();

    private int id;

    FileReorderWrites(FilePathReorderWrites file, FileChannel base, FileChannel readBase) {
        this.file = file;
        this.base = base;
        this.readBase = readBase;
    }

    @Override
    public void implCloseChannel() throws IOException {
        base.close();
        readBase.close();
        closed = true;
    }

    @Override
    public long position() throws IOException {
        return readBase.position();
    }

    @Override
    public long size() throws IOException {
        return readBase.size();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return readBase.read(dst);
    }

    @Override
    public int read(ByteBuffer dst, long pos) throws IOException {
        return readBase.read(dst, pos);
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        readBase.position(pos);
        return this;
    }

    @Override
    public FileChannel truncate(long newSize) throws IOException {
        long oldSize = readBase.size();
        if (oldSize <= newSize) {
            return this;
        }
        addOperation(new FileOperation(id++, newSize, null));
        return this;
    }

    private int addOperation(FileOperation op) throws IOException {
        trace("op " + op);
        checkError();
        notAppliedList.add(op);
        long now = op.getTime();
        for (int i = 0; i < notAppliedList.size() - 1; i++) {
            FileOperation old = notAppliedList.get(i);
            boolean applyOld = false;
            // String reason = "";
            if (old.getTime() + 45000 < now) {
                // reason = "old";
                applyOld = true;
            } else if (old.overlaps(op)) {
                // reason = "overlap";
                applyOld = true;
            } else if (file.getRandom().nextInt(100) < 10) {
                // reason = "random";
                applyOld = true;
            }
            if (applyOld) {
                trace("op apply " + op);
                old.apply(base);
                notAppliedList.remove(i);
                i--;
            }
        }
        return op.apply(readBase);
    }

    private void applyAll() throws IOException {
        trace("applyAll");
        for (FileOperation op : notAppliedList) {
            op.apply(base);
        }
        notAppliedList.clear();
    }

    @Override
    public void force(boolean metaData) throws IOException {
        checkError();
        readBase.force(metaData);
        applyAll();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return addOperation(new FileOperation(id++, readBase.position(), src));
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        return addOperation(new FileOperation(id++, position, src));
    }

    private void checkError() throws IOException {
        if (closed) {
            throw new IOException("Closed");
        }
        file.checkError();
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return readBase.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return file.getScheme() + ":" + file.toString();
    }

    private static void trace(String message) {
        if (FilePathReorderWrites.TRACE) {
            System.out.println(message);
        }
    }

    /**
     * A file operation (that might be re-ordered with other operations, or not
     * be applied on power failure).
     */
    static class FileOperation {
        private final int id;
        private final long time;
        private final ByteBuffer buffer;
        private final long position;

        FileOperation(int id, long position, ByteBuffer src) {
            this.id = id;
            this.time = System.currentTimeMillis();
            if (src == null) {
                buffer = null;
            } else {
                int len = src.limit() - src.position();
                this.buffer = ByteBuffer.allocate(len);
                buffer.put(src);
                buffer.flip();
            }
            this.position = position;
        }

        public long getTime() {
            return time;
        }

        /**
         * Check whether the file region of this operation overlaps with
         * another operation.
         *
         * @param other the other operation
         * @return if there is an overlap
         */
        boolean overlaps(FileOperation other) {
            if (isTruncate() && other.isTruncate()) {
                // we just keep the latest truncate operation
                return true;
            }
            if (isTruncate()) {
                return position < other.getEndPosition();
            } else if (other.isTruncate()) {
                return getEndPosition() > other.position;
            }
            return position < other.getEndPosition() &&
                    getEndPosition() > other.position;
        }

        private boolean isTruncate() {
            return buffer == null;
        }

        private long getEndPosition() {
            return position + getLength();
        }

        private int getLength() {
            return buffer == null ? 0 : buffer.limit() - buffer.position();
        }

        /**
         * Apply the operation to the channel.
         *
         * @param channel the channel
         * @return the return value of the operation
         */
        int apply(FileChannel channel) throws IOException {
            if (isTruncate()) {
                channel.truncate(position);
                return -1;
            }
            // TODO support the case were part is not written
            int len = channel.write(buffer, position);
            buffer.flip();
            return len;
        }

        @Override
        public String toString() {
            String s = "[" + id + "]: @" + position + (
                    isTruncate() ? "-truncate" : ("+" + getLength()));
            return s;
        }
    }

}