package org.h2.time;

/**
 * Time source that uses elapsed time in nanoseconds to measure time.
 *
 * This time source generates timestamps that are based on the starting time
 * of the session, plus the elapsed time since session start in nanoseconds.
 * This provides high precision, but may not be in sync with system time if
 * the system time changes after the session has been opened (e.g. due to
 * NTP updates).
 *
 */
public final class ElapsedNanoTimeSource implements TimeSource {
    private long currentTimeMillisAtStart = System.currentTimeMillis();
    private long nanosAtStart = System.nanoTime();
    @Override
    public Timestamp getCurrentTimestamp() {
        long nanoDiff = System.nanoTime() - nanosAtStart;
        long millis = currentTimeMillisAtStart + ((nanoDiff + 500000) / 1000000);
        int fraction = (int) (nanoDiff % 1000000);
        return new Timestamp(millis, fraction);
    }
}
