package org.h2.time;

/**
 * Time source that uses System.currentTimeMillis(), with 0 as millisecond fractions.
 *
 */
public final class MillisecondTimeSource implements TimeSource {
    @Override
    public Timestamp getCurrentTimestamp() {
        return new Timestamp(System.currentTimeMillis(), 0);
    }
}
