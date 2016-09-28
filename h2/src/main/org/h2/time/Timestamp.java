package org.h2.time;

/**
 * Immutable combination of milliseconds since 1970 and nanosection fraction of the millisecond.
 *
 */
public final class Timestamp {
    private final long millis;
    private final int fraction;

    public Timestamp(long millis, int fraction) {
        this.millis = millis;
        if (fraction > 1000000) {
            throw new IllegalArgumentException("Nanosecond fraction is not less than 1 millisecond: " + fraction);
        }
        this.fraction = fraction;
    }

    /**
     * Get the millisecond part of the time.
     * @see System#currentTimeMillis
     */
    public long getSystemTimeInMillis() {
        return millis;
    }

    /**
     * Get the fraction of a millisecond, as nanoseconds.
     */
    public int getFraction() {
        return fraction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Timestamp)) {
            return false;
        }

        Timestamp timestamp = (Timestamp) o;

        if (millis != timestamp.millis) {
            return false;
        }
        return fraction == timestamp.fraction;

    }

    @Override
    public int hashCode() {
        int result = (int) (millis ^ (millis >>> 32));
        result = 31 * result + fraction;
        return result;
    }
}
