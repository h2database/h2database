package org.h2.time;

/**
 * Interface for pluggable sources of timestamps.
 *
 * The time source to use for a connection can be specified using the
 * @code TIME_SOURCE setting. Implementations of this interface must be
 * default constructible.
 */
public interface TimeSource {
    /**
     * Get the current time
     */
    Timestamp getCurrentTimestamp();
}
