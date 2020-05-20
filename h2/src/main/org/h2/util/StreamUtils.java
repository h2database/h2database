package org.h2.util;

import java.util.function.Predicate;

public abstract class StreamUtils {
  private StreamUtils() {
  }

  /** Negates a predicate for example used in {@link java.util.stream.Stream#filter} */
  public static <R> Predicate<R> not(Predicate<R> predicate) {
    return predicate.negate();
  }
}
