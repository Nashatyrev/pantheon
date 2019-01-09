package tech.pegasys.pantheon.util;

/**
 * Pair of values
 */
public final class Pair<T1, T2> {

  public static <T1, T2> Pair<T1, T2> of(final T1 first, final T2 second) {
    return new Pair<>(first, second);
  }

  private final T1 first;
  private final T2 second;

  private Pair(final T1 first, final T2 second) {
    this.first = first;
    this.second = second;
  }

  public T1 first() {
    return first;
  }

  public T2 second() {
    return second;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final Pair<?,?> pair = (Pair<?,?>) o;

    if (first != null ? !first.equals(pair.first) : pair.first != null) return false;
    return second != null ? second.equals(pair.second) : pair.second == null;
  }

  @Override
  public int hashCode() {
    int result = first != null ? first.hashCode() : 0;
    result = 31 * result + (second != null ? second.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Pair[" + first + " ," + second + ']';
  }
}
