package tech.pegasys.pantheon.ethereum.eth.sync.fast.rx;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;

import java.util.function.Function;

/**
 * Specific buffer for traversing a tree structure.
 * It tries to keep the specified size bounds by adding newly arrived tree nodes to
 * either end or beginning of the buffer queue thus balancing
 * between depth-first and breadth-first traversing
 *
 * The buffer can be bounded by the estimated elements size if sizeEstimator function is supplied
 */
public class TreeTraverseBuffer<T> implements FlowableTransformer<T, T> {
  private final int bufferSize;
  private final Function<T, Integer> sizeEstimator;

  public TreeTraverseBuffer(final int bufferSize) {
    this(bufferSize, o -> 1);
  }

  public TreeTraverseBuffer(final int bufferSize,
                            final Function<T, Integer> sizeEstimator) {
    this.bufferSize = bufferSize;
    this.sizeEstimator = sizeEstimator;
  }

  @Override
  public Publisher<T> apply(final Flowable<T> upstream) {
    // TODO
    return upstream;
  }
}
