package tech.pegasys.pantheon.ethereum.eth.sync.fast.rx;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;

import java.util.function.Function;

/**
 *
 */
public class PrefetchBuffer<T> implements FlowableTransformer<T, T> {
  private final int bufferSize;
  private final Function<T, Integer> sizeEstimator;

  public PrefetchBuffer(final int bufferSize) {
    this(bufferSize, o -> 1);
  }

  public PrefetchBuffer(final int bufferSize,
                        final Function<T, Integer> sizeEstimator) {
    this.bufferSize = bufferSize;
    this.sizeEstimator = sizeEstimator;
  }

  @Override
  public Publisher<T> apply(final Flowable<T> upstream) {
    return upstream;
  }
}