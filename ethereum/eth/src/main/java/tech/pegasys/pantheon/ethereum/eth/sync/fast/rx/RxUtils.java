package tech.pegasys.pantheon.ethereum.eth.sync.fast.rx;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.Function;
import tech.pegasys.pantheon.util.Pair;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by Anton Nashatyrev on 26.11.2018.
 */
public class RxUtils {

  public static <T> Flowable<T> singleFuture(Supplier<CompletableFuture<T>> futureSupplier) {
    return Flowable.create(emitter -> futureSupplier.get().whenComplete((result, error) -> {
      if (error != null) {
        emitter.onError(error);
      } else {
        emitter.onNext(result);
        emitter.onComplete();
      }
    }), BackpressureStrategy.ERROR);
  }

  public static <T> Flowable<T> fromFuture(CompletableFuture<T> future) {
    return future == null ? Flowable.empty() :
        Flowable.create(emitter -> future.whenComplete((result, error) -> {
      if (error != null) {
        emitter.onError(error);
      } else {
        emitter.onNext(result);
        emitter.onComplete();
      }
    }), BackpressureStrategy.ERROR);
  }

  public static <T> FlowableTransformer<T, Pair<T, T>> pairsTransform(boolean addNullFirst, boolean addLastNull) {
    return f -> f
        .scan(Pair.<T, T>of(null, null), (prevPair, curVal) -> Pair.of(prevPair.second(), curVal))
        .filter(p -> p.second() != null && (addNullFirst || p.first() != null))
        .compose(addLastNull ?
            addLastTransform(last -> Pair.of(last.second(), null)) :
            f1 -> f1);
  }
  public static <T> FlowableTransformer<T, T> addLastTransform(Function<T, T> convertLast) {
    return f -> addLast(f, convertLast);
  }

//  public static <T> FlowableTransformer<T, T> parallelOrdered(Function<T, T> convertLast) {
//
//  }
//

  public static <T> Flowable<T> addLast(Flowable<T> f, Function<T, T> convertLast) {
    Flowable<T> f1 = f.replay(1).autoConnect();     // available multiple subscribers
    return Flowable.concat(f1, f1
        .takeLast(1)
        .map(convertLast));
  }

  public static <T> FlowableTransformer<T, T> debugOutOp(String name) {
    return debugOutOp(name, System.out::println);
  }

  public static <T> FlowableTransformer<T, T> debugOutOp(String name, Consumer<String> printer) {
    return f -> debugOut(name, f, printer);
  }

  public static <T> Flowable<T> debugOut(String name, Flowable<T> flowable, Consumer<String> printer) {
    return flowable
        .doOnEach(event -> printer.accept("[" + name + "] (" + Thread.currentThread() + ")" + event))
        .doOnLifecycle(s -> printer.accept("[" + name + "]* (" + Thread.currentThread() + ") onSubscribe"),
            l -> {
              String reqAmt = "" + l;
              if (l == Long.MAX_VALUE) {
                reqAmt = "INFINITE";  // handful for placing breakpoint
              }
              printer.accept("[" + name + "]* ("  + Thread.currentThread() + ")" + " onRequest: " + l);
            },
            ()-> printer.accept("[" + name + "]* (" + Thread.currentThread() + ") onCancel"))
        ;
  }
}
