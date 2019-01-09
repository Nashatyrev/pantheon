package tech.pegasys.pantheon.ethereum.eth.sync.fast.flows;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.eth.manager.AbstractPeerTask;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.SyncContext;
import tech.pegasys.pantheon.ethereum.eth.sync.state.SyncTarget;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.GetHeadersFromPeerByHashTask;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.exceptions.SyncTargetException;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.exceptions.TargetMissingHeadersException;
import tech.pegasys.pantheon.util.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static hu.akarnokd.rxjava2.operators.FlowableTransformers.expand;
import static tech.pegasys.pantheon.ethereum.eth.sync.fast.rx.RxUtils.fromFuture;

/**
 * The Flowable operator which takes SyncTarget events (found/lost target) and emits
 * checkpoint block headers downloaded from the sync target peer.
 * All checkpoints issued are guaranteed to have increasing block numbers with
 * the period of {@link tech.pegasys.pantheon.ethereum.eth.sync.SynchronizerConfiguration#downloaderHeaderRequestSize()}
 *
 * If the SyncTarget is changed eventually (due to unrecoverable error or due to peer disconnect)
 * the new sequence of checkpoint headers is emitted starting from the SyncTarget common ancestor header
 *
 * Unrecoverable errors of this operator are:
 * - target peer miss headers it announced
 * - several timeouts in a raw when requesting headers
 */
public class CheckpointHeadersFlow<C> implements FlowableTransformer<Optional<SyncTarget>, BlockHeader> {
  private static final Logger LOG = LogManager.getLogger();

  private final SyncContext<C> syncContext;

  public CheckpointHeadersFlow(final SyncContext<C> syncContext) {
    this.syncContext = syncContext;
  }

  @Override
  public Publisher<BlockHeader> apply(final Flowable<Optional<SyncTarget>> upstream) {
    return upstream
//        // leave only items when new sync target found
//        .flatMap(maybeTarget -> maybeTarget.map(Flowable::just).orElse(Flowable.empty()))
        // when target changes creates new stream for it which asynchronously downloads headers
        .switchMap(target -> !target.isPresent() ? Flowable.empty() : Flowable
                // start 'recursion' from <target, [common_ancestor_header]>
                .just(Pair.of(target.get(), Collections.singletonList(target.get().commonAncestor())))
                // 'recursively' downloading headers <target, [h1,h2,h3]> -> <target, [h3,h4,h5]> ... -> null
                // terminating null causes this substream to Complete
                // this operator may throw TargetMissingHeadersException
                .compose(expand(targetHeaders -> fromFuture(processHeadersAndRequestNext(targetHeaders))))
            , 2)
        // flatten lists (<target, [h1, h2, h3]>, <target, [h3, h4, h5]>) => (h1, h2, h3, h3, h4, h5)
        .flatMap(targetHeaders -> Flowable.fromIterable(targetHeaders.second()))
        // remove duplicates  (h1, h2, h3, h3, h4, h5) =>  (h1, h2, h3, h4, h5)
        .distinctUntilChanged();
  }

  private CompletableFuture<Pair<SyncTarget, List<BlockHeader>>> processHeadersAndRequestNext(
      final Pair<SyncTarget, List<BlockHeader>> targetHeaders){
    final CompletableFuture<List<BlockHeader>> next =
        processHeadersAndRequestNext(targetHeaders.first(), targetHeaders.second());
    return next == null ? null : next.thenApply(list -> Pair.of(targetHeaders.first(), list));
  }

  private CompletableFuture<List<BlockHeader>> processHeadersAndRequestNext(final SyncTarget target,
                                                                            final List<BlockHeader> headers){
    if (headers.isEmpty()) {
      // Invalid sync target peer: should return at least on header.
      throw new TargetMissingHeadersException(target, null);
    }
    if (headers.size() == 1) {
      // no more headers on the target peer
      final BlockHeader header = headers.get(0);
      if (header.getNumber() + syncContext.getConfig().downloaderChainSegmentSize() * 2 < target.estimatedTargetHeight()) {
        // Invalid sync target peer: reports highest block which can't return
        throw new TargetMissingHeadersException(target, header);
      } else {
        // retrieved all checkpoints
        return null;
      }
    } else {
      return requestHeaders(target, headers.get(headers.size() - 1));
    }
  }

  private CompletableFuture<List<BlockHeader>> requestHeaders(final SyncTarget target,
                                                              final BlockHeader startHeader) {
    return GetHeadersFromPeerByHashTask.startingAtHash(
        syncContext.getProtocolSchedule(),
        syncContext.getEthContext(),
        startHeader.getHash(),
        startHeader.getNumber(),
        syncContext.getConfig().downloaderHeaderRequestSize() + 1,
        syncContext.getConfig().downloaderChainSegmentSize() - 1)
        .assignPeer(target.peer())
        .run()
        .thenApply(AbstractPeerTask.PeerTaskResult::getResult);
  }
}
