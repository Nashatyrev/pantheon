package tech.pegasys.pantheon.ethereum.eth.sync.fast.flows;

import io.reactivex.disposables.Disposable;
import io.reactivex.processors.BehaviorProcessor;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.schedulers.Schedulers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import tech.pegasys.pantheon.ethereum.ProtocolContext;
import tech.pegasys.pantheon.ethereum.eth.manager.ChainState;
import tech.pegasys.pantheon.ethereum.eth.manager.EthContext;
import tech.pegasys.pantheon.ethereum.eth.manager.EthPeer;
import tech.pegasys.pantheon.ethereum.eth.manager.EthPeers;
import tech.pegasys.pantheon.ethereum.eth.sync.SynchronizerConfiguration;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.SyncContext;
import tech.pegasys.pantheon.ethereum.eth.sync.state.SyncState;
import tech.pegasys.pantheon.ethereum.eth.sync.state.SyncTarget;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.DetermineCommonAncestorTask;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.WaitForPeerTask;
import tech.pegasys.pantheon.ethereum.mainnet.ProtocolSchedule;
import tech.pegasys.pantheon.util.uint.UInt256;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by Anton Nashatyrev on 22.11.2018.
 */
public class SyncTargetFlow<C> implements Publisher<Optional<SyncTarget>>{
  private static final Logger LOG = LogManager.getLogger();

  private final SyncContext<C> syncContext;

  private final FlowableProcessor<Optional<SyncTarget>> processor = BehaviorProcessor.create();

  private Duration checkTargetPeriod = Duration.ofMinutes(1);
  private long syncTargetDisconnectListenerId;
  private Disposable checkTargetTask;

  private boolean running;

  public SyncTargetFlow(final SyncContext<C> syncContext) {
    this.syncContext = syncContext;
    this.processor
        .doOnCancel(this::maybeStop)
        .doOnTerminate(this::maybeStop);
  }

  public SyncTargetFlow<C> withCheckTargetPeriod(final Duration checkTargetPeriod) {
    this.checkTargetPeriod = checkTargetPeriod;
    return this;
  }

  @Override
  public void subscribe(final Subscriber<? super Optional<SyncTarget>> subscriber) {
    processor.subscribe(subscriber);
    start();
  }

  private synchronized void start() {
    running = true;
    searchTarget();
    checkTargetTask = Schedulers.single().schedulePeriodicallyDirect(() ->
        {
          try {
            checkSyncTarget();
          } catch (Exception e) {
            LOG.warn("checkSyncTarget unexpected error", e);
          }
        },
        checkTargetPeriod.toMillis(), checkTargetPeriod.toMillis(), TimeUnit.MILLISECONDS);
  }

  private synchronized void maybeStop() {
    if (!processor.hasSubscribers()) {
      running = false;
      clearSyncTarget();
      checkTargetTask.dispose();
    }
  }

  private void emit(final Optional<SyncTarget> newTarget) {
    processor.onNext(newTarget);
  }

  public CompletableFuture<SyncTarget> getSyncTarget() {
    final CompletableFuture<SyncTarget> ret = new CompletableFuture<>();
    processor.filter(Optional::isPresent).take(1).subscribe(t -> ret.complete(t.get()));
    return ret;
  }

  private void searchTarget() {
    if (running) {
      findSyncTarget().handle((target, err) -> {
        if (err != null) {
          LOG.info("findSyncTarget() exception: " + err);
          searchTarget();
        } else {
          emit(Optional.of(target));
        }
        return null;
      });
    }
  }

  private CompletableFuture<SyncTarget> findSyncTarget() {
    final Optional<SyncTarget> maybeSyncTarget = syncContext.getSyncState().syncTarget();
    if (maybeSyncTarget.isPresent()) {
      // Nothing to do
      return CompletableFuture.completedFuture(maybeSyncTarget.get());
    }

    final Optional<EthPeer> maybeBestPeer = syncContext.getEthContext().getEthPeers().bestPeer();
    if (!maybeBestPeer.isPresent()) {
      LOG.info("No sync target, wait for peers.");
      return waitForPeerAndThenSetSyncTarget();
    } else {
      final EthPeer bestPeer = maybeBestPeer.get();
      final long peerHeight = bestPeer.chainState().getEstimatedHeight();
      final UInt256 peerTd = bestPeer.chainState().getBestBlock().getTotalDifficulty();
      if (peerTd.compareTo(syncContext.getSyncState().chainHeadTotalDifficulty()) <= 0
          && peerHeight <= syncContext.getSyncState().chainHeadNumber()) {
        // We're caught up to our best peer, try again when a new peer connects
        LOG.debug("Caught up to best peer: " + bestPeer.chainState().getEstimatedHeight());
        return waitForPeerAndThenSetSyncTarget();
      }
      return DetermineCommonAncestorTask.create(
          syncContext.getProtocolSchedule(),
          syncContext.getProtocolContext(),
          syncContext.getEthContext(),
          bestPeer,
          syncContext.getConfig().downloaderHeaderRequestSize())
          .run()
          .handle((r, t) -> r)
          .thenCompose(
              (target) -> {
                if (target == null) {
                  return waitForPeerAndThenSetSyncTarget();
                }
                final SyncTarget syncTarget = syncContext.getSyncState().setSyncTarget(bestPeer, target);
                LOG.info(
                    "Found common ancestor with peer {} at block {}", bestPeer, target.getNumber());
                syncTargetDisconnectListenerId =
                    bestPeer.subscribeDisconnect(this::onSyncTargetPeerDisconnect);
                return CompletableFuture.completedFuture(syncTarget);
              });
    }
  }

  private void onSyncTargetPeerDisconnect(final EthPeer ethPeer) {
    LOG.info("Sync target disconnected: {}", ethPeer);
    emit(Optional.empty());
    searchTarget();
  }

  private CompletableFuture<SyncTarget> waitForPeerAndThenSetSyncTarget() {
    return waitForNewPeer().handle((r, t) -> r).thenCompose((r) -> findSyncTarget());
  }
  private CompletableFuture<?> waitForNewPeer() {
    return syncContext.getEthContext()
        .getScheduler()
        .timeout(WaitForPeerTask.create(syncContext.getEthContext()), Duration.ofSeconds(5));
  }

  public void clearSyncTarget() {
    syncContext.getSyncState().syncTarget().ifPresent(this::clearSyncTarget);
  }

  private void clearSyncTarget(final SyncTarget syncTarget) {
    syncTarget.peer().unsubscribeDisconnect(syncTargetDisconnectListenerId);
    syncContext.getSyncState().clearSyncTarget();
    emit(Optional.empty());
    searchTarget();
  }

  private void checkSyncTarget() {
    final Optional<SyncTarget> maybeSyncTarget = syncContext.getSyncState().syncTarget();
    if (!maybeSyncTarget.isPresent()) {
      // Nothing to do
      return;
    }

    final SyncTarget syncTarget = maybeSyncTarget.get();
    if (shouldSwitchSyncTarget(syncTarget)) {
      LOG.info("Better sync target found, clear current sync target: {}.", syncTarget);
      clearSyncTarget(syncTarget);
    }
  }

  private boolean shouldSwitchSyncTarget(final SyncTarget currentTarget) {
    final EthPeer currentPeer = currentTarget.peer();
    final ChainState currentPeerChainState = currentPeer.chainState();
    final Optional<EthPeer> maybeBestPeer = syncContext.getEthContext().getEthPeers().bestPeer();

    return maybeBestPeer
        .map(
            bestPeer -> {
              if (EthPeers.BEST_CHAIN.compare(bestPeer, currentPeer) <= 0) {
                // Our current target is better or equal to the best peer
                return false;
              }
              // Require some threshold to be exceeded before switching targets to keep some
              // stability
              // when multiple peers are in range of each other
              final ChainState bestPeerChainState = bestPeer.chainState();
              final long heightDifference =
                  bestPeerChainState.getEstimatedHeight()
                      - currentPeerChainState.getEstimatedHeight();
              if (heightDifference == 0 && bestPeerChainState.getEstimatedHeight() == 0) {
                // Only check td if we don't have a height metric
                final UInt256 tdDifference =
                    bestPeerChainState
                        .getBestBlock()
                        .getTotalDifficulty()
                        .minus(currentPeerChainState.getBestBlock().getTotalDifficulty());
                return tdDifference.compareTo(syncContext.getConfig().downloaderChangeTargetThresholdByTd()) > 0;
              }
              return heightDifference > syncContext.getConfig().downloaderChangeTargetThresholdByHeight();
            })
        .orElse(false);
  }
}
