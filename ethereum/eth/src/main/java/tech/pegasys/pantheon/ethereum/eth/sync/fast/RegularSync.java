package tech.pegasys.pantheon.ethereum.eth.sync.fast;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;
import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.Transaction;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.flows.*;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.rx.PrefetchBuffer;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.rx.RxUtils;
import tech.pegasys.pantheon.ethereum.eth.sync.state.SyncTarget;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.PersistBlockTask;
import tech.pegasys.pantheon.ethereum.mainnet.HeaderValidationMode;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Created by Anton Nashatyrev on 27.11.2018.
 */
public class RegularSync<C> {
  private static final Logger LOG = LogManager.getLogger();
  private static final int MAX_HEADER_SIZE = 800;
  private static final int MBytes = 1024 * 1024;

  private final SyncContext<C> syncContext;

  private final Publisher<Optional<SyncTarget>> syncTargetFlow;
  private final FlowableTransformer<Optional<SyncTarget>, BlockHeader> checkpointHeadersFlow;
  private final FlowableTransformer<BlockHeader, BlockHeader>  downloadHeadersFlow;
  private final FlowableTransformer<BlockHeader, BlockHeader> headersPrefetchBuffer;
  private final FlowableTransformer<BlockHeader, Block> downloadBlockBodiesFlow;
  private final FlowableTransformer<Block, Block> blocksPrefetchBuffer;

  private Disposable subscription;

  public RegularSync(final SyncContext<C> syncContext) {
    this(syncContext,
        new SyncTargetFlow<>(syncContext)
            .withCheckTargetPeriod(Duration.ofMinutes(1)),
        new CheckpointHeadersFlow<>(syncContext),
        new DownloadHeadersFlow<>(syncContext)
            .withDownloaderSettings(10, 5)
            .withValidation(true, 16),
        new PrefetchBuffer<>(10 * MBytes, h -> MAX_HEADER_SIZE),
        new DownloadBlockBodiesFlow<>(syncContext)
            .withDownloaderSettings(20, 5),
        new PrefetchBuffer<>(100 * MBytes, Block::calculateSize));
  }

  public RegularSync(final SyncContext<C> syncContext,
                     final Publisher<Optional<SyncTarget>> syncTargetFlow,
                     final FlowableTransformer<Optional<SyncTarget>, BlockHeader> checkpointHeadersFlow,
                     final FlowableTransformer<BlockHeader, BlockHeader> downloadHeadersFlow,
                     final FlowableTransformer<BlockHeader, BlockHeader> headersPrefetchBuffer,
                     final FlowableTransformer<BlockHeader, Block> downloadBlockBodiesFlow,
                     final FlowableTransformer<Block, Block> blocksPrefetchBuffer) {
    this.syncContext = syncContext;
    this.syncTargetFlow = syncTargetFlow;
    this.checkpointHeadersFlow = checkpointHeadersFlow;
    this.downloadHeadersFlow = downloadHeadersFlow;
    this.headersPrefetchBuffer = headersPrefetchBuffer;
    this.downloadBlockBodiesFlow = downloadBlockBodiesFlow;
    this.blocksPrefetchBuffer = blocksPrefetchBuffer;
  }

  public synchronized void start() {

    subscription = Flowable
        // selecting sync target and checking for better targets in background
        .fromPublisher(syncTargetFlow)
        // download checkpoint headers from sync target peer
        .compose(wrap(checkpointHeadersFlow))
        // download block headers
        .compose(wrap(downloadHeadersFlow))
        // skipping genesis block
        .filter(header -> header.getNumber() > 0)
        // eagerly pre-fetch block headers
        .compose(wrap(headersPrefetchBuffer))
        // download block bodies
        .compose(wrap(downloadBlockBodiesFlow))
        // parallel CPU-hard transaction sender precomputing
        .concatMapEager(block -> Flowable.just(precomputeTxSender(block)))
          .observeOn(Schedulers.computation())
        // eagerly pre-fetch blocks
        .compose(wrap(blocksPrefetchBuffer))
        // importing blocks
        .flatMap(block -> RxUtils.fromFuture(importBlocks(block)))
        // restart infinitely on any unrecoverable errors:
        // block import, header validation, no more checkpoint headers
        .doOnError(this::reportError).retry()
        .subscribe();
  }

  public synchronized void stop() {
    if (subscription != null) {
      subscription.dispose();
    }
  }

  private void reportError(final Throwable t) {
    LOG.warn("Error during sync: ", t);
  }

  private Block precomputeTxSender(final Block block) {
    block.getBody().getTransactions().forEach(Transaction::getSender);
    return block;
  }

  private CompletableFuture<Block> importBlocks(final Block block) {
    final Supplier<CompletableFuture<Block>> task =
        PersistBlockTask.forSingleBlock(
            syncContext.getProtocolSchedule(), syncContext.getProtocolContext(),
            block,
            HeaderValidationMode.NONE); // headers were already validated
    return syncContext.getEthContext().getScheduler().scheduleWorkerTask(task);
  }

  private <U, D> FlowableTransformer<U, D> wrap(final FlowableTransformer<U, D> transformer) {
    return fl -> wrapFlow(transformer, fl);
  }

  protected <U,D> Publisher<D> wrapFlow(final FlowableTransformer<U, D> transformer, final Flowable<U> upstream) {
    return transformer.apply(upstream);
  }
}
