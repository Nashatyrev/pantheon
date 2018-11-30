package tech.pegasys.pantheon.ethereum.eth.sync.fast.flows;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;
import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.SyncContext;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.rx.RxUtils;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.CompleteBlocksTask;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by Anton Nashatyrev on 22.11.2018.
 */
public class DownloadBlockBodiesFlow<C> implements FlowableTransformer<BlockHeader, Block> {
  private final SyncContext<C> syncContext;

  private int maxDownloadConcurrency = 50;
  private int downloadPrefetch = 200;

  public DownloadBlockBodiesFlow(SyncContext<C> syncContext) {
    this.syncContext = syncContext;
  }

  public DownloadBlockBodiesFlow<C> withDownloaderSettings(int maxConcurrency, int prefetch) {
    this.maxDownloadConcurrency = maxConcurrency;
    this.downloadPrefetch = prefetch;
    return this;
  }

  @Override
  public Publisher<Block> apply(Flowable<BlockHeader> upstream) {
    return upstream
        // split headers to batches
        .buffer(128) // TODO Flux.buffertimeout(128, 1minute)
        // split header batches to maxDownloadConcurrency parallel downloads and then assemble
        // downloaded blocks back preserving their order
        .concatMapEager(headers ->
            RxUtils.fromFuture(requestBodies(headers)), maxDownloadConcurrency, downloadPrefetch)
        // flatten block batches
        .flatMap(Flowable::fromIterable);
  }

  protected CompletableFuture<List<Block>> requestBodies(List<BlockHeader> headers) {
    // TODO maxRetries ?
    return CompleteBlocksTask.forHeaders(syncContext.getProtocolSchedule(),
        syncContext.getEthContext(), headers).run();
  }
}
