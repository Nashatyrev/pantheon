package tech.pegasys.pantheon.ethereum.eth.sync.fast.flows;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.TransactionReceipt;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.SyncContext;
import tech.pegasys.pantheon.util.Pair;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.rx.RxUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by Anton Nashatyrev on 22.11.2018.
 */
public class DownloadReceiptsFlow<C> implements
    FlowableTransformer<BlockHeader, Pair<BlockHeader, List<TransactionReceipt>>> {

  private final SyncContext<C> syncContext;

  private int maxDownloadConcurrency = 50;

  public DownloadReceiptsFlow(SyncContext<C> syncContext) {
    this.syncContext = syncContext;
  }

  public DownloadReceiptsFlow<C> withDownloaderSettings(int maxConcurrency) {
    this.maxDownloadConcurrency = maxConcurrency;
    return this;
  }

  @Override
  public Publisher<Pair<BlockHeader, List<TransactionReceipt>>> apply(Flowable<BlockHeader> upstream) {
    return upstream
        .buffer(32)
        .flatMap(headers -> RxUtils.fromFuture(requestReceipts(headers)),
            Pair::of, maxDownloadConcurrency)
        .flatMap(p -> {
          ArrayList<Pair<BlockHeader, List<TransactionReceipt>>> ret = new ArrayList<>();
          for (int i = 0; i < p.first().size(); i++) {
            ret.add(Pair.of(p.first().get(i), p.second().get(i)));
          }
          return Flowable.fromIterable(ret);
        });
  }

  protected CompletableFuture<List<List<TransactionReceipt>>> requestReceipts(List<BlockHeader> blockHashes) {
    return null;
  }

}
