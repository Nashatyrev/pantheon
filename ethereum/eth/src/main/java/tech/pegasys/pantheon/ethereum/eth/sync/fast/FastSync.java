package tech.pegasys.pantheon.ethereum.eth.sync.fast;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import tech.pegasys.pantheon.ethereum.chain.MutableBlockchain;
import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.core.TransactionReceipt;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.flows.*;
import tech.pegasys.pantheon.util.Pair;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.rx.RxUtils;
import tech.pegasys.pantheon.ethereum.eth.sync.state.FastSyncState;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static tech.pegasys.pantheon.ethereum.eth.sync.state.FastSyncState.State.*;

/**
 * Created by Anton Nashatyrev on 27.11.2018.
 */
public class FastSync<C> {
  private Disposable subscription;

  private final SyncContext<C> syncContext;

  public FastSync(SyncContext<C> syncContext) {
    this.syncContext = syncContext;
  }

  public synchronized void start() {
    SyncTargetFlow<C> syncTargetFlow = new SyncTargetFlow<>(syncContext);
    CheckpointHeadersFlow<C> checkpointHeadersFlow = new CheckpointHeadersFlow<>(syncContext);
    DownloadHeadersFlow<C> downloadHeadersFlow = new DownloadHeadersFlow<>(syncContext);
    DownloadStateNodesFlow downloadStateNodesFlow = new DownloadStateNodesFlow();

    DownloadBlockBodiesFlow<C> downloadBlockBodiesFlow = new DownloadBlockBodiesFlow<>(syncContext);
    DownloadReceiptsFlow<C> downloadReceiptsFlow = new DownloadReceiptsFlow<>(syncContext);

    FastSyncState fastSyncState = loadFastSyncState();
    MutableBlockchain blockchain = syncContext.getProtocolContext().getBlockchain();

    Flowable<BlockHeader> pivotHeaderFlow;
    if (fastSyncState.getState().isBeforeOrSame(HeadersDownload)) {
      pivotHeaderFlow = Flowable
          .fromPublisher(syncTargetFlow)
          .compose(checkpointHeadersFlow)
          .compose(downloadHeadersFlow)
          .doOnNext(this::storeHeader)
          .takeLast(1024).firstElement().toFlowable();// take pivot block
    } else {
      Optional<BlockHeader> pivotHeader = blockchain.getBlockHeader(fastSyncState.getPivotBlockHash());
      pivotHeaderFlow = Flowable.just(pivotHeader.orElseThrow(() -> new RuntimeException("No pivot block header found")));
    }

    Flowable<StateNode> rootStateNodesFlow;
    if (fastSyncState.getState().isBeforeOrSame(StateDownload)) {
      rootStateNodesFlow = pivotHeaderFlow
          .map(pivotHeader -> new StateNode(
              StateNode.NodeType.STATE, pivotHeader.getStateRoot(), BytesValue.EMPTY))
          .compose(new MissingStateNodesFlow());
    } else {
      rootStateNodesFlow = Flowable.empty();
    }

    Flowable<StateNode> stateNodeFlow = rootStateNodesFlow
        .compose(downloadStateNodesFlow)
        .doOnNext(node -> storeStateNode(node.getNodeHash(), node.getNodeRlp()));

    Flowable<?> regularSyncComplete = RxUtils.singleFuture(this::startRegularSync);

    Flowable<Block> blockBodiesFlow;
    if (fastSyncState.getState().isBeforeOrSame(BlockBodiesDownload)) {
      blockBodiesFlow = Flowable
          .fromPublisher(new DBHeadersFlow(blockchain, fastSyncState.getLastBlockBodyNumber()))
          .compose(downloadBlockBodiesFlow)
          .doOnNext(this::storeBlock);
    } else {
      blockBodiesFlow = Flowable.empty();
    }

    Flowable<Pair<BlockHeader, List<TransactionReceipt>>> receiptsFlow;
    if (fastSyncState.getState().isBeforeOrSame(ReceiptsDownload)) {
      receiptsFlow = Flowable
          .fromPublisher(new DBHeadersFlow(blockchain, fastSyncState.getLastReceiptsBlockNumber()))
          .compose(downloadReceiptsFlow)
          .doOnNext(this::storeReceipts);
    } else {
      receiptsFlow = Flowable.empty();
    }

    subscription = Flowable
        .concat(
            stateNodeFlow,
            regularSyncComplete,
            blockBodiesFlow,
            receiptsFlow)
        .subscribe();
  }

  public synchronized void stop() {
    if (subscription != null) {
      subscription.dispose();
    }
  }

  private FastSyncState loadFastSyncState() {
    return new FastSyncState(syncContext.getConfig());
  }

  private void storeFastSyncState(FastSyncState fastSyncState) {

  }

  private void storeStateNode(Hash nodeHash, BytesValue nodeRLP) {

  }

  private CompletableFuture<?> startRegularSync() {
    return CompletableFuture.completedFuture(null);
  }

  private void storeBlock(Block block) {

  }
  private void storeHeader(BlockHeader header) {
//    protocolContext.getBlockchain().
  }

  private void storeReceipts(Pair<BlockHeader, List<TransactionReceipt>> blockHeaderListPair) {

  }
}
