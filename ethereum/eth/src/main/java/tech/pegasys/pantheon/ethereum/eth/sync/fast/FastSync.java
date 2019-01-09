package tech.pegasys.pantheon.ethereum.eth.sync.fast;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import tech.pegasys.pantheon.ethereum.chain.MutableBlockchain;
import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.core.TransactionReceipt;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.flows.*;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
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
  private static final int MBytes = 1024 * 1024;
  private Disposable subscription;

  private final SyncContext<C> syncContext;

  public FastSync(final SyncContext<C> syncContext) {
    this.syncContext = syncContext;
  }

  public synchronized void start() {
    final SyncTargetFlow<C> syncTargetFlow = new SyncTargetFlow<>(syncContext);
    final CheckpointHeadersFlow<C> checkpointHeadersFlow = new CheckpointHeadersFlow<>(syncContext);
    final DownloadHeadersFlow<C> downloadHeadersFlow = new DownloadHeadersFlow<>(syncContext)
        .withDownloaderSettings(50, 10)
        .withValidation(true, 128);
    final DownloadStateNodesFlow downloadStateNodesFlow = new DownloadStateNodesFlow()
        .withSettings(50, 128 * MBytes);

    final DownloadBlockBodiesFlow<C> downloadBlockBodiesFlow = new DownloadBlockBodiesFlow<>(syncContext)
        .withDownloaderSettings(10, 1);
    final DownloadReceiptsFlow<C> downloadReceiptsFlow = new DownloadReceiptsFlow<>(syncContext)
        .withDownloaderSettings(10);

    final FastSyncState fastSyncState = loadFastSyncState();
    final MutableBlockchain blockchain = syncContext.getProtocolContext().getBlockchain();

    final Flowable<BlockHeader> pivotHeaderFlow;
    if (fastSyncState.getState().isBeforeOrSame(HeadersDownload)) {
      pivotHeaderFlow = Flowable
          // selecting sync target and checking for better targets in background
          .fromPublisher(syncTargetFlow)
          // download checkpoint headers from sync target peer
          .compose(checkpointHeadersFlow)
          // download block headers
          .compose(downloadHeadersFlow)
          // skipping genesis block
          .filter(header -> header.getNumber() > 0)
          // store headers
          .doOnNext(this::storeHeader)
          // take pivot block
          .takeLast(1024).firstElement().toFlowable();
    } else {
      // if headers were already downloaded during previous run then just take stored pivot header
      final Optional<BlockHeader> pivotHeader = blockchain.getBlockHeader(fastSyncState.getPivotBlockHash());
      pivotHeaderFlow = Flowable.just(pivotHeader.orElseThrow(() -> new RuntimeException("No pivot block header found")));
    }

    final Flowable<StateNode> missingStateNodesFlow;
    if (fastSyncState.getState().isBeforeOrSame(StateDownload)) {
      missingStateNodesFlow = pivotHeaderFlow
          // convert BlockHeader => initial root StateNode
          .map(pivotHeader -> new StateNode(
              StateNode.NodeType.STATE, pivotHeader.getStateRoot(), BytesValue.EMPTY))
          // if some state nodes were downloaded previously then it worth
          // traversing the trie and find missing nodes.
          // If nothing downloaded yet then the only missing node would be the root node
          .compose(new MissingStateNodesFlow(getStateStorage()));
    } else {
      // if all nodes were already downloaded during previous run then nothing to do here
      missingStateNodesFlow = Flowable.empty();
    }

    final Flowable<StateNode> stateNodeFlow = missingStateNodesFlow
        // taking the flow of missing nodes download them and their children recursively
        .compose(downloadStateNodesFlow)
        // store new nodes
        .doOnNext(node -> storeStateNode(node.getNodeHash(), node.getNodeRlp()));

    // after all state nodes complete then we should proceed with regular sync process
    // starting from the block = pivotBlock + 1
    final Flowable<?> regularSyncComplete = RxUtils.singleFuture(this::startRegularSync);

    final Flowable<Block> blockBodiesFlow;
    if (fastSyncState.getState().isBeforeOrSame(BlockBodiesDownload)) {
      blockBodiesFlow = Flowable
          // enumerate all the headers stored in db starting the last downloaded block in previous run
          .fromPublisher(new DBHeadersFlow(blockchain, fastSyncState.getLastBlockBodyHash()))
          // download block bodies
          .compose(downloadBlockBodiesFlow)
          // store blocks
          .doOnNext(this::storeBlock);
    } else {
      // if all block are already here then nothing to do
      blockBodiesFlow = Flowable.empty();
    }

    final Flowable<Pair<BlockHeader, List<TransactionReceipt>>> receiptsFlow;
    if (fastSyncState.getState().isBeforeOrSame(ReceiptsDownload)) {
      receiptsFlow = Flowable
          // enumerate all the headers stored in db starting the last downloaded receipts in previous run
          .fromPublisher(new DBHeadersFlow(blockchain, fastSyncState.getLastReceiptsBlockHash()))
          // download receipts
          .compose(downloadReceiptsFlow)
          // store receipts
          .doOnNext(this::storeReceipts);
    } else {
      // if all receipts are already here then nothing to do
      receiptsFlow = Flowable.empty();
    }

    // execute these steps sequentially :
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
    // TODO
    return new FastSyncState(syncContext.getConfig());
  }

  private void storeFastSyncState(final FastSyncState fastSyncState) {
    // TODO
  }

  private void storeStateNode(final Hash nodeHash, final BytesValue nodeRLP) {
    // TODO
  }

  private WorldStateStorage getStateStorage() {
    // TODO
    return null;
  }

  private CompletableFuture<?> startRegularSync() {
    // TODO
    return CompletableFuture.completedFuture(null);
  }

  private void storeBlock(final Block block) {
    // TODO
  }
  private void storeHeader(final BlockHeader header) {
    // TODO
//    protocolContext.getBlockchain().
  }

  private void storeReceipts(final Pair<BlockHeader, List<TransactionReceipt>> blockHeaderListPair) {
    // TODO
  }
}
