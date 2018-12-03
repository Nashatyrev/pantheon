package tech.pegasys.pantheon.ethereum.eth.sync.fast.flows;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.processors.AsyncProcessor;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.UnicastProcessor;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.StateNode;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.rx.TreeTraverseBuffer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Taking initial trie nodes as input, recursively traverses state trie, downloads new nodes and
 * emits newly found trie nodes (including state trie nodes, code nodes and contract storage nodes)
 * If a node if referred from several parents this node is emitted exactly the same times to support
 * correct ref-counting (if any)
 *
 * The general flow scheme is below
 *
 *                     /---------------------------------------------------\
 *                    /                                                     \
 *                   V               /---> [missing nodes] ----------------->\
 * -- [root nodes] --> [downloader] -----> [child nodes] --> [nodes buffer] ->\
 *                                   \---> [ready nodes] ----------------------------------->
 */
public class DownloadStateNodesFlow implements FlowableTransformer<StateNode, StateNode> {

  private int maxDownloadConcurrency = 50;
  private int nodesBufferSize = 16 * 1024 * 1024;

  public DownloadStateNodesFlow withSettings(final int maxDownloadConcurrency,
                                             final int nodesBufferSize) {
    this.maxDownloadConcurrency = maxDownloadConcurrency;
    this.nodesBufferSize = nodesBufferSize;
    return this;
  }

  @Override
  public Publisher<StateNode> apply(final Flowable<StateNode> rootNodes) {
    final FlowableProcessor<StateNode> processor = UnicastProcessor.create();

    final ConnectableFlowable<StateNode> downloader = processor
        // split nodes to chunks
        .buffer(100, TimeUnit.MILLISECONDS, 128)
        .filter(l -> !l.isEmpty())
        // download nodes from 50 peers max
        .flatMap(node -> Flowable.fromFuture(download(node)), maxDownloadConcurrency)
        .flatMap(Flowable::fromIterable)
        // breaking stream recursive loop
        .observeOn(Schedulers.io())
        .publish();

    // nodes failed to download
    final Flowable<StateNode> missingNodes = downloader
        .filter(node -> !node.hasRLP())
        .doOnNext(n -> System.out.println("missingNodes: " + n));
    // downloaded nodes
    final Flowable<StateNode> loadedNodes = downloader
        .filter(StateNode::hasRLP)
        .share();

    final Flowable<StateNode> children = loadedNodes
        .map(StateNode::createChildRequests)
        .flatMap(Flowable::fromIterable)
        .compose(new TreeTraverseBuffer<>(nodesBufferSize, StateNode::estimateSize));

    final Flowable<StateNode> mergedFlow = Flowable.merge(rootNodes, missingNodes, children)
        .doOnComplete(() -> System.out.println("mergedFlow complete"));

    mergedFlow.subscribe(processor);

    downloader.connect();
    return loadedNodes;
  }

  protected CompletableFuture<List<StateNode>> download(final List<StateNode> nodes) {
    return null;
  }

}
