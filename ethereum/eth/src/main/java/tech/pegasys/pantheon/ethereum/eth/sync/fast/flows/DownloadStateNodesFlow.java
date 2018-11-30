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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by Anton Nashatyrev on 20.11.2018.
 */
public class DownloadStateNodesFlow implements FlowableTransformer<StateNode, StateNode> {

  @Override
  public Publisher<StateNode> apply(Flowable<StateNode> rootNodes) {
    FlowableProcessor<StateNode> processor = UnicastProcessor.create();

    ConnectableFlowable<StateNode> downloader = processor
        // split nodes to chunks
        .buffer(100, TimeUnit.MILLISECONDS, 128)
        .filter(l -> !l.isEmpty())
        // download nodes from 50 peers max
        .flatMap(node -> Flowable.fromFuture(download(node)), 50)
        .flatMap(Flowable::fromIterable)
        // breaking stream recursive loop
        .observeOn(Schedulers.single())
        .publish();

    Flowable<StateNode> missingNodes = downloader
        .filter(node -> !node.hasRLP())
        .doOnNext(n -> System.out.println("missingNodes: " + n));
    Flowable<StateNode> loadedNodes = downloader
        .filter(StateNode::hasRLP)
        .share();

    Flowable<StateNode> children = loadedNodes
        .map(StateNode::createChildRequests)
        .flatMap(Flowable::fromIterable);

    Flowable<StateNode> mergedFlow = Flowable.merge(rootNodes, missingNodes, children)
        .doOnComplete(() -> System.out.println("mergedFlow complete"));

    mergedFlow.subscribe(processor);

    downloader.connect();
    return loadedNodes;
  }

  protected CompletableFuture<List<StateNode>> download(List<StateNode> nodes) {
    return null;
  }

}
