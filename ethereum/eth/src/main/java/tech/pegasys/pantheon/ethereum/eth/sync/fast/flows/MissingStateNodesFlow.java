package tech.pegasys.pantheon.ethereum.eth.sync.fast.flows;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.StateNode;

/**
 * Created by Anton Nashatyrev on 20.11.2018.
 */
public class MissingStateNodesFlow implements FlowableTransformer<StateNode, StateNode> {

  private Flowable<StateNode> flow;

  public MissingStateNodesFlow() {
  }

  @Override
  public Publisher<StateNode> apply(Flowable<StateNode> upstream) {
    return upstream;
  }
}