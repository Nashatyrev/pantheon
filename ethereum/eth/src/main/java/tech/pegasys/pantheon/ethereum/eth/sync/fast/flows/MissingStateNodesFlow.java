package tech.pegasys.pantheon.ethereum.eth.sync.fast.flows;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.StateNode;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;

/**
 * Taking root state node(s) flow (normally a single state root node) and supplied {@link WorldStateStorage}
 * emits nodes which are missing from DB.
 * In case of sync from scratch emits just the original passed node
 */
public class MissingStateNodesFlow implements FlowableTransformer<StateNode, StateNode> {

  private Flowable<StateNode> flow;

  public MissingStateNodesFlow(WorldStateStorage stateStorage) {
  }

  @Override
  public Publisher<StateNode> apply(Flowable<StateNode> upstream) {
    return upstream;
  }
}