package tech.pegasys.pantheon.ethereum.eth.sync.fast.flows;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.db.BlockchainStorage;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.StateNode;

/**
 * Enumerates BlockHeaders from DB starting with specified block and ending
 * with topmost known block
 */
public class DBHeadersFlow implements Publisher<BlockHeader> {

  // TODO
  public DBHeadersFlow(Blockchain storage, Hash startFromBlock) {
  }

  @Override
  public void subscribe(Subscriber<? super BlockHeader> subscriber) {
  }
}
