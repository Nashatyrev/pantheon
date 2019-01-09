package tech.pegasys.pantheon.ethereum.eth.sync.tasks.exceptions;

import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.eth.sync.state.SyncTarget;

/**
 * Created by Anton Nashatyrev on 23.11.2018.
 */
public class TargetMissingHeadersException extends SyncTargetException {
  private final BlockHeader topmostHeaderReturned;

  public TargetMissingHeadersException(SyncTarget syncTarget, BlockHeader topmostHeaderReturned) {
    super("Target peer didn't return requested headers: " + syncTarget + ", topmost header: " + topmostHeaderReturned
        , syncTarget);
    this.topmostHeaderReturned = topmostHeaderReturned;
  }

  public BlockHeader getTopmostHeaderReturned() {
    return topmostHeaderReturned;
  }
}
