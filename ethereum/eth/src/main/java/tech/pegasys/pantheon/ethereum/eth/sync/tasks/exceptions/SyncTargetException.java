package tech.pegasys.pantheon.ethereum.eth.sync.tasks.exceptions;

import tech.pegasys.pantheon.ethereum.eth.sync.state.SyncTarget;

/**
 * Created by Anton Nashatyrev on 03.12.2018.
 */
public class SyncTargetException  extends RuntimeException {
  private final SyncTarget syncTarget;

  public SyncTargetException(final String message, final SyncTarget syncTarget) {
    super(message);
    this.syncTarget = syncTarget;
  }

  public SyncTarget getSyncTarget() {
    return syncTarget;
  }
}
