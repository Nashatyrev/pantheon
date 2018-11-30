/*
 * Copyright 2018 ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package tech.pegasys.pantheon.ethereum.eth.sync.state;

import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.eth.sync.SynchronizerConfiguration;

public final class FastSyncState {

  public enum State {
    Initial,
    HeadersDownload,
    StateDownload,
    BlockBodiesDownload,
    ReceiptsDownload,
    Complete;

    public boolean isAfter(State otherState) {
      return this.ordinal() > otherState.ordinal();
    }
    public boolean isAfterOrSame(State otherState) {
      return this.ordinal() >= otherState.ordinal();
    }
    public boolean isBefore(State otherState) {
      return this.ordinal() < otherState.ordinal();
    }
    public boolean isBeforeOrSame(State otherState) {
      return this.ordinal() <= otherState.ordinal();
    }
  }

  private final SynchronizerConfiguration config;

  private State state = State.Initial;

  private Hash lastHeaderHash;
  private Hash pivotBlockHash;
  private long stateNodesComplete;
  private long lastBlockBodyNumber;
  private long lastReceiptsBlockNumber;

  public FastSyncState(final SynchronizerConfiguration config) {
    this.config = config;
  }

  public State getState() {
    return state;
  }

  public Hash getPivotBlockHash() {
    return pivotBlockHash;
  }

  public long getLastBlockBodyNumber() {
    return lastBlockBodyNumber;
  }

  public long getLastReceiptsBlockNumber() {
    return lastReceiptsBlockNumber;
  }

  //  /**
//   * Registers the chain height that we're trying to sync to.
//   *
//   * @param blockNumber the height of the chain we are syncing to.
//   */
//  public void setFastSyncChainTarget(final long blockNumber) {
//    pivotBlockNumber = blockNumber;
//  }
//
//  /** @return the block number at which we switch from fast sync to full sync */
//  public long pivot() {
//    return Math.max(pivotBlockNumber - config.fastSyncPivotDistance(), 0);
//  }
}
