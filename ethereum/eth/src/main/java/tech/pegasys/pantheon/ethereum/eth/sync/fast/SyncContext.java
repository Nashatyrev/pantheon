package tech.pegasys.pantheon.ethereum.eth.sync.fast;

import tech.pegasys.pantheon.ethereum.ProtocolContext;
import tech.pegasys.pantheon.ethereum.eth.manager.EthContext;
import tech.pegasys.pantheon.ethereum.eth.sync.SynchronizerConfiguration;
import tech.pegasys.pantheon.ethereum.eth.sync.state.SyncState;
import tech.pegasys.pantheon.ethereum.mainnet.ProtocolSchedule;

/**
 * Just a container for all context stuff needed for sync
 */
public class SyncContext<C> {
  private final SynchronizerConfiguration config;
  private final ProtocolSchedule<C> protocolSchedule;
  private final ProtocolContext<C> protocolContext;
  private final EthContext ethContext;
  private final SyncState syncState;

  public SyncContext(SynchronizerConfiguration config, ProtocolSchedule<C> protocolSchedule, ProtocolContext<C> protocolContext, EthContext ethContext, SyncState syncState) {
    this.config = config;
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.syncState = syncState;
  }

  public SynchronizerConfiguration getConfig() {
    return config;
  }

  public ProtocolSchedule<C> getProtocolSchedule() {
    return protocolSchedule;
  }

  public ProtocolContext<C> getProtocolContext() {
    return protocolContext;
  }

  public EthContext getEthContext() {
    return ethContext;
  }

  public SyncState getSyncState() {
    return syncState;
  }
}
