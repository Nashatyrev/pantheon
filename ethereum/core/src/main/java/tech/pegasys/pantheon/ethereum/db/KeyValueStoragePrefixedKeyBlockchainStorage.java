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
package tech.pegasys.pantheon.ethereum.db;

import tech.pegasys.pantheon.ethereum.chain.TransactionLocation;
import tech.pegasys.pantheon.ethereum.core.BlockBody;
import tech.pegasys.pantheon.ethereum.core.BlockHashFunction;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.core.TransactionReceipt;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.source.DataSource;
import tech.pegasys.pantheon.util.source.WriteCacheImpl;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.util.*;

public class KeyValueStoragePrefixedKeyBlockchainStorage implements BlockchainStorage {

  private final BlockchainSources sources;
  private final DataSource<BytesValue, BytesValue>  storage;
  private final BlockHashFunction blockHashFunction;

  public KeyValueStoragePrefixedKeyBlockchainStorage(
      final DataSource<BytesValue, BytesValue>  storage, final BlockHashFunction blockHashFunction) {
    this.storage = storage;
    this.blockHashFunction = blockHashFunction;
    sources = new BlockchainSources(storage, blockHashFunction);
  }

  @Override
  public Optional<Hash> getChainHead() {
    return sources.getChainHeadValue().get();
  }

  @Override
  public Collection<Hash> getForkHeads() {
    return sources.getForkHeadsValue().get().orElse(Collections.emptyList());
  }

  @Override
  public Optional<BlockHeader> getBlockHeader(final Hash blockHash) {
    return sources.getBlockHeaderSource().get(blockHash);
  }

  @Override
  public Optional<BlockBody> getBlockBody(final Hash blockHash) {
    return sources.getBlockBodySource().get(blockHash);
  }

  @Override
  public Optional<List<TransactionReceipt>> getTransactionReceipts(final Hash blockHash) {
    return sources.getTxReceiptsSource().get(blockHash);
  }

  @Override
  public Optional<Hash> getBlockHash(final long blockNumber) {
    return sources.getBlockHashSource().get(blockNumber);
  }

  @Override
  public Optional<UInt256> getTotalDifficulty(final Hash blockHash) {
    return sources.getTotalDifficultySource().get(blockHash);
  }

  @Override
  public Optional<TransactionLocation> getTransactionLocation(final Hash transactionHash) {
    return sources.getTxLocationSource().get(transactionHash);
  }

  @Override
  public Updater updater() {
    return new Updater(storage, blockHashFunction);
  }

  public static class Updater implements BlockchainStorage.Updater {

    private final BlockchainSources sources;
    private final WriteCacheImpl<BytesValue, BytesValue> writeCache;

    private Updater(final DataSource<BytesValue, BytesValue> storage, final BlockHashFunction blockHashFunction) {
      writeCache = new WriteCacheImpl<>(storage);
      sources = new BlockchainSources(writeCache, blockHashFunction);
    }

    @Override
    public void putBlockHeader(final Hash blockHash, final BlockHeader blockHeader) {
      sources.getBlockHeaderSource().put(blockHash, blockHeader);
    }

    @Override
    public void putBlockBody(final Hash blockHash, final BlockBody blockBody) {
      sources.getBlockBodySource().put(blockHash, blockBody);
    }

    @Override
    public void putTransactionLocation(
        final Hash transactionHash, final TransactionLocation transactionLocation) {
      sources.getTxLocationSource().put(transactionHash, transactionLocation);
    }

    @Override
    public void putTransactionReceipts(
        final Hash blockHash, final List<TransactionReceipt> transactionReceipts) {
      sources.getTxReceiptsSource().put(blockHash, transactionReceipts);
    }

    @Override
    public void putBlockHash(final long blockNumber, final Hash blockHash) {
      sources.getBlockHashSource().put(blockNumber, blockHash);
    }

    @Override
    public void putTotalDifficulty(final Hash blockHash, final UInt256 totalDifficulty) {
      sources.getTotalDifficultySource().put(blockHash, totalDifficulty);
    }

    @Override
    public void setChainHead(final Hash blockHash) {
      sources.getChainHeadValue().set(blockHash);
    }

    @Override
    public void setForkHeads(final Collection<Hash> forkHeadHashes) {
      sources.getForkHeadsValue().set(new ArrayList<>(forkHeadHashes));
    }

    @Override
    public void removeBlockHash(final long blockNumber) {
      sources.getBlockHashSource().remove(blockNumber);
    }

    @Override
    public void removeTransactionLocation(final Hash transactionHash) {
      sources.getTxLocationSource().remove(transactionHash);
    }

    @Override
    public void commit() {
      writeCache.flush();
    }

    @Override
    public void rollback() {
      writeCache.reset();
    }

  }
}
