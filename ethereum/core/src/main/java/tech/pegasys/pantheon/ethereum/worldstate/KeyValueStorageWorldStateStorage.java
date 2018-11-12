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
package tech.pegasys.pantheon.ethereum.worldstate;

import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.services.kvstore.KeyValueStorage;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.source.*;

import java.util.Optional;
import java.util.function.Function;

public class KeyValueStorageWorldStateStorage implements WorldStateStorage {

  private final DataSource<BytesValue, BytesValue> keyValueStorage;
  private WriteCacheDataSource<BytesValue, BytesValue> writeCache;
  private final boolean commitSource;

  public KeyValueStorageWorldStateStorage(final DataSource<BytesValue, BytesValue> keyValueStorage) {
    this(keyValueStorage, false);
  }

  public KeyValueStorageWorldStateStorage(final DataSource<BytesValue, BytesValue> keyValueStorage,
                                          final boolean commitSource) {
    this.keyValueStorage = keyValueStorage;
    this.commitSource = commitSource;
    recreateWriteCache();
  }

  @Override
  public DataSource<Hash, BytesValue> getCodeSource() {
    return new CodecSource.KeyOnly<>(writeCache, k -> k);
  }

  @Override
  public DataSource<Bytes32, BytesValue> getAccountStateTrieNodeSource() {
    return new CodecSource.KeyOnly<>(writeCache, k -> k);
  }

  @Override
  public DataSource<Bytes32, BytesValue> getAccountStorageTrieNodeSource() {
    return new CodecSource.KeyOnly<>(writeCache, k -> k);
  }

  @Override
  public void commit() {
    writeCache.commit();
    if (commitSource) {
      keyValueStorage.commit();
    }
  }

  @Override
  public void rollback() {
    recreateWriteCache();
  }

  private void recreateWriteCache() {
    this.writeCache = new WriteCacheImpl<>(keyValueStorage);
  }

}
