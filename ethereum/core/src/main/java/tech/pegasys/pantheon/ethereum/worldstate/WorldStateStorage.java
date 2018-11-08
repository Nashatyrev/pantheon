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
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.source.DataSource;
import tech.pegasys.pantheon.util.source.ReadonlyDataSource;

import java.util.Optional;

public interface WorldStateStorage {

  ReadonlyDataSource<Hash, BytesValue> getCodeSource();

  ReadonlyDataSource<Bytes32, BytesValue> getAccountStateTrieNodeSource();

  ReadonlyDataSource<Bytes32, BytesValue> getAccountStorageTrieNodeSource();

  Updater updater();

  interface Updater {

    DataSource<Hash, BytesValue> getCodeSource();

    DataSource<Bytes32, BytesValue> getAccountStateTrieNodeSource();

    DataSource<Bytes32, BytesValue> getAccountStorageTrieNodeSource();

    void commit();

    void rollback();
  }
}
