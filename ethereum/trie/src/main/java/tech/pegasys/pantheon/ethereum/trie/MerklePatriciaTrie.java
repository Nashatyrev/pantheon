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
package tech.pegasys.pantheon.ethereum.trie;

import static tech.pegasys.pantheon.crypto.Hash.keccak256;

import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.source.DataSource;
import tech.pegasys.pantheon.util.source.LinkedDataSource;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

/**
 * An Merkle Patricial Trie.
 */
public interface MerklePatriciaTrie<K extends BytesValue, V> extends LinkedDataSource<K, V, Bytes32, BytesValue> {

  Bytes32 EMPTY_TRIE_ROOT_HASH = keccak256(RLP.NULL);

  /**
   * Returns an {@code Optional} of value mapped to the hash if it exists; otherwise empty.
   *
   * @param key The key for the value.
   * @return an {@code Optional} of value mapped to the hash if it exists; otherwise empty
   */
  Optional<V> get(@Nonnull K key);

  /**
   * Updates the value mapped to the specified key, creating the mapping if one does not already
   * exist.
   *
   * @param key   The key that corresponds to the value to be updated.
   * @param value The value to associate the key with.
   */
  void put(@Nonnull K key, @Nonnull V value);

  /**
   * Deletes the value mapped to the specified key, if such a value exists (Optional operation).
   *
   * @param key The key of the value to be deleted.
   */
  void remove(@Nonnull K key);

  /**
   * Returns the KECCAK256 hash of the root node of the trie.
   *
   * @return The KECCAK256 hash of the root node of the trie.
   */
  Bytes32 getRootHash();

  /**
   * Commits any pending changes to the underlying storage.
   */
  void flush();

  /**
   * Retrieve up to {@code limit} storage entries beginning from the first entry with hash equal to
   * or greater than {@code startKeyHash}.
   *
   * @param startKeyHash the first key hash to return.
   * @param limit        the maximum number of entries to return.
   * @return the requested storage entries as a map of key hash to value.
   */
  Map<Bytes32, V> entriesFrom(Bytes32 startKeyHash, int limit);
}
