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

import static com.google.common.base.Preconditions.checkNotNull;
import static tech.pegasys.pantheon.ethereum.trie.CompactEncoding.bytesToPath;

import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.source.AbstractLinkedDataSource;
import tech.pegasys.pantheon.util.source.DataSource;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * A {@link MerklePatriciaTrie} that persists trie nodes to a key/value store.
 *
 * @param <V> The type of values stored by this trie.
 */
public class StoredMerklePatriciaTrie<K extends BytesValue, V>
    extends AbstractLinkedDataSource<K, V, Bytes32, BytesValue>
    implements MerklePatriciaTrie<K, V> {

  private final GetVisitor<V> getVisitor = new GetVisitor<>();
  private final RemoveVisitor<V> removeVisitor = new RemoveVisitor<>();
  private final StoredNodeFactory<V> nodeFactory;

  private Node<V> root;

  /**
   * Create a trie.
   * @param valueSerializer A function for serializing values to bytes.
   * @param valueDeserializer A function for deserializing values from bytes.
   * @param upstreamSource Trie backing storage
   */
  public StoredMerklePatriciaTrie(
      final DataSource<Bytes32, BytesValue> upstreamSource,
      final Function<V, BytesValue> valueSerializer,
      final Function<BytesValue, V> valueDeserializer) {
    this(upstreamSource, MerklePatriciaTrie.EMPTY_TRIE_ROOT_HASH, valueSerializer, valueDeserializer);
  }

  /**
   * Create a trie.
   *
   * @param upstreamSource Trie backing storage
   * @param rootHash The initial root has for the trie, which should be already present in {@code
   *     storage}.
   * @param valueSerializer A function for serializing values to bytes.
   * @param valueDeserializer A function for deserializing values from bytes.
   */
  public StoredMerklePatriciaTrie(
      final DataSource<Bytes32, BytesValue> upstreamSource,
      final Bytes32 rootHash,
      final Function<V, BytesValue> valueSerializer,
      final Function<BytesValue, V> valueDeserializer) {
    super(upstreamSource);
    this.nodeFactory = new StoredNodeFactory<>(upstreamSource, valueSerializer, valueDeserializer);
    this.root =
        rootHash.equals(MerklePatriciaTrie.EMPTY_TRIE_ROOT_HASH)
            ? NullNode.instance()
            : new StoredNode<>(nodeFactory, rootHash);
  }

  @Override
  public Optional<V> get(@Nonnull final K key) {
    checkNotNull(key);
    return root.accept(getVisitor, bytesToPath(key)).getValue();
  }

  @Override
  public void put(@Nonnull final K key, @Nonnull final V value) {
    checkNotNull(key);
    checkNotNull(value);
    this.root = root.accept(new PutVisitor<>(nodeFactory, value), bytesToPath(key));
  }

  @Override
  public void remove(@Nonnull final K key) {
    checkNotNull(key);
    this.root = root.accept(removeVisitor, bytesToPath(key));
  }

  @Override
  public void doCommit() {
    final CommitVisitor<V> commitVisitor = new CommitVisitor<>(
        (hash, value) -> getUpstream().put(hash, value));
    root.accept(commitVisitor);
    // Make sure root node was stored
    if (root.isDirty() && root.getRlpRef().size() < 32) {
      getUpstream().put(root.getHash(), root.getRlpRef());
    }
    // Reset root so dirty nodes can be garbage collected
    final Bytes32 rootHash = root.getHash();
    this.root =
        rootHash.equals(MerklePatriciaTrie.EMPTY_TRIE_ROOT_HASH)
            ? NullNode.instance()
            : new StoredNode<>(nodeFactory, rootHash);
  }

  @Override
  public Map<Bytes32, V> entriesFrom(final Bytes32 startKeyHash, final int limit) {
    return StorageEntriesCollector.collectEntries(root, startKeyHash, limit);
  }

  @Override
  public Bytes32 getRootHash() {
    return root.getHash();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + getRootHash() + "]";
  }
}
