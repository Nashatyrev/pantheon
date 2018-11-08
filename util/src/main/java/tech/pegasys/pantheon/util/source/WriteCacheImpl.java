package tech.pegasys.pantheon.util.source;

import java.util.Optional;

/**
 * This is mock implementation
 *
 * Created by Anton Nashatyrev on 12.11.2018.
 */
public class WriteCacheImpl<K, V> extends AbstractLinkedDataSource<K,V,K,V>
    implements WriteCacheDataSource<K, V> {

  public WriteCacheImpl(final DataSource<K, V> upstreamSource) {
    super(upstreamSource);
  }

  @Override
  public Optional<V> get(final K key) {
    return Optional.empty();
  }

  @Override
  public void put(final K key, final V value) {

  }

  @Override
  public void remove(final K key) {

  }

  @Override
  public void doCommit() {

  }

  @Override
  public int getCachedEntriesCount() {
    return 0;
  }

  @Override
  public Optional<Optional<V>> getCacheEntry(final K key) {
    return Optional.empty();
  }
}
