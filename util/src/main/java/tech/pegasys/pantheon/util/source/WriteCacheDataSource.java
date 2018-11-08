package tech.pegasys.pantheon.util.source;

import java.util.Optional;

/**
 * Created by Anton Nashatyrev on 07.11.2018.
 */
public interface WriteCacheDataSource<KeyType, ValueType>
    extends LinkedDataSource<KeyType, ValueType, KeyType, ValueType> {

  int getCachedEntriesCount();

  /**
   * If the value is not cached returns
   *   Optional.empty()
   * If the value cached and the value is null returns
   *   Optional.of(Optional.empty())
   * If the value cached and the value is not null returns
   *   Optional.of(Optional.of(value))
   */
  Optional<Optional<ValueType>> getCacheEntry(KeyType key);

}
