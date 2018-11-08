package tech.pegasys.pantheon.util.source;

import java.util.Optional;

/**
 * Created by Anton Nashatyrev on 07.11.2018.
 */
public interface DataSource<KeyType, ValueType> extends ReadonlyDataSource<KeyType, ValueType> {

  Optional<ValueType> get(KeyType key);

  void put(KeyType key, ValueType value);

  void remove(KeyType key);

  void commit();

  default void rollback() {
    // normally this method is obsolete since discarding changes can be achieved
    // just by throwing away underlying SourceCache
  }
}
