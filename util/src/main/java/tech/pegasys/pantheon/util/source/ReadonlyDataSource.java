package tech.pegasys.pantheon.util.source;

import java.util.Optional;

/**
 * Created by Anton Nashatyrev on 07.11.2018.
 */
public interface ReadonlyDataSource<KeyType, ValueType> {

    Optional<ValueType> get(KeyType key);
}
