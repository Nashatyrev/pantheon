package tech.pegasys.pantheon.util.source;

/**
 * Created by Anton Nashatyrev on 07.11.2018.
 */
public interface LinkedDataSource<KeyType, ValueType, UpKeyType, UpValueType>
    extends DataSource<KeyType, ValueType> {

  DataSource<UpKeyType, UpValueType> getUpstream();

  /**
   * Optional method.
   *
   * Normally the whole tree of data sources is created during initialization,
   * but it could be useful sometimes to modify this pipeline after creation.
   *
   * Not every implementation may support 'hot swap', i.e. when setUpstream() is
   * called after data flow is already started.
   *   *
   * @throws IllegalStateException when this source doesn't support 'hot swap' and
   * this source started processing data already
   */
  default void setUpstream(DataSource<UpKeyType, UpValueType> newUpstream) {
    throw new UnsupportedOperationException();
  };
}
