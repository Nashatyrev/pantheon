package tech.pegasys.pantheon.util.source;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Created by Anton Nashatyrev on 08.11.2018.
 */
public abstract class AbstractLinkedDataSource<KeyType, ValueType, UpKeyType, UpValueType> implements
    LinkedDataSource<KeyType, ValueType, UpKeyType, UpValueType> {

  private final DataSource<UpKeyType, UpValueType> upstreamSource;
  private final boolean upstreamCommit;

  protected AbstractLinkedDataSource(final DataSource<UpKeyType, UpValueType> upstreamSource) {
    this(upstreamSource, false);
  }
  protected AbstractLinkedDataSource(final DataSource<UpKeyType, UpValueType> upstreamSource,
                                     final boolean upstreamCommit) {
    this.upstreamSource = requireNonNull(upstreamSource);
    this.upstreamCommit = upstreamCommit;
  }

  @Override
  public DataSource<UpKeyType, UpValueType> getUpstream() {
    return upstreamSource;
  }

  @Override
  public final void commit() {
    doCommit();
    if (upstreamCommit) {
      getUpstream().commit();
    }
  }

  protected void doCommit() {}
}
