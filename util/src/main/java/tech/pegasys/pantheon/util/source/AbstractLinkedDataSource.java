package tech.pegasys.pantheon.util.source;

import javax.annotation.Nonnull;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Abstract implementation of {@link LinkedDataSource}
 *
 * It can optionally do cascade commit of the upstream {@link DataSource}
 * if the corresponding flag was explicitly specified in the constructor.
 */
public abstract class AbstractLinkedDataSource<KeyType, ValueType, UpKeyType, UpValueType> implements
    LinkedDataSource<KeyType, ValueType, UpKeyType, UpValueType> {

  private final DataSource<UpKeyType, UpValueType> upstreamSource;
  private final boolean upstreamCommit;

  /**
   * Creates an instance with upstream source.
   * Cascade commit is disabled.
   */
  protected AbstractLinkedDataSource(@Nonnull final DataSource<UpKeyType, UpValueType> upstreamSource) {
    this(upstreamSource, false);
  }

  /**
   * Creates an instance with upstream source and cascade commit enabled/disabled
   * @param upstreamCommit whether upstream DataSource should be committed during this.commit()
   */
  protected AbstractLinkedDataSource(@Nonnull final DataSource<UpKeyType, UpValueType> upstreamSource,
                                     final boolean upstreamCommit) {
    this.upstreamSource = requireNonNull(upstreamSource);
    this.upstreamCommit = upstreamCommit;
  }

  @Override
  @Nonnull
  public DataSource<UpKeyType, UpValueType> getUpstream() {
    return upstreamSource;
  }

  /**
   * If cascade commit is enabled then call {@link #doCommit()} and then invokes
   * upstream <code>commit()</code>
   * If cascade commit is disabled then just call {@link #doCommit()}
   * The method is made final so all the implementation specific commit functionality
   * should be performed in overridden {@link #doCommit()}
   */
  @Override
  public final void commit() {
    doCommit();
    if (upstreamCommit) {
      getUpstream().commit();
    }
  }

  /**
   * Override this method if the implementation needs to propagate collected updates
   * to upstream source.
   * Don't call upstream <code>commit()</code> inside this method,
   * this is performed by {@link #commit()} method
   * By default does nothing.
   */
  protected void doCommit() {}
}
