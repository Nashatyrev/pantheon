package tech.pegasys.pantheon.util.source;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Created by Anton Nashatyrev on 08.11.2018.
 */
public class CodecSource<KeyType, ValueType, UpKeyType, UpValueType> extends
    AbstractLinkedDataSource<KeyType, ValueType, UpKeyType, UpValueType> {

  private final Function<KeyType, UpKeyType> keyCoder;
  private final Function<ValueType, UpValueType> valueCoder;
  private final Function<UpValueType, ValueType> valueDecoder;

  public CodecSource(final DataSource<UpKeyType, UpValueType> upstreamSource,
                     final Function<KeyType, UpKeyType> keyCoder,
                     final Function<ValueType, UpValueType> valueCoder,
                     final Function<UpValueType, ValueType> valueDecoder) {
    super(upstreamSource, true);
    this.keyCoder = requireNonNull(keyCoder);
    this.valueCoder = requireNonNull(valueCoder);
    this.valueDecoder = requireNonNull(valueDecoder);
  }

  @Override
  public Optional<ValueType> get(final KeyType key) {
    return getUpstream().get(keyCoder.apply(key)).map(valueDecoder);
  }

  @Override
  public void put(final KeyType key, final ValueType value) {
    getUpstream().put(keyCoder.apply(key), valueCoder.apply(value));
  }

  @Override
  public void remove(final KeyType key) {
    getUpstream().remove(keyCoder.apply(key));
  }

  public static class KeyOnly<KeyType, ValueType, UpKeyType> extends
      CodecSource<KeyType, ValueType, UpKeyType, ValueType> {
    public KeyOnly(final DataSource<UpKeyType, ValueType> upstreamSource,
                   final Function<KeyType, UpKeyType> keyCoder) {
      super(upstreamSource, keyCoder, Function.identity(), Function.identity());
    }
  }
  public static class ValueOnly<KeyType, ValueType, UpValueType> extends
      CodecSource<KeyType, ValueType, KeyType, UpValueType> {
    public ValueOnly(final DataSource<KeyType, UpValueType> upstreamSource,
                     final Function<ValueType, UpValueType> valueCoder,
                     final Function<UpValueType, ValueType> valueDecoder) {
      super(upstreamSource, Function.identity(), valueCoder, valueDecoder);
    }
  }
}
