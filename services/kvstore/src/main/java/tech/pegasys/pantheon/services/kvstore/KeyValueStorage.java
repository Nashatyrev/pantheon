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
package tech.pegasys.pantheon.services.kvstore;

import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.source.DataSource;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/** Service provided by pantheon to facilitate persistent data storage. */
public interface KeyValueStorage
    extends DataSource<BytesValue, BytesValue>, Closeable {

  /**
   * @param key Index into persistent data repository.
   * @return The value persisted at the key index.
   */
  @Override
  Optional<BytesValue> get(@Nonnull BytesValue key) throws StorageException;

  /**
   * @param key Index into persistent data repository.
   * @param value The value persisted at the key index.
   */
  @Override
  void put(@Nonnull BytesValue key, @Nonnull BytesValue value) throws StorageException;

  /**
   * Remove the data corresponding to the given key.
   *
   * @param key Index into persistent data repository.
   */
  @Override
  void remove(@Nonnull BytesValue key) throws StorageException;

  @Override
  void flush() throws StorageException;

  /**
   * Stream all stored key-value pairs.
   *
   * @return A stream of the contained key-value pairs.
   */
  Stream<Entry> entries();

  class Entry {
    private final BytesValue key;
    private final BytesValue value;

    private Entry(final BytesValue key, final BytesValue value) {
      this.key = key;
      this.value = value;
    }

    public static Entry create(final BytesValue key, final BytesValue value) {
      return new Entry(key, value);
    }

    public BytesValue getKey() {
      return key;
    }

    public BytesValue getValue() {
      return value;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Entry)) {
        return false;
      }
      final Entry other = (Entry) obj;
      return Objects.equals(getKey(), other.getKey())
          && Objects.equals(getValue(), other.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }
  }

  class StorageException extends RuntimeException {
    public StorageException(final Throwable t) {
      super(t);
    }
  }

}
