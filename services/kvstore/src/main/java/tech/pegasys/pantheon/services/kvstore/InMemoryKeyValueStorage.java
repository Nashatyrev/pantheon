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

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemoryKeyValueStorage implements KeyValueStorage {

  private final Map<BytesValue, BytesValue> hashValueStore = new HashMap<>();
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  @Override
  public Optional<BytesValue> get(@Nonnull final BytesValue key) {
    final Lock lock = rwLock.readLock();
    try {
      lock.lock();
      return Optional.ofNullable(hashValueStore.get(key));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void put(@Nonnull final BytesValue key, @Nonnull final BytesValue value) {
    final Lock lock = rwLock.writeLock();
    try {
      lock.lock();
      hashValueStore.put(key, value);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void remove(@Nonnull final BytesValue key) throws StorageException {
    final Lock lock = rwLock.writeLock();
    try {
      lock.lock();
      hashValueStore.remove(key);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void flush() {
    // nothing to do
  }

  @Override
  public Stream<Entry> entries() {
    final Lock lock = rwLock.readLock();
    try {
      lock.lock();
      // Ensure we have collected all entries before releasing the lock and returning
      return hashValueStore
          .entrySet()
          .stream()
          .map(e -> Entry.create(e.getKey(), e.getValue()))
          .collect(Collectors.toSet())
          .stream();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {}
}
