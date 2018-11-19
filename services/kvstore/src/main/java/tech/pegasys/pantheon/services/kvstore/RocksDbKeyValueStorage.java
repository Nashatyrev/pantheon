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

import org.rocksdb.*;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;

public class RocksDbKeyValueStorage implements KeyValueStorage, Closeable {

  private static final Logger LOG = LogManager.getLogger();

  private final Options options;
  private final TransactionDBOptions txOptions;
  private final TransactionDB db;
  private final ReadOptions txReadOptions = new ReadOptions();
  private final WriteOptions txWriteOptions = new WriteOptions();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private org.rocksdb.Transaction currentTransaction = null;

  static {
    RocksDB.loadLibrary();
  }

  public static KeyValueStorage create(final Path storageDirectory) throws StorageException {
    return new RocksDbKeyValueStorage(storageDirectory);
  }

  private RocksDbKeyValueStorage(final Path storageDirectory) {
    try {
      options = new Options().setCreateIfMissing(true);
      txOptions = new TransactionDBOptions();
      db = TransactionDB.open(options, txOptions, storageDirectory.toString());
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Optional<BytesValue> get(@Nonnull final BytesValue key) throws StorageException {
    throwIfClosed();
    try {
      return Optional.ofNullable(currentTransaction().get(txReadOptions, key.extractArray())).map(BytesValue::wrap);
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void put(@Nonnull final BytesValue key, @Nonnull final BytesValue value) throws StorageException {
    throwIfClosed();
    try {
      currentTransaction().put(key.extractArray(), value.extractArray());
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void remove(@Nonnull final BytesValue key) throws StorageException {
    throwIfClosed();
    try {
      currentTransaction().delete(key.extractArray());
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  private synchronized org.rocksdb.Transaction currentTransaction() throws StorageException {
    if (currentTransaction == null) {
      currentTransaction = startTransaction();
    }
    return currentTransaction;
  }

  private org.rocksdb.Transaction startTransaction() throws StorageException {
    throwIfClosed();
    return db.beginTransaction(txWriteOptions);
  }

  @Override
  public void flush() throws StorageException {
    final org.rocksdb.Transaction tx;
    synchronized (this) {
      if (currentTransaction == null) return;
      tx = currentTransaction;
      currentTransaction = null;
    }
    try {
      tx.commit();
      tx.close();
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public Stream<Entry> entries() {
    throwIfClosed();
    final RocksIterator rocksIt = db.newIterator();
    rocksIt.seekToFirst();
    return new RocksDbEntryIterator(rocksIt).toStream();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      txOptions.close();
      options.close();
      db.close();
    }
  }

  private void throwIfClosed() {
    if (closed.get()) {
      LOG.error("Attempting to use a closed RocksDbKeyValueStorage");
      throw new IllegalStateException("Storage has been closed");
    }
  }

  /**
   * Iterates over rocksDB key-value entries. Reads from a db snapshot implicitly taken when the
   * RocksIterator passed to the constructor was created.
   *
   * <p>Implements {@link AutoCloseable} and can be used with try-with-resources construct. When
   * transformed to a stream (see {@link #toStream}), iterator is automatically closed when the
   * stream is closed.
   */
  private static class RocksDbEntryIterator implements Iterator<Entry>, AutoCloseable {
    private final RocksIterator rocksIt;
    private volatile boolean closed = false;

    RocksDbEntryIterator(final RocksIterator rocksIt) {
      this.rocksIt = rocksIt;
    }

    @Override
    public boolean hasNext() {
      return rocksIt.isValid();
    }

    @Override
    public Entry next() {
      if (closed) {
        throw new IllegalStateException("Attempt to read from a closed RocksDbEntryIterator.");
      }
      try {
        rocksIt.status();
      } catch (final RocksDBException e) {
        LOG.error("RocksDbEntryIterator encountered a problem while iterating.", e);
      }
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final Entry entry =
          Entry.create(BytesValue.wrap(rocksIt.key()), BytesValue.wrap(rocksIt.value()));
      rocksIt.next();
      return entry;
    }

    public Stream<Entry> toStream() {
      final Spliterator<Entry> split =
          Spliterators.spliteratorUnknownSize(
              this, Spliterator.IMMUTABLE | Spliterator.DISTINCT | Spliterator.NONNULL);

      return StreamSupport.stream(split, false).onClose(this::close);
    }

    @Override
    public void close() {
      rocksIt.close();
      closed = true;
    }
  }
}
