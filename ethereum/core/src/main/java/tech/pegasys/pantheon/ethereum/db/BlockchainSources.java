package tech.pegasys.pantheon.ethereum.db;

import tech.pegasys.pantheon.ethereum.chain.TransactionLocation;
import tech.pegasys.pantheon.ethereum.core.*;
import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.Bytes32Backed;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.source.CodecSource;
import tech.pegasys.pantheon.util.source.DataSource;
import tech.pegasys.pantheon.util.source.SingleValueSource;
import tech.pegasys.pantheon.util.uint.UInt256;
import tech.pegasys.pantheon.util.uint.UInt256Bytes;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static tech.pegasys.pantheon.util.bytes.BytesValues.concatenate;

/**
 * Created by Anton Nashatyrev on 19.11.2018.
 */
public class BlockchainSources {
  private static final BytesValue CHAIN_HEAD_KEY =
      BytesValue.wrap("chainHeadHash".getBytes(StandardCharsets.UTF_8));
  private static final BytesValue FORK_HEADS_KEY =
      BytesValue.wrap("forkHeads".getBytes(StandardCharsets.UTF_8));

  private static final BytesValue CONSTANTS_PREFIX = BytesValue.of(1);
  private static final BytesValue BLOCK_HEADER_PREFIX = BytesValue.of(2);
  private static final BytesValue BLOCK_BODY_PREFIX = BytesValue.of(3);
  private static final BytesValue TRANSACTION_RECEIPTS_PREFIX = BytesValue.of(4);
  private static final BytesValue BLOCK_HASH_PREFIX = BytesValue.of(5);
  private static final BytesValue TOTAL_DIFFICULTY_PREFIX = BytesValue.of(6);
  private static final BytesValue TRANSACTION_LOCATION_PREFIX = BytesValue.of(7);

  private final SingleValueSource<Hash> chainHeadValue;
  private final SingleValueSource<List<Hash>> forkHeadsValue;
  private final DataSource<Hash, BlockHeader> blockHeaderSource;
  private final DataSource<Hash, BlockBody> blockBodySource;
  private final DataSource<Hash, List<TransactionReceipt>> txReceiptsSource;
  private final DataSource<Long, Hash> blockHashSource;
  private final DataSource<Hash, UInt256> totalDifficultySource;
  private final DataSource<Hash, TransactionLocation> txLocationSource;

  public BlockchainSources(@Nonnull final DataSource<BytesValue, BytesValue> storage,
                           @Nonnull final BlockHashFunction blockHashFunction) {

    final DataSource<BytesValue, BytesValue> constantsSource = new CodecSource.KeyOnly<>(storage,
        k -> concatenate(CONSTANTS_PREFIX, k));
    this.chainHeadValue = SingleValueSource.fromDataSource(constantsSource, CHAIN_HEAD_KEY,
        h -> h, BlockchainSources::bytesToHash);
    this.forkHeadsValue = SingleValueSource.fromDataSource(constantsSource, FORK_HEADS_KEY,
        forkHeadHashes -> RLP.encode(o -> o.writeList(forkHeadHashes, (val, out) -> out.writeBytesValue(val))),
        bytes -> RLP.input(bytes).readList(in -> bytesToHash(in.readBytes32())));
    this.blockHeaderSource = new CodecSource<>(storage,
        h -> concatenate(BLOCK_HEADER_PREFIX, h),
        blockHeader -> RLP.encode(blockHeader::writeTo),
        headerRlp -> BlockHeader.readFrom(RLP.input(headerRlp), blockHashFunction));
    this.blockBodySource = new CodecSource<>(storage,
        h -> concatenate(BLOCK_BODY_PREFIX, h),
        blockBody -> RLP.encode(blockBody::writeTo),
        bodyRlp -> BlockBody.readFrom(RLP.input(bodyRlp), blockHashFunction));
    this.txReceiptsSource = new CodecSource<>(storage,
        h -> concatenate(TRANSACTION_RECEIPTS_PREFIX, h),
        receipts -> RLP.encode(o -> o.writeList(receipts, TransactionReceipt::writeTo)),
        rlp -> RLP.input(rlp).readList(TransactionReceipt::readFrom));
    this.blockHashSource = new CodecSource<>(storage,
        n -> concatenate(BLOCK_HASH_PREFIX, UInt256Bytes.of(n)),
        v -> v,
        BlockchainSources::bytesToHash);
    this.totalDifficultySource = new CodecSource<>(storage,
        h -> concatenate(TOTAL_DIFFICULTY_PREFIX, h),
        Bytes32Backed::getBytes,
        b -> UInt256.wrap(Bytes32.wrap(b, 0)));
    this.txLocationSource = new CodecSource<>(storage,
        h -> concatenate(TRANSACTION_LOCATION_PREFIX, h),
        transactionLocation -> RLP.encode(transactionLocation::writeTo),
        bytesValue -> TransactionLocation.readFrom(RLP.input(bytesValue)));
  }

  public SingleValueSource<Hash> getChainHeadValue() {
    return chainHeadValue;
  }

  public SingleValueSource<List<Hash>> getForkHeadsValue() {
    return forkHeadsValue;
  }

  public DataSource<Hash, BlockHeader> getBlockHeaderSource() {
    return blockHeaderSource;
  }

  public DataSource<Hash, BlockBody> getBlockBodySource() {
    return blockBodySource;
  }

  public DataSource<Hash, List<TransactionReceipt>> getTxReceiptsSource() {
    return txReceiptsSource;
  }

  public DataSource<Long, Hash> getBlockHashSource() {
    return blockHashSource;
  }

  public DataSource<Hash, UInt256> getTotalDifficultySource() {
    return totalDifficultySource;
  }

  public DataSource<Hash, TransactionLocation> getTxLocationSource() {
    return txLocationSource;
  }

  private static Hash bytesToHash(final BytesValue bytesValue) {
    return Hash.wrap(Bytes32.wrap(bytesValue, 0));
  }
}
