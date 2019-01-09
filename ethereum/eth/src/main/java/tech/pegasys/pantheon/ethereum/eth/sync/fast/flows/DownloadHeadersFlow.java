package tech.pegasys.pantheon.ethereum.eth.sync.fast.flows;

import com.google.common.base.Preconditions;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.SyncContext;
import tech.pegasys.pantheon.ethereum.mainnet.BlockHeaderValidator;
import tech.pegasys.pantheon.ethereum.mainnet.HeaderValidationMode;
import tech.pegasys.pantheon.util.Pair;
import tech.pegasys.pantheon.ethereum.eth.sync.fast.rx.RxUtils;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.GetHeadersFromPeerByHashTask;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.exceptions.InvalidBlockException;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static tech.pegasys.pantheon.ethereum.mainnet.HeaderValidationMode.FULL;
import static tech.pegasys.pantheon.ethereum.mainnet.HeaderValidationMode.LIGHT;

/**
 * Taking checkpoint headers as input downloads and optionally validates
 * the headers chain in parallel from all available peers.
 */
public class DownloadHeadersFlow<C> implements FlowableTransformer<BlockHeader, BlockHeader> {
  private final SecureRandom rnd = new SecureRandom();

  private final SyncContext<C> syncContext;

  private int maxDownloadConcurrency = 50;
  private int downloadPrefetch = 10;
  private boolean validateHeaders = true;
  private int validateEveryNthPow = 1;

  public DownloadHeadersFlow(final SyncContext<C> syncContext) {
    this.syncContext = syncContext;
  }

  /**
   * Modifies default headers downloading options
   * @param maxConcurrency how many concurrent download requests can be issued
   * @param prefetch hints about the number of downloaded cached headers
   *                 for assembling them back in the right order
   */
  public DownloadHeadersFlow<C> withDownloaderSettings(final int maxConcurrency, final int prefetch) {
    this.maxDownloadConcurrency = maxConcurrency;
    this.downloadPrefetch = prefetch;
    return this;
  }

  /**
   * Sets headers validation options
   * @param validateHeaders if headers need to be validated
   * @param validateEveryNthPow performs FULL PoW validation only for each Nth header randomly
   *                            other headers are validated with LIGHT mode
   *                            1 means validating every header
   */
  public DownloadHeadersFlow<C> withValidation(final boolean validateHeaders,
                                               final int validateEveryNthPow) {
    Preconditions.checkArgument(validateEveryNthPow > 0);
    this.validateHeaders = validateHeaders;
    this.validateEveryNthPow = validateEveryNthPow;
    return this;
  }

  @Override
  public Publisher<BlockHeader> apply(final Flowable<BlockHeader> upstream) {
    return upstream
        // split chunks to maxDownloadConcurrency parallel downloads and then assemble
        // downloaded chunks back preserving their order
        .concatMapEager(startHeader ->
                RxUtils.fromFuture(requestHeaders(startHeader)),
            maxDownloadConcurrency, downloadPrefetch)
        // flatten header chunks
        .flatMap(Flowable::fromIterable)
        // split to (parent, child) pairs :
        // (h1, h2, h3) => ((null, h1), (h1, h2), (h2, h3))
        .compose(RxUtils.pairsTransform(true, false))
        // validate child-parent
        .doOnNext(parentChild -> validateHeader(parentChild.second(), parentChild.first()))
        // ((null, h1), (h1, h2), (h2, h3)) => (h1, h2, h3)
        .map(Pair::second);
  }

  protected void validateHeader(final BlockHeader header, final BlockHeader parent) throws InvalidBlockException {
    if (!validateHeaders) return;

    final boolean fullValidation = rnd.nextInt(validateEveryNthPow) == 0;
    final BlockHeaderValidator<C> blockHeaderValidator = syncContext.getProtocolSchedule()
          .getByBlockNumber(header.getNumber()).getBlockHeaderValidator();
    if (blockHeaderValidator.validateHeader(
        header, parent, syncContext.getProtocolContext(), fullValidation ? FULL : LIGHT)) {
      throw new InvalidBlockException("Invalid header.", header.getNumber(), header.getHash());
    }
  }

  protected CompletableFuture<List<BlockHeader>> requestHeaders(final BlockHeader start) {
    return GetHeadersFromPeerByHashTask.startingAtHash(
        syncContext.getProtocolSchedule(),
        syncContext.getEthContext(),
        start.getHash(),
        start.getNumber(),
        syncContext.getConfig().downloaderHeaderRequestSize(),0)

        .run().thenApply(res -> res.getResult());
  }
}
