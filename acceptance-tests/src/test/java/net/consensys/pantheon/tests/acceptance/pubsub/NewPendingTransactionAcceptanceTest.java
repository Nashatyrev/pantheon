package net.consensys.pantheon.tests.acceptance.pubsub;

import static net.consensys.pantheon.tests.acceptance.dsl.node.PantheonNodeConfig.pantheonMinerNode;
import static net.consensys.pantheon.tests.acceptance.dsl.node.PantheonNodeConfig.pantheonNode;
import static org.assertj.core.api.Assertions.assertThat;

import net.consensys.pantheon.ethereum.core.Hash;
import net.consensys.pantheon.tests.acceptance.dsl.AcceptanceTestBase;
import net.consensys.pantheon.tests.acceptance.dsl.account.Account;
import net.consensys.pantheon.tests.acceptance.dsl.node.PantheonNode;
import net.consensys.pantheon.tests.acceptance.dsl.pubsub.Subscription;
import net.consensys.pantheon.tests.acceptance.dsl.pubsub.WebSocket;

import java.math.BigInteger;

import io.vertx.core.Vertx;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NewPendingTransactionAcceptanceTest extends AcceptanceTestBase {

  private Vertx vertx;
  private Account accountOne;
  private WebSocket minerWebSocket;
  private WebSocket archiveWebSocket;
  private PantheonNode minerNode;
  private PantheonNode archiveNode;

  @Before
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
    minerNode = cluster.create(pantheonMinerNode("miner-node1"));
    archiveNode = cluster.create(pantheonNode("full-node1"));
    cluster.start(minerNode, archiveNode);
    accountOne = accounts.createAccount("account-one");
    minerWebSocket = new WebSocket(vertx, minerNode);
    archiveWebSocket = new WebSocket(vertx, archiveNode);
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  @Test
  public void transactionRemovedByChainReorganisationMustPublishEvent() throws Exception {

    // Create the light fork
    final Subscription lightForkSubscription = minerWebSocket.subscribe();

    final Hash lightForkEvent = accounts.transfer(accountOne, 5, minerNode);
    cluster.awaitPropagation(accountOne, 5);

    minerWebSocket.verifyTotalEventsReceived(1);
    lightForkSubscription.verifyEventReceived(lightForkEvent);

    final BigInteger lighterForkBlockNumber = minerNode.eth().blockNumber();

    cluster.stop();

    // Create the heavy fork
    final PantheonNode minerNodeTwo = cluster.create(pantheonMinerNode("miner-node2"));
    cluster.start(minerNodeTwo);

    final WebSocket heavyForkWebSocket = new WebSocket(vertx, minerNodeTwo);
    final Subscription heavyForkSubscription = heavyForkWebSocket.subscribe();

    final Account accountTwo = accounts.createAccount("account-two");

    // Keep both forks transactions valid by using a different benefactor
    final Account heavyForkBenefactor = accounts.getSecondaryBenefactor();

    final Hash heavyForkEventOne =
        accounts.transfer(heavyForkBenefactor, accountTwo, 1, minerNodeTwo);
    cluster.awaitPropagation(accountTwo, 1);
    final Hash heavyForkEventTwo =
        accounts.transfer(heavyForkBenefactor, accountTwo, 2, minerNodeTwo);
    cluster.awaitPropagation(accountTwo, 1 + 2);
    final Hash heavyForkEventThree =
        accounts.transfer(heavyForkBenefactor, accountTwo, 3, minerNodeTwo);
    cluster.awaitPropagation(accountTwo, 1 + 2 + 3);

    heavyForkWebSocket.verifyTotalEventsReceived(3);
    heavyForkSubscription.verifyEventReceived(heavyForkEventOne);
    heavyForkSubscription.verifyEventReceived(heavyForkEventTwo);
    heavyForkSubscription.verifyEventReceived(heavyForkEventThree);

    final BigInteger heavierForkBlockNumber = minerNodeTwo.eth().blockNumber();

    cluster.stop();

    // Restart the two nodes on the light fork with the additional node from the heavy fork
    cluster.start(minerNode, archiveNode, minerNodeTwo);

    final WebSocket minerMergedForksWebSocket = new WebSocket(vertx, minerNode);
    final WebSocket minerTwoMergedForksWebSocket = new WebSocket(vertx, minerNodeTwo);
    final WebSocket archiveMergedForksWebSocket = new WebSocket(vertx, archiveNode);
    final Subscription minerMergedForksSubscription = minerMergedForksWebSocket.subscribe();
    final Subscription minerTwoMergedForksSubscription = minerTwoMergedForksWebSocket.subscribe();
    final Subscription archiveMergedForksSubscription = archiveMergedForksWebSocket.subscribe();

    // Check that all node have loaded their respective forks, i.e. not begin new chains
    assertThat(minerNode.eth().blockNumber()).isGreaterThanOrEqualTo(lighterForkBlockNumber);
    assertThat(archiveNode.eth().blockNumber()).isGreaterThanOrEqualTo(lighterForkBlockNumber);
    assertThat(minerNodeTwo.eth().blockNumber()).isGreaterThanOrEqualTo(heavierForkBlockNumber);

    // This publish give time needed for heavy fork to be chosen
    final Hash mergedForksEventOne =
        accounts.transfer(accounts.getSecondaryBenefactor(), accountTwo, 3, minerNodeTwo);
    cluster.awaitPropagation(accountTwo, 9);

    minerMergedForksWebSocket.verifyTotalEventsReceived(1);
    minerMergedForksSubscription.verifyEventReceived(lightForkEvent);
    archiveMergedForksWebSocket.verifyTotalEventsReceived(1);
    archiveMergedForksSubscription.verifyEventReceived(lightForkEvent);
    minerTwoMergedForksWebSocket.verifyTotalEventsReceived(2);
    minerTwoMergedForksSubscription.verifyEventReceived(lightForkEvent);
    minerTwoMergedForksSubscription.verifyEventReceived(mergedForksEventOne);

    // Check that account two (funded in heavier chain) can be mined on miner one (from lighter
    // chain)
    final Hash mergedForksEventTwo = accounts.transfer(accountTwo, 3, minerNode);
    cluster.awaitPropagation(accountTwo, 9 + 3);

    // Check that account one (funded in lighter chain) can be mined on miner two (from heavier
    // chain)
    final Hash mergedForksEventThree = accounts.transfer(accountOne, 2, minerNodeTwo);
    cluster.awaitPropagation(accountOne, 5 + 2);

    minerMergedForksWebSocket.verifyTotalEventsReceived(1 + 1 + 1);
    minerMergedForksSubscription.verifyEventReceived(mergedForksEventTwo);
    minerMergedForksSubscription.verifyEventReceived(mergedForksEventThree);
    archiveMergedForksWebSocket.verifyTotalEventsReceived(1 + 1 + 1);
    archiveMergedForksSubscription.verifyEventReceived(mergedForksEventTwo);
    archiveMergedForksSubscription.verifyEventReceived(mergedForksEventThree);
    minerTwoMergedForksWebSocket.verifyTotalEventsReceived(2 + 1 + 1);
    minerTwoMergedForksSubscription.verifyEventReceived(mergedForksEventTwo);
    minerTwoMergedForksSubscription.verifyEventReceived(mergedForksEventThree);
  }

  @Test
  public void subscriptionToMinerNodeMustReceivePublishEvent() {
    final Subscription minerSubscription = minerWebSocket.subscribe();

    final Hash event = accounts.transfer(accountOne, 4, minerNode);
    cluster.awaitPropagation(accountOne, 4);

    minerWebSocket.verifyTotalEventsReceived(1);
    minerSubscription.verifyEventReceived(event);

    minerWebSocket.unsubscribe(minerSubscription);
  }

  @Test
  public void subscriptionToArchiveNodeMustReceivePublishEvent() {
    final Subscription archiveSubscription = archiveWebSocket.subscribe();

    final Hash event = accounts.transfer(accountOne, 23, minerNode);
    cluster.awaitPropagation(accountOne, 23);

    archiveWebSocket.verifyTotalEventsReceived(1);
    archiveSubscription.verifyEventReceived(event);

    archiveWebSocket.unsubscribe(archiveSubscription);
  }

  @Test
  public void everySubscriptionMustReceivePublishEvent() {
    final Subscription minerSubscriptionOne = minerWebSocket.subscribe();
    final Subscription minerSubscriptionTwo = minerWebSocket.subscribe();
    final Subscription archiveSubscriptionOne = archiveWebSocket.subscribe();
    final Subscription archiveSubscriptionTwo = archiveWebSocket.subscribe();
    final Subscription archiveSubscriptionThree = archiveWebSocket.subscribe();

    final Hash event = accounts.transfer(accountOne, 30, minerNode);
    cluster.awaitPropagation(accountOne, 30);

    minerWebSocket.verifyTotalEventsReceived(2);
    minerSubscriptionOne.verifyEventReceived(event);
    minerSubscriptionTwo.verifyEventReceived(event);

    archiveWebSocket.verifyTotalEventsReceived(3);
    archiveSubscriptionOne.verifyEventReceived(event);
    archiveSubscriptionTwo.verifyEventReceived(event);
    archiveSubscriptionThree.verifyEventReceived(event);

    minerWebSocket.unsubscribe(minerSubscriptionOne);
    minerWebSocket.unsubscribe(minerSubscriptionTwo);
    archiveWebSocket.unsubscribe(archiveSubscriptionOne);
    archiveWebSocket.unsubscribe(archiveSubscriptionTwo);
    archiveWebSocket.unsubscribe(archiveSubscriptionThree);
  }

  @Test
  public void subscriptionToMinerNodeMustReceiveEveryPublishEvent() {
    final Subscription minerSubscription = minerWebSocket.subscribe();

    final Hash eventOne = accounts.transfer(accountOne, 1, minerNode);
    cluster.awaitPropagation(accountOne, 1);

    minerWebSocket.verifyTotalEventsReceived(1);
    minerSubscription.verifyEventReceived(eventOne);

    final Hash eventTwo = accounts.transfer(accountOne, 4, minerNode);
    final Hash eventThree = accounts.transfer(accountOne, 5, minerNode);
    cluster.awaitPropagation(accountOne, 1 + 4 + 5);

    minerWebSocket.verifyTotalEventsReceived(3);
    minerSubscription.verifyEventReceived(eventTwo);
    minerSubscription.verifyEventReceived(eventThree);

    minerWebSocket.unsubscribe(minerSubscription);
  }

  @Test
  public void subscriptionToArchiveNodeMustReceiveEveryPublishEvent() {
    final Subscription archiveSubscription = archiveWebSocket.subscribe();

    final Hash eventOne = accounts.transfer(accountOne, 2, minerNode);
    final Hash eventTwo = accounts.transfer(accountOne, 5, minerNode);
    cluster.awaitPropagation(accountOne, 2 + 5);

    archiveWebSocket.verifyTotalEventsReceived(2);
    archiveSubscription.verifyEventReceived(eventOne);
    archiveSubscription.verifyEventReceived(eventTwo);

    final Hash eventThree = accounts.transfer(accountOne, 8, minerNode);
    cluster.awaitPropagation(accountOne, 2 + 5 + 8);

    archiveWebSocket.verifyTotalEventsReceived(3);
    archiveSubscription.verifyEventReceived(eventThree);

    archiveWebSocket.unsubscribe(archiveSubscription);
  }

  @Test
  public void everySubscriptionMustReceiveEveryPublishEvent() {
    final Subscription minerSubscriptionOne = minerWebSocket.subscribe();
    final Subscription minerSubscriptionTwo = minerWebSocket.subscribe();
    final Subscription archiveSubscriptionOne = archiveWebSocket.subscribe();
    final Subscription archiveSubscriptionTwo = archiveWebSocket.subscribe();
    final Subscription archiveSubscriptionThree = archiveWebSocket.subscribe();

    final Hash eventOne = accounts.transfer(accountOne, 10, minerNode);
    final Hash eventTwo = accounts.transfer(accountOne, 5, minerNode);
    cluster.awaitPropagation(accountOne, 10 + 5);

    minerWebSocket.verifyTotalEventsReceived(4);
    minerSubscriptionOne.verifyEventReceived(eventOne);
    minerSubscriptionOne.verifyEventReceived(eventTwo);
    minerSubscriptionTwo.verifyEventReceived(eventOne);
    minerSubscriptionTwo.verifyEventReceived(eventTwo);

    archiveWebSocket.verifyTotalEventsReceived(6);
    archiveSubscriptionOne.verifyEventReceived(eventOne);
    archiveSubscriptionOne.verifyEventReceived(eventTwo);
    archiveSubscriptionTwo.verifyEventReceived(eventOne);
    archiveSubscriptionTwo.verifyEventReceived(eventTwo);
    archiveSubscriptionThree.verifyEventReceived(eventOne);
    archiveSubscriptionThree.verifyEventReceived(eventTwo);

    minerWebSocket.unsubscribe(minerSubscriptionOne);
    minerWebSocket.unsubscribe(minerSubscriptionTwo);
    archiveWebSocket.unsubscribe(archiveSubscriptionOne);
    archiveWebSocket.unsubscribe(archiveSubscriptionTwo);
    archiveWebSocket.unsubscribe(archiveSubscriptionThree);
  }
}