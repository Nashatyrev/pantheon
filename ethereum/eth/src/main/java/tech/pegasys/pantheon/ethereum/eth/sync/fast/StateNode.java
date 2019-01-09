package tech.pegasys.pantheon.ethereum.eth.sync.fast;

import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.List;

/**
 * Represents a single state trie node, which may be one of STATE, STORAGE or CODE node
 */
public class StateNode {
  public enum NodeType {
    STATE,
    STORAGE,
    CODE
  }

  private final NodeType type;
  private final Hash nodeHash;
  private final BytesValue nodePath;
  private BytesValue nodeRlp = null;

  public StateNode(NodeType type, Hash nodeHash, BytesValue nodePath) {
    this.type = type;
    this.nodeHash = nodeHash;
    this.nodePath = nodePath;
  }

  public List<StateNode> createChildRequests() {
    return null;
  }

  public void setNodeRlp(BytesValue nodeRlp) {
    if (this.nodeRlp != null) throw new RuntimeException("Node RLP already set");
    this.nodeRlp = nodeRlp;
  }

  public boolean hasRLP() {
    return nodeRlp != null;
  }

  public Hash getNodeHash() {
    return nodeHash;
  }

  public BytesValue getNodeRlp() {
    return nodeRlp;
  }

  public int estimateSize() {
    return 4 * 8 + 32 + nodePath.size() + (nodeRlp == null ? 0 : nodeRlp.size());
  }
}
