package org.apache.jena.dboe.trans.bplustree;

import java.util.ArrayList;
import java.util.List;

public class CardinalityNode {

    Integer sum = 0;

    List<CardinalityNode> children = new ArrayList<>();

    public CardinalityNode() {
    }

    public CardinalityNode(Integer sum) {
        this.sum = sum;
    }

    public CardinalityNode addChild(CardinalityNode child) {
        this.sum += child.sum;
        this.children.add(child);
        return this;
    }

}
