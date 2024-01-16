package org.apache.jena.dboe.trans.bplustree;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

    public int getRandomWeightedIndex() {
        int random = new Random().nextInt(this.sum); // TODO static class that does random
        if (children.isEmpty()) { // leaf
            return random;
        }

        int currentSum = 0;
        int i = 0;
        while (currentSum <= random && i < this.children.size()) {
            currentSum += this.children.get(i).sum;
            ++i;
        }
        return i - 1;
    }

}
