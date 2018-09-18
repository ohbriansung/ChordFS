package edu.usfca.cs.dfs;

import java.util.ArrayList;
import java.util.List;

class FingerTable {
    private final int capacity;
    private final Node[] finger;

    /**
     * Initialize the first node in the ring.
     *
     * @param m - the capacity of the ring = 2^m
     */
    FingerTable(int m, Node self) {
        this.capacity = (0b1 << m);
        this.finger = new Node[m];

        for (int i = 0; i < m; i++) {
            this.finger[i] = self;
        }
    }

    Node getNode(int i) {
        return this.finger[i];
    }

    public String toString() {
        List<Integer> list = new ArrayList<>();

        for (Node node : this.finger) {
            list.add(node.getId());
        }

        return list.toString();
    }
}
