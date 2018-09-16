package edu.usfca.cs.dfs;

import java.util.Map;

class StorageNode {
    private String predecessor;
    private FingerTable finger;

    StorageNode(int m) {
        this.finger = new FingerTable(m);
        this.predecessor = "";
    }
}
