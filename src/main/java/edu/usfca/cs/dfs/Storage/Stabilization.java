package edu.usfca.cs.dfs.Storage;

import edu.usfca.cs.dfs.DFS;

public class Stabilization implements Runnable {
    private final StorageNode node;

    public Stabilization(StorageNode node) {
        this.node = node;
    }

    @Override
    public void run() {
        while (DFS.alive) {
            this.node.stabilize();
            this.node.fixFingers();
            this.node.keepAlive();
            this.node.stabilizeSecSucc();
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
