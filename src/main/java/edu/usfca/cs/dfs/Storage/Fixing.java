package edu.usfca.cs.dfs.Storage;

import edu.usfca.cs.dfs.DFS;

public class Fixing implements Runnable {
    private final StorageNode node;

    public Fixing(StorageNode node) {
        this.node = node;
    }

    @Override
    public void run() {
        while (DFS.alive) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.node.fixFingers();
        }
    }
}
