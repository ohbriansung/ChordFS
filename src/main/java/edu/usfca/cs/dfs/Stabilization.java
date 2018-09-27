package edu.usfca.cs.dfs;

class Stabilization implements Runnable {
    private final StorageNode node;

    Stabilization(StorageNode node) {
        this.node = node;
    }

    @Override
    public void run() {
        while (DFS.alive) {
            this.node.stabilize();
            this.node.fixFingers();
            this.node.checkPredecessor();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
