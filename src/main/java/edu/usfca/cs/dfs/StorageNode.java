package edu.usfca.cs.dfs;

class StorageNode {
    private final int m;
    private final Node self;
    private final FingerTable finger;

    /**
     * Initialize the m bits identifier ring.
     * The predecessor will be self when it is the first node in the ring.
     *
     * @param host
     * @param port
     * @param m
     */
    StorageNode(String host, int port, int m) {
        this.m = m;
        this.self = new Node(host, port);
        this.finger = new FingerTable(this.m, this.self);
    }

    Node findSuccessor(int id) {
        Node node = findPredecessor(id);
        return node.getSuccessor();
    }

    Node findPredecessor(int id) {
        Node node = this.self;

        return node;
    }

    Node closestPrecedingFinger(int id) {
        for (int i = this.m - 1; i >= 0; i--) {
            Node node = this.finger.getNode(i);

            if (this.self.getId() < id) {
                if (node.getId() > this.self.getId() && node.getId() < id) {
                    return node;
                }
            }
            else {  // passing zero, opposite of the condition above
                if (!(node.getId() <= this.self.getId() && node.getId() >= id)) {
                    return node;
                }
            }
        }

        return this.self;
    }
}
