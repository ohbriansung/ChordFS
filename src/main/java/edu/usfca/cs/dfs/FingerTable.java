package edu.usfca.cs.dfs;


class FingerTable {
    private final int capacity;
    private final Node[] finger;
    private final int id;

    /**
     * Initialize the first node in the ring.
     *
     * @param m - the capacity of the ring = 2^m
     */
    FingerTable(int m, Node self) {
        this.capacity = (0b1 << m);
        this.finger = new Node[m];
        this.id = generateId(m);

        self.setId(generateId(m));
        self.setPredecessor(self);
        self.setSuccessor(self);

        for (int i = 0; i < m; i++) {
            this.finger[i] = self;
        }
    }

    Node getNode(int i) {
        return this.finger[i];
    }

    private int generateId(int m) {
        return Math.abs(Long.hashCode(System.currentTimeMillis()) % (0b1 << m));
    }
}
