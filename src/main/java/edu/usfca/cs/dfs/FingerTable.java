package edu.usfca.cs.dfs;


class FingerTable {
    private final int[] start;
    private final int[][] interval;
    private final int[] successor;
    private final int id;

    private int predecessor;

    /**
     * Default capacity of the ring is 2^4 = 16.
     */
    FingerTable() {
        this(4);
    }

    /**
     * Initialize the first node in the ring.
     *
     * @param m - the capacity of the ring = 2^m
     */
    FingerTable(int m) {
        this.start = new int[m];
        this.interval = new int[m][2];
        this.successor = new int[m];
        this.id = Long.hashCode(System.currentTimeMillis()) % m;
        this.predecessor = this.id;

        for (int i = 0; i < m; i++) {
            this.start[i] = this.id + 2 ^ i;
            this.interval[i][0] = this.start[i];

            if (i < m - 1) {
                this.interval[i][1] = this.start[i + 1];
            }
            else {
                this.interval[i][1] = this.id;
            }

            this.successor[i] = this.id;
        }

        printId();
    }

    private void printId() {
        System.out.println("Current node id = " + this.id);
    }
}
