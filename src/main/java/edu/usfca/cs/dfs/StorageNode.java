package edu.usfca.cs.dfs;

import java.net.InetSocketAddress;

class StorageNode extends Asker {
    private Node self;
    private int m;
    private int capacity;
    private Between between;
    private FingerTable finger;

    /**
     * General constructor for storage node, initialize data structures and self node.
     * @param host
     * @param port
     */
    StorageNode(String host, int port) {
        super();
        this.self = new Node(host, port);
    }

    /**
     * Initialize the m bits identifier ring.
     * The predecessor will be self when it is the first node in the ring.
     *
     * @param host
     * @param port
     * @param m
     */
    StorageNode(String host, int port, int m) {
        this(host, port);

        this.m = m;
        this.capacity = (0b1 << this.m);
        this.between = new Between(this.capacity);

        //this.self.setId(generateId(this.capacity));
        this.self.setId(DFS.ID);
        String address = this.self.getAddress();
        this.self.setPredecessor(address);
        this.self.setSuccessor(address);
        this.self.setSuccessorId(this.self.getId());

        this.finger = new FingerTable(this.m, this.self);
        System.out.println(this.finger.toString());
    }

    /**
     * Ask existingNode for finer table and predecessor.
     *
     * @param existingNode
     */
    void prepare(InetSocketAddress existingNode) {
        this.m = askM(existingNode);
        this.capacity = (0b1 << this.m);
        this.between = new Between(this.capacity);

        Node successor = askIdAndSuccessor(this.m, this.self, existingNode);

        this.finger = new FingerTable(this.m);
        join(successor);
    }

    /**
     * Randomly generate id for node by current timestamp in the range of the capacity.
     *
     * @param capacity
     * @return int
     */
    private int generateId(int capacity) {
        return Math.abs(Long.hashCode(System.currentTimeMillis()) % capacity);
    }

    /**
     * Ask current node to find id's successor.
     *
     * @param id
     * @return Node - successor
     */
    Node findSuccessor(int id) {
        Node node = findPredecessor(id);

        if (node.getSuccessorId() == this.self.getId()) {
            // if successor of predecessor is self, no need to send message to retrieve detail of successor
            return this.self;
        }

        String address[] = node.getSuccessor().split(":");
        return askNodeDetail(address[0], Integer.parseInt(address[1]));
    }

    /**
     * Ask current node to find id's predecessor.
     *
     * @param id
     * @return Node - predecessor
     */
    Node findPredecessor(int id) {
        Node node = this.self;

        while (!(this.between.includesRight(id, node.getId(), node.getSuccessorId()))) {
            node = askClosestPrecedingFinger(id, node.getHost(), node.getPort());
        }

        return node;
    }

    /**
     * Ask current node to find id's closest preceding finger
     *
     * @param id
     * @return Node - closest preceding finger
     */
    Node closestPrecedingFinger(int id) {
        for (int i = this.m - 1; i >= 0; i--) {  // m down to 1 in the paper
            Node node = this.finger.getFinger(i);

            if (this.between.in(node.getId(), this.self.getId(), id)) {
                return node;
            }
        }

        return this.self;
    }

    /**
     * Current node join to the network.
     * Successor is an arbitrary node already in the network and is successor of current node.
     *
     * @param successor
     */
    private void join(Node successor) {
        initFingerTable(successor);
        updateOthers();
    }

    /**
     * Initialize finger table of current node.
     * Successor is an arbitrary node already in the network and is successor of current node.
     *
     * @param successor
     */
    private void initFingerTable(Node successor) {
        this.finger.setFinger(0, successor);
        this.self.setSuccessor(successor.getAddress());
        this.self.setSuccessorId(successor.getId());
        this.self.setPredecessor(successor.getPredecessor());
        successor.setPredecessor(this.self.getAddress());

        // tell successor to update the predecessor to current node
        InetSocketAddress address = new InetSocketAddress(successor.getHost(), successor.getPort());
        updatePredecessor(address, this.self);

        for (int i = 0; i < this.m - 1; i++) {
            // finger􏰩[i + 1].􏰠􏰪􏰁start
            int fingerStart = (this.self.getId() + (0b1 << (i + 1))) % this.capacity;

            if (this.between.includesLeft(fingerStart, this.self.getId(), this.finger.getFinger(i).getId())) {
                this.finger.setFinger(i + 1, this.finger.getFinger(i));
            }
            else {
                this.finger.setFinger(i + 1, askSuccessor(fingerStart, address));
            }
        }

        System.out.println(this.finger.toString());
    }

    void updateOthers() {
        for (int i = 0; i < this.m; i++) {
            int ithId = this.self.getId() - (0b1 << i);
            while (ithId < 0) {
                ithId += this.capacity;
            }

            Node p = findPredecessor(ithId);
            if (p.getId() != self.getId()) {
                InetSocketAddress address = new InetSocketAddress(p.getHost(), p.getPort());
                askToUpdateFingerTable(address, this.self, i);
            }
        }
    }

    void updateFingerTable(Node s, int i) {
        System.out.println("updateFingerTable");
        if (this.between.includesLeft(s.getId(), this.self.getId(), this.finger.getFinger(i).getId())) {
            this.finger.setFinger(i, s);
        }

        //TODO: seems to be bug in the paper?
        String[] p = self.getPredecessor().split(":");
        if (p[0].equals(this.self.getHost()) && Integer.parseInt(p[1]) == this.self.getPort()) {
            // the predecessor is current node
            updateFingerTable(s, i);
        }
        else {
            InetSocketAddress address = new InetSocketAddress(p[0], Integer.parseInt(p[1]));
            askToUpdateFingerTable(address, this.self, i);
        }

        System.out.println("Finger table updated: " + this.finger.toString());
    }

    void selfUpdate(Node n) {
        for (int i = 0; i < this.m; i++) {
            // finger􏰩[i + 1].􏰠􏰪􏰁start
            int fingerStart = (this.self.getId() + (0b1 << (i + 1))) % this.capacity;

            if (this.between.includesRight(n.getId(), fingerStart, this.finger.getFinger(i).getId())) {
                this.finger.setFinger(i, n);

                if (i == 0) {
                    this.self.setSuccessor(n.getAddress());
                    this.self.setSuccessorId(n.getId());
                }
            }
        }

        System.out.println("Finger table updated (self): " + this.finger.toString());
    }

    int getM() {
        return this.m;
    }

    Node getSelf() {
        return this.self;
    }
}
