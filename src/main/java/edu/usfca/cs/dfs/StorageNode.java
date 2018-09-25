package edu.usfca.cs.dfs;

import java.net.InetSocketAddress;
import java.util.Random;

class StorageNode extends Asker {
    private Node self;
    private int m;
    private int capacity;
    private Between between;
    private FingerTable finger;

    /**
     * General constructor for storage node, initialize data structures and self node.
     *
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
        this.finger = new FingerTable(this.m, this.self);

        selfInit();
    }

    private void selfInit() {
        this.self.setId(generateId(this.capacity));
        this.self.setSuccessor(this.self.getAddress());
        this.self.setSuccessorId(this.self.getId());
        this.self.setPredecessor(this.self.getAddress());
        this.self.setPredecessorId(this.self.getId());
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
        this.finger = new FingerTable(this.m);

        join(existingNode);
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
    private Node findPredecessor(int id) {
        Node node = this.self;

        while (!(this.between.includesRight(id, node.getId(), node.getSuccessorId()))) {
            if (node.getId() != this.self.getId()) {
                node = askClosestPrecedingFinger(id, node.getHost(), node.getPort());
            }
            else {
                node = closestPrecedingFinger(id);
            }
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
        // m down to 1 in the paper
        for (int i = this.m - 1; i >= 0; i--) {
            Node node = this.finger.getFinger(i);

            if (node != null && this.between.in(node.getId(), this.self.getId(), id)) {
                return node;
            }
        }

        return this.self;
    }

    /**
     * Stabilized join for concurrent operations.
     *
     * @param existingNode
     */
    private void join(InetSocketAddress existingNode) {
        // predecessor is already null
        // initial self id and get successor from existing node
        Node successor = askIdAndSuccessor(this.capacity, this.self, existingNode);
        updateSuccessor(successor);
    }

    /**
     * Periodically verify self's immediate successor,
     * and tell the successor about self.
     */
    void stabilize() {
        if (this.self.getSuccessorId() == this.self.getId() && this.self.getPredecessorId() == this.self.getId()) {
            // no need to update anything if there is only one node in ring
            return;
        }

        // x = successor.predecessor
        String[] address = this.self.getSuccessor().split(":");
        InetSocketAddress successor = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
        Node x = askPredecessor(successor);

        if (this.between.in(x.getId(), this.self.getId(), this.self.getSuccessorId())) {
            updateSuccessor(x);

            address = this.self.getSuccessor().split(":");
            successor = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
        }

        super.notify(successor, this.self);
    }

    /**
     * p thinks it might be the predecessor.
     *
     * @param p
     */
    void notify(Node p) {
        if (this.self.getPredecessor() == null ||
                this.between.in(p.getId(), this.self.getPredecessorId(), this.self.getId())) {
            this.self.setPredecessor(p.getAddress());
            this.self.setPredecessorId(p.getId());

            System.out.println("Updated predecessor to " + p.getId());
        }
    }

    /**
     * Periodically refresh finger table entries.
     */
    void fixFingers(){
        Random random = new Random();
        int i = 1 + random.nextInt(this.m - 1);
        Node successor = findSuccessor(start(i));
        this.finger.setFinger(i, successor);

        System.out.println("Fixed finger " + i + " to " + successor.getId());
        System.out.println(this.finger.toString());  // print current finger table after fixing fingers
    }

    private void updateSuccessor(Node successor) {
        this.finger.setFinger(0, successor);
        this.self.setSuccessor(successor.getAddress());
        this.self.setSuccessorId(successor.getId());

        System.out.println("Updated successor to " + successor.getId());
    }

    int getM() {
        return this.m;
    }

    Node getSelf() {
        return this.self;
    }

    Node predecessor() {
        if (this.self.getPredecessorId() == this.self.getId()) {
            return this.self;
        }
        else {
            String[] address = this.self.getPredecessor().split(":");
            return askNodeDetail(address[0], Integer.parseInt(address[1]));
        }
    }

    /**
     * Start point of ith finger.
     *
     * @param i
     * @return int
     */
    private int start(int i) {
        return (this.self.getId() + (0b1 << i)) % this.capacity;
    }
}
