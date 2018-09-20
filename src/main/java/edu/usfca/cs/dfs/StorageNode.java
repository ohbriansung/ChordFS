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
        join(successor, this.self.getId());
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

//        if (node.getId() != node.getSuccessorId()) {
//            // id not in (node, node.successor]
//            while (!(this.between.includesRight(id, node.getId(), node.getSuccessorId()))) {
//                node = askClosestPrecedingFinger(id, node.getHost(), node.getPort());
//            }
//        }

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

    private void join(Node successor, int id) {
        initFingerTable(successor, id);
        // updateOthers();
    }

    private void initFingerTable(Node successor, int id) {
        this.finger.setFinger(0, successor);
        this.self.setPredecessor(successor.getPredecessor());
        successor.setPredecessor(this.self.getAddress());

        // update the predecessor of successor
        InetSocketAddress address = new InetSocketAddress(successor.getHost(), successor.getPort());
        updatePredecessor(address, this.self);

        for (int i = 0; i < this.m - 1; i++) {
            int startPoint = (id + (0b1 << (i + 1))) % (0b1 << this.m);

            int previousId = this.finger.getFinger(i).getId();
            if (previousId <= id) {
                previousId += (0b1 << this.m);
            }

            System.out.println("startPoint = [" + startPoint + "], previousId = [" + previousId + "]");

            // if finger[i + 1].start in [n, finger[i].node)
            if (startPoint >= id && startPoint < previousId) {
                this.finger.setFinger(i + 1, this.finger.getFinger(i));
            }
            else {
                Node node = askSuccessor(startPoint, address);
                this.finger.setFinger(i + 1, node);
            }
        }

        System.out.println(this.finger.toString());
    }

    void updateFingerTable(Node node) {
        int id = this.self.getId();
        System.out.println(id);

        int s = node.getId();
        if (s < id) {
            s += (0b1 << this.m);
        }
        System.out.println(s);

        for (int i = 0; i < this.m; i++) {
            int fingerId = this.finger.getFinger(i).getId();
            if (fingerId <= id) {
                fingerId += (0b1 << this.m);
            }
            System.out.println(fingerId);

            // if s in [n, finger[i].node)
            if (s >= id + (0b1 << i) && s < fingerId) {
                this.finger.setFinger(i, node);

//                if (i == 0) {
//                    this.self.setSuccessor(node.getAddress());
//                    this.self.setSuccessorId(node.getId());
//                }
            }
        }

        System.out.println(this.finger.toString());
    }

    int getM() {
        return this.m;
    }

    Node getSelf() {
        return this.self;
    }
}
