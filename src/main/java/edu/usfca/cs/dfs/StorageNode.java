package edu.usfca.cs.dfs;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Reference: https://en.wikipedia.org/wiki/Chord_(peer-to-peer)
 * successor = fingers[0]
 * np = n'
 * @author Brian Sung
 */
class StorageNode extends Sender {
    private int m;
    private int next;
    private Node n;
    private Node predecessor;
    private FingerTable fingers;
    private Utility util;

    private Map<Integer, String> currentStorage;

    StorageNode(String host, int port) {
        this.n = new Node(host, port);
        this.next = 0;
    }

    StorageNode(String host, int port, int m) {
        this(host, port);
        this.m = m;
        prepare();
    }

    private void prepare() {
        this.fingers = new FingerTable(this.m);
        this.util = new Utility(0b1 << this.m);
    }

    /**
     * Ask n to find the successor of id.
     * @param id
     * @return Node
     */
    Node findSuccessor(int id) {
        Node successor = this.fingers.getFinger(0);

        try {
            if (this.util.includesRight(id, this.n.getId(), successor.getId())) {
                return successor;
            } else {
                Node n0;

                if (successor.getId() == this.n.getId()) {
                    n0 = closestPrecedingNode(id);
                } else {
                    n0 = ask(successor.getAddress(), StorageMessages.infoType.CLOSEST_PRECEDING_NODE, id);
                }

                if (n0.getId() == this.n.getId()) {
                    return findSuccessor(id);
                } else {
                    return ask(n0.getAddress(), StorageMessages.infoType.ASK_SUCCESSOR, id);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return successor;
    }

    /**
     * Search the local table for the highest predecessor of id.
     * @param id
     * @return Node
     */
    Node closestPrecedingNode(int id) {
        for (int i = this.m - 1; i >= 0; i--) {
            Node fingerI = this.fingers.getFinger(i);

            if (fingerI != null && this.util.in(fingerI.getId(), this.n.getId(), id)) {
                return fingerI;
            }
        }

        return this.n;
    }

    /**
     * Creating new Chord ring.
     * Set current node as successor.
     */
    void create() {
        this.predecessor = null;
        int id = this.util.genId();
        this.n.setId(id);
        this.fingers.setFinger(0, this.n);
    }

    /**
     * Join a Chord ring contains node np.
     * Ask np the size of the Chord ring.
     * Id of current node could be already existed in the Chord ring,
     * keep generating new id until it is unique.
     * @param np
     */
    void join(InetSocketAddress np) throws IOException {
        this.m = askM(np);
        prepare();

        this.predecessor = null;
        do {
            int id = this.util.genId();
            this.n.setId(id);
            Node successor = ask(np, StorageMessages.infoType.ASK_SUCCESSOR, this.n.getId());
            this.fingers.setFinger(0, successor);
        } while (this.n.getId() == this.fingers.getFinger(0).getId());
    }

    /**
     * Called periodically.
     * n asks the successor about its predecessor, verifies if n's immediate
     * successor is consistent, and tells the successor about n.
     */
    void stabilize() {
        Node successor = this.fingers.getFinger(0);
        Node x;

        try {
            if (successor.getId() == this.n.getId()) {
                x = this.predecessor;
            }
            else {
                x = ask(successor.getAddress(), StorageMessages.infoType.ASK_PREDECESSOR);
            }

            if (x != null) {
                if (this.util.in(x.getId(), this.n.getId(), successor.getId())) {
                    successor = x;
                    this.fingers.setFinger(0, successor);
                }
            }

            if (successor.getId() == this.n.getId()) {
                notify(this.n);
            }
            else {
                notify(successor.getAddress(), this.n);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * np thinks it might be our predecessor.
     * @param np
     */
    void notify(Node np) {
        if (this.predecessor == null || this.util.in(np.getId(), this.predecessor.getId(), this.n.getId())) {
            this.predecessor = np;
            System.out.println("predecessor = [" + np.getId() + "]");
        }
    }

    /**
     * Called periodically.
     * Refreshes finger table entries.
     * Next, stores the index of the finger to fix.
     */
    void fixFingers() {
        this.next = (this.next + 1) % this.m;
        Node np = findSuccessor(this.util.start(this.n.getId(), this.next));
        this.fingers.setFinger(this.next, np);
        System.out.println("finger[" + this.next + "] = [" + np.getId() + "]");
    }

    /**
     * Called periodically.
     * Checks whether predecessor has failed.
     */
    void checkPredecessor() {
        if (this.predecessor != null) {
            try {
                heartbeat(this.predecessor.getAddress());
            } catch (IOException ignore) {
                // if the predecessor is unreachable, will catch IOException
                System.out.println("Predecessor has failed, removing...");
                this.predecessor = null;
            }
        }
    }

    /**
     * Find the host (successor) that is responsible for the particular hashcode.
     * @param hash
     * @return Node
     */
    Node findHost(BigInteger hash) {
        int key = this.util.getKey(hash);
        return findSuccessor(key);
    }

    int getM() {
        return this.m;
    }

    Node getN() {
        return this.n;
    }

    Node getPredecessor() {
        return this.predecessor;
    }
}
