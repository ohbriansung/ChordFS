package edu.usfca.cs.dfs.Storage;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.cs.dfs.*;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author Brian Sung
 *
 * Reference:
 * [1] https://www.cs.usfca.edu/~mmalensek/cs677/schedule/papers/stoica2001chord.pdf
 * [2] https://slideplayer.com/slide/4168285/
 * [3] https://en.wikipedia.org/wiki/Chord_(peer-to-peer)
 * [4] http://www.cs.utah.edu/~stutsman/cs6963/lecture/16/
 * successor = fingers[0]
 * np = n'
 */
abstract class Chord extends Sender {
    private int m;
    private int next;
    private Node predecessor;
    private Node secondSuccessor;
    Node n;
    FingerTable fingers;
    Utility util;

    Chord(String host, int port) {
        this.n = new Node(host, port);
        this.next = 0;
    }

    Chord(String host, int port, int m) {
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
    public Node findSuccessor(int id) {
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
                    try {
                        return ask(n0.getAddress(), StorageMessages.infoType.ASK_SUCCESSOR, id);
                    } catch (IOException ignore) {
                        try {
                            Thread.sleep(500); // wait for updating fingers
                        } catch (InterruptedException ignored) {}
                        return findSuccessor(id);
                    }
                }
            }
        } catch (IOException ignore) {
            updateSuccessor(successor);
        }

        return successor;
    }

    /**
     * Search the local table for the highest predecessor of id.
     * @param id
     * @return Node
     */
    public Node closestPrecedingNode(int id) {
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
     * Set fingers, successors, and predecessor to current node.
     */
    public void create() {
        int id = this.util.genId();
        this.n.setId(id);
        this.predecessor = this.n;
        for (int i = 0; i < this.m; i++) {
            this.fingers.setFinger(i, this.n);
        }
        this.secondSuccessor = this.n;
    }

    /**
     * Join a Chord ring contains node np.
     * Ask np the size of the Chord ring.
     * Id of current node could be already existed in the Chord ring,
     * keep generating new id until it is unique.
     * @param np
     */
    public void join(InetSocketAddress np) throws IOException {
        this.m = askM(np);
        prepare();

        this.predecessor = null;
        do {
            int id = this.util.genId();
            this.n.setId(id);
            Node successor = ask(np, StorageMessages.infoType.ASK_SUCCESSOR, this.n.getId());
            this.fingers.setFinger(0, successor);
        } while (this.n.getId() == this.fingers.getFinger(0).getId());
        System.out.println("successor = [" + this.fingers.getFinger(0).getId() + "]");
        this.secondSuccessor = null;
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
                    System.out.println("successor = [" + successor.getId() + "]");
                }
            }

            if (successor.getId() == this.n.getId()) {
                notify(this.n);
            }
            else {
                ask(successor.getAddress(), StorageMessages.infoType.NOTIFY, this.n);
            }
        } catch (IOException ignore) {
            updateSuccessor(successor);
        }
    }

    /**
     * np thinks it might be our predecessor.
     * @param np
     */
    public void notify(Node np) {
        if (this.predecessor == null) {
            this.predecessor = np;
            System.out.println("predecessor = [" + np.getId() + "]");
        }
        else {
            try {
                heartbeat(this.predecessor.getAddress());
                if (this.util.in(np.getId(), this.predecessor.getId(), this.n.getId())) {
                    this.predecessor = np;
                    System.out.println("predecessor = [" + np.getId() + "]");
                }
            } catch (IOException ignore) {
                System.out.println("Predecessor [" + this.predecessor.getId() + "] has failed, removing...");
                this.predecessor = np;
                System.out.println("predecessor = [" + np.getId() + "]");
            }
        }
    }

    /**
     * Called periodically.
     * Refreshes finger table entries.
     * Next, stores the index of the finger to fix.
     */
    void fixFingers() {
        this.next = (this.next + 1) % this.m;
        if (this.next == 0) {
            this.next++;
        }
        Node np = findSuccessor(this.util.start(this.n.getId(), this.next));
        this.fingers.setFinger(this.next, np);
        System.out.println("finger[" + this.next + "] = [" + np.getId() + "]");
    }

    /**
     * Called periodically.
     * Checks whether successor has failed.
     * Key step in failure recovery is maintaining correct successor pointer.
     * So we need to make sure successor is always pointed to a live node.
     */
    void keepAlive() {
        Node successor = this.fingers.getFinger(0);

        try {
            heartbeat(successor.getAddress());
        } catch (IOException ignore) {
            updateSuccessor(successor);
        }
    }

    /**
     * Called periodically.
     * Keep asking successor for its successor.
     * Maintain an additional successor increases capability of the
     * fault tolerance as I mentioned above.
     */
    void stabilizeSecSucc() {
        Node successor = this.fingers.getFinger(0);

        try {
            if (successor.getId() == this.n.getId()) {
                this.secondSuccessor = successor;
            }
            else {
                Node sec = ask(successor.getAddress(), StorageMessages.infoType.ASK_SUCCESSOR, successor.getId() + 1);
                if (this.secondSuccessor == null || sec.getId() != this.secondSuccessor.getId()) {
                    this.secondSuccessor = sec;
                    System.out.println("second_successor = [" + sec.getId() + "]");
                }
            }
        } catch (IOException ignore) {
            updateSuccessor(successor);
            stabilizeSecSucc();
        }
    }

    /**
     * When successor fails, update it to the second successor,
     * and tell the new successor that current node is its new predecessor.
     * Start the update process to other node.
     * @param successor - the failed successor node
     */
    private void updateSuccessor(Node successor) {
        System.out.println("Successor [" + successor.getId() + "] has failed, removing...");

        if (successor.getId() == this.predecessor.getId()) {
            // when there were only two node in ring, and one failed, the rest is the only node in ring.
            this.predecessor = this.n;
            for (int i = 0; i < this.m; i++) {
                this.fingers.setFinger(i, this.n);
            }
            this.secondSuccessor = this.n;
        }
        else {
            try {
                // notify the second successor that current node is its new predecessor
                if (this.secondSuccessor.getId() == this.n.getId()) {
                    this.predecessor = this.n;
                }
                else {
                    ask(this.secondSuccessor.getAddress(), StorageMessages.infoType.NOTIFY, this.n);
                }

                // update the successor to second successor
                this.fingers.setFinger(0, this.secondSuccessor);
                System.out.println("successor = [" + this.secondSuccessor.getId() + "]");
                for (int i = 1; i < this.m; i++) {
                    if (this.fingers.getFinger(i).getId() == successor.getId()) {
                        this.fingers.setFinger(i, this.secondSuccessor);
                        System.out.println("finger[" + i + "] = [" + this.secondSuccessor.getId() + "]");
                    }
                }

                // update m predecessor that could have failed node in their fingers
                if (this.predecessor.getId() != this.n.getId()) {
                    tellPreToUpdate(successor, this.secondSuccessor, this.n, this.m);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Send the update message to m predecessors that could potentially have
     * the failed node in their fingers. (No overshooting)
     * @param s - old successor
     * @param ss - second successor (new successor)
     * @param n - current node
     * @param m - m count of predecessors
     * @throws IOException
     */
    private void tellPreToUpdate(Node s, Node ss, Node n, int m) throws IOException {
        StorageMessages.Update updateMessage = StorageMessages.Update.newBuilder().setOld(s.serialize())
                .setNew(ss.serialize()).setWhoSent(n.serialize()).setCount(m).build();
        StorageMessages.Message message = StorageMessages.Message.newBuilder()
                .setType(StorageMessages.messageType.UPDATE).setData(updateMessage.toByteString()).build();
        send(this.predecessor.getAddress(), message);
    }

    /**
     * Receive a request to update finger table.
     * Update the old node in the message to the new node in the message.
     * Ask predecessor to update as well when the count down hasn't finish,
     * and the predecessor is not the node starts the request.
     * @param m - request message
     * @throws InvalidProtocolBufferException
     */
    public void updateFinger(StorageMessages.Message m) throws InvalidProtocolBufferException {
        StorageMessages.Update updateMessage = StorageMessages.Update.parseFrom(m.getData());
        Node old = new Node(updateMessage.getOld());
        Node _new = new Node(updateMessage.getNew());
        int whoSent = updateMessage.getWhoSent().getId();
        int count = updateMessage.getCount() - 1;

        for (int i = 0; i < this.m; i++) {
            if (this.fingers.getFinger(i).getId() == old.getId()) {
                this.fingers.setFinger(i, _new);
                System.out.println("finger[" + i + "] = [" + _new.getId() + "]");
            }
        }

        if (count > 0 && this.predecessor.getId() != whoSent) {
            updateMessage = updateMessage.toBuilder().setCount(count).build();
            m = m.toBuilder().setData(updateMessage.toByteString()).build();
            try {
                send(this.predecessor.getAddress(), m);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public int getM() {
        return this.m;
    }

    public Node getN() {
        return this.n;
    }

    public Node getPredecessor() {
        return this.predecessor;
    }
}
