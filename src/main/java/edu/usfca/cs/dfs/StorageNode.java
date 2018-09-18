package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;

class StorageNode extends Serializer {
    private int m;
    private Node self;
    private FingerTable finger;
    private Hashtable<String, CountDownLatch> awaitTasks;
    private Hashtable<String, Object> answers;

    /**
     * Initialize the m bits identifier ring.
     * The predecessor will be self when it is the first node in the ring.
     *
     * @param host
     * @param port
     * @param m
     */
    StorageNode(String host, int port, int m) {
        this.awaitTasks = new Hashtable<>();
        this.answers = new Hashtable<>();

        this.m = m;

        this.self = new Node(host, port);
        this.self.setId(generateId(m));
        String address = this.self.getAddress();
        this.self.setPredecessor(address);
        this.self.setSuccessor(address);
        this.self.setSuccessorId(this.self.getId());

        this.finger = new FingerTable(this.m, this.self);
        System.out.println(this.finger.toString());
    }

    StorageNode(String host, int port) {
        this.awaitTasks = new Hashtable<>();
        this.answers = new Hashtable<>();
        this.self = new Node(host, port);
    }

    void prepare(InetSocketAddress existingNode) {
        this.m = askM(existingNode);

        Node successor = askIdAndSuccessor(this.m, this.self, existingNode);

        this.finger = new FingerTable(this.m);
        join(successor, this.self.getId());
        System.out.println(this.finger.toString());
    }

    private int generateId(int m) {
        return Math.abs(Long.hashCode(System.currentTimeMillis()) % (0b1 << m));
    }

    Node findSuccessor(int id) {
        Node node = findPredecessor(id);

        Node successor;
        if (node.getSuccessorId().equals(this.self.getSuccessorId())) {
            successor = this.self;
        }
        else {
            String address[] = node.getSuccessor().split(":");
            successor = askNodeDetail(address[0], Integer.parseInt(address[1]));
        }

        return successor;
    }

    private Node findPredecessor(int id) {
        Node node = this.self;

        if (node.getId() != node.getSuccessorId()) {
            // id not in (node, node.successor]
            while (!(id > node.getId() && id <= node.getSuccessorId())) {
                node = askClosestPrecedingFinger(id, node.getHost(), node.getPort());
            }
        }

        return node;
    }

    Node closestPrecedingFinger(int id) {
        for (int i = this.m - 1; i >= 0; i--) {
            Node node = this.finger.getFinger(i);

            // finger[i] in (self, id)
            if (this.self.getId() < id) {
                if (node.getId() > this.self.getId() && node.getId() < id) {
                    return node;
                }
            }
            else if (this.self.getId() > id) {
                // passing zero, opposite of the condition above
                if (!(node.getId() <= this.self.getId() && node.getId() >= id)) {
                    return node;
                }
            }
            else {
                // closest_preceding_finger is self when (id == self.id)
                break;
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
            int startPoint = id + (0b1 << (i + 1));

            int previousId = this.finger.getFinger(i).getId();
            if (previousId <= id) {
                previousId += (0b1 << this.m);
            }

            // if finger[i + 1].start in [n, finger[i].node)
            if (startPoint >= id && startPoint < previousId) {
                this.finger.setFinger(i + 1, this.finger.getFinger(i));
            }
            else {
                Node node = askSuccessor(startPoint  % (0b1 << this.m), address);
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
            }
        }

        System.out.println(this.finger.toString());
    }

    /**
     * Identify the request by timestamp.
     *
     * @param id
     * @param host
     * @param port
     * @return Node
     */
    private Node askClosestPrecedingFinger(int id, String host, int port) {
        String time = setTaskAndGetTime();
        createInfoAndSend(new InetSocketAddress(host, port), StorageMessages.infoType.CLOSEST_PRECEDING_FINGER,
                time, ByteString.copyFromUtf8(String.valueOf(id)));

        try {
            // wait for the reply from the node we just asked
            CountDownLatch signal = this.awaitTasks.get(time);
            signal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Node node = (Node) this.answers.get(time);
        this.awaitTasks.remove(time);
        this.answers.remove(time);

        return node;
    }

    private int askM(InetSocketAddress address) {
        String time = setTaskAndGetTime();
        createInfoAndSend(address, StorageMessages.infoType.ASK_M, time);

        try {
            CountDownLatch signal = this.awaitTasks.get(time);
            signal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int m = (int) this.answers.get(time);
        this.awaitTasks.remove(time);
        this.answers.remove(time);

        return m;
    }

    private Node askIdAndSuccessor(int m, Node self, InetSocketAddress address) {
        Node successor;
        int id;

        do {
            id = generateId(m);
            successor = askSuccessor(id, address);
        } while (successor.getId() == id);  // if there exists a node with same id, regenerate the id and try again.

        self.setId(id);
        return successor;
    }

    private Node askSuccessor(int id, InetSocketAddress address) {
        String time = setTaskAndGetTime();
        createInfoAndSend(address, StorageMessages.infoType.ASK_SUCCESSOR,
                time, ByteString.copyFromUtf8(String.valueOf(id)));

        try {
            CountDownLatch signal = this.awaitTasks.get(time);
            signal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Node successor = (Node) this.answers.get(time);
        this.awaitTasks.remove(time);
        this.answers.remove(time);

        return successor;
    }

    private Node askNodeDetail(String host, int port) {
        String time = setTaskAndGetTime();
        createInfoAndSend(new InetSocketAddress(host, port), StorageMessages.infoType.ASK_NODE_DETAIL, time);

        try {
            // wait for the reply from the node we just asked
            CountDownLatch signal = this.awaitTasks.get(time);
            signal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Node node = (Node) this.answers.get(time);
        this.awaitTasks.remove(time);
        this.answers.remove(time);

        return node;
    }

    private void updatePredecessor(InetSocketAddress address, Node node) {
        String time = setTaskAndGetTime();
        createInfoAndSend(address, StorageMessages.infoType.UPDATE_PREDECESSOR, time, node.serialize().toByteString());

        try {
            // wait for the reply from the node we just asked
            CountDownLatch signal = this.awaitTasks.get(time);
            signal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.awaitTasks.remove(time);
    }

    private String setTaskAndGetTime() {
        String time = String.valueOf(System.currentTimeMillis());
        this.awaitTasks.put(time, new CountDownLatch(1));

        return time;
    }

    void awaitTasksCountDown(String time) {
        if (this.awaitTasks.containsKey(time)) {
            this.awaitTasks.get(time).countDown();
        }
    }

    void addAnswer(String time, Object ans) {
        if (this.awaitTasks.containsKey(time)) {
            this.answers.put(time, ans);
        }
    }

    int getM() {
        return this.m;
    }

    Node getSelf() {
        return this.self;
    }
}
