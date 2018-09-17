package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;

class StorageNode extends Serializer {
    private final int m;
    private final Node self;
    private final FingerTable finger;
    private final Hashtable<Long, CountDownLatch> awaitTasks;
    private final Hashtable<Long, Node> answers;

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
        this.awaitTasks = new Hashtable<>();
        this.answers = new Hashtable<>();
    }

    Node findSuccessor(int id) {
        Node node = findPredecessor(id);

        String address[] = node.getSuccessor().split(":");
        int successorId = node.getSuccessorId();
        Node successor = new Node(address[0], Integer.parseInt(address[1]));
        successor.setId(successorId);

        return successor;
    }

    Node findPredecessor(int id) {
        Node node = this.self;

        // id not in (node, node.successor]
        while (!(id > node.getId() && id <= node.getSuccessorId())) {
            if (node.getId() == this.self.getId()) {
                node = closestPrecedingFinger(id);
            }
            else {
                // node.closestPrecedingFinger(id)
                node = ask(id, node.getHost(), node.getPort());
            }
        }

        return node;
    }

    Node closestPrecedingFinger(int id) {
        for (int i = this.m - 1; i >= 0; i--) {
            Node node = this.finger.getNode(i);

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

    /**
     * Identify the request by timestamp.
     *
     * @param id
     * @param host
     * @param port
     * @return Node
     */
    Node ask(int id, String host, int port) {
        long time = System.currentTimeMillis();
        CountDownLatch goSignal = new CountDownLatch(1);
        this.awaitTasks.put(time, goSignal);

        // serialize info
        ByteString askingId = ByteString.copyFromUtf8(String.valueOf(id));
        StorageMessages.Info info = serializeInfo(StorageMessages.infoType.CLOSEST_PRECEDING_FINGER, askingId, time);

        // serialize request with info type
        ByteString data = info.toByteString();
        StorageMessages.Message request = serializeMessage(StorageMessages.messageType.INFO, data);

        // send request to node
        InetSocketAddress address = new InetSocketAddress(host, port);
        DFS.sender.send(request, address);

        try {
            // wait for the reply from the node we just asked
            goSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Node node = this.answers.get(time);
        this.awaitTasks.remove(time);
        this.answers.remove(time);

        return node;
    }

    void awaitTasksCountDown(long time) {
        if (this.awaitTasks.contains(time)) {
            this.awaitTasks.get(time).countDown();
        }
    }

    void addAnswer(long time, Node node) {
        if (this.awaitTasks.contains(time)) {
            this.answers.put(time, node);
        }
    }
}
