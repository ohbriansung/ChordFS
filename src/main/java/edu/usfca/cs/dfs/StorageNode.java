package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;

class StorageNode {
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
        return node.getSuccessor();
    }

    Node findPredecessor(int id) {
        Node node = this.self;

        // id not in (node, node.successor]
        while (!(id > node.getId() && id <= node.getSuccessor().getId())) {
            if (node.getId() == this.self.getId()) {
                node = closestPrecedingFinger(id);
            }
            else {
                // node.closestPrecedingFinger(id)
                node = ask(node.getAddress());
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
     * @param address
     * @return Node
     */
    Node ask(InetSocketAddress address) {
        long time = System.currentTimeMillis();
        CountDownLatch goSignal = new CountDownLatch(1);
        this.awaitTasks.put(time, goSignal);

        // serialize info
        int type = StorageMessages.Info.infoType.CLOSEST_PRECEDING_FINGER_VALUE;
        StorageMessages.Info info = StorageMessages.Info.newBuilder().setTypeValue(type).setTime(time).build();

        // serialize request with info type
        type = StorageMessages.Request.requestType.INFO_VALUE;
        ByteString data = info.toByteString();
        StorageMessages.Request request = StorageMessages.Request.newBuilder().setTypeValue(type).setData(data).build();

        // send request to node
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
}
