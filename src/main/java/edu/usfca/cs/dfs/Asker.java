package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;

class Asker extends Serializer {
    private Hashtable<String, CountDownLatch> awaitTasks;
    private Hashtable<String, Object> answers;

   Asker() {
       this.awaitTasks = new Hashtable<>();
       this.answers = new Hashtable<>();
   }

    /**
     * Identify the request by timestamp.
     *
     * @param id
     * @param host
     * @param port
     * @return Node
     */
     Node askClosestPrecedingFinger(int id, String host, int port) {
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

    int askM(InetSocketAddress address) {
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

    Node askIdAndSuccessor(int m, Node self, InetSocketAddress address) {
        Node successor;
        int id;

        do {
            id = DFS.ID;
            successor = askSuccessor(id, address);
        } while (successor.getId() == id);  // if there exists a node with same id, regenerate the id and try again.

        self.setId(id);
        return successor;
    }

    Node askSuccessor(int id, InetSocketAddress address) {
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

    Node askNodeDetail(String host, int port) {
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

    void updatePredecessor(InetSocketAddress address, Node node) {
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

}
