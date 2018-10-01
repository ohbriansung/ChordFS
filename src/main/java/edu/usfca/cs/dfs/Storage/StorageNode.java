package edu.usfca.cs.dfs.Storage;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Hashtable;

/**
 * Chord Reference: https://en.wikipedia.org/wiki/Chord_(peer-to-peer)
 * successor = fingers[0]
 * np = n'
 * @author Brian Sung
 */
public class StorageNode extends Chord {
    private Hashtable<Integer, Metadata> currentStorage;

    public StorageNode(String host, int port) {
        super(host, port);
        this.currentStorage = new Hashtable<>();
    }

    public StorageNode(String host, int port, int m) {
        super(host, port, m);
        this.currentStorage = new Hashtable<>();
    }

    /**
     * Find the host (successor) that is responsible for the particular hashcode.
     * @param hash
     * @return Node
     */
    public Node findHost(BigInteger hash) {
        int key = this.util.getKey(hash);
        return findSuccessor(key);
    }

    public void recordMetadata(StorageMessages.Message message) {
        String filename = message.getFileName();
        int total = message.getTotalChunk();
        int i = message.getChunkId();
        BigInteger hash = new BigInteger(message.getHash().toByteArray());
        int key = this.util.getKey(hash);

        if (!this.currentStorage.containsKey(key)) {
            this.currentStorage.put(key, new Metadata());
        }

        Metadata md = this.currentStorage.get(key);
        md.add(filename, total, i);
        System.out.println("Metadata of file [" + filename + i + "] has been recorded.");
    }

    /**
     * The first node replicates the data to its successor,
     * and the successor will replicates the data again to its successor.
     * Check the replica variable in the message, replicate 3 times.
     * @param message
     */
    public void replicate(StorageMessages.Message message) {
        int replica = message.getReplica();
        message = message.toBuilder().setReplica(replica + 1).build();  // increase the replicate time after storing.
        Node successor = this.fingers.getFinger(0);

        if (message.getReplica() >= 3 || successor.getId() == this.n.getId()) {
            // if there are 3 replicas already or if the successor is self, stop replicating.
            System.out.println("Replication completed.");
            return;
        }

        Thread task = new Thread(new Replicate(message, successor.getAddress()));
        task.start();
    }

    /**
     * Return total chunk number of a particular file.
     * @param message
     * @return int
     */
    public int getTotalChunk(StorageMessages.Message message) {
        String filename = message.getFileName();
        BigInteger hash = new BigInteger(message.getHash().toByteArray());
        int key = this.util.getKey(hash);

        if (this.currentStorage.containsKey(key)) {
            Metadata m = this.currentStorage.get(key);
            return m.getTotalChunk(filename);
        }

        return 0;
    }

    /**
     * Connect successor to retrieve system data.
     * @param info
     * @return StorageMessages.Info
     * @throws IOException - Node is unreachable.
     */
    public StorageMessages.Info listNode(StorageMessages.Info info) throws IOException {
        String data = info.getData().toStringUtf8();
        int n = info.getIntegerData();
        if (data.length() == 0) {
            n = this.n.getId();
        }

        String space = this.util.getFreeSpace(DFS.volume);
        data += this.n.getId() + " " + this.n.getAddress() + " " + space + " " + DFS.receiver.getRequestCount() + " ";
        info = info.toBuilder().setData(ByteString.copyFromUtf8(data)).setIntegerData(n).build();

        Node successor = this.fingers.getFinger(0);
        if (successor.getId() != n) {
            // if hasn't formed a cycle, send request to successor.
            info = list(successor.getAddress(), info);
        }

        return info;
    }

    /**
     * Return all Metadata.
     * @return StorageMessages.Info
     */
    public StorageMessages.Info listFile() {
        StringBuilder sb = new StringBuilder();

        for (Metadata metadata : this.currentStorage.values()) {
            sb.append(metadata.toString());
        }

        return StorageMessages.Info.newBuilder().setData(ByteString.copyFromUtf8(sb.toString())).build();
    }
}
