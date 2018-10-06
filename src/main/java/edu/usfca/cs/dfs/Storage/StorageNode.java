package edu.usfca.cs.dfs.Storage;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Brian Sung
 */
public class StorageNode extends Chord {
    private final Hashtable<Integer, Metadata> currentStorage;

    public StorageNode(String host, int port) {
        super(host, port);
        this.currentStorage = new Hashtable<>();
    }

    public StorageNode(String host, int port, int m) {
        super(host, port, m);
        this.currentStorage = new Hashtable<>();
    }

    /**
     * When joining network, tell the successor to backup file chunks to current node.
     * @throws IOException - successor is unreachable
     */
    public void tellSuccToBackup() throws IOException {
        Node successor = this.fingers.getFinger(0);
        StorageMessages.Message message = StorageMessages.Message.newBuilder()
                .setType(StorageMessages.messageType.BACKUP).setReplica(this.n.getId()).build();
        send(successor.getAddress(), message);
    }

    /**
     * Node receives backup request from its future predecessor,
     * which is still in startup process and not in the network.
     * Node, its successor, and its second successor (3 replicas) need to copy file to the new node.
     * The key range for each node above to copy will be:
     * Node - (Third predecessor, Second predecessor]
     * Successor - (Second predecessor, Predecessor]
     * Second successor - (Predecessor, New node's id]
     * @param addr - new node address
     * @param id - new node id
     */
    public void backup(InetSocketAddress addr,  int id) {
        System.out.println("Starting backup to " + addr.toString());

        Node s = this.fingers.getFinger(0);
        Node ss = this.secondSuccessor;
        boolean delete = true;
        if (s.getId() == this.n.getId()
                || ss.getId() == this.n.getId()) {
            // only one or two nodes in the ring, copy all data but not delete
            delete = false;
        }

        Node p1 = this.getPredecessor();
        int p2;
        int p3;
        if (p1.getId() == this.n.getId()) {
            // if predecessor is current node, use current node's id directly.
            p2 = p1.getId();
            p3 = p1.getId();
            backup(addr, p3, p2, delete);
            System.out.println("Backup to " + addr.toString() + " has completed.");
            return;
        }

        StorageMessages.Info info = StorageMessages.Info.newBuilder()
                .setType(StorageMessages.infoType.ASK_TWO_PREDECESSOR).build();

        StorageMessages.Info p2And3;
        try {
            p2And3 = list(p1.getAddress(), info);
        } catch (IOException ignore) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
            backup(addr, id);  // wait for half second and retry
            return;
        }
        String[] ids = p2And3.getData().toStringUtf8().split(" ");
        p2 = Integer.parseInt(ids[0]);
        p3 = Integer.parseInt(ids[1]);

        backup(addr, p3, p2, delete);
        tellNodeToSendData(s, addr, p2, p1.getId(), delete);
        if (ss.getId() != this.n.getId()) {
            // if there s only two node in the ring, the above two steps will copy all the data
            tellNodeToSendData(ss, addr, p1.getId(), id, delete);
        }

        System.out.println("Backup to " + addr.toString() + " has completed.");
    }

    /**
     * Start copy process and delete process if indicated.
     * @param addr - new node address
     * @param start - data key range starting point
     * @param end - data key range ending point
     * @param delete - if delete after replication
     */
    public void backup(InetSocketAddress addr, int start, int end, boolean delete) {
        synchronized (this.currentStorage) {
            int i = this.util.start(start, 0);
            int init = i;

            while (this.util.includesRight(i, start, end)) {
                Metadata metadata;

                if ((metadata = this.currentStorage.get(i)) != null) {
                    ReadWriteFileNIO rw = readAndSend(addr, metadata);

                    if (delete) {
                        rw.delete();
                        this.currentStorage.remove(i);
                    }
                }

                i = this.util.start(i, 0);
                if (i == init) {
                    break;
                }
            }

            if (delete) {
                System.out.println("Deleted key in (" + start + ", " + end + "].");
            }
        }
    }

    /**
     * Read all files in metadata from the disk and send to the new node.
     * @param addr - new node address.
     * @param metadata
     */
    private ReadWriteFileNIO readAndSend(InetSocketAddress addr, Metadata metadata) {
        ReadWriteFileNIO rw = new ReadWriteFileNIO(metadata);
        List<StorageMessages.Message> messages = rw.read();

        if (!messages.isEmpty()) {
            CountDownLatch count = new CountDownLatch(messages.size());
            ExecutorService pool = Executors.newFixedThreadPool(4);
            while (!messages.isEmpty()) {
                pool.submit(new Backup(addr, messages.get(0), count));
                messages.remove(0);
            }

            try {
                count.await();
                pool.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return rw;
    }

    /**
     * Tell successor or second successor to replicate data to new node.
     * If successor or second successor is current node, do backup directly.
     * @param n - successor or second successor
     * @param addr - new node address
     * @param start - data key range starting point
     * @param end - data key range ending point
     * @param delete - if delete after replication
     */
    private void tellNodeToSendData(Node n, InetSocketAddress addr, int start, int end, boolean delete) {
        if (n.getId() == this.n.getId()) {
            backup(addr, start, end, delete);
            return;
        }

        String address = addr.toString().replaceAll("/", "");
        StorageMessages.infoType type = delete ? StorageMessages.infoType.SEND_DATA_AND_DELETE : StorageMessages.infoType.SEND_DATA;
        StorageMessages.Info info = StorageMessages.Info.newBuilder().setType(type)
                .setData(ByteString.copyFromUtf8(address)).setIntegerData(start).setIntegerData2(end).build();
        StorageMessages.Message message = StorageMessages.Message.newBuilder().setType(StorageMessages.messageType.INFO)
                .setData(info.toByteString()).build();

        try {
            send(n.getAddress(), message);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    /**
     * Record the metadata of the file chunks that has been stored on the node disk.
     * @param message
     */
    public void recordMetadata(StorageMessages.Message message) {
        String filename = message.getFileName();
        int total = message.getTotalChunk();
        int i = message.getChunkId();
        BigInteger hash = new BigInteger(message.getHash().toByteArray());
        int key = this.util.getKey(hash);

        synchronized (this.currentStorage) {
            if (!this.currentStorage.containsKey(key)) {
                this.currentStorage.put(key, new Metadata());
            }
            Metadata md = this.currentStorage.get(key);
            md.add(filename, total, i, hash);
        }
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

        synchronized (this.currentStorage) {
            if (this.currentStorage.containsKey(key)) {
                Metadata m = this.currentStorage.get(key);
                return m.getTotalChunk(filename);
            }
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

        synchronized (this.currentStorage) {
            for (Metadata metadata : this.currentStorage.values()) {
                sb.append(metadata.toString());
            }
        }

        return StorageMessages.Info.newBuilder().setData(ByteString.copyFromUtf8(sb.toString())).build();
    }

    /**
     * Ask predecessor for its predecessor.
     * Return two predecessor's ids.
     * @return StorageMessages.Info
     * @throws IOException
     */
    public StorageMessages.Info askPreItsPre() throws IOException {
        Node pre = getPredecessor();
        Node itsPre = ask(pre.getAddress(), StorageMessages.infoType.ASK_PREDECESSOR);
        String ids = "" + pre.getId() + " " + itsPre.getId();

        return StorageMessages.Info.newBuilder().setData(ByteString.copyFromUtf8(ids)).build();
    }
}
