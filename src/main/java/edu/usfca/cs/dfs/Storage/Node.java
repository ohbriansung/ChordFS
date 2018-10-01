package edu.usfca.cs.dfs.Storage;

import edu.usfca.cs.dfs.StorageMessages;

import java.net.InetSocketAddress;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Node {
    private final ReentrantReadWriteLock lock;
    private final String host;
    private final int port;
    private Integer id;

    public Node(String host, int port) {
        this.host = host;
        this.port = port;
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * Directly deserialize from StorageMessages.Node
     *
     * @param node
     */
    public Node(StorageMessages.Node node) {
        this(node.getHost(), node.getPort());
        this.id = node.getId();
    }

    void setId(int id) {
        this.lock.writeLock().lock();
        this.id = id;
        this.lock.writeLock().unlock();

        printId();
    }

    int getId() {
        this.lock.readLock().lock();
        int id = this.id;
        this.lock.readLock().unlock();

        return id;
    }

    public InetSocketAddress getAddress() {
        this.lock.readLock().lock();
        InetSocketAddress address = new InetSocketAddress(this.host, this.port);
        this.lock.readLock().unlock();

        return address;
    }

    /**
     * Serialize into protocol buffer object.
     *
     * @return StorageMessages.Node
     */
    public StorageMessages.Node serialize() {
        this.lock.readLock().lock();

        // serialize node
        StorageMessages.Node.Builder builder = StorageMessages.Node.newBuilder().setHost(this.host).setPort(this.port);

        if (this.id != null) {
            builder.setId(this.id);
        }

        this.lock.readLock().unlock();

        return builder.build();
    }

    private void printId() {
        this.lock.readLock().lock();
        System.out.println("Current node id = [" + this.id + "]");
        this.lock.readLock().unlock();
    }
}
