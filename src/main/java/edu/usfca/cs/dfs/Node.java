package edu.usfca.cs.dfs;

import java.util.concurrent.locks.ReentrantReadWriteLock;

class Node {
    private final ReentrantReadWriteLock lock;
    private final String host;
    private final int port;
    private Integer id;
    private String successor;
    private Integer successorId;
    private String predecessor;
    private Integer predecessorId;

    Node(String host, int port) {
        this.host = host;
        this.port = port;
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * Directly deserialize from StorageMessages.Node
     *
     * @param node
     */
    Node(StorageMessages.Node node) {
        this(node.getHost(), node.getPort());

        this.id = node.getId();
        this.successor = node.getSuccessor();
        this.successorId = node.getSuccessorId();
        this.predecessor = node.getPredecessor();
        this.predecessorId = node.getPredecessorId();
    }

    /**
     * Immutable id.
     *
     * @param id
     */
    void setId(int id) {
        this.lock.writeLock().lock();
        if (this.id == null) {
            this.id = id;
            printId();
        } else {
            System.out.println("Failed to set id: id has already been set.");
        }
        this.lock.writeLock().unlock();
    }

    void setSuccessor(String successor) {
        this.lock.writeLock().lock();
        this.successor = successor;
        this.lock.writeLock().unlock();
    }

    void setSuccessorId(Integer successorId) {
        this.lock.writeLock().lock();
        this.successorId = successorId;
        this.lock.writeLock().unlock();
    }

    void setPredecessor(String predecessor) {
        this.lock.writeLock().lock();
        this.predecessor = predecessor;
        this.lock.writeLock().unlock();
    }

    void setPredecessorId(Integer predecessorId) {
        this.lock.writeLock().lock();
        this.predecessorId = predecessorId;
        this.lock.writeLock().unlock();
    }

    String getHost() {
        this.lock.readLock().lock();
        String host = this.host;
        this.lock.readLock().unlock();

        return host;
    }

    int getPort() {
        this.lock.readLock().lock();
        int port = this.port;
        this.lock.readLock().unlock();

        return port;
    }

    int getId() {
        this.lock.readLock().lock();
        int id = this.id;
        this.lock.readLock().unlock();

        return id;
    }

    String getSuccessor() {
        this.lock.readLock().lock();
        String successor = this.successor;
        this.lock.readLock().unlock();

        return successor;
    }

    Integer getSuccessorId() {
        this.lock.readLock().lock();
        Integer successorId = this.successorId;
        this.lock.readLock().unlock();

        return successorId;
    }

    String getPredecessor() {
        this.lock.readLock().lock();
        String predecessor = this.predecessor;
        this.lock.readLock().unlock();

        return predecessor;
    }

    Integer getPredecessorId() {
        this.lock.readLock().lock();
        Integer predecessorId = this.predecessorId;
        this.lock.readLock().unlock();

        return predecessorId;
    }

    StorageMessages.Node serialize() {
        this.lock.readLock().lock();

        // serialize node
        StorageMessages.Node.Builder builder = StorageMessages.Node.newBuilder().setHost(this.host).setPort(this.port);

        if (this.id != null) {
            builder.setId(this.id);
        }

        if (this.successor != null) {
            builder.setSuccessor(this.successor);
        }

        if (this.successorId != null) {
            builder.setSuccessorId(this.successorId);
        }

        if (this.predecessor != null) {
            builder.setPredecessor(this.predecessor);
        }

        if (this.predecessorId != null) {
            builder.setPredecessorId(this.predecessorId);
        }

        this.lock.readLock().unlock();

        return builder.build();
    }

    String getAddress() {
        this.lock.readLock().lock();
        String address = this.host + ":" + this.port;
        this.lock.readLock().unlock();

        return address;
    }

    private void printId() {
        // no lock needed, only called by setId()
        System.out.println("Current node id: " + this.id);
    }
}
