package edu.usfca.cs.dfs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class FingerTable {
    private ReentrantReadWriteLock lock;
    private final Node[] finger;

    FingerTable(int m) {
        this.finger = new Node[m];
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * Initialize the first node in the ring.
     * All fingers will be self if it is the first node in the ring.
     *
     * @param m - the capacity of the ring = 2^m
     */
    FingerTable(int m, Node self) {
        this(m);

        for (int i = 0; i < m; i++) {
            this.finger[i] = self;
        }
    }

    void setFinger(int i, Node node) {
        this.lock.writeLock().lock();
        this.finger[i] = node;
        this.lock.writeLock().unlock();
    }

    Node getFinger(int i) {
        this.lock.readLock().lock();
        Node finger = this.finger[i];
        this.lock.readLock().unlock();

        return finger;
    }

    public String toString() {
        List<Integer> list = new ArrayList<>();

        this.lock.readLock().lock();
        for (Node node : this.finger) {
            if (node == null) {
                list.add(-1);
            }
            else {
                list.add(node.getId());
            }
        }
        this.lock.readLock().unlock();

        return list.toString();
    }
}
