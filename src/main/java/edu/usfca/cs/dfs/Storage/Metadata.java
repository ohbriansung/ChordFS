package edu.usfca.cs.dfs.Storage;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class Metadata {
    private ReentrantReadWriteLock lock;
    private final Set<String> files;
    private final Map<String, Integer> size;
    private final Map<String, Set<Integer>> chunks;

    Metadata() {
        this.lock = new ReentrantReadWriteLock();
        this.files = new HashSet<>();
        this.size = new HashMap<>();
        this.chunks = new HashMap<>();
    }

    void add(String filename, int size, int i) {
        this.lock.writeLock().lock();
        this.files.add(filename);
        this.size.put(filename, size);
        Set<Integer> chunks = this.chunks.getOrDefault(filename, new TreeSet<>());
        chunks.add(i);
        this.chunks.put(filename, chunks);
        this.lock.writeLock().unlock();
    }

    int getTotalChunk(String filename) {
        return this.size.getOrDefault(filename, 0);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        this.lock.readLock().lock();
        for (String file : this.files) {
            Set<Integer> chunks = this.chunks.get(file);

            sb.append(file).append("\t\t");
            sb.append(chunks.size()).append(" of ").append(this.size.get(file)).append("\t");
            sb.append(chunks.toString()).append("\n");
        }
        this.lock.readLock().unlock();

        return sb.toString();
    }
}
