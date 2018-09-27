package edu.usfca.cs.dfs;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Metadata {
    private ReentrantReadWriteLock lock;
    private final Set<String> files;
    private final Map<String, Integer> fileSize;
    private final Map<String, Set<Integer>> chunks;

    public Metadata() {
        this.lock = new ReentrantReadWriteLock();
        this.files = new HashSet<>();
        this.fileSize = new HashMap<>();
        this.chunks = new HashMap<>();
    }

    void addFileAndChunk(String filename, int i) {
        this.lock.writeLock().lock();
        this.files.add(filename);
        Set<Integer> chunks = this.chunks.getOrDefault(filename, new TreeSet<>());
        chunks.add(i);
        this.chunks.put(filename, chunks);
        this.lock.writeLock().unlock();
    }
}
