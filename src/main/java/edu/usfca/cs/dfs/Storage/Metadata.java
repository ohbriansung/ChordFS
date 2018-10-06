package edu.usfca.cs.dfs.Storage;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class Metadata {
    private ReentrantReadWriteLock lock;
    private final Set<String> files;
    private final Map<String, Integer> size;
    private final Map<String, Map<Integer, BigInteger>> chunks;
    private final Map<String, Map<Integer, BigInteger>> checksum;

    Metadata() {
        this.lock = new ReentrantReadWriteLock();
        this.files = new HashSet<>();
        this.size = new HashMap<>();
        this.chunks = new HashMap<>();
        this.checksum = new HashMap<>();
    }

    void add(String filename, int size, int i, BigInteger hash, BigInteger checksum) {
        this.lock.writeLock().lock();
        this.files.add(filename);
        this.size.put(filename, size);

        Map<Integer, BigInteger> chunks = this.chunks.getOrDefault(filename, new HashMap<>());
        chunks.put(i, hash);
        this.chunks.put(filename, chunks);

        Map<Integer, BigInteger> checksums = this.checksum.getOrDefault(filename, new HashMap<>());
        checksums.put(i, checksum);
        this.checksum.put(filename, checksums);
        this.lock.writeLock().unlock();
    }

    List<String> getFile() {
        this.lock.readLock().lock();
        List<String> files = new ArrayList<>(this.files);
        this.lock.readLock().unlock();

        return files;
    }

    int getTotalChunk(String filename) {
        this.lock.readLock().lock();
        int c = this.size.getOrDefault(filename, 0);
        this.lock.readLock().unlock();

        return c;
    }

    List<Integer> getChunks(String filename) {
        this.lock.readLock().lock();
        List<Integer> chunks = new ArrayList<>(this.chunks.get(filename).keySet());
        this.lock.readLock().unlock();

        return chunks;
    }

    BigInteger getHash(String filename, int chunk) {
        this.lock.readLock().lock();
        BigInteger hash = this.chunks.get(filename).get(chunk);
        this.lock.readLock().unlock();

        return hash;
    }

    boolean verify(String filename, int i, BigInteger chucksum) {
        try {
            return chucksum.equals(this.checksum.get(filename).get(i));
        } catch (NullPointerException ignore) {
            return false;
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        this.lock.readLock().lock();
        for (String file : this.files) {
            List<Integer> chunks = new ArrayList<>(this.chunks.get(file).keySet());

            sb.append(file).append("\t\t");
            sb.append(chunks.size()).append(" of ").append(this.size.get(file)).append("\t");
            sb.append(chunks.toString()).append("\n");
        }
        this.lock.readLock().unlock();

        return sb.toString();
    }
}
