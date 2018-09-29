package edu.usfca.cs.dfs.Storage;

import edu.usfca.cs.dfs.StorageMessages;

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
}
