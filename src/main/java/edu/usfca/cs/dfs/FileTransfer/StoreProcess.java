package edu.usfca.cs.dfs.FileTransfer;

import edu.usfca.cs.dfs.StorageMessages;

/**
 * Called by RequestHandler.
 * It's already multi-threaded.
 */
public class StoreProcess {
    private final StorageMessages.Message message;

    public StoreProcess(StorageMessages.Message message) {
        this.message = message;
    }

    public void store() {
        String filename = this.message.getFileName();
        int total = this.message.getTotalChunk();
        int i = this.message.getChunkId();
        byte[] chunk = this.message.getData().toByteArray();
    }
}
