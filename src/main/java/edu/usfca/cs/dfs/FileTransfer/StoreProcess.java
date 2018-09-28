package edu.usfca.cs.dfs.FileTransfer;

import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Called by RequestHandler. Thus, it's already multi-threaded.
 * Responsible for storing file chunks on disk.
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

        try {
            File file = new File(DFS.volume + filename + i);
            if (file.exists()) {
                // override file
                file.delete();
            }
            file.createNewFile();

            FileChannel channel = new FileOutputStream(file, false).getChannel();
            channel.write(ByteBuffer.wrap(chunk));

            System.out.println("File [" + filename + i + "] has been stored into [" + DFS.volume + "]");
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //TODO: store metadata
    }
}
