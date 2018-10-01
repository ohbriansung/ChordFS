package edu.usfca.cs.dfs.FileTransfer;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Called by RequestHandler. Thus, it's already multi-threaded.
 * Responsible for storing file chunks on disk.
 */
public class StorageProcess {
    private final StorageMessages.Message message;

    public StorageProcess(StorageMessages.Message message) {
        this.message = message;
    }

    public void store() {
        String filename = this.message.getFileName();
        int i = this.message.getChunkId();
        byte[] chunk = this.message.getData().toByteArray();

        try {
            File file = new File(DFS.volume + filename + i);
            if (file.exists()) {
                // override file
                file.delete();
            }
            file.createNewFile();

            FileChannel channel = new FileOutputStream(file).getChannel();
            channel.write(ByteBuffer.wrap(chunk));

            System.out.println("File [" + filename + i + "] has been stored into [" + DFS.volume + "]");
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public StorageMessages.Message retrieve() throws IOException {
        String filename = this.message.getFileName();
        int i = this.message.getChunkId();

        Path file = Paths.get(DFS.volume + filename + i);
        byte[] chunk = Files.readAllBytes(file);

        return StorageMessages.Message.newBuilder().setData(ByteString.copyFrom(chunk)).build();
    }
}
