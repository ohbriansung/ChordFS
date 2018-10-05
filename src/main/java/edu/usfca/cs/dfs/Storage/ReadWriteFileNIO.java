package edu.usfca.cs.dfs.Storage;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

class ReadWriteFileNIO {
    private final Metadata metadata;
    private final List<StorageMessages.Message> data;

    ReadWriteFileNIO(Metadata metadata) {
        this.metadata = metadata;
        this.data = new ArrayList<>();
    }

    List<StorageMessages.Message> read() {
        List<Thread> tasks = new ArrayList<>();
        List<String> files = this.metadata.getFile();

        for (String file : files) {
            int total = this.metadata.getTotalChunk(file);
            List<Integer> chunks = this.metadata.getChunks(file);

            for (int chunk : chunks) {
                BigInteger hash = this.metadata.getHash(file, chunk);

                StorageMessages.Message.Builder b = StorageMessages.Message.newBuilder()
                        .setType(StorageMessages.messageType.DATA).setFileName(file).setTotalChunk(total)
                        .setChunkId(chunk).setHash(ByteString.copyFrom(hash.toByteArray())).setReplica(3);

                Thread task = new Thread(new ReadTask(file, chunk, b));
                tasks.add(task);
                task.start();
            }
        }

        for (Thread task : tasks) {
            try {
                task.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return this.data;
    }

    private class ReadTask implements Runnable {
        private final String filename;
        private final int id;
        private final StorageMessages.Message.Builder b;

        private ReadTask(String filename, int id, StorageMessages.Message.Builder b) {
            this.filename = filename;
            this.id = id;
            this.b = b;
        }

        @Override
        public void run() {
            try {
                Path file = Paths.get(DFS.volume + this.filename + this.id);
                byte[] data = Files.readAllBytes(file);
                this.b.setData(ByteString.copyFrom(data));

                synchronized (ReadWriteFileNIO.this.data) {
                    ReadWriteFileNIO.this.data.add(this.b.build());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
