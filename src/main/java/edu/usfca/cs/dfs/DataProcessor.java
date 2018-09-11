package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;  // https://docs.oracle.com/javase/10/docs/api/java/nio/ByteBuffer.html
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

class DataProcessor {
    private static final int MAX_CHUNK_SIZE = 16777216;  // 16Mb

    /**
     * Use java.nio to read files faster.
     * FileChannel to stream bytes so we can read files larger than 2Gb.
     * Remove the remaining of the last chunk.
     *
     * @param fileName
     * @return List
     * @throws IOException
     */
    List<byte[]> breakFile(String fileName) throws IOException {
        List<byte[]> chunks = new ArrayList<>();
        Path path = Paths.get(fileName);

        FileChannel channel = (FileChannel) Files.newByteChannel(path);
        ByteBuffer buffer = ByteBuffer.allocate(MAX_CHUNK_SIZE);
        while (channel.read(buffer) > 0) {
            int remaining = buffer.remaining();

            if (remaining == 0) {
                chunks.add(buffer.array());
            } else {
                byte[] tail = new byte[MAX_CHUNK_SIZE - remaining];
                buffer.get(tail, 0, tail.length);
                chunks.add(tail);
            }
            buffer.clear();
        }
        channel.close();

        return chunks;
    }

    void restoreFile(String fileName, List<byte[]> chunks) throws IOException {
        if (chunks.size() == 0) {
            return;
        }

        File file = new File(fileName);
        FileChannel channel = new FileOutputStream(file).getChannel();
        for (byte[] bytes : chunks) {
            channel.write(ByteBuffer.wrap(bytes));
        }

        channel.close();
    }
}
