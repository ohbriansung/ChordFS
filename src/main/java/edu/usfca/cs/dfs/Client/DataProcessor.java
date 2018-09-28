package edu.usfca.cs.dfs.Client;

import edu.usfca.cs.dfs.DFS;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;  // https://docs.oracle.com/javase/10/docs/api/java/nio/ByteBuffer.html
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class DataProcessor {

    /**
     * Use java.nio to read files faster.
     * FileChannel to stream bytes so we can read files larger than 2Gb.
     * Deep copy into ArrayList for later usage.
     *
     * @param path
     * @return List
     * @throws IOException
     */
    List<byte[]> breakFile(Path path) throws IOException {
        List<byte[]> chunks = new ArrayList<>();
        FileChannel channel = (FileChannel) Files.newByteChannel(path);
        ByteBuffer buffer = ByteBuffer.allocate(DFS.MAX_CHUNK_SIZE);

        int bytesCount;
        while ((bytesCount = channel.read(buffer)) > 0) {
            chunks.add(Arrays.copyOf(buffer.array(), bytesCount));  // deep copy
            buffer.clear();
        }
        channel.close();

        return chunks;
    }

    void restoreFile(String filename, List<byte[]> chunks) throws IOException {
        if (chunks.size() == 0) {
            return;
        }

        File file = new File(filename);
        FileChannel channel = new FileOutputStream(file).getChannel();
        for (byte[] bytes : chunks) {
            channel.write(ByteBuffer.wrap(bytes));
        }

        channel.close();
    }
}