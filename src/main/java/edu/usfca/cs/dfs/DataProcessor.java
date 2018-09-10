package edu.usfca.cs.dfs;

import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class DataProcessor {
    private static final int MAX_CHUNK_SIZE = 16777216;  // 16Mb

    List<byte[]> breakFile(String fileName) {
        List<byte[]> chunks = new ArrayList<>();

        try {
            byte[] bytes = Files.readAllBytes(Paths.get(fileName));  //TODO: need to support files larger than 2Gb.
            System.out.println(bytes.length);  //TODO: remove debug

            int from = 0;
            int to = MAX_CHUNK_SIZE > bytes.length ? bytes.length : MAX_CHUNK_SIZE;
            while (from < bytes.length) {
                chunks.add(Arrays.copyOfRange(bytes, from , to));
                from = to;
                to = from + MAX_CHUNK_SIZE > bytes.length ? bytes.length : from + MAX_CHUNK_SIZE;
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return chunks;
    }

    void restoreFile(String fileName, List<byte[]> chunks) {
        int totalBytes = (chunks.size() - 1) * MAX_CHUNK_SIZE + chunks.get(chunks.size() - 1).length;
        byte[] bytes = new byte[totalBytes];
        System.out.println(bytes.length);  //TODO: remove debug

        for (int i = 0; i < chunks.size(); i++) {
            byte[] src = chunks.get(i);
            int from = i * MAX_CHUNK_SIZE;
            System.arraycopy(src, 0, bytes, from, src.length);
        }

        try {
            Files.write(Paths.get(fileName), bytes);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
