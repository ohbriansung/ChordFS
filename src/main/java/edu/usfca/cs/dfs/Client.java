package edu.usfca.cs.dfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

class Client {

    public static void main(String[] args) {
        DataProcessor dp = new DataProcessor();

        try {
            List<byte[]> chunks = dp.breakFile("test.txt");
            dp.restoreFile("test-copy.txt", chunks);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public void upload(String fileName) throws IOException {
        Path path = Paths.get(fileName);

        if (Files.isDirectory(path)) {
            System.out.println("The path indicated a directory, please compress it then try again.");
            return;
        }

        DataProcessor dp = new DataProcessor();
        List<byte[]> chunks = dp.breakFile(fileName);

        //TODO: send chunks
    }
}
