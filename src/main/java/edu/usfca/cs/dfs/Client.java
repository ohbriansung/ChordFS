package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.List;

class Client {

    public static void main(String[] args) {
        DataProcessor dp = new DataProcessor();

        try {
            List<byte[]> chunks = dp.breakFile("/Users/brian/Downloads/movies.txt");
            dp.restoreFile("/Users/brian/Downloads/_movies.txt", chunks);
//            List<byte[]> chunks = dp.breakFile("test.txt");
//            dp.restoreFile("test-copy.txt", chunks);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
