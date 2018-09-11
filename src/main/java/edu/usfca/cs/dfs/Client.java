package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.List;

class Client {

    public static void main(String[] args) {
        DataProcessor dp = new DataProcessor();

        try {
            List<byte[]> chunks = dp.breakFile("/Users/brian/Downloads/movies.txt");
            dp.restoreFile("/Users/brian/Downloads/movies-copy.txt", chunks);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
