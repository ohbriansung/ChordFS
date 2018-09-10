package edu.usfca.cs.dfs;

import java.util.List;

class Client {

    public static void main(String[] args) {
        DataProcessor dp = new DataProcessor();

        String fileName = "test.txt";
        List<byte[]> chunks = dp.breakFile(fileName);
        dp.restoreFile("copy_" + fileName, chunks);
    }

}
