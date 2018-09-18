package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

class Client extends Serializer {
    private InetSocketAddress storageNodeAddress;

    Client() {
        try {
            // wait for receiver and sender
            DFS.READY.await();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    void startUI() {
        Scanner scanner = new Scanner(System.in);

        String command;
        printInfo();
        while (!(command = scanner.nextLine()).equals("exit")) {
            // serialize info
            ByteString bytesCommand = ByteString.copyFromUtf8(command);
            StorageMessages.Info info = serializeInfo(StorageMessages.infoType.ASK_SUCCESSOR, bytesCommand);

            // serialize message
            ByteString data = info.toByteString();
            StorageMessages.Message message = serializeMessage(StorageMessages.messageType.INFO, data);

            // TODO: detete debug
            this.storageNodeAddress = new InetSocketAddress("localhost", 13000);
            DFS.sender.send(message, this.storageNodeAddress);

            printInfo();
        }

        DFS.receiver.close();
    }

    private void printInfo() {
        System.out.print("Please input command or type \"help\": ");
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

    public void download(String fileName, List<byte[]> chunks) {
        DataProcessor dp = new DataProcessor();

        try {
            dp.restoreFile(fileName, chunks);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    // TODO: delete debug
    public static void main(String[] args) {
        DataProcessor dp = new DataProcessor();

        try {
            List<byte[]> chunks = dp.breakFile("/Users/brian/Downloads/Cell_Phones_and_Accessories_5.json");
            System.out.println(chunks.size());
            dp.restoreFile("/Users/brian/Downloads/Cell_Phones_and_Accessories_5_copy.json", chunks);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
