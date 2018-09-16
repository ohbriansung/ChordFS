package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

class Client {
    private final Receiver receiver;
    private final Sender sender;

    Client(Receiver receiver, Sender sender) {
        this.receiver = receiver;
        this.sender = sender;

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
            System.out.println("Command: " + command);

            ByteString bs = ByteString.copyFromUtf8(command);
            StorageMessages.Info info = StorageMessages.Info.newBuilder()
                    .setType(StorageMessages.Info.infoType.LIST_NODE).setData(bs).build();

            this.sender.send(info);

            printInfo();
        }

        this.receiver.close();
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
}
