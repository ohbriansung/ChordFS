package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.UnsupportedEncodingException;
import java.util.Scanner;

class UserInterface {
    private final Receiver receiver;
    private final Sender sender;

    UserInterface(Receiver receiver, Sender sender) {
        this.receiver = receiver;
        this.sender = sender;

        try {
            // wait for receiver and sender
            DFS.READY.await();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    void start() {
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
}
