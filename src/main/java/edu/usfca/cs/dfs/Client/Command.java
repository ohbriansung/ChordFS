package edu.usfca.cs.dfs.Client;

import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.Sender;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.net.InetSocketAddress;

abstract class Command extends Sender {
    Command() {
        super();
    }

    void list(InetSocketAddress addr) {
        StorageMessages.Info info = serializeInfo(StorageMessages.infoType.LIST_NODE);

        try {
            StorageMessages.Info response = list(addr, info);
            String[] list = response.getData().toStringUtf8().split("\\s+");

            System.out.println("[Node]\t[Address]\t\t\t[FreeSpace]\t[Request]");
            for (int i = 0; i < list.length; i += 5) {
                System.out.println(list[i] + "\t\t" + list[i + 1] + "\t" + list[i + 2] + " " + list[i + 3] + "\t" + list[i + 4]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void file(String addr) {
        String[] temp = addr.split(":");
        StorageMessages.Info info = serializeInfo(StorageMessages.infoType.LIST_FILE);

        InetSocketAddress n;
        try {
            n = new InetSocketAddress(temp[0], Integer.parseInt(temp[1]));
        } catch (NullPointerException | NumberFormatException ignore) {
            System.out.println("Invalid address [" + addr + "]");
            return;
        }

        try {
            StorageMessages.Info response = list(n, info);

            System.out.println("[File]\t\t\t[Total]\t[Chunks]");
            System.out.println(response.getData().toStringUtf8());

            ((Client) DFS.currentNode).addOneNode(n);
        } catch (IOException e) {
            System.out.println("Node [" + addr + "] is unreachable.");
            ((Client) DFS.currentNode).removeOneNode(n);
        }
    }

    void help() {
        StringBuilder sb = new StringBuilder();

        sb.append("[Command]\t\t\t\t[Usage]").append(System.lineSeparator());
        sb.append("upload <file_name>\t\tUpload the file to the file system.").append(System.lineSeparator());
        sb.append("download <file_name>\tDownload the file from the file system.").append(System.lineSeparator());
        sb.append("list\t\t\t\t\tList all nodes in the file system and their free space.").append(System.lineSeparator());
        sb.append("file -l <node_address>\tList all files and chunks stored on a particular node.").append(System.lineSeparator());
        sb.append("help\t\t\t\t\tList all existing commands and usages.").append(System.lineSeparator());
        sb.append("exit\t\t\t\t\tTerminate the program.");

        System.out.println(sb.toString());
    }

    void exit() {
        System.out.println("Shutting down...");
        DFS.alive = false;
        DFS.receiver.close();
        super.close();  // close sender thread pool
        System.exit(0);
    }

    void printInfo() {
        System.out.print("> ");
    }

    void invalid() {
        System.out.println("Invalid command.");
        help();
    }
}
