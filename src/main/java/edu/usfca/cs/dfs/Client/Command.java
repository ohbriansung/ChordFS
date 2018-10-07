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

    String list(InetSocketAddress addr) {
        StorageMessages.Info info = serializeInfo(StorageMessages.infoType.LIST_NODE);
        String list;

        try {
            StorageMessages.Info response = list(addr, info);
            list = response.getData().toStringUtf8();
            System.out.println(list);
        } catch (IOException ignore) {
            System.out.println("Node [" + addr + "] is unreachable.");
            ((Client) DFS.currentNode).removeOneNode(addr);
            addr = ((Client) DFS.currentNode).getOneNode();
            return list(addr);
        }

        return list;
    }

    InetSocketAddress connect(String addr) {
        String[] hostAndPort = addr.split(":");
        InetSocketAddress address = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));

        try {
            heartbeat(address);
        } catch (IOException ignore) {
            System.out.println("There is no live node with address [" + addr + "]");
            return null;
        }

        return address;
    }

    void help() {
        StringBuilder sb = new StringBuilder();

        sb.append("[Command]             [Usage]").append(System.lineSeparator());
        sb.append("upload <file_name>    Upload the file to the file system.").append(System.lineSeparator());
        sb.append("download <file_name>  Download the file from the file system.").append(System.lineSeparator());
        sb.append("connect <address>     Connect to a particular node using address:port.").append(System.lineSeparator());
        sb.append("list                  List all nodes in the file system and their details.").append(System.lineSeparator());
        sb.append("help                  List all existing commands and usages.").append(System.lineSeparator());
        sb.append("exit                  Terminate the program.");

        System.out.println(sb.toString());
    }

    void exit() {
        DFS.shutdown();
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
