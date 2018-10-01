package edu.usfca.cs.dfs.Client;

import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.Sender;

abstract class Command extends Sender {
    Command() {
        super();
    }

    void list() {

    }

    void help() {
        StringBuilder sb = new StringBuilder();

        sb.append("[Command]\t\t\t\t[Usage]").append(System.lineSeparator());
        sb.append("upload <file_name>\t\tUpload the file to the file system.").append(System.lineSeparator());
        sb.append("download <file_name>\tDownload the file from the file system.").append(System.lineSeparator());
        sb.append("list\t\t\t\t\tList all nodes in the file system and their free space.").append(System.lineSeparator());
        sb.append("help\t\t\t\t\tList all existing commands and usages.").append(System.lineSeparator());
        sb.append("exit\t\t\t\t\tTerminate the program.");

        System.out.println(sb.toString());
    }

    void exit() {
        System.out.println("Shutting down...");
        DFS.alive = false;
        DFS.receiver.close();
        close();  // close sender thread pool
        System.exit(0);
    }

    void printInfo() {
        System.out.print("> ");
    }
}
