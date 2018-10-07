package edu.usfca.cs.dfs.Storage;

import edu.usfca.cs.dfs.Sender;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.net.InetSocketAddress;

class Backup extends Sender implements Runnable {
    private final InetSocketAddress addr;
    private final StorageMessages.Message m;

    Backup(InetSocketAddress addr, StorageMessages.Message m) {
        this.addr = addr;
        this.m = m;
    }

    @Override
    public void run() {
        try {
            send(this.addr, this.m);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
