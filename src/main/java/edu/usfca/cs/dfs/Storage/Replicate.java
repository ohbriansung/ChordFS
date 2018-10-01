package edu.usfca.cs.dfs.Storage;

import edu.usfca.cs.dfs.Sender;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.net.InetSocketAddress;

class Replicate extends Sender implements Runnable {
    private final StorageMessages.Message m;
    private final InetSocketAddress s;

    Replicate(StorageMessages.Message m, InetSocketAddress s) {
        this.m = m;
        this.s = s;
    }

    @Override
    public void run() {
        try {
            send(this.s, this.m);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
