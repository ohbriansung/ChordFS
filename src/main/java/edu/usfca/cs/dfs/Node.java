package edu.usfca.cs.dfs;

import java.net.InetSocketAddress;

class Node {
    private final InetSocketAddress address;

    private Integer id;
    private Node successor;
    private Node predecessor;

    Node(String host, int port) {
        this.address = new InetSocketAddress(host, port);
    }

    /**
     * Immutable id.
     *
     * @param id
     */
    void setId(int id) {
        if (this.id == null) {
            this.id = id;
            printId();
        } else {
            System.out.println("Failed to set id: id has already been set.");
        }
    }

    void setSuccessor(Node successor) {
        this.successor = successor;
    }

    void setPredecessor(Node predecessor) {
        this.predecessor = predecessor;
    }

    InetSocketAddress getAddress() {
        return this.address;
    }

    int getId() {
        return this.id;
    }

    Node getSuccessor() {
        return this.successor;
    }

    Node getPredecessor() {
        return this.predecessor;
    }

    private void printId() {
        System.out.println("Current node id: " + this.id);
    }
}
