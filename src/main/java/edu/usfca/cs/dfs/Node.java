package edu.usfca.cs.dfs;

class Node {
    private final String host;
    private final int port;
    private Integer id;
    private String successor;
    private Integer successorId;
    private String predecessor;

    Node(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Directly deserialize from StorageMessages.Node
     *
     * @param node
     */
    Node(StorageMessages.Node node) {
        this.host = node.getHost();
        this.port = node.getPort();
        this.id = node.getId();
        this.successor = node.getSuccessor();
        this.successorId = node.getSuccessorId();
        this.predecessor = node.getPredecessor();
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

    public void setSuccessorId(Integer successorId) {
        this.successorId = successorId;
    }

    void setSuccessor(String successor) {
        this.successor = successor;
    }

    void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    String getHost() {
        return this.host;
    }

    int getPort() {
        return this.port;
    }

    int getId() {
        return this.id;
    }

    String getSuccessor() {
        return this.successor;
    }

    Integer getSuccessorId() {
        return successorId;
    }

    String getPredecessor() {
        return this.predecessor;
    }

    StorageMessages.Node serialize() {
        // serialize node
        StorageMessages.Node.Builder builder = StorageMessages.Node.newBuilder().setHost(this.host).setPort(this.port);

        if (this.id != null) {
            builder.setId(this.id);
        }

        if (this.successor != null) {
            builder.setSuccessor(this.successor);
        }

        if (this.successorId != null) {
            builder.setSuccessorId(this.successorId);
        }

        if (this.predecessor != null) {
            builder.setPredecessor(this.predecessor);
        }

        return builder.build();
    }

    String getAddress() {
        return this.host + ":" + this.port;
    }

    private void printId() {
        System.out.println("Current node id: " + this.id);
    }
}