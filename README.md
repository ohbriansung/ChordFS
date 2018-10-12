# ChordFS

## Introduction

ChordFS is a distributed/decentralized file system that implements Chord protocol [[2]](#references). Thus, there is no coordinator (NameNode) in the architecture. The client will be interacting with the Chord network (ring) directly. Each Storage Node holds a finger (router) table with M entries, M is indicated when the ring is created. The data will be separated into chunks for storing in varies of nodes. 

## Components

### Storage Node

#### Chord Network Architecture

Example of 4 bits network:
<table>
    <tr><td>Index</td><td>Node Id</td></tr>
    <tr><td>0</td><td>CurrentId + 2^0</td></tr>
    <tr><td>1</td><td>CurrentId + 2^1</td></tr>
    <tr><td>2</td><td>CurrentId + 2^2</td></tr>
    <tr><td>3</td><td>CurrentId + 2^3</td></tr>
</table>

Successor = finger[0]

Second Successor = Successor.finger[0]

Current Node = Predecessor.finger[0]

*Second Successor is used for fail handling*

#### Initializing the ring

If creating a new Chord ring, add "--m <number_of_m>" to indicate the m bits capacity of the network, and imagine that the StorageNode network is a ring. The first node creates the finger table that all the finger is itself, and indicates the predecessor and the second successor to itself.

#### Node joins

In the paper, "We assume that the new node N learns the identity of an existing Chord node􏰕􏰂 N' by some external mechanism. Node 􏰕N uses 􏰕􏰂N' to initialize its state and add itself to the existing Chord network."

On startup, adding a parameter "--known.node <node ip:port>" in command to add itself to the existing Chord ring.

Then, new node N asks N' for its predecessor and finger table, and add itself into existing nodes' finger table.

Finally, when a new node N joins the ring, we need to move responsibility for all the keys for which node N􏰕 is now the successor.

#### Heartbeats

Each node in the ring performs heartbeats to its successor node. If a node does not receive heartbeat from its predecessor for a while, it considers it is the failure of its predecessor, and notifies other nodes to update their finger table and keys.

#### Replication

The node is responsible for storing data (chunk) with key K will replicate the data to its X successors including itself. X = 3, if total number of nudes in the ring >= 3. Otherwise, X = total number of nudes in the ring.

#### Accepts messages

Storage Node accepts requests: Upload file, download file, heartbeat, list all the details of the nodes in the network including id, address, requests, files and chunks.

#### Failures

If nodes fail and go down, the predecessor will figure out and reconcile the finger tables by contact the second successor, and update the finger table of the nodes that could store the failed node. 

### Client

Client connects to one existing StorageNode in the ring. Client maintains a buffer that stores some StorageNode that it has seen. Each request from client will be randomly sent to one of the StorageNode in the buffer. The main purpose of the buffer is failure handling, remove failed node from the buffer and get another live node to send the request. Client can ask StorageNode to present the details of the file system. It is using java.nio for faster data processing.

#### Stores data

Breaks file into chunks, asks Storage Nodes where to store them, breaks chunks into fragments, and then sends them to the appropriate node(s). The replication will be proceeded by the storage nodes, client is only responsible for send the first replica. Replacing the old data chunks if the file is already existed and remove redundant data chunks if file is smaller.

Chunk size = 64 Mb

#### Retrieves data

Retrieving files in parallel. Each chunk in the file being retrieved will be requested and transferred on a separate thread. Once the chunks are retrieved, the file is reconstructed on the client machine.

## Usage example

<details>
<summary>Start the first storage node and initialize the chord network</summary>

```shell
java -cp dfs.jar edu.usfca.cs.dfs.DFS --run storage --port 13000 --m 5 --volume /bigdata/csung4/1/
```

</details>

<details>
<summary>Start new storage node and join the chord network</summary>

```shell
java -cp dfs.jar edu.usfca.cs.dfs.DFS --run storage --port 13001 --node localhost:13000 --volume /bigdata/csung4/2/
```

</details>

<details>
<summary>Start client</summary>

```shell
java -cp dfs.jar edu.usfca.cs.dfs.DFS --run client --port 13099 --node localhost:13000
```

</details>

<details>
<summary>Client functionality</summary>

```shell
[Command]             [Usage]
upload <file_name>    Upload the file to the file system.
download <file_name>  Download the file from the file system.
connect <address>     Connect to a particular node using address:port.
list                  List all nodes in the file system and their details.
help                  List all existing commands and usages.
exit                  Terminate the program.
```

</details>

## Milestones and Checkpoints

* **Sep. 09, 2018**: design document complete, basic Protocol Buffer messaging implementation.
* **Sep. 16, 2018**: client chunking functionality, Chord network data structures and messaging.
* Checkpoint 1
* **Sep. 23, 2018**: storage node implementation, heartbeats.
* **Sep. 30, 2018**: failure detection and recovery, parallel retrievals.
* **Oct. 05, 2018**: wrap-up, client functions, retrospective.
* Checkpoint 2

## References

* [1] [University of San Francisco](https://www.usfca.edu)
* [2] [Chord paper](https://www.cs.usfca.edu/~mmalensek/cs677/schedule/papers/stoica2001chord.pdf)
* [3] [Chord WiKi](https://en.wikipedia.org/wiki/Chord_(peer-to-peer))
* [4] [Chord](https://slideplayer.com/slide/4168285/)
* [5] [Chord, DHTs, and Naming](http://www.cs.utah.edu/~stutsman/cs6963/lecture/16/)

## Author and contributors

* **Brian Sung** - *Graduate student in department of Computer Science at University of San Francisco* - [LinkedIn](https://www.linkedin.com/in/ohbriansung/)
* **Dr. Malensek** - *Assistant Professor in department of Computer Science at University of San Francisco* - [page](https://www.cs.usfca.edu/~mmalensek/)
