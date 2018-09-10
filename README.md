# Project 1 - Distributed File System

## Introduction

For this project, I will be implementing a Distributed File System with Chord protocol [[2]](#references). Thus, there is no coordinator in the architecture. The client will be interacting with the Chord network (ring) directly. Each Storage Node holds a finger (router) table with M entries, M is indicated when the ring is created. The data will be separated into chunks for storing in varies of nodes. 

## Components

### Storage Node

#### Initializing the ring

If creating a new Chord ring, add "--m <number of m>" to indicate the m bits capacity of the ring. The first node indicates its predecessor as itself, and creates the finger table that all the entry is itself.

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

Storage Node accepts requests: Store chunk, get number of chunks, get chunk location, retrieve chunk, list chunks and file names, and get copy of current hash space.

#### Failures

If nodes fail and go down, the successors will figure out and reconcile the finger tables, keys, and maintain the correct number of replicas.

### Client

Client connects to one Storage Node in the ring, is able to send the requests, and also represents the details of the system.

#### Stores data

Breaking files into chunks, asking Storage Nodes where to store them, and then sending them to the appropriate node(s). The replication will be proceeded by the storage nodes, client is only responsible for send the first replica. Replacing the old data chunks if the file is already existed and remove redundant data chunks if file is smaller.

Chunk size = 16 Mb (pending)

#### Retrieves data

Retrieving files in parallel. Each chunk in the file being retrieved will be requested and transferred on a separate thread. Once the chunks are retrieved, the file is reconstructed on the client machine.

## Milestones and Checkpoints

* **Sep. 09, 2018**: design document complete, basic Protocol Buffer messaging implementation.
* **Sep. 16, 2018**: client chunking functionality, Chord network data structures and messaging.
* Checkpoint 1
* **Sep. 23, 2018**: storage node implementation, heartbeats.
* **Sep. 30, 2018**: failure detection and recovery, parallel retrievals.
* Checkpoint 2
* **Oct. 05, 2018**: wrap-up, client functions, retrospective.

## References

* [1] [University of San Francisco](https://www.usfca.edu)
* [2] [Chord](https://www.cs.usfca.edu/~mmalensek/cs677/schedule/papers/stoica2001chord.pdf)
