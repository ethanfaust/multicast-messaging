## What is this?
This software is a work in progress.

It is a Java CLI program that uses UDP multicast to discover other nodes on the same local network running the software, and then communicate with them. The messaging layer is used to run a distributed consensus protocol ([Paxos](https://en.wikipedia.org/wiki/Paxos_%28computer_science%29)).

## Status

### Messaging
Currently multicast messaging works (IPv4 and IPv6). The only messages exchanged are heartbeat messages.

### Paxos
[Basic Paxos](https://en.wikipedia.org/wiki/Paxos_%28computer_science%29#Basic_Paxos) implementation is done (minimum viable product), including basic tests and REPL for interactive testing.

### Current defects
1. Subsequent prepare requests are not automatically sent
2. No detection of quorum acceptance / authorative transaction log
   ```
   "An Acceptor can accept multiple proposals... This can happen when another Proposer, unaware of the new value being decided, starts a new round with a higher identification number n... These proposals may even have different values"
   ```
   (see ```PaxosNodeTest.testConflictingPrepareSequence2```)
3. Heartbeats are currently used for discovery but there is no threshold/logic for failure detection

## How to build
```
mvn package
```

My environment:
```
$ mvn --version | grep version
Java version: 11.0.9.1, vendor: Ubuntu, runtime: /usr/lib/jvm/java-11-openjdk-amd64
OS name: "linux", version: "5.8.0-7630-generic", arch: "amd64", family: "unix"
```

## How to run
```
java -cp target/collab-1.0-SNAPSHOT.jar dev.efaust.collab.Collab
```
