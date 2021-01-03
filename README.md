## What is this?
This software is a work in progress.

It is a Java CLI program that uses UDP multicast to discover other nodes on the same local network running the software, and then communicate with them. The intent is to use the messaging to implement a distributed consensus protocol (e.g. Paxos).

Currently multicast messaging works (IPv4 and IPv6). The only messages exchanged are heartbeat messages.

[Basic Paxos](https://en.wikipedia.org/wiki/Paxos_%28computer_science%29#Basic_Paxos) implementation is done (minimum viable product), including basic tests and REPL for interactive testing. Current defects:
1. subsequent prepare requests are not automatically sent
2. no detection of quoroum acceptance / authorative transaction log

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
