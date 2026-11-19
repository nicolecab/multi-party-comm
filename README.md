# MPComm - Multiparty Communication
Very simple demo of multicast communication without coordination. The intent is to demonstrate how things can go wrong as we scale a distributed system (in this case, scaling in the geographical dimension) without taking appropriate measures to coordinate the behavior of its components. Later, this example will serve as the basis to implement coordination protocols, notably for message ordering, as part of course assignments.

## Overall structure
A set of **peer processes** is established and each process multicasts a sequence of messages to all the others at random intervals. Messages are stamped with the ID of the sending process and a local sequence number defined by the sending process. This is a simple attempt to demonstrate the problem of message ordering (or, indeed, the lack of message ordering).

The peer processes run the *peerCommunicatorUDP.py* program, which has two separate threads, one for sending and the other for receiving messages (plus a third thread that serves the RPC calls described below). After starting, a peer processes waits for a signal (from a process that coordinates the experiment) in order to actually start multicasting the sequence of messages. Each peer collects all the received messages (in the order that they were delivered, as described below) and sends the list of messages to the coordinator process, which will compare the order of messages as received by all the peers (as described next).

A separate process runs *comparisonServer.py*, which, besides coordinating the start of the peer processes (by sending the start signal mentioned above), waits for the list of received messages from each peer and compares their order. It reports a metric that indicates the occurrence of out-of-order messages (actually, the number of message exchange rounds in which at least one process received a different message form the others). In an ideal scenario, all processes should receive all the same messages in the same order. Since lost messages are recovered (as described below), all processes deliver the same set of messages, and the server also checks that each list is complete and reports the number of FIFO violations, i.e., messages from the same sender delivered in a different order from the one in which they were sent. Note, however, that FIFO ordering does not ensure that messages from different senders are delivered in the same order by all processes, so out-of-order rounds may still occur.

A second server process, running *GroupMngr.py*, is used for peers to register themselves (notably their IP addresses) as well as to discover the other peers.

Two kinds of communication are used. The multicast among the peers (handshakes, data messages, and the confirmations and retransmission requests described below) uses plain UDP sockets. All the client-server interactions, i.e., registering with the group manager and getting the list of peers from it, sending the start signal to the peers, and sending the message logs to the comparison server, are done through remote procedure calls (RPC), using the [RPyC](https://rpyc.readthedocs.io/) library. Each server process exposes its operations as an RPyC service. Note that the peers also run a small RPyC service of their own, in order to receive the start signal from the comparison server.

In order to actually see the problem of message ordering, it is necessary to introduce variability on message communication latency, such as by running the peer processes on different networks, e.g., running some of the peers in one region of the cloud, whereas the others are run on another region. This illustrates the issue of geographic scalability, when the system works fine when all its components are in the same network, but starts to behave erroneously when its components are spread across the Internet.

## Reliable delivery and FIFO ordering
Since UDP does not guarantee that datagrams are delivered, nor that they arrive in the order in which they were sent, the peers implement a simple protocol on top of it. During the handshake, each peer keeps resending its handshake to the other peers until it gets a confirmation from each of them, and only then starts sending data messages. For the data messages, losses are detected by the receivers, based on the sequence numbers: when a message arrives and some previous message from the same sender is missing, the receiver sends a negative acknowledgement (NACK) to the sender, asking for the missing messages, which are then retransmitted (each peer keeps the messages it has sent in the current round). Duplicate messages are discarded. Moreover, if no message arrives for a while (*NACK_TIMEOUT*, configured in *constMP.py*), the receiver asks again for all the messages it is still missing, which covers the loss of NACKs, of retransmissions, and of the last message sent by a peer.

As each peer knows how many messages it must receive from each of the others, no message is needed to signal the end of the sequence: a peer sends its list of messages to the comparison server as soon as it has delivered all of them, but keeps answering retransmission requests until it receives the next start signal (which is only sent after all the peers have finished).

Finally, receiving a message is distinguished from delivering it (i.e., appending it to the list of messages). A message is only delivered after all the previous messages from the same sender have been delivered; messages that arrive ahead of time are held back until the missing ones arrive. This ensures FIFO ordering: each peer delivers the messages from a given sender in the order in which they were sent. Note, however, that messages from different senders may still be delivered in different orders by different peers.

## Socket and RPC Comparison

Sockets provide direct communication between processes over a network, requiring programmers to manage connection handling, message exchange, and data serialization. RPC (Remote Procedure Call) offers a higher-level abstraction, allowing a process to invoke procedures offered by another process as if they were local calls, while the RPC library handles the underlying communication.

Therefore, RPC provides access transparency by allowing remote operations to be invoked through a simpler interface, reducing the need to manage communication details explicitly. However, this abstraction introduces additional processing overhead and may result in higher execution times compared to direct socket operations.

In the socket-based implementation, client-server interactions are handled explicitly: the programmer creates TCP sockets, establishes connections, serializes requests and responses with pickle, sends and receives data, and closes connections manually. In the RPC implementation, these steps are replaced by calls to remote methods, such as ```conn.root.register()```, ```conn.root.list()```, ```conn.root.start()``` and ```conn.root.submit_log()```, while RPyC handles the underlying communication and data serialization. Although the RPC interface simplifies these interactions, network communication remains necessary and depends on the RPC framework, whereas the socket implementation exposes these mechanisms directly and gives the programmer more control over them.

In order to better compare these implementations, AWS instances were run in different geographical setups. Three measurements were collected over five executions on the server side to evaluate the communication overhead of each implementation. Two configurations were tested: all six peers running in Virginia, and four peers in Virginia with two in Oregon. 

The following metrics were recorded:
- *Peer Discovery*: time taken by the comparison server to obtain the peer list.
- *Peer Start*: time taken to send the start signal to each peer.
- *Total Experiment*: total execution time, including the UDP message exchange and the wait for peers to transmit their logs.

| Configuration	| Peer Discovery (ms) |	Peer Start – Virginia (ms) | Peer Start – Oregon (ms) | Total Experiment (ms) |
| --- | --- | --- | --- | --- |
|Sockets – 6 Virginia|0.694 ± 0.102|0.865 ± 0.034|—|1244.214 ± 47.638|
|RPC – 6 Virginia|5.284 ± 1.166|4.228 ± 0.200|—|1291.229 ± 79.656|
|Sockets – 4 Virginia + 2 Oregon|0.659 ± 0.066|0.918 ± 0.060|124.772 ± 3.777|1760.151 ± 75.321|
|RPC – 4 Virginia + 2 Oregon|5.256 ± 0.739|4.372 ± 0.153|430.423 ± 12.477|2754.922 ± 62.152|

The results show that RPC introduced higher execution times across all measured metrics, with particularly significant differences in the geographically distributed configuration. While the total execution time was only slightly higher for RPC when all peers were in Virginia (1291 ms vs. 1244 ms), the difference increased considerably when peers were distributed between Virginia and Oregon (2755 ms vs. 1760 ms). This suggests that the overhead introduced by RPC becomes more significant in geographically distributed scenarios.

## Setup
- Create one instance (virtual machine in the cloud) to run the two servers, and a number of instances (e.g., 6 in one cloud region, 2 on another) to run the peers.
- On all the instances, install the Python dependencies: `pip install rpyc requests`
- Make sure the firewall rules (security groups in AWS jargon) allow incoming TCP traffic on ports 5678, 5679 and 5680 (used by the RPC calls) and incoming UDP traffic on port 6789 (used by the multicast), as configured in *constMP.py*.
- Edit *constMP.py* with the correct IP address of the instance where the two servers will run. Tip: allocate a fixed IP address (elastic IP in AWS jargon) to the server instance. Also edit the file to configure the number of peers processes in the experiment and, if needed, the timeout used to ask again for missing messages (*NACK_TIMEOUT*).
- On the server instance, run *GroupMngr.py*
- On some of the peer instances: run *peerCommunicatorUDP.py*
- Back on the server instance: run *comparisonServer.py*
