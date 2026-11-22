from socket import *
import pickle
from constMP import *
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
# Allow immediate reuse of the port after closing
serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)

def recvAll(conn):
	# Reads until the peer closes the connection (logs can be larger than one recv)
	chunks = []
	while True:
		data = conn.recv(65536)
		if not data:
			break
		chunks.append(data)
	return b''.join(chunks)

def mainLoop():
	cont = 1
	while 1:
		nMsgs = promptUser()
		clientSock = socket(AF_INET, SOCK_STREAM)
		clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
		req = {"op":"list"}
		msg = pickle.dumps(req)
		clientSock.send(msg)
		msg = clientSock.recv(2048)
		clientSock.close()
		peerList = pickle.loads(msg)
		print("List of Peers: ", peerList)
		startPeers(peerList,nMsgs)
		if nMsgs != 0:
			print('Now, wait for the message logs from the communicating peers...')
			waitForLogsAndCompare(nMsgs)
		else:
			print('Stopping.')
			serverSock.close()
			
			# Tell group manager to stop
			cSock = socket(AF_INET, SOCK_STREAM)
			cSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
			req = {"op":"stop"}
			msg = pickle.dumps(req)
			cSock.send(msg)
			cSock.close()
			break

def promptUser():
	nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
	return nMsgs

def startPeers(peerList,nMsgs):
	# Connect to each of the peers and send the 'initiate' signal:
	peerNumber = 0
	for peer in peerList:
		clientSock = socket(AF_INET, SOCK_STREAM)
		clientSock.connect((peer, PEER_TCP_PORT))
		msg = (peerNumber,nMsgs)
		msgPack = pickle.dumps(msg)
		clientSock.send(msgPack)
		try:
			msgPack = clientSock.recv(512)
			print(pickle.loads(msgPack))
		except (EOFError, ConnectionError):
			if nMsgs != 0:
				print('Peer ' + peer + ' failed')
				raise
		clientSock.close()
		peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS):
	# Loop to wait for the message logs for comparison:
	numPeers = 0
	msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

	# Receive the logs of messages from the peer processes
	while numPeers < N:
		(conn, addr) = serverSock.accept()
		msgPack = recvAll(conn)
		print ('Received log from peer')
		conn.close()
		msgs.append(pickle.loads(msgPack))
		numPeers = numPeers + 1

	unordered = 0

	# Check that every log has the expected number of messages
	expected = N * N_MSGS
	for i in range(N):
		if len(msgs[i]) != expected:
			print('Log ' + str(i) + ' has ' + str(len(msgs[i])) + ' messages, expected ' + str(expected))

	# Check FIFO order per sender in each log
	fifoViolations = 0
	for k in range(N):
		lastSeq = [-1]*N
		for (sender, seq) in msgs[k]:
			if seq != lastSeq[sender] + 1:
				fifoViolations += 1
			lastSeq[sender] = seq
	print('Found ' + str(fifoViolations) + ' FIFO violations')

	# Compare the lists of messages (total order across peers)
	minLen = min(len(m) for m in msgs)
	for j in range(minLen):
		firstMsg = msgs[0][j]
		for i in range(1,N):
			if firstMsg != msgs[i][j]:
				unordered = unordered + 1
				break
	
	print ('Found ' + str(unordered) + ' unordered message rounds')


# Initiate server:
mainLoop()