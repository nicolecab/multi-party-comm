from constMP import *
import time
import sys
import rpyc
from rpyc.utils.server import ThreadedServer
import threading

logs = []
logs_lock = threading.Lock()
all_logs_event = threading.Event()

class ComparisonService(rpyc.Service):
	def exposed_submit_log(self, log):
		with logs_lock:
			logs.append(log)
			if len(logs)==N:
				all_logs_event.set()
		print("Received log from peer")

server = ThreadedServer(ComparisonService, port=SERVER_PORT)
t_server = threading.Thread(target=server.start, daemon=True)
t_server.start()

def mainLoop():
	cont = 1
	while 1:
		nMsgs = promptUser()
		with rpyc.connect(GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT) as conn:
			peerList = conn.root.list()
		print("List of Peers: ", peerList)
		with logs_lock:
			logs.clear()
			all_logs_event.clear()
		startPeers(peerList,nMsgs)
		if nMsgs != 0:
			print('Now, wait for the message logs from the communicating peers...')
			waitForLogsAndCompare(nMsgs)
		else:
			print('Stopping.')
			with rpyc.connect(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT) as conn:
				try:
					conn.root.stop()
				except EOFError:
					pass
			server.close()
			break

def promptUser():
	nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
	return nMsgs

def startPeers(peerList,nMsgs):
	# Connect to each of the peers and send the 'initiate' signal:
	peerNumber = 0
	for peer in peerList:
		with rpyc.connect(peer, PEER_TCP_PORT) as conn:
			try:
				print(conn.root.start(peerNumber, nMsgs))
			except EOFError:
				if nMsgs!=0:
					print('Peer ' + peer + ' failed')
					raise
		peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS):
	all_logs_event.wait()
	unordered = 0

	# Compare the lists of messages
	with logs_lock:
		msgs = list(logs)
	for j in range(0,N_MSGS-1):
		firstMsg = msgs[0][j]
		for i in range(1,N-1):
			if firstMsg != msgs[i][j]:
				unordered = unordered + 1
				break
	
	print ('Found ' + str(unordered) + ' unordered message rounds')


# Initiate server:
mainLoop()
