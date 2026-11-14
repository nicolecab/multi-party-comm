from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle
from requests import get
import rpyc
import queue
from rpyc.utils.server import ThreadedServer

ackQueue = queue.Queue()
sentMsgs = {}
stopHandler = threading.Event()

PEERS = []

# UDP sockets to send and receive data messages:
# Create send socket
sendSocket = socket(AF_INET, SOCK_DGRAM)
#Create and bind receive socket
recvSocket = socket(AF_INET, SOCK_DGRAM)
# Allow immediate reuse of the port after closing
recvSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

class PeerCommunicatorService(rpyc.Service):
  start_queue = queue.Queue()

  def exposed_start(self, peer_id, n_msgs):
    self.start_queue.put((peer_id, n_msgs))
    return 'Peer process ' + str(peer_id) + ' started'

server = ThreadedServer(PeerCommunicatorService, port=PEER_TCP_PORT)
t_server = threading.Thread(target=server.start, daemon=True)
t_server.start() 

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

# Function to register this peer with the group manager
def registerWithGroupManager():
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  with rpyc.connect(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT) as conn:
    conn.root.register(ipAddr, PEER_UDP_PORT)

def getListOfPeers():
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  with rpyc.connect(GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT) as conn:
    print ('Getting list of peers from group manager: ', GROUPMNGR_ADDR)
    PEERS = conn.root.list()
    print ('Got list of peers: ', PEERS)
    return PEERS

def sendHandshakeAck(addr):
  ack = pickle.dumps(('READY_ACK', myself))
  sendSocket.sendto(ack, (addr[0], PEER_UDP_PORT))

def sendNack(sender, missing):
  nack = pickle.dumps(('NACK', myself, missing))
  sendSocket.sendto(nack, (PEERS[sender], PEER_UDP_PORT))

class MsgHandler(threading.Thread):
  def __init__(self, sock, nMsgs):
    threading.Thread.__init__(self)
    self.sock = sock
    self.nMsgs = nMsgs

  def submitLog(self, logList):
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    print('Sending the list of messages to the server for comparison...')
    with rpyc.connect(SERVER_ADDR, SERVER_PORT) as conn:
      conn.root.submit_log(tuple(logList))

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')
    logList = []
    complete = False
    received = [set() for _ in range(N)]
    nextSeq = [0]*N
    self.sock.settimeout(NACK_TIMEOUT)
    while not stopHandler.is_set():
      try:
        msgPack, addr = self.sock.recvfrom(65535)
      except TimeoutError:
        for sender in range(N):
          missing = [s for s in range(self.nMsgs) if s not in received[sender]]
          if missing:
            print('Timeout - asking', sender, 'for', missing)
            sendNack(sender, missing)
        continue
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':
        sendHandshakeAck(addr)
        print('--- Handshake received: ', msg[1])
      elif msg[0] == 'READY_ACK':
        ackQueue.put(msg[1])
      elif msg[0] == 'NACK':
        print('NACK from', msg[1], 'for', msg[2])
        for seq in msg[2]:
          if seq in sentMsgs:
            sendSocket.sendto(sentMsgs[seq], (addr[0], PEER_UDP_PORT))
      else:
        sender, seq = msg
        if seq in received[sender]:
          continue
        received[sender].add(seq)
        if seq > nextSeq[sender]:
          missing = [s for s in range(nextSeq[sender], seq) if s not in received[sender]]
          print('Gap from', sender, '- asking for', missing)
          sendNack(sender, missing)
        while nextSeq[sender] in received[sender]:
          nextSeq[sender] += 1
        print('Message ' + str(seq) + ' from process ' + str(sender))
        logList.append(msg)
        if not complete and all(len(r) == self.nMsgs for r in received):
          complete = True
          self.submitLog(logList)
    exit(0)


# From here, code is executed when program starts:
registerWithGroupManager()
msgHandler = None
while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = PeerCommunicatorService.start_queue.get()
  print('I am up, and my ID is: ', str(myself))

  if msgHandler is not None:
    stopHandler.set()
    msgHandler.join()
    stopHandler.clear()

  if nMsgs == 0:
    print('Terminating.')
    exit(0)

  PEERS = getListOfPeers()
  # Create receiving message handler
  sentMsgs.clear()
  msgHandler = MsgHandler(recvSocket, nMsgs)
  msgHandler.start()
  print('Handler started')


  acked = set()
  while len(acked) < N:
    for peerId, addrToSend in enumerate(PEERS):
      if peerId not in acked:
        print('Sending handshake to ', addrToSend)
        sendSocket.sendto(pickle.dumps(('READY', myself)), (addrToSend, PEER_UDP_PORT))
    try:
      while len(acked) < N:
        acked.add(ackQueue.get(timeout=0.5))
    except queue.Empty:
      pass
  print('Main Thread: all peers confirmed the handshake.')

  # Send a sequence of data messages to all other processes 
  for msgNumber in range(0, nMsgs):
    # Wait some random time between successive messages
    time.sleep(random.randrange(10,100)/1000)
    msg = (myself, msgNumber)
    msgPack = pickle.dumps(msg)
    sentMsgs[msgNumber] = msgPack
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
      print('Sent message ' + str(msgNumber))
