from socket import *
from constMP import *
import threading
import random
import time
import pickle
from metrics import register_metric, set_metrics_file

set_metrics_file("metrics_rpc.csv")
import rpyc
import queue
from requests import get
from rpyc.utils.server import ThreadedServer

handShakeCount = 0
PEERS = []

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
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

def registerWithGroupManager():
  ipAddr = get_public_ip()
  start = time.perf_counter()
  success = False
  try:
    with rpyc.connect(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT) as conn:
      conn.root.register(ipAddr, PEER_UDP_PORT)
      success = True
  finally:
    duration = time.perf_counter() - start
    register_metric("Peer Registration", duration, success)

def getListOfPeers():
  start = time.perf_counter()
  success = False
  try:
    with rpyc.connect(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT) as conn:
      print('Getting list of peers from group manager: ', GROUPMNGR_ADDR)
      peers = conn.root.list()
      print('Got list of peers: ', peers)
      success = True
      return peers
  finally:
    duration = time.perf_counter() - start
    register_metric("Peer Discovery", duration, success)

class MsgHandler(threading.Thread):
  def __init__(self, sock, peer_id):
    threading.Thread.__init__(self)
    self.sock = sock
    self.peer_id = peer_id

  def run(self):
    global handShakeCount
    print('Handler is ready. Waiting for the handshakes...')
    logList = []
    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':
        handShakeCount += 1
        print('--- Handshake received: ', msg[1])
    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')
    stopCount = 0
    while True:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == -1:
        stopCount += 1
        if stopCount == N:
          break
      else:
        print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
        logList.append(msg)
    with open('logfile' + str(self.peer_id) + '.log', 'w') as logFile:
      logFile.writelines(str(logList))
    print('Sending the list of messages to the server for comparison...')
    start = time.perf_counter()
    success = False
    try:
      with rpyc.connect(SERVER_ADDR, SERVER_PORT) as conn:
        conn.root.submit_log(tuple(logList))
        success = True
    finally:
      duration = time.perf_counter() - start
      register_metric("Log Transmission", duration, success)
    handShakeCount = 0

registerWithGroupManager()

while True:
  print('Waiting for signal to start...')
  myself, nMsgs = PeerCommunicatorService.start_queue.get()
  print('I am up, and my ID is: ', str(myself))
  if nMsgs == 0:
    print('Terminating.')
    server.close()
    exit(0)
  msgHandler = MsgHandler(recvSocket, myself)
  msgHandler.start()
  print('Handler started')
  PEERS = getListOfPeers()
  for addrToSend in PEERS:
    print('Sending handshake to ', addrToSend)
    msg = ('READY', myself)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
  print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))
  while handShakeCount < N:
    pass
  for msgNumber in range(0, nMsgs):
    time.sleep(random.randrange(10, 100) / 1000)
    msg = (myself, msgNumber)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
      print('Sent message ' + str(msgNumber))
  for addrToSend in PEERS:
    msg = (-1, -1)
    msgPack = pickle.dumps(msg)

    sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
