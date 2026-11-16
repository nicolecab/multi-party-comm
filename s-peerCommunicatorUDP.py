
from socket import *
from constMP import *
import threading
import random
import time
import pickle
import csv
import os
from requests import get

handShakeCount = 0
PEERS = []

sendSocket = socket(AF_INET, SOCK_DGRAM)

recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)


def register_metric(operation, duration, success=True):
  file = "metrics.csv"
  exists = os.path.isfile(file)

  with open(file, "a", newline="") as f:
    writer = csv.writer(f)

    if not exists:
      writer.writerow(["Operation", "Duration_ms", "Success"])

    writer.writerow([
      operation,
      round(duration * 1000, 3),
      success
    ])


def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr


def registerWithGroupManager():
  ipAddr = get_public_ip()
  start = time.perf_counter()
  success = False

  try:
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))

    req = {
      "op": "register",
      "ipaddr": ipAddr,
      "port": PEER_UDP_PORT
    }

    msg = pickle.dumps(req)
    print('Registering with group manager: ', req)

    clientSock.sendall(msg)
    result = clientSock.recv(2048)
    success = result == b"OK"

    clientSock.close()

  finally:
    duration = time.perf_counter() - start
    register_metric("Peer Registration", duration, success)

  return success


def getListOfPeers():
  start = time.perf_counter()
  success = False
  clientSock = None

  try:
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))

    req = {"op": "list"}
    msg = pickle.dumps(req)

    print('Getting list of peers from group manager: ', req)

    clientSock.sendall(msg)
    msg = clientSock.recv(2048)

    peers = pickle.loads(msg)
    success = True

    print('Got list of peers: ', peers)
    return peers

  finally:
    if clientSock:
      clientSock.close()

    duration = time.perf_counter() - start
    register_metric("Peer Discovery", duration, success)


class MsgHandler(threading.Thread):

  def __init__(self, sock, peer_id):
    threading.Thread.__init__(self)
    self.sock = sock
    self.peer_id = peer_id

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')

    global handShakeCount

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

    logFile = open('logfile' + str(self.peer_id) + '.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()

    print('Sending the list of messages to the server for comparison...')

    start = time.perf_counter()
    success = False
    clientSock = None

    try:
      clientSock = socket(AF_INET, SOCK_STREAM)
      clientSock.connect((SERVER_ADDR, SERVER_PORT))

      msgPack = pickle.dumps(logList)
      clientSock.sendall(msgPack)

      result = clientSock.recv(1024)
      success = result == b"OK"

    finally:
      if clientSock:
        clientSock.close()

      duration = time.perf_counter() - start
      register_metric("Log Transmission", duration, success)

    handShakeCount = 0


def waitToStart():
  conn, addr = serverSock.accept()

  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)

  myself = msg[0]
  nMsgs = msg[1]

  conn.sendall(pickle.dumps('Peer process ' + str(myself) + ' started.'))
  conn.close()

  return myself, nMsgs


registerWithGroupManager()

while True:
  print('Waiting for signal to start...')

  myself, nMsgs = waitToStart()

  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
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
