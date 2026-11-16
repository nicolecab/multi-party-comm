
from socket import *
import pickle
from constMP import *
import time
import csv
import os

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)


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


def mainLoop():
  while True:
    nMsgs = promptUser()

    if nMsgs == 0:
      print('Stopping.')
      serverSock.close()

      cSock = socket(AF_INET, SOCK_STREAM)
      cSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))

      req = {"op": "stop"}
      msg = pickle.dumps(req)

      cSock.sendall(msg)
      cSock.close()
      break

    experimentStart = time.perf_counter()
    experimentSuccess = False

    try:
      peerList = getListOfPeers()

      print("List of Peers: ", peerList)

      startPeers(peerList, nMsgs)

      print('Now, wait for the message logs from the communicating peers...')

      waitForLogsAndCompare(nMsgs)

      experimentSuccess = True

    finally:
      totalDuration = time.perf_counter() - experimentStart

      register_metric(
        "Total Experiment",
        totalDuration,
        experimentSuccess
      )


def promptUser():
  nMsgs = int(input(
    'Enter the number of messages for each peer to send (0 to terminate)=> '
  ))
  return nMsgs


def getListOfPeers():
  start = time.perf_counter()
  success = False
  clientSock = None

  try:
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))

    req = {"op": "list"}
    msg = pickle.dumps(req)

    clientSock.sendall(msg)

    msg = clientSock.recv(2048)
    peerList = pickle.loads(msg)

    success = True
    return peerList

  finally:
    if clientSock:
      clientSock.close()

    duration = time.perf_counter() - start

    register_metric(
      "Peer Discovery - Comparison Server",
      duration,
      success
    )


def startPeers(peerList, nMsgs):
  peerNumber = 0

  for peer in peerList:
    start = time.perf_counter()
    success = False
    clientSock = None

    try:
      clientSock = socket(AF_INET, SOCK_STREAM)
      clientSock.connect((peer, PEER_TCP_PORT))

      msg = (peerNumber, nMsgs)
      msgPack = pickle.dumps(msg)

      clientSock.sendall(msgPack)

      msgPack = clientSock.recv(512)
      print(pickle.loads(msgPack))

      success = True

    finally:
      if clientSock:
        clientSock.close()

      duration = time.perf_counter() - start
      register_metric("Peer Start", duration, success)

    peerNumber += 1


def waitForLogsAndCompare(N_MSGS):
  numPeers = 0
  msgs = []

  while numPeers < N:
    conn, addr = serverSock.accept()

    try:
      msgPack = conn.recv(32768)
      print('Received log from peer')

      msgs.append(pickle.loads(msgPack))

      conn.sendall(b"OK")

    finally:
      conn.close()

    numPeers += 1

  unordered = 0

  for j in range(0, N_MSGS - 1):
    firstMsg = msgs[0][j]

    for i in range(1, N - 1):
      if firstMsg != msgs[i][j]:
        unordered += 1
        break

  print('Found ' + str(unordered) + ' unordered message rounds')


mainLoop()
