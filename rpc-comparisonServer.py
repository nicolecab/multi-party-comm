from constMP import *
import time
from metrics import register_metric, set_metrics_file

set_metrics_file("metrics_rpc.csv")
import rpyc
import threading
from rpyc.utils.server import ThreadedServer
logs = []
logs_lock = threading.Lock()
all_logs_event = threading.Event()

class ComparisonService(rpyc.Service):
  def exposed_submit_log(self, log):
    with logs_lock:
      logs.append(log)
      if len(logs) == N:
        all_logs_event.set()
    print("Received log from peer")

server = ThreadedServer(ComparisonService, port=SERVER_PORT)
t_server = threading.Thread(target=server.start, daemon=True)
t_server.start()

def mainLoop():
  while True:
    nMsgs = promptUser()
    if nMsgs == 0:
      print('Stopping.')
      startPeers(getListOfPeers(), nMsgs)
      with rpyc.connect(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT) as conn:
        try:
          conn.root.stop()
        except EOFError:
          pass
      server.close()
      break
    experimentStart = time.perf_counter()
    experimentSuccess = False
    try:
      peerList = getListOfPeers()
      print("List of Peers: ", peerList)
      with logs_lock:
        logs.clear()
        all_logs_event.clear()
      startPeers(peerList, nMsgs)
      print('Now, wait for the message logs from the communicating peers...')
      waitForLogsAndCompare(nMsgs)
      experimentSuccess = True

    finally:
      totalDuration = time.perf_counter() - experimentStart
      register_metric("Total Experiment", totalDuration, experimentSuccess)

def promptUser():
  nMsgs = int(input(
    'Enter the number of messages for each peer to send (0 to terminate)=> '
  ))
  return nMsgs

def getListOfPeers():
  start = time.perf_counter()
  success = False
  try:
    with rpyc.connect(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT) as conn:
      peerList = conn.root.list()
      success = True
      return peerList

  finally:
    duration = time.perf_counter() - start
    register_metric("Peer Discovery - Comparison Server", duration, success)

def startPeers(peerList, nMsgs):
  peerNumber = 0
  for peer in peerList:
    start = time.perf_counter()
    success = False
    try:
      with rpyc.connect(peer, PEER_TCP_PORT) as conn:
        print(conn.root.start(peerNumber, nMsgs))
        success = True
    finally:
      duration = time.perf_counter() - start
      register_metric("Peer Start", duration, success)
    peerNumber += 1

def waitForLogsAndCompare(N_MSGS):
  all_logs_event.wait()
  unordered = 0
  with logs_lock:
    msgs = list(logs)
  for j in range(N_MSGS):
    firstMsg = msgs[0][j]
    for i in range(1, N):
      if firstMsg != msgs[i][j]:
        unordered += 1
        break
  print('Found ' + str(unordered) + ' unordered message rounds')

mainLoop()
