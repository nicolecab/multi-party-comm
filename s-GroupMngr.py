from socket import *
import pickle
from constMP import *

port = GROUPMNGR_TCP_PORT
membership = []

def serverLoop():
  serverSock = socket(AF_INET, SOCK_STREAM)
  serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
  serverSock.bind(('0.0.0.0', port))
  serverSock.listen(6)
  while True:
    conn, addr = serverSock.accept()
    try:
      msgPack = conn.recv(2048)
      req = pickle.loads(msgPack)
      if req["op"] == "register":
        membership.append((req["ipaddr"], req["port"]))
        print('Registered peer: ', req)
        conn.sendall(b"OK")
      elif req["op"] == "list":
        peerList = []
        for member in membership:
          peerList.append(member[0])
        print('List of peers sent to client: ', peerList)
        conn.sendall(pickle.dumps(peerList))
      elif req["op"] == "unregister":
        pass
      elif req["op"] == "stop":
        print("Stopping.")
        break
      else:
        conn.sendall(b"ERROR")
    finally:
      conn.close()
  serverSock.close()

serverLoop()
