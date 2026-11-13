import threading
import rpyc
from rpyc.utils.server import ThreadedServer
from constMP import *

membership = []
lock = threading.Lock()
stop_event = threading.Event()

class GroupManagerService(rpyc.Service):
    def exposed_register(self, ipaddr, port):
        with lock:
            membership.append((ipaddr, port))
        print("Registered peer: ", (ipaddr, port))

    def exposed_list(self):
        with lock:
            tupled_membership = tuple(pair[0] for pair in membership)
        print("Peers sent to server: ", tupled_membership)
        return tupled_membership

    def exposed_stop(self):
        print("Stopping server...")
        stop_event.set()

server = ThreadedServer(GroupManagerService, port=GROUPMNGR_TCP_PORT)
t_server = threading.Thread(target=server.start, daemon=True)
t_server.start()
stop_event.wait()
server.close()