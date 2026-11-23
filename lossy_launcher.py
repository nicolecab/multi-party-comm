"""
Roda um peer SEM alterar o código dele, simulando perda e atraso nas
mensagens de DADOS enviadas por UDP.

Uso:
    python3 lossy_launcher.py peerCommunicatorUDP.py

Variáveis de ambiente:
    SIM_LOSS       probabilidade de descartar cada mensagem de dados (0 a 1)   [0]
    SIM_DELAY_MIN  atraso mínimo em ms                                         [0]
    SIM_DELAY_MAX  atraso máximo em ms (sorteado uniformemente por mensagem)   [0]
    SIM_SEED       seed do gerador (mesma seed => mesmas perdas)               [1]
    SIM_IP         (opcional) IP de loopback deste peer, ex.: 127.0.0.2.
                   Permite rodar vários peers na mesma máquina: o bind de
                   '0.0.0.0' vira bind nesse IP e o "IP público" informado ao
                   group manager passa a ser ele (sem chamar o ipify).

Só são afetadas as mensagens de dados (sender, seq). Handshake (READY /
READY_ACK), NACK e a mensagem de parada (-1, -1) do código original passam
intactos. Retransmissões da versão tratada são mensagens de dados e SÃO
sujeitas a perda/atraso.
"""
import atexit
import os
import pickle
import random
import runpy
import socket as _socket
import sys
import threading
import types

LOSS = float(os.getenv('SIM_LOSS', '0'))
DMIN = float(os.getenv('SIM_DELAY_MIN', '0'))
DMAX = float(os.getenv('SIM_DELAY_MAX', '0'))
SEED = int(os.getenv('SIM_SEED', '1'))
IP = os.getenv('SIM_IP')

_rng = random.Random(SEED)
_lock = threading.Lock()
_stats = {'enviadas': 0, 'descartadas': 0, 'atrasadas': 0}
_bound = set()

_orig_sendto = _socket.socket.sendto
_orig_bind = _socket.socket.bind


def _is_data(data):
    try:
        msg = pickle.loads(data)
    except Exception:
        return False
    return (isinstance(msg, tuple) and len(msg) == 2
            and isinstance(msg[0], int) and msg[0] >= 0
            and isinstance(msg[1], int))


def _bind(self, address):
    if IP and isinstance(address, tuple) and address[0] in ('0.0.0.0', ''):
        address = (IP,) + tuple(address[1:])
    _bound.add(id(self))
    return _orig_bind(self, address)


def _safe_send(sock, data, addr):
    try:
        _orig_sendto(sock, data, addr)
    except OSError:
        pass


def _sendto(self, data, *args):
    # Com SIM_IP, o socket de envio UDP precisa sair do IP do peer; senão o
    # remetente aparece como 127.0.0.1 e ACK/NACK/retransmissões voltam para o lugar errado.
    if IP and self.type == _socket.SOCK_DGRAM and id(self) not in _bound:
        try:
            _orig_bind(self, (IP, 0))
        except OSError:
            pass
        _bound.add(id(self))

    if len(args) == 1 and _is_data(data):
        with _lock:
            r = _rng.random()
            delay = _rng.uniform(DMIN, DMAX) / 1000.0
            if r < LOSS:
                _stats['descartadas'] += 1
                return len(data)
            _stats['enviadas'] += 1
            if delay > 0:
                _stats['atrasadas'] += 1
        if delay > 0:
            threading.Timer(delay, _safe_send, (self, data, args[0])).start()
            return len(data)
    return _orig_sendto(self, data, *args)


_socket.socket.bind = _bind
_socket.socket.sendto = _sendto

if IP:
    class _Resp:
        def __init__(self, content):
            self.content = content

    _fake = types.ModuleType('requests')
    _fake.get = lambda url, *a, **k: _Resp(IP.encode())
    sys.modules['requests'] = _fake


def _report():
    print('[sim] loss=%s delay=%s-%sms seed=%s enviadas=%d descartadas=%d atrasadas=%d' % (
        LOSS, DMIN, DMAX, SEED, _stats['enviadas'], _stats['descartadas'], _stats['atrasadas']),
        flush=True)


atexit.register(_report)

if len(sys.argv) < 2:
    sys.exit('uso: python3 lossy_launcher.py <peer.py>')
script = sys.argv[1]
sys.argv = sys.argv[1:]
sys.path.insert(0, os.getcwd())  # para achar o constMP.py do diretório de execução
runpy.run_path(script, run_name='__main__')