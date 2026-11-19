#!/usr/bin/env python3
"""Servidor independente do sistema distribuído.

Cada instância deveria ser executada em um processo separado. O coordenador
distribui usuários, encaminha MSG_* e replica o estado para os demais nós.
"""

import base64
import codecs
import json
import math
import os
import re
import socket
import socketserver
import subprocess
import sys
import threading
import time
import random
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


def load_env(path):
    values = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip("\"'")
    return values


ENV = load_env(BASE_DIR / ".env")


def cfg(name, default=None):
    return os.environ.get(name, ENV.get(name, default))


def integer(name, default, minimum=None):
    value = int(str(cfg(name, default)))
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} deve ser >= {minimum}")
    return value


ENCODING = cfg("ENCODING", "utf-8")
codecs.lookup(ENCODING)
SERVER_IP = cfg("SERVER_IP", "127.0.0.1")


def parse_ports():
    raw = cfg("PORTS", "5001,5002,5003,5004,5005")
    try:
        ports = [int(item.strip()) for item in raw.strip("[]() ").split(",") if item.strip()]
    except ValueError as exc:
        raise ValueError(f"PORTS inválido: {raw}") from exc
    if not ports or any(port < 1 or port > 65535 for port in ports):
        raise ValueError("PORTS deve conter portas entre 1 e 65535")
    if len(set(ports)) != len(ports):
        raise ValueError("PORTS não pode conter portas repetidas")
    return ports


PORTS = parse_ports()
SERVER_PORT = integer("SERVER_PORT", PORTS[0], 1)
if SERVER_PORT not in PORTS:
    raise ValueError("SERVER_PORT deve estar presente em PORTS")
SERVER_ID = integer("SERVER_ID", PORTS.index(SERVER_PORT), 0)
CONTROL_IP = cfg("CONTROL_IP", "127.0.0.1")
CONTROL_PORT = integer("CONTROL_PORT", 6001, 1)
MAX_USERS = integer("MAX_USERS", 5, 1)
HEARTBEAT_INTERVAL = float(cfg("HEARTBEAT_INTERVAL", 2))
FAILURE_TIMEOUT = float(cfg("FAILURE_TIMEOUT", 5))
STATE_FILE = Path(cfg("STATE_FILE", str(BASE_DIR / f"servidor_{SERVER_ID}.json")))
LOG_FILE = Path(cfg("SERVER_LOG_FILE", str(BASE_DIR / f"servidor_{SERVER_ID}.jsonl")))


def parse_peers():
    peers = {}
    raw = cfg("PEERS", "")
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ident, address = item.split("@", 1)
            host, port = address.rsplit(":", 1)
            peers[int(ident)] = (host, int(port))
        except ValueError as exc:
            raise ValueError(f"PEERS inválido: {item}") from exc
    if not raw:
        peers = {index: (SERVER_IP, port) for index, port in enumerate(PORTS)}
    peers[SERVER_ID] = (SERVER_IP, SERVER_PORT)
    return peers


PEERS = parse_peers()
MSG_RE = re.compile(r"MSG_(\d{1,32})_(\d{1,18})_(\d{1,18})_(.*)", re.DOTALL)
NEWUSER_RE = re.compile(r"NEWUSER_(\d{1,18})(?:_(.*))?", re.DOTALL)
KILL_RE = re.compile(r"KILL_(\d{1,18})")
JOIN_RE = re.compile(r"JOIN_([^_]+)_(\d{1,5})")
ADDPEER_RE = re.compile(r"ADDPEER_(.*)")
JOIN_TOKEN = cfg("JOIN_TOKEN", "")


def encoded(value):
    return base64.urlsafe_b64encode(
        json.dumps(value, ensure_ascii=False).encode("utf-8")
    ).decode("ascii")


def decoded(value):
    return json.loads(base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8"))


class ServerState:
    def __init__(self):
        self.lock = threading.RLock()
        self.users = {}
        self.assignments = {}
        self.processed = set()
        self.coordinator = SERVER_ID
        self.epoch = 0
        self.load()

    def load(self):
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return
        with self.lock:
            self.users = {str(k): v for k, v in data.get("users", {}).items()}
            self.assignments = {str(k): int(v) for k, v in data.get("assignments", {}).items()}
            self.processed = set(data.get("processed", []))
            self.coordinator = int(data.get("coordinator", SERVER_ID))
            self.epoch = int(data.get("epoch", 0))

    def save(self):
        with self.lock:
            payload = {
                "users": self.users,
                "assignments": self.assignments,
                "processed": sorted(self.processed),
                "coordinator": self.coordinator,
                "epoch": self.epoch,
            }
        temporary = STATE_FILE.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(STATE_FILE)


state = ServerState()
log_lock = threading.Lock()
election_lock = threading.Lock()
launched_servers = set()


def metric(name, value=1, **details):
    entry = {
        "ts": round(time.time(), 3),
        "componente": "servidor",
        "id": SERVER_ID,
        "metrica": name,
        "valor": value,
        **details,
    }
    with log_lock:
        with LOG_FILE.open("a", encoding="utf-8") as file:
            file.write(json.dumps(entry, ensure_ascii=False) + "\n")


def send_line(server_id, message, timeout=2):
    address = PEERS.get(int(server_id))
    if not address:
        return False, "ERROR_UNKNOWN_SERVER"
    try:
        with socket.create_connection(address, timeout=timeout) as sock:
            sock.settimeout(timeout)
            sock.sendall((message + "\n").encode(ENCODING))
            reply = sock.recv(4096).decode(ENCODING, errors="replace").strip()
            return True, reply
    except OSError as exc:
        return False, str(exc)


def send_to_control(message):
    try:
        with socket.create_connection((CONTROL_IP, CONTROL_PORT), timeout=2) as sock:
            sock.sendall((message + "\n").encode(ENCODING))
            return sock.recv(4096).decode(ENCODING, errors="replace").strip()
    except OSError as exc:
        metric("falha_envio_controle", 1, erro=str(exc))
        return "ERROR_CONTROL_UNAVAILABLE"


def is_coordinator():
    with state.lock:
        return state.coordinator == SERVER_ID


def user_count(server_id):
    with state.lock:
        return sum(1 for owner in state.assignments.values() if owner == server_id)


def snapshot():
    with state.lock:
        return {
            "users": state.users,
            "assignments": state.assignments,
            "processed": sorted(state.processed),
            "coordinator": state.coordinator,
            "epoch": state.epoch,
        }


def replicate():
    payload = encoded(snapshot())
    for server_id in PEERS:
        if server_id != SERVER_ID:
            started = time.monotonic()
            ok, reply = send_line(server_id, f"SYNC_{payload}", timeout=1)
            if ok and reply == "ACK_SYNC":
                metric("atraso_replicacao",
                       time.monotonic() - started, replica=server_id)


def choose_server():
    available = [
        server_id for server_id in PEERS
        if user_count(server_id) < MAX_USERS and server_alive(server_id)
    ]
    return min(available, key=lambda server_id: (user_count(server_id), server_id)) if available else None


def server_alive(server_id):
    if server_id == SERVER_ID:
        return True
    ok, reply = send_line(server_id, "PING", timeout=min(1, FAILURE_TIMEOUT))
    return ok and reply == "PONG"


def assign_user(user_id, name):
    with state.lock:
        if str(user_id) in state.assignments:
            return state.assignments[str(user_id)]
    target = choose_server()
    if target is None:
        for next_id in sorted(PEERS):
            if next_id == SERVER_ID or server_alive(next_id):
                continue
            port = PORTS[next_id]
            started = time.monotonic()
            env = os.environ.copy()
            env.update({
                "SERVER_ID": str(next_id),
                "SERVER_PORT": str(port),
                "STATE_FILE": str(BASE_DIR / f"servidor_{next_id}.json"),
                "SERVER_LOG_FILE": str(BASE_DIR / f"servidor_{next_id}.jsonl"),
            })
            if next_id not in launched_servers:
                launched_servers.add(next_id)
                started = time.monotonic()
                subprocess.Popen([sys.executable, str(Path(__file__).resolve())], env=env)
                send_to_control(f"NEWID_{next_id}")
                metric("tempo_criacao_servidor", 0, servidor=next_id)
            for _ in range(30):
                time.sleep(0.2)
                target = choose_server()
                if target is not None:
                    metric("tempo_criacao_servidor",
                           time.monotonic() - started, servidor=next_id)
                    break
            break
        if target is None:
            return None
    payload = encoded({"id": user_id, "name": name})
    ok, reply = send_line(target, f"ASSIGN_{payload}")
    if not ok or not reply.startswith("ACK_ASSIGN"):
        return None
    with state.lock:
        state.users[str(user_id)] = name
        state.assignments[str(user_id)] = target
    replicate()
    emit_balance_metrics()
    return target


def emit_balance_metrics():
    counts = [user_count(server_id) for server_id in PEERS]
    users = sum(counts)
    if not counts:
        return
    active = sum(server_alive(server_id) for server_id in PEERS)
    metric("desbalanceamento", max(counts) - min(counts))
    metric("desvio_padrao_usuarios", math.sqrt(
        sum((count - (users / len(counts))) ** 2 for count in counts) / len(counts)
    ))
    metric("servidores_ativos", active)
    metric("escalabilidade", active / max(1, math.ceil(users / MAX_USERS)))
    metric("usuarios_sem_servidor", sum(
        1 for owner in state.assignments.values() if owner not in PEERS
    ))


def maybe_deactivate_idle_server():
    if not is_coordinator():
        return
    with state.lock:
        users = sum(1 for _ in state.users)
    required = max(1, math.ceil(users / MAX_USERS))
    candidates = [server_id for server_id in sorted(PEERS)
                  if server_id != SERVER_ID and user_count(server_id) == 0]
    excess = len([server_id for server_id in PEERS if server_alive(server_id)]) - required
    for server_id in candidates:
        if excess <= 0:
            break
        ok, reply = send_line(server_id, "SHUTDOWN")
        if ok and reply == "ACK_SHUTDOWN":
            PEERS.pop(server_id, None)
            redistribute()
            replicate()
            metric("tempo_desativacao_servidor", 0, servidor=server_id)
            excess -= 1
            for peer_id in list(PEERS):
                if peer_id != SERVER_ID:
                    send_line(peer_id, f"REMOVEPEER_{server_id}", timeout=1)


def handle_newuser(raw):
    match = NEWUSER_RE.fullmatch(raw)
    if not match:
        return "ERROR_INVALID_NEWUSER"
    user_id, name = int(match.group(1)), match.group(2) or ""
    target = assign_user(user_id, name)
    if target is None:
        return "ERROR_NO_SERVER_CAPACITY"
    return f"ACK_NEWUSER_{user_id}_{target}"


def handle_message(raw):
    match = MSG_RE.fullmatch(raw)
    if not match:
        return "ERROR_INVALID_MESSAGE"
    message_id, sender, destination, content = match.groups()
    with state.lock:
        target = state.assignments.get(str(destination))
    if target is None:
        return "ERROR_DESTINATION_NOT_FOUND"
    if target == SERVER_ID:
        return receive_message(message_id, sender, destination, content)
    payload = encoded({
        'id': message_id, 'sender': sender, 'destination': destination, 'content': content
    })
    ok, reply = send_line(target, f"DELIVER_{payload}")
    metric("mensagens_enviadas", 1)
    if not ok:
        replacement = next_available_after(target)
        if replacement is None:
            return "ERROR_DESTINATION_UNAVAILABLE"
        with state.lock:
            state.assignments[str(destination)] = replacement
            state.save()
        ok, reply = send_line(replacement, f"DELIVER_{payload}")
        if not ok:
            return "ERROR_DESTINATION_UNAVAILABLE"
        metric("mensagem_redirecionada_anel", 1, servidor_morto=target,
               proximo_servidor=replacement)
    return reply


def receive_message(message_id, sender, destination, content):
    started = time.monotonic()
    with state.lock:
        if message_id in state.processed:
            metric("acks_reenviados_mensagens_processadas", 1, msg_id=message_id)
            return f"ACK_{message_id}"
        state.processed.add(message_id)
        state.save()
    time.sleep(1)
    metric("receive_msg_execucoes", 1, msg_id=message_id)
    metric("tempo_processamento_receive_msg", time.monotonic() - started)
    replicate()
    return f"ACK_{message_id}"


def next_ring_server(ring, server_id):
    index = ring.index(server_id)
    return ring[(index + 1) % len(ring)]


def next_available_after(server_id):
    ring = sorted(PEERS)
    if len(ring) <= 1:
        return None
    start = ring.index(server_id) if server_id in ring else -1
    for offset in range(1, len(ring) + 1):
        candidate = ring[(start + offset) % len(ring)]
        if server_alive(candidate):
            return candidate
    return None


def add_peer(server_id, host, port):
    with state.lock:
        PEERS[int(server_id)] = (host, int(port))


def handle_join(raw):
    match = JOIN_RE.fullmatch(raw)
    if not match:
        return "ERROR_INVALID_JOIN"
    token, port = match.groups()
    port = int(port)
    if not JOIN_TOKEN or token != JOIN_TOKEN:
        return "ERROR_INVALID_JOIN_TOKEN"
    if port not in PORTS:
        return "ERROR_PORT_NOT_CONFIGURED"
    server_id = PORTS.index(port)
    if server_id == SERVER_ID:
        return f"ACK_JOIN_{server_id}"
    if not is_coordinator():
        ok, reply = send_line(state.coordinator, raw)
        return reply if ok else "ERROR_COORDINATOR_UNAVAILABLE"

    add_peer(server_id, SERVER_IP, port)
    payload = encoded({"id": server_id, "host": SERVER_IP, "port": port})
    for peer_id in list(PEERS):
        if peer_id != SERVER_ID:
            send_line(peer_id, f"ADDPEER_{payload}", timeout=1)
    send_to_control(f"NEWID_{server_id}")
    return f"ACK_JOIN_{server_id}"


def handle_add_peer(payload):
    data = decoded(payload)
    add_peer(int(data["id"]), data["host"], int(data["port"]))
    return f"ACK_ADDPEER_{data['id']}"


def handle_remove_peer(payload):
    try:
        server_id = int(payload)
    except ValueError:
        return "ERROR_INVALID_REMOVEPEER"
    with state.lock:
        PEERS.pop(server_id, None)
    return f"ACK_REMOVEPEER_{server_id}"


def handle_election_token(payload):
    token = decoded(payload)
    ring = [int(server_id) for server_id in token["ring"]]
    origin = int(token["origin"])
    candidate = max(int(token["candidate"]), SERVER_ID)

    if SERVER_ID == origin:
        winner = candidate
        with state.lock:
            state.coordinator = winner
            state.epoch = max(state.epoch, int(token["epoch"]))
            state.save()
        coordinator = encoded({
            "epoch": token["epoch"],
            "origin": origin,
            "winner": winner,
            "ring": ring,
        })
        if len(ring) > 1:
            ok, reply = send_line(next_ring_server(ring, SERVER_ID),
                                  f"COORDINATOR_{coordinator}")
            if not ok:
                return "ERROR_COORDINATOR_ANNOUNCEMENT"
            return reply
        return f"COORDINATOR_DONE_{winner}"

    next_server = next_ring_server(ring, SERVER_ID)
    forwarded = encoded({
        "epoch": token["epoch"],
        "origin": origin,
        "candidate": candidate,
        "ring": ring,
    })
    ok, reply = send_line(next_server, f"ELECTION_{forwarded}")
    return reply if ok else "ERROR_ELECTION_FORWARD"


def handle_coordinator_token(payload):
    token = decoded(payload)
    ring = [int(server_id) for server_id in token["ring"]]
    origin = int(token["origin"])
    winner = int(token["winner"])
    with state.lock:
        state.coordinator = winner
        state.epoch = max(state.epoch, int(token["epoch"]))
        state.save()

    if SERVER_ID == origin:
        return f"COORDINATOR_DONE_{winner}"

    forwarded = encoded(token)
    ok, reply = send_line(next_ring_server(ring, SERVER_ID),
                          f"COORDINATOR_{forwarded}")
    return reply if ok else "ERROR_COORDINATOR_FORWARD"


def elect(initiator=None):
    started = time.monotonic()
    with election_lock:
        if initiator is not None and int(initiator) != SERVER_ID:
            ok, reply = send_line(int(initiator), "ELECT_START")
            return reply if ok else "ERROR_INITIATOR_UNAVAILABLE"
        alive = [server_id for server_id in sorted(PEERS) if server_alive(server_id)]
        if not alive:
            return "ERROR_NO_SERVERS"
        with state.lock:
            state.epoch += 1
            epoch = state.epoch
            state.save()

        token = encoded({
            "epoch": epoch,
            "origin": SERVER_ID,
            "candidate": SERVER_ID,
            "ring": alive,
        })
        if len(alive) == 1:
            reply = handle_election_token(token)
        else:
            ok, reply = send_line(next_ring_server(alive, SERVER_ID),
                                  f"ELECTION_{token}")
            if not ok:
                return "ERROR_ELECTION_FORWARD"

        winner = reply.rsplit("_", 1)[-1] if reply.startswith("COORDINATOR_DONE_") else None
        if winner is None:
            return reply
        winner = int(winner)
        metric("eleicao_anel", 1, eleito=winner, participantes=alive,
               saltos_eleicao=len(alive), saltos_coordenador=len(alive))
        coordinator_address = PEERS.get(winner)
        if coordinator_address:
            host, port = coordinator_address
            send_to_control(f"NEWSERVERIP_{host}_{port}")
        if winner == SERVER_ID:
            redistribute()
        metric("tempo_eleicao", time.monotonic() - started)
        replicate()
        return f"COORDINATOR_{winner}"


def redistribute():
    with state.lock:
        users = list(state.users.items())
        old_assignments = dict(state.assignments)
        state.assignments = {}
    migrated = 0
    for user_id, name in users:
        target = assign_user(int(user_id), name)
        old = old_assignments.get(str(user_id))
        if target is not None and (old is None or int(old) != target):
            migrated += 1
    metric("usuarios_migrados_evento", migrated, evento="redistribuicao")


def handle_command(raw):
    if raw == "PING":
        return "PONG"
    if raw.startswith("JOIN_"):
        return handle_join(raw)
    if raw.startswith("ADDPEER_"):
        return handle_add_peer(raw.split("_", 1)[1])
    if raw.startswith("REMOVEPEER_"):
        return handle_remove_peer(raw.split("_", 1)[1])
    if raw == "ELECT":
        return elect()
    if raw == "ELECT_START":
        return elect()
    if raw.startswith("ELECT_"):
        try:
            return elect(int(raw.split("_", 1)[1]))
        except ValueError:
            return "ERROR_INVALID_ELECTION_INITIATOR"
    if raw.startswith("ELECTION_"):
        return handle_election_token(raw.split("_", 1)[1])
    if raw.startswith("COORDINATOR_"):
        return handle_coordinator_token(raw.split("_", 1)[1])
    if raw.startswith("NEWUSER_") and is_coordinator():
        return handle_newuser(raw)
    if raw.startswith("KILL_") and is_coordinator():
        match = KILL_RE.fullmatch(raw)
        if not match:
            return "ERROR_INVALID_KILL"
        target = int(match.group(1))
        if target == SERVER_ID:
            return "ERROR_COORDINATOR_CANNOT_KILL_ITSELF"
        ok, reply = send_line(target, "SHUTDOWN")
        if ok and reply == "ACK_SHUTDOWN":
            PEERS.pop(target, None)
            redistribute()
            return f"ACK_KILL_{target}"
        return "ERROR_SERVER_UNAVAILABLE"
    if raw.startswith("MSG_") and is_coordinator():
        return handle_message(raw)
    if raw.startswith("ASSIGN_"):
        data = decoded(raw.split("_", 1)[1])
        with state.lock:
            state.users[str(data["id"])] = data["name"]
            state.assignments[str(data["id"])] = SERVER_ID
            state.save()
        metric("usuario_associado", 1, usuario=data["id"])
        return f"ACK_ASSIGN_{data['id']}"
    if raw.startswith("DELIVER_"):
        data = decoded(raw.split("_", 1)[1])
        return receive_message(data["id"], data["sender"], data["destination"], data["content"])
    if raw.startswith("SYNC_"):
        data = decoded(raw.split("_", 1)[1])
        with state.lock:
            state.users = data["users"]
            state.assignments = {str(k): int(v) for k, v in data["assignments"].items()}
            state.processed.update(data.get("processed", []))
            state.coordinator = int(data["coordinator"])
            state.epoch = max(state.epoch, int(data["epoch"]))
            state.save()
        if data.get("sync_started"):
            metric("tempo_sincronizacao_retorno",
                   max(0.0, time.monotonic() - float(data["sync_started"])))
        metric("atraso_replicacao", 0)
        return "ACK_SYNC"
    if raw == "SYNC_REQUEST":
        payload = dict(snapshot())
        payload["sync_started"] = time.monotonic()
        return f"SYNC_{encoded(payload)}"
    return "ERROR_INVALID_COMMAND"


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        for raw in self.rfile:
            try:
                command = raw.decode(ENCODING).strip()
                if command == "SHUTDOWN":
                    self.wfile.write(b"ACK_SHUTDOWN\n")
                    self.wfile.flush()
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                    return
                reply = handle_command(command)
            except (ValueError, KeyError, json.JSONDecodeError, UnicodeError) as exc:
                reply = f"ERROR_{type(exc).__name__.upper()}"
            self.wfile.write((reply + "\n").encode(ENCODING))
            self.wfile.flush()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def send_random_user_messages():
    with state.lock:
        users = list(state.users)
    if len(users) < 2:
        return
    sender = random.choice(users)
    destination = random.choice([user_id for user_id in users if user_id != sender])
    message_id = f"{int(time.time() * 1000)}-{SERVER_ID}"
    content = random.choice(["Ola", "Tudo bem?", "Mensagem automatica"])
    sent = 0
    attempts = 0
    for attempt in range(3):
        attempts += 1
        reply = handle_message(
            f"MSG_{message_id}_{sender}_{destination}_{content}"
        )
        if reply.startswith("ACK_"):
            sent += 1
            break
        time.sleep(0.2)
        metric("mensagens_perdidas", 1, msg_id=message_id)
    if attempts > 1:
        metric("reenvios_mensagens", attempts - 1,
               mensagens_enviadas=max(sent, 1), msg_id=message_id)
    metric("mensagens_confirmadas", sent, msg_id=message_id)


def monitor():
    time.sleep(HEARTBEAT_INTERVAL)
    while True:
        if is_coordinator():
            for server_id in list(PEERS):
                if server_id != SERVER_ID and not server_alive(server_id):
                    metric("tempo_deteccao_falha", FAILURE_TIMEOUT, servidor=server_id)
                    redistribute()
            send_random_user_messages()
            maybe_deactivate_idle_server()
        else:
            if not server_alive(state.coordinator):
                elect()
        time.sleep(random.uniform(0.1, 10.0))


def main():
    server = Server((SERVER_IP, SERVER_PORT), Handler)
    with state.lock:
        state.coordinator = state.coordinator if state.coordinator in PEERS else SERVER_ID
        state.save()
    for peer_id in PEERS:
        if peer_id != SERVER_ID:
            ok, reply = send_line(peer_id, "SYNC_REQUEST", timeout=1)
            if ok and reply.startswith("SYNC_"):
                try:
                    data = decoded(reply.split("_", 1)[1])
                    with state.lock:
                        state.users = data["users"]
                        state.assignments = {str(k): int(v) for k, v in data["assignments"].items()}
                        state.processed.update(data.get("processed", []))
                        state.coordinator = int(data["coordinator"])
                        state.epoch = max(state.epoch, int(data["epoch"]))
                        state.save()
                    metric("tempo_sincronizacao_retorno", 0)
                    break
                except (ValueError, KeyError, json.JSONDecodeError):
                    pass
    try:
        time.sleep(random.uniform(1, 2))
        send_to_control(f"NEWID_{SERVER_ID}")
        if JOIN_TOKEN:
            time.sleep(random.uniform(1, 2))
            ok, reply = send_line(state.coordinator,
                                  f"JOIN_{JOIN_TOKEN}_{SERVER_PORT}")
            if not ok or not reply.startswith("ACK_JOIN_"):
                metric("falha_entrada_anel", 1, resposta=reply)
    except (OSError, socket.error) as exc:
        metric("registro_inicial_pendente", 1, erro=str(exc))
        time.sleep(random.uniform(1, 2))
    threading.Thread(target=monitor, daemon=True).start()
    print(f"Servidor {SERVER_ID} em {SERVER_IP}:{SERVER_PORT}; coordenador {state.coordinator}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
