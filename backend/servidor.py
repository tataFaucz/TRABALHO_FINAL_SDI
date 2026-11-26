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
SERVER_IP = cfg("SERVER_IP")
SERVER_PORT_RAW = cfg("SERVER_PORT")
if not SERVER_IP or not str(SERVER_IP).strip():
    raise ValueError("SERVER_IP é obrigatório no .env ou nas variáveis de ambiente")
if SERVER_PORT_RAW is None or not str(SERVER_PORT_RAW).strip():
    raise ValueError("SERVER_PORT é obrigatório no .env ou nas variáveis de ambiente")
SERVER_PORT = integer("SERVER_PORT", None, 1)


def parse_ports():
    raw = cfg("PORTS", str(SERVER_PORT))
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
if SERVER_PORT not in PORTS:
    raise ValueError("SERVER_PORT deve estar presente em PORTS")
SERVER_ID = integer("SERVER_ID", PORTS.index(SERVER_PORT), 0)
if SERVER_ID != PORTS.index(SERVER_PORT):
    raise ValueError("SERVER_ID deve corresponder à posição de SERVER_PORT em PORTS")
INITIAL_SERVER_ID = integer("INITIAL_SERVER_ID", 0, 0)
CONTROL_IP = cfg("CONTROL_IP", "127.0.0.1")
CONTROL_PORT = integer("CONTROL_PORT", 6010, 1)
MAX_USERS = integer("MAX_USERS", 5, 1)
HEARTBEAT_INTERVAL = float(cfg("HEARTBEAT_INTERVAL", 2))
FAILURE_TIMEOUT = float(cfg("FAILURE_TIMEOUT", 5))
SERVER_RECOVERY_DELAY = 5.0
STATE_FILE = Path(cfg("STATE_FILE", str(BASE_DIR / f"servidor_{SERVER_ID}.json")))
LOG_FILE = Path(cfg("SERVER_LOG_FILE", str(BASE_DIR / f"servidor_{SERVER_ID}.jsonl")))
STATE_DIR = Path(cfg("STATE_DIR", str(STATE_FILE.parent)))
STATE_FILE_EXISTS_AT_START = STATE_FILE.is_file()


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
DELUSER_RE = re.compile(r"DELUSER_(\d{1,18})")
KILL_RE = re.compile(r"KILL_(\d{1,18})")
JOIN_RE = re.compile(r"JOIN_([^_]+)_(\d{1,5})")
ADDPEER_RE = re.compile(r"ADDPEER_(.*)")
JOIN_TOKEN = cfg("JOIN_TOKEN", "")
ALLOW_LOCAL_BOOTSTRAP = str(cfg("ALLOW_LOCAL_BOOTSTRAP", "0")).strip().lower() in {
    "1", "true", "yes", "on"
}


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
        self.messages = {}
        self.in_flight = {}
        self.deactivated_servers = set()
        self.coordinator = INITIAL_SERVER_ID
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
            self.messages = {
                str(k): v for k, v in data.get("messages", {}).items()
            }
            self.deactivated_servers = {
                int(server_id) for server_id in data.get("deactivated_servers", [])
            }
            self.coordinator = int(data.get("coordinator", SERVER_ID))
            self.epoch = int(data.get("epoch", 0))

    def save(self):
        with self.lock:
            payload = {
                "users": dict(self.users),
                "assignments": dict(self.assignments),
                "processed": sorted(self.processed),
                "messages": {
                    key: dict(value) for key, value in self.messages.items()
                },
                "deactivated_servers": sorted(self.deactivated_servers),
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
user_sender_lock = threading.Lock()
user_senders = {}
user_sender_events = {}
assignment_lock = threading.Lock()
allocation_lock = threading.RLock()
assigning_users = set()
shutdown_event = threading.Event()
recovery_lock = threading.Lock()
redistribution_lock = threading.Lock()
recovering_servers = set()
deactivating_servers = set()
failure_handled = set()
last_seen = {}
ever_seen_servers = set()


COORDINATOR_METRICS = {
    "desbalanceamento",
    "desvio_padrao_usuarios",
    "usuarios_migrados_evento",
    "servidores_ativos",
    "escalabilidade",
    "tempo_criacao_servidor",
    "tempo_desativacao_servidor",
    "tempo_deteccao_falha",
    "tempo_redistribuicao",
    "usuarios_sem_servidor",
}


def metric(name, value=1, **details):
    entry = {
        "ts": round(time.time(), 3),
        "componente": (
            "coordenador"
            if name in COORDINATOR_METRICS and is_coordinator()
            else "servidor"
        ),
        "id": SERVER_ID,
        "metrica": name,
        "valor": value,
        **details,
    }
    with log_lock:
        with LOG_FILE.open("a", encoding="utf-8") as file:
            file.write(json.dumps(entry, ensure_ascii=False) + "\n")


def user_metric(user_id, name, value=1, **details):
    entry = {
        "ts": round(time.time(), 3),
        "componente": "usuario",
        "id": int(user_id),
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
            with sock.makefile("rb") as stream:
                response = stream.readline()
            if not response:
                return False, "ERROR_EMPTY_RESPONSE"
            reply = response.decode(ENCODING, errors="replace").strip()
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
            "users": dict(state.users),
            "assignments": dict(state.assignments),
            "processed": sorted(state.processed),
            "messages": {
                key: dict(value) for key, value in state.messages.items()
            },
            "deactivated_servers": sorted(state.deactivated_servers),
            "coordinator": state.coordinator,
            "epoch": state.epoch,
        }


def replicate():
    data = snapshot()
    data["replication_sent_at"] = time.time()
    payload = encoded(data)
    for server_id in PEERS:
        if server_id != SERVER_ID:
            ok, reply = send_line(server_id, f"SYNC_{payload}", timeout=1)
            if not ok or reply != "ACK_SYNC":
                metric("falha_replicacao", 1, replica=server_id,
                       resposta=reply)


def choose_server():
    available = [
        server_id for server_id in PEERS
        if server_id not in state.deactivated_servers
        and server_id not in deactivating_servers
        and user_count(server_id) < MAX_USERS and server_alive(server_id)
    ]
    return min(available, key=lambda server_id: (user_count(server_id), server_id)) if available else None


def server_alive(server_id):
    if server_id == SERVER_ID:
        return True
    ok, reply = send_line(server_id, "PING", timeout=min(1, FAILURE_TIMEOUT))
    alive = ok and reply == "PONG"
    if alive:
        last_seen[server_id] = time.monotonic()
        ever_seen_servers.add(server_id)
        failure_handled.discard(server_id)
    return alive


def can_bootstrap_local_state():
    return (
        ALLOW_LOCAL_BOOTSTRAP
        and SERVER_ID == INITIAL_SERVER_ID
        and STATE_FILE_EXISTS_AT_START
        and state.coordinator == SERVER_ID
    )


def assign_user(user_id, name):
    user_key = str(user_id)
    with assignment_lock:
        with state.lock:
            if user_key in state.assignments:
                return state.assignments[user_key]
        if user_key in assigning_users:
            return None
        assigning_users.add(user_key)
    try:
        with allocation_lock:
            return _assign_user(user_id, name)
    finally:
        with assignment_lock:
            assigning_users.discard(user_key)


def _assign_user(user_id, name):
    with state.lock:
        if str(user_id) in state.assignments:
            return state.assignments[str(user_id)]
    target = choose_server()
    if target is None:
        for next_id in sorted(PEERS):
            if (next_id == SERVER_ID or next_id in recovering_servers
                    or (next_id in ever_seen_servers
                        and next_id not in state.deactivated_servers)
                    or server_alive(next_id)):
                continue
            started = time.monotonic()
            if next_id not in launched_servers:
                with state.lock:
                    was_deactivated = next_id in state.deactivated_servers
                    state.deactivated_servers.discard(next_id)
                    state.save()
                if was_deactivated:
                    replicate()
                launched_servers.add(next_id)
                try:
                    start_server_process(next_id)
                except OSError as exc:
                    launched_servers.discard(next_id)
                    metric("falha_criacao_servidor", 1, servidor=next_id,
                           erro=str(exc))
                    continue
            for _ in range(30):
                time.sleep(0.2)
                target = choose_server()
                if target is not None:
                    metric("tempo_criacao_servidor",
                           time.monotonic() - started, servidor=next_id)
                    metric("usuarios_migrados_evento", 0,
                           evento="criacao", servidor=next_id)
                    break
            if target is not None:
                break
            launched_servers.discard(next_id)
            metric("falha_criacao_servidor", 1, servidor=next_id,
                   erro="servidor não ficou disponível no prazo")
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
    if target == SERVER_ID:
        start_user_sender(str(user_id))
    return target


def start_server_process(server_id):
    env = os.environ.copy()
    env.update({
        "SERVER_ID": str(server_id),
        "SERVER_PORT": str(PORTS[server_id]),
        "STATE_DIR": str(STATE_DIR),
        "STATE_FILE": str(STATE_DIR / f"servidor_{server_id}.json"),
        "SERVER_LOG_FILE": str(STATE_DIR / f"servidor_{server_id}.jsonl"),
    })
    return subprocess.Popen([sys.executable, str(Path(__file__).resolve())], env=env)


def emit_balance_metrics():
    with state.lock:
        peer_ids = list(PEERS)
        assignments = dict(state.assignments)
        registered_users = set(state.users)
        deactivated = set(state.deactivated_servers)
    active_peer_ids = [
        server_id for server_id in peer_ids
        if server_id not in deactivated and server_alive(server_id)
    ]
    counts = [
        sum(1 for owner in assignments.values() if owner == server_id)
        for server_id in active_peer_ids
    ]
    if not active_peer_ids:
        return
    users = len(registered_users)
    active = len(active_peer_ids)
    metric("desbalanceamento", max(counts) - min(counts))
    metric("desvio_padrao_usuarios", math.sqrt(
        sum((count - (users / len(counts))) ** 2 for count in counts) / len(counts)
    ))
    metric("servidores_ativos", active)
    metric("escalabilidade", active / max(1, math.ceil(users / MAX_USERS)))
    metric("usuarios_sem_servidor", sum(
        1 for user_id in registered_users
        if assignments.get(user_id) not in active_peer_ids
    ))


def maybe_deactivate_idle_server():
    with allocation_lock:
        _deactivate_idle_servers()


def _deactivate_idle_servers():
    if not is_coordinator():
        return
    with state.lock:
        users = sum(1 for _ in state.users)
        deactivated = set(state.deactivated_servers)
    required = max(1, math.ceil(users / MAX_USERS))
    candidates = [server_id for server_id in sorted(PEERS)
                  if server_id != SERVER_ID and server_id not in deactivated
                  and user_count(server_id) == 0 and server_alive(server_id)]
    excess = sum(
        1 for server_id in PEERS
        if server_id not in deactivated and server_alive(server_id)
    ) - required
    for server_id in candidates:
        if excess <= 0:
            break
        with state.lock:
            state.deactivated_servers.add(server_id)
            state.save()
        replicate()
        deactivation_started = time.monotonic()
        ok, reply = send_line(server_id, "SHUTDOWN")
        if ok and reply == "ACK_SHUTDOWN":
            while (time.monotonic() - deactivation_started < FAILURE_TIMEOUT
                   and server_alive(server_id)):
                time.sleep(0.05)
            if server_alive(server_id):
                with state.lock:
                    state.deactivated_servers.discard(server_id)
                    state.save()
                replicate()
                metric("falha_desativacao_servidor", 1,
                       servidor=server_id, motivo="servidor ainda responde")
                continue
            metric("tempo_desativacao_servidor",
                   time.monotonic() - deactivation_started, servidor=server_id)
            metric("usuarios_migrados_evento", 0,
                   evento="remocao", servidor=server_id)
            send_to_control(f"REMOVEDID_{server_id}")
            excess -= 1
            launched_servers.discard(server_id)
            last_seen.pop(server_id, None)
            emit_balance_metrics()
        else:
            with state.lock:
                state.deactivated_servers.discard(server_id)
                state.save()
            replicate()


def compact_servers():
    with allocation_lock:
        with state.lock:
            total_users = len(state.users)
            coordinator = state.coordinator
            peer_ids = list(PEERS)
            deactivated = set(state.deactivated_servers)
        active = [
            server_id for server_id in peer_ids
            if server_id not in deactivated and server_alive(server_id)
        ]
        required = max(1, math.ceil(total_users / MAX_USERS))
        ordered = ([coordinator] if coordinator in active else [])
        ordered.extend(server_id for server_id in sorted(active)
                       if server_id not in ordered)
        victims = ordered[required:]
        with state.lock:
            deactivating_servers.update(victims)

        for server_id in victims:
            with state.lock:
                affected = [
                    (user_id, state.users[user_id])
                    for user_id, owner in state.assignments.items()
                    if owner == server_id and user_id in state.users
                ]
            redistribution_started = time.monotonic()
            migrated = 0
            all_migrated = True
            try:
                for user_id, name in affected:
                    with state.lock:
                        state.assignments.pop(user_id, None)
                        state.save()
                    target = assign_user(int(user_id), name)
                    if target is None:
                        with state.lock:
                            state.assignments[user_id] = server_id
                            state.save()
                        replicate()
                        all_migrated = False
                        break
                    migrated += 1

                metric("usuarios_migrados_evento", migrated,
                       evento="reducao_demanda", servidor=server_id)
                metric("tempo_redistribuicao",
                       time.monotonic() - redistribution_started,
                       servidor=server_id,
                       usuarios=len(affected))
                if not all_migrated:
                    continue

                with state.lock:
                    state.deactivated_servers.add(server_id)
                    state.save()
                replicate()
                deactivation_started = time.monotonic()
                ok, reply = send_line(server_id, "SHUTDOWN")
                if not ok or reply != "ACK_SHUTDOWN":
                    with state.lock:
                        state.deactivated_servers.discard(server_id)
                        state.save()
                    replicate()
                    metric("falha_desativacao_servidor", 1,
                           servidor=server_id, resposta=reply)
                    continue

                while (time.monotonic() - deactivation_started < FAILURE_TIMEOUT
                       and server_alive(server_id)):
                    time.sleep(0.05)
                if server_alive(server_id):
                    with state.lock:
                        state.deactivated_servers.discard(server_id)
                        state.save()
                    replicate()
                    metric("falha_desativacao_servidor", 1,
                           servidor=server_id,
                           motivo="servidor ainda responde")
                    continue
                metric("tempo_desativacao_servidor",
                       time.monotonic() - deactivation_started,
                       servidor=server_id,
                       motivo="reducao_demanda")
                launched_servers.discard(server_id)
                last_seen.pop(server_id, None)
                send_to_control(f"REMOVEDID_{server_id}")
            finally:
                with state.lock:
                    deactivating_servers.discard(server_id)
        emit_balance_metrics()


def handle_newuser(raw):
    match = NEWUSER_RE.fullmatch(raw)
    if not match:
        return "ERROR_INVALID_NEWUSER"
    user_id, name = int(match.group(1)), match.group(2) or ""
    target = assign_user(user_id, name)
    if target is None:
        return "ERROR_NO_SERVER_CAPACITY"
    return f"ACK_NEWUSER_{user_id}_{target}"


def handle_delete_user(raw):
    match = DELUSER_RE.fullmatch(raw)
    if not match:
        return "ERROR_INVALID_DELUSER"
    user_id = match.group(1)
    with allocation_lock:
        with state.lock:
            if user_id not in state.users:
                return "ERROR_USER_NOT_FOUND"
            owner = state.assignments.pop(user_id, None)
            state.users.pop(user_id)
            state.save()
        if owner == SERVER_ID:
            stop_user_sender(user_id)
        replicate()
        emit_balance_metrics()
        compact_servers()
    return f"ACK_DELUSER_{user_id}"


def handle_message(raw):
    match = MSG_RE.fullmatch(raw)
    if not match:
        return "ERROR_INVALID_MESSAGE"
    message_id, sender, destination, content = match.groups()
    with state.lock:
        if str(sender) not in state.assignments:
            return "ERROR_SENDER_NOT_FOUND"
        target = state.assignments.get(str(destination))
    if target is None:
        return "ERROR_DESTINATION_NOT_FOUND"
    if target == SERVER_ID:
        return receive_message(message_id, sender, destination, content)
    payload = encoded({
        'id': message_id, 'sender': sender, 'destination': destination, 'content': content
    })
    candidates = [target] + [
        server_id for server_id in sorted(PEERS) if server_id != target
    ]
    for candidate in candidates:
        if not server_alive(candidate):
            continue
        for attempt in range(3):
            ok, reply = send_line(
                candidate, f"DELIVER_{payload}",
                timeout=max(2, FAILURE_TIMEOUT * 2),
            )
            if ok and reply == f"ACK_{message_id}":
                metric("mensagens_enviadas", 1, msg_id=message_id)
                if candidate != target:
                    with state.lock:
                        state.assignments[str(destination)] = candidate
                        state.save()
                    replicate()
                    metric("mensagem_redirecionada_anel", 1,
                           servidor_morto=target, proximo_servidor=candidate)
                return reply
            if not server_alive(candidate):
                break
            if attempt < 2:
                metric("reenvios_coordenador", 1, msg_id=message_id,
                       servidor=candidate, tentativa=attempt + 1)
    return "ERROR_DESTINATION_UNAVAILABLE"


def receive_msg(message):
    if message is None:
        return False
    time.sleep(1)
    return True


def receive_message(message_id, sender, destination, content):
    started = time.monotonic()
    with state.lock:
        if message_id in state.processed:
            metric("receive_msg_execucoes_duplicadas", 0, msg_id=message_id)
            metric("acks_reenviados_mensagens_processadas", 1, msg_id=message_id)
            return f"ACK_{message_id}"
        event = state.in_flight.get(message_id)
        if event is None:
            event = threading.Event()
            state.in_flight[message_id] = event
            owns_processing = True
        else:
            owns_processing = False

    if not owns_processing:
        if not event.wait(timeout=max(FAILURE_TIMEOUT, 1)):
            return "ERROR_RECEIVE_IN_PROGRESS"
        with state.lock:
            if message_id not in state.processed:
                return "ERROR_RECEIVE_FAILED"
            metric("receive_msg_execucoes_duplicadas", 0, msg_id=message_id)
            metric("acks_reenviados_mensagens_processadas", 1, msg_id=message_id)
            return f"ACK_{message_id}"

    try:
        if not receive_msg(content):
            return "ERROR_RECEIVE_FAILED"
        with state.lock:
            state.processed.add(message_id)
            state.messages[message_id] = {
                "sender": str(sender),
                "destination": str(destination),
                "content": content,
                "received_at": round(time.time(), 3),
            }
            state.save()
        metric("receive_msg_execucoes", 1, msg_id=message_id)
        metric("receive_msg_execucoes_duplicadas", 0, msg_id=message_id)
        metric("tempo_processamento_receive_msg", time.monotonic() - started)
        replicate()
        return f"ACK_{message_id}"
    finally:
        with state.lock:
            state.in_flight.pop(message_id, None)
            event.set()


def send_user_message(sender):
    with state.lock:
        if state.assignments.get(str(sender)) != SERVER_ID:
            return False
        destinations = [
            user_id for user_id in state.assignments
            if user_id != str(sender)
        ]
    if not destinations:
        return False

    destination = random.choice(destinations)
    message_id = str(time.time_ns() * 100 + SERVER_ID)
    content = random.choice(["Ola", "Tudo bem?", "Mensagem automatica"])
    message = f"MSG_{message_id}_{sender}_{destination}_{content}"
    user_metric(sender, "mensagens_enviadas", 1,
                msg_id=message_id, destino=destination)
    reply = "ERROR_COORDINATOR_UNAVAILABLE"
    for attempt in range(3):
        with state.lock:
            coordinator = state.coordinator
        if coordinator == SERVER_ID:
            reply = handle_message(message)
        else:
            ok, reply = send_line(coordinator, message, timeout=FAILURE_TIMEOUT)
            if not ok:
                reply = "ERROR_COORDINATOR_UNAVAILABLE"
        if reply == f"ACK_{message_id}":
            metric("mensagens_confirmadas", 1, msg_id=message_id,
                   usuario=sender, destino=destination)
            user_metric(sender, "mensagens_confirmadas", 1,
                        msg_id=message_id, destino=destination)
            return True
        if attempt < 2:
            user_metric(sender, "reenvios_mensagens", 1,
                        msg_id=message_id, tentativa=attempt + 1)
            time.sleep(0.2)
    metric("mensagens_perdidas", 1, msg_id=message_id,
           usuario=sender, destino=destination, resposta=reply)
    user_metric(sender, "mensagens_perdidas", 1,
                msg_id=message_id, destino=destination, resposta=reply)
    return False


def user_message_loop(user_id, stop_event):
    while not shutdown_event.is_set():
        if stop_event.wait(random.uniform(0, 10)) or shutdown_event.is_set():
            return
        with state.lock:
            if state.assignments.get(user_id) != SERVER_ID:
                return
        send_user_message(user_id)


def start_user_sender(user_id):
    with user_sender_lock:
        thread = user_senders.get(user_id)
        if thread is not None and thread.is_alive():
            return
        stop_event = threading.Event()
        user_sender_events[user_id] = stop_event
        thread = threading.Thread(target=user_message_loop,
                                  args=(user_id, stop_event),
                                  daemon=True)
        user_senders[user_id] = thread
        thread.start()


def stop_user_sender(user_id):
    with user_sender_lock:
        stop_event = user_sender_events.get(str(user_id))
        if stop_event is not None:
            stop_event.set()


def next_ring_server(ring, server_id):
    index = ring.index(server_id)
    return ring[(index + 1) % len(ring)]


def add_peer(server_id, host, port):
    with state.lock:
        PEERS[int(server_id)] = (host, int(port))


def announce_new_peer(peer_id, joining_id, payload):
    ok, reply = send_line(peer_id, f"ADDPEER_{payload}", timeout=1)
    if not ok or reply != f"ACK_ADDPEER_{joining_id}":
        metric("falha_propagacao_novo_servidor", 1,
               servidor=joining_id, peer=peer_id, resposta=reply)


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
        if peer_id not in (SERVER_ID, server_id):
            threading.Thread(
                target=announce_new_peer,
                args=(peer_id, server_id, payload),
                daemon=True,
            ).start()
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
        state.deactivated_servers.discard(server_id)
    return f"ACK_REMOVEPEER_{server_id}"


def handle_election_token(payload):
    token = decoded(payload)
    ring = [int(server_id) for server_id in token["ring"]]
    origin = int(token["origin"])
    candidate = max(int(token["candidate"]), SERVER_ID)

    if SERVER_ID == origin:
        winner = candidate
        with state.lock:
            state.epoch = max(state.epoch, int(token["epoch"]))
            state.save()
        if len(ring) == 1:
            with state.lock:
                state.coordinator = winner
                state.save()
            return f"COORDINATOR_DONE_{winner}"
        coordinator = encoded({
            "epoch": token["epoch"],
            "origin": origin,
            "winner": winner,
            "ring": ring,
        })
        ok, reply = send_line(next_ring_server(ring, SERVER_ID),
                              f"COORDINATOR_{coordinator}")
        if not ok:
            return "ERROR_COORDINATOR_ANNOUNCEMENT"
        return reply

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
        if int(token["epoch"]) < state.epoch:
            return "ERROR_STALE_COORDINATOR"
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
        alive = [
            server_id for server_id in sorted(PEERS)
            if server_id not in state.deactivated_servers and server_alive(server_id)
        ]
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
        metric("tempo_eleicao", time.monotonic() - started)
        replicate()
        return f"COORDINATOR_{winner}"

def redistribute(server_id=None, evento="redistribuicao"):
    with redistribution_lock:
        _redistribute(server_id, evento)


def rebalance_over_capacity():
    with allocation_lock:
        with state.lock:
            owner_counts = {}
            for owner in state.assignments.values():
                owner_counts[owner] = owner_counts.get(owner, 0) + 1
        if not any(count > MAX_USERS for count in owner_counts.values()):
            return False
        redistribute(evento="correcao_capacidade")
        return True


def _redistribute(server_id=None, evento="redistribuicao"):
    started = time.monotonic()
    with state.lock:
        if server_id is None:
            users = list(state.users.items())
            old_assignments = dict(state.assignments)
            state.assignments = {}
        else:
            old_assignments = {
                user_id: owner for user_id, owner in state.assignments.items()
                if owner == server_id
            }
            users = [
                (user_id, state.users[user_id])
                for user_id in old_assignments if user_id in state.users
            ]
            for user_id in old_assignments:
                state.assignments.pop(user_id, None)
            state.save()
    migrated = 0
    for user_id, name in users:
        target = assign_user(int(user_id), name)
        old = old_assignments.get(str(user_id))
        if target is not None and (old is None or int(old) != target):
            migrated += 1
    metric("usuarios_migrados_evento", migrated, evento=evento)
    metric("tempo_redistribuicao", time.monotonic() - started,
           servidor=server_id, usuarios=len(users))
    emit_balance_metrics()


def recover_server(server_id):
    with recovery_lock:
        if server_id in recovering_servers:
            return
        recovering_servers.add(server_id)

    def restart():
        started = time.monotonic()
        try:
            time.sleep(SERVER_RECOVERY_DELAY)
            with redistribution_lock:
                if server_alive(server_id):
                    return
                launched_servers.discard(server_id)
                launched_servers.add(server_id)
                try:
                    start_server_process(server_id)
                except OSError as exc:
                    launched_servers.discard(server_id)
                    metric("falha_reinicio_servidor", 1, servidor=server_id,
                           erro=str(exc))
                    return
            metric("tempo_espera_reinicio_servidor",
                   time.monotonic() - started, servidor=server_id)
        finally:
            with recovery_lock:
                recovering_servers.discard(server_id)

    threading.Thread(target=restart, daemon=True).start()


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
    if raw.startswith("DELUSER_") and is_coordinator():
        return handle_delete_user(raw)
    if raw.startswith(("NEWUSER_", "DELUSER_", "KILL_", "MSG_")) and not is_coordinator():
        ok, reply = send_line(state.coordinator, raw, timeout=FAILURE_TIMEOUT)
        return reply if ok else "ERROR_COORDINATOR_UNAVAILABLE"
    if raw.startswith("KILL_") and is_coordinator():
        match = KILL_RE.fullmatch(raw)
        if not match:
            return "ERROR_INVALID_KILL"
        target = int(match.group(1))
        if target == SERVER_ID:
            return "ERROR_COORDINATOR_CANNOT_KILL_ITSELF"
        with allocation_lock:
            started = time.monotonic()
            ok, reply = send_line(target, "SHUTDOWN")
            if ok and reply == "ACK_SHUTDOWN":
                while time.monotonic() - started < FAILURE_TIMEOUT and server_alive(target):
                    time.sleep(0.05)
                if server_alive(target):
                    metric("falha_desativacao_servidor", 1,
                           servidor=target,
                           motivo="servidor ainda responde")
                    return "ERROR_SERVER_SHUTDOWN_TIMEOUT"
                metric("tempo_desativacao_servidor",
                       time.monotonic() - started, servidor=target, motivo="KILL")
                PEERS.pop(target, None)
                redistribute(target, evento="remocao")
                for peer_id in list(PEERS):
                    if peer_id != SERVER_ID:
                        send_line(peer_id, f"REMOVEPEER_{target}", timeout=1)
                return f"ACK_KILL_{target}"
            return "ERROR_SERVER_UNAVAILABLE"
    if raw.startswith("MSG_") and is_coordinator():
        return handle_message(raw)
    if raw.startswith("ASSIGN_"):
        data = decoded(raw.split("_", 1)[1])
        with state.lock:
            user_id = str(data["id"])
            if state.assignments.get(user_id) != SERVER_ID:
                local_count = sum(
                    1 for owner in state.assignments.values()
                    if owner == SERVER_ID
                )
                if local_count >= MAX_USERS:
                    return "ERROR_SERVER_FULL"
            state.users[user_id] = data["name"]
            state.assignments[user_id] = SERVER_ID
            state.save()
        metric("usuario_associado", 1, usuario=data["id"])
        start_user_sender(str(data["id"]))
        return f"ACK_ASSIGN_{data['id']}"
    if raw.startswith("DELIVER_"):
        data = decoded(raw.split("_", 1)[1])
        return receive_message(data["id"], data["sender"], data["destination"], data["content"])
    if raw == "SYNC_REQUEST":
        return f"SYNC_{encoded(snapshot())}"
    if raw.startswith("SYNC_"):
        data = decoded(raw.split("_", 1)[1])
        with state.lock:
            state.users = data["users"]
            state.assignments = {str(k): int(v) for k, v in data["assignments"].items()}
            state.processed.update(data.get("processed", []))
            state.messages.update({
                str(k): v for k, v in data.get("messages", {}).items()
            })
            state.coordinator = int(data["coordinator"])
            state.epoch = max(state.epoch, int(data["epoch"]))
            state.deactivated_servers = {
                int(server_id) for server_id in data.get("deactivated_servers", [])
            }
            state.save()
        with user_sender_lock:
            sender_ids = list(user_sender_events)
        with state.lock:
            no_longer_local = [
                user_id for user_id in sender_ids
                if state.assignments.get(user_id) != SERVER_ID
            ]
        for user_id in no_longer_local:
            stop_user_sender(user_id)
        sent_at = data.get("replication_sent_at")
        if sent_at is not None:
            metric("atraso_replicacao",
                   max(0.0, time.time() - float(sent_at)))
        return "ACK_SYNC"
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


def monitor():
    while True:
        if is_coordinator():
            for server_id in list(PEERS):
                if server_id == SERVER_ID:
                    continue
                with state.lock:
                    if server_id in state.deactivated_servers:
                        continue
                if server_alive(server_id):
                    last_seen[server_id] = time.monotonic()
                    failure_handled.discard(server_id)
                    continue
                failed_at = last_seen.setdefault(server_id, time.monotonic())
                elapsed = time.monotonic() - failed_at
                if elapsed >= FAILURE_TIMEOUT and server_id not in failure_handled:
                    failure_handled.add(server_id)
                    if server_id not in ever_seen_servers:
                        continue
                        metric("falha_servidor_desconhecido", 1, servidor=server_id)
                        continue
                    metric("tempo_deteccao_falha", elapsed, servidor=server_id)
                    recover_server(server_id)
                    redistribute(server_id, evento="falha")
            with state.lock:
                pending_users = [
                    (user_id, name) for user_id, name in state.users.items()
                    if user_id not in state.assignments
                ]
            for user_id, name in pending_users:
                assign_user(int(user_id), name)
            rebalance_over_capacity()
            maybe_deactivate_idle_server()
        else:
            if not server_alive(state.coordinator):
                alive = [
                    server_id for server_id in sorted(PEERS)
                    if server_alive(server_id)
                ]
                if alive and SERVER_ID == min(alive):
                    elect()
        shutdown_event.wait(HEARTBEAT_INTERVAL)


def main():
    started = time.monotonic()
    best_snapshot = None
    for peer_id in PEERS:
        if peer_id != SERVER_ID:
            ok, reply = send_line(peer_id, "SYNC_REQUEST", timeout=1)
            if ok and reply.startswith("SYNC_"):
                try:
                    data = decoded(reply.split("_", 1)[1])
                    candidate = (int(data["epoch"]), int(data["coordinator"]), data)
                    if best_snapshot is None or candidate[:2] > best_snapshot[:2]:
                        best_snapshot = candidate
                except (ValueError, KeyError, json.JSONDecodeError) as exc:
                    metric("erro_sincronizacao_retorno", 1, servidor=peer_id,
                           erro=str(exc))
    if best_snapshot is not None:
        data = best_snapshot[2]
        with state.lock:
            state.users = data["users"]
            state.assignments = {
                str(k): int(v) for k, v in data["assignments"].items()
            }
            state.processed.update(data.get("processed", []))
            state.messages.update({
                str(k): v for k, v in data.get("messages", {}).items()
            })
            state.deactivated_servers = {
                int(server_id) for server_id in data.get("deactivated_servers", [])
            }
            state.coordinator = int(data["coordinator"])
            state.epoch = max(state.epoch, int(data["epoch"]))
            state.save()
    elif (SERVER_ID != INITIAL_SERVER_ID
          or (STATE_FILE_EXISTS_AT_START and not can_bootstrap_local_state())):
        metric("erro_sincronizacao_retorno", 1,
               erro="nenhum servidor ativo respondeu à sincronização")
        raise RuntimeError("Servidor não iniciado: sincronização com o coordenador indisponível")
    elif can_bootstrap_local_state():
        metric("bootstrap_coordenador_estado_local", 1,
               epoch=state.epoch, usuarios=len(state.users))
    if SERVER_ID in state.deactivated_servers:
        metric("erro_inicializacao_servidor", 1,
               erro="servidor consta como desativado na configuração atual")
        raise RuntimeError("Servidor não iniciado: está desativado na configuração atual")
    metric("tempo_sincronizacao_retorno", time.monotonic() - started,
           origem="inicializacao", sincronizado=best_snapshot is not None)

    server = Server((SERVER_IP, SERVER_PORT), Handler)
    try:
        if JOIN_TOKEN and SERVER_ID != state.coordinator:
            ok, reply = send_line(state.coordinator,
                                  f"JOIN_{JOIN_TOKEN}_{SERVER_PORT}")
            if not ok or not reply.startswith("ACK_JOIN_"):
                metric("falha_entrada_anel", 1, resposta=reply)
                server.server_close()
                raise RuntimeError(f"Falha ao ingressar no anel: {reply}")
        send_to_control(f"NEWID_{SERVER_ID}")
    except (OSError, socket.error) as exc:
        metric("registro_inicial_pendente", 1, erro=str(exc))
        server.server_close()
        raise
    with state.lock:
        local_users = [
            user_id for user_id, owner in state.assignments.items()
            if owner == SERVER_ID
        ]
    for user_id in local_users:
        start_user_sender(user_id)
    shutdown_event.clear()
    threading.Thread(target=monitor, daemon=True).start()
    print(f"Servidor {SERVER_ID} em {SERVER_IP}:{SERVER_PORT}; coordenador {state.coordinator}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        shutdown_event.set()
        server.server_close()

if __name__ == "__main__":
    main()