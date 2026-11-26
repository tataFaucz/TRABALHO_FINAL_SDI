import json
import os
import socketserver
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch


_TEMP_DIR = tempfile.TemporaryDirectory()
os.environ.update({
    "SERVER_IP": "127.0.0.1",
    "PORTS": "25101,25102",
    "SERVER_PORT": "25101",
    "SERVER_ID": "0",
    "INITIAL_SERVER_ID": "0",
    "CONTROL_PORT": "16010",
    "STATE_FILE": str(Path(_TEMP_DIR.name) / "state.json"),
    "SERVER_LOG_FILE": str(Path(_TEMP_DIR.name) / "server.jsonl"),
    "JOIN_TOKEN": "",
})

from backend import servidor


class ServerBehaviorTests(unittest.TestCase):
    def setUp(self):
        with servidor.state.lock:
            servidor.state.users = {"1": "Ana", "2": "Bia"}
            servidor.state.assignments = {"1": 0, "2": 1}
            servidor.state.processed = set()
            servidor.state.messages = {}
            servidor.state.in_flight = {}
            servidor.state.deactivated_servers = set()
            servidor.state.coordinator = servidor.SERVER_ID
            servidor.state.epoch = 0
        servidor.ever_seen_servers.clear()
        servidor.last_seen.clear()
        servidor.launched_servers.clear()
        servidor.recovering_servers.clear()
        servidor.deactivating_servers.clear()
        Path(servidor.LOG_FILE).unlink(missing_ok=True)

    def test_user_message_id_is_numeric(self):
        sent = []

        def acknowledge(message):
            sent.append(message)
            match = servidor.MSG_RE.fullmatch(message)
            return f"ACK_{match.group(1)}"

        with patch.object(servidor, "handle_message", side_effect=acknowledge):
            self.assertTrue(servidor.send_user_message("1"))

        match = servidor.MSG_RE.fullmatch(sent[0])
        self.assertIsNotNone(match)
        self.assertTrue(match.group(1).isdigit())

    def test_duplicate_delivery_stores_content_once_and_emits_metric(self):
        message_id = "17955481101420"
        with patch.object(servidor.time, "sleep") as sleep, patch.object(
            servidor, "replicate"
        ):
            expected_ack = f"ACK_{message_id}"
            self.assertEqual(
                servidor.receive_message(message_id, "1", "2", "hello"),
                expected_ack,
            )
            self.assertEqual(
                servidor.receive_message(message_id, "1", "2", "hello"),
                expected_ack,
            )
        sleep.assert_called_once_with(1)

        self.assertEqual(servidor.state.messages[message_id]["content"], "hello")
        self.assertEqual(len(servidor.state.messages), 1)
        rows = [
            json.loads(line)
            for line in Path(servidor.LOG_FILE).read_text().splitlines()
        ]
        metrics = [row["metrica"] for row in rows]
        self.assertEqual(metrics.count("receive_msg_execucoes"), 1)
        duplicate_metrics = [
            row for row in rows
            if row["metrica"] == "receive_msg_execucoes_duplicadas"
        ]
        self.assertEqual(len(duplicate_metrics), 2)
        self.assertTrue(all(row["valor"] == 0 for row in duplicate_metrics))
        self.assertEqual(metrics.count("acks_reenviados_mensagens_processadas"), 1)

    def test_receive_msg_none_fails_without_ack_or_processing(self):
        self.assertFalse(servidor.receive_msg(None))
        reply = servidor.receive_message("100", "1", "2", None)
        self.assertEqual(reply, "ERROR_RECEIVE_FAILED")
        self.assertNotIn("100", servidor.state.processed)

    def test_redistribution_moves_only_failed_server_users(self):
        assignments = []

        def reassign(user_id, _name):
            user_id = str(user_id)
            assignments.append(user_id)
            with servidor.state.lock:
                servidor.state.assignments[user_id] = 0
            return 0

        with patch.object(servidor, "assign_user", side_effect=reassign):
            servidor.redistribute(1)

        self.assertEqual(assignments, ["2"])
        self.assertEqual(servidor.state.assignments, {"1": 0, "2": 0})

    def test_sync_request_is_not_parsed_as_sync_payload(self):
        reply = servidor.handle_command("SYNC_REQUEST")
        self.assertTrue(reply.startswith("SYNC_"))
        data = servidor.decoded(reply.split("_", 1)[1])
        self.assertEqual(data["assignments"], servidor.state.assignments)

    def test_send_line_reads_large_responses_to_newline(self):
        response_body = "x" * 5000

        class LongReplyHandler(socketserver.StreamRequestHandler):
            def handle(self):
                self.rfile.readline()
                self.wfile.write((response_body + "\n").encode())

        with socketserver.TCPServer(("127.0.0.1", 0), LongReplyHandler) as listener:
            worker = threading.Thread(target=listener.serve_forever)
            worker.start()
            try:
                with patch.dict(
                    servidor.PEERS, {99: listener.server_address}
                ):
                    ok, reply = servidor.send_line(99, "PING", timeout=2)
                self.assertTrue(ok)
                self.assertEqual(reply, response_body)
            finally:
                listener.shutdown()
                worker.join(timeout=2)

    def test_local_bootstrap_requires_explicit_flag_and_initial_coordinator(self):
        with patch.object(servidor, "STATE_FILE_EXISTS_AT_START", True), patch.object(
            servidor, "ALLOW_LOCAL_BOOTSTRAP", False
        ):
            self.assertFalse(servidor.can_bootstrap_local_state())
        with patch.object(servidor, "STATE_FILE_EXISTS_AT_START", True), patch.object(
            servidor, "ALLOW_LOCAL_BOOTSTRAP", True
        ):
            self.assertTrue(servidor.can_bootstrap_local_state())
            with patch.object(servidor.state, "coordinator", 1):
                self.assertFalse(servidor.can_bootstrap_local_state())

    def test_join_does_not_wait_for_unavailable_peers(self):
        started = []

        class DeferredThread:
            def __init__(self, *, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                started.append(self.args[0])

        peers = {
            0: ("127.0.0.1", 25101),
            1: ("127.0.0.1", 25102),
            2: ("127.0.0.1", 25103),
        }
        with patch.object(servidor, "JOIN_TOKEN", "test-token"), patch.object(
            servidor, "send_to_control", return_value="ACK_NEWID_1"
        ), patch.object(
            servidor.threading, "Thread", DeferredThread
        ), patch.dict(servidor.PEERS, peers, clear=True):
            reply = servidor.handle_join("JOIN_test-token_25102")
        self.assertEqual(reply, "ACK_JOIN_1")
        self.assertEqual(started, [2])

    def test_manual_reactivation_uses_coordinator_and_starts_disabled_peer(self):
        with servidor.state.lock:
            servidor.state.deactivated_servers = {1}
        payload = servidor.encoded({"id": 1, "token": ""})
        with patch.object(servidor, "server_alive", return_value=False), patch.object(
            servidor, "start_server_process"
        ) as start_server, patch.object(servidor, "replicate") as replicate:
            reply = servidor.handle_command(f"REACTIVATE_{payload}")

        self.assertEqual(reply, "ACK_REACTIVATE_1")
        self.assertNotIn(1, servidor.state.deactivated_servers)
        start_server.assert_called_once_with(1)
        replicate.assert_called_once()

    def test_manual_reactivation_requires_disabled_peer(self):
        payload = servidor.encoded({"id": 1, "token": ""})
        with patch.object(servidor, "server_alive", return_value=False), patch.object(
            servidor, "start_server_process"
        ) as start_server:
            reply = servidor.handle_command(f"REACTIVATE_{payload}")

        self.assertEqual(reply, "ERROR_SERVER_NOT_DEACTIVATED_1")
        start_server.assert_not_called()

    def test_manual_reactivation_restores_disabled_state_if_start_fails(self):
        with servidor.state.lock:
            servidor.state.deactivated_servers = {1}
        payload = servidor.encoded({"id": 1, "token": ""})
        with patch.object(servidor, "server_alive", return_value=False), patch.object(
            servidor, "start_server_process", side_effect=OSError("cannot start")
        ), patch.object(servidor, "replicate"):
            reply = servidor.handle_command(f"REACTIVATE_{payload}")

        self.assertEqual(reply, "ERROR_SERVER_START_FAILED_1")
        self.assertIn(1, servidor.state.deactivated_servers)
        self.assertNotIn(1, servidor.launched_servers)

    def test_reactivation_cli_requests_coordinator_then_waits_for_server(self):
        requests = []

        def fake_send_line(server_id, message, timeout=2):
            requests.append((server_id, message))
            if message.startswith("REACTIVATE_"):
                return True, "ACK_REACTIVATE_0"
            if message == "PING":
                return True, "PONG"
            return False, "ERROR_UNEXPECTED_REQUEST"

        with patch.object(servidor, "discover_coordinator", return_value=1), patch.object(
            servidor, "send_line", side_effect=fake_send_line
        ):
            servidor.request_reactivation(0)

        self.assertEqual(requests[0][0], 1)
        self.assertTrue(requests[0][1].startswith("REACTIVATE_"))
        payload = servidor.decoded(requests[0][1].split("_", 1)[1])
        self.assertEqual(payload, {"id": 0, "token": servidor.JOIN_TOKEN})
        self.assertEqual(requests[1], (0, "PING"))

    def test_server_rejects_assignments_over_capacity(self):
        with servidor.state.lock:
            servidor.state.users = {str(user_id): "" for user_id in range(1, 6)}
            servidor.state.assignments = {
                str(user_id): servidor.SERVER_ID for user_id in range(1, 6)
            }
        payload = servidor.encoded({"id": 6, "name": "Leo"})
        self.assertEqual(
            servidor.handle_command(f"ASSIGN_{payload}"),
            "ERROR_SERVER_FULL",
        )

    def test_scale_up_starts_peer_never_seen_by_monitor(self):
        with servidor.state.lock:
            servidor.state.users = {
                str(user_id): "" for user_id in range(1, 6)
            }
            servidor.state.assignments = {
                str(user_id): servidor.SERVER_ID for user_id in range(1, 6)
            }
            servidor.last_seen[1] = 0
        with patch.object(
            servidor, "choose_server", side_effect=[None, 1]
        ), patch.object(
            servidor, "server_alive", return_value=False
        ), patch.object(
            servidor, "start_server_process"
        ) as start_server, patch.object(
            servidor, "send_line", return_value=(True, "ACK_ASSIGN_6")
        ), patch.object(servidor, "replicate"), patch.object(
            servidor, "emit_balance_metrics"
        ), patch.object(servidor.time, "sleep"):
            self.assertEqual(servidor._assign_user(6, "Leo"), 1)
        start_server.assert_called_once_with(1)

    def test_over_capacity_state_triggers_rebalancing(self):
        with servidor.state.lock:
            servidor.state.users = {
                str(user_id): "" for user_id in range(1, 7)
            }
            servidor.state.assignments = {
                str(user_id): servidor.SERVER_ID for user_id in range(1, 7)
            }
        with patch.object(servidor, "redistribute") as redistribute:
            self.assertTrue(servidor.rebalance_over_capacity())
        redistribute.assert_called_once_with(evento="correcao_capacidade")

    def test_delete_user_removes_its_mapping(self):
        with patch.object(servidor, "replicate"), patch.object(
            servidor, "emit_balance_metrics"
        ), patch.object(servidor, "compact_servers"):
            self.assertEqual(
                servidor.handle_command("DELUSER_1"),
                "ACK_DELUSER_1",
            )
        self.assertNotIn("1", servidor.state.users)
        self.assertNotIn("1", servidor.state.assignments)

    def test_coordinator_and_user_metrics_have_correct_components(self):
        servidor.metric("tempo_deteccao_falha", 1.2)
        servidor.user_metric("2", "mensagens_enviadas", 1)
        rows = [
            json.loads(line)
            for line in Path(servidor.LOG_FILE).read_text().splitlines()
        ]
        self.assertEqual(rows[0]["componente"], "coordenador")
        self.assertEqual(rows[1]["componente"], "usuario")
        self.assertEqual(rows[1]["id"], 2)


if __name__ == "__main__":
    unittest.main()
