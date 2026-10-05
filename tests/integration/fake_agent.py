"""Stand-in for the scaling agents: one HTTP server, one agent per stage.

The controller POSTs {"observation": {...}} to <url>/action; here <url> ends with the stage
name, so /ffmpeg2/action is the ffmpeg2 agent. Each agent answers 2, 3, 1, 2, 3, 1, ...
instances, so the test sees stages scaled up and down while jobs are running.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CYCLE = [2, 3, 1]


class FakeAgents:

    def __init__(self, port):
        self.requests = []          # (stage, body, action)
        self.lock = threading.Lock()
        agents = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                parts = self.path.strip("/").split("/")
                if len(parts) != 2 or parts[1] != "action":
                    self.send_error(404)
                    return
                stage = parts[0]
                with agents.lock:
                    action = CYCLE[sum(1 for s, _, _ in agents.requests if s == stage) % len(CYCLE)]
                    agents.requests.append((stage, body, action))
                payload = json.dumps({"action": action}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def url(self, stage):
        return f"http://127.0.0.1:{self.server.server_port}/{stage}"

    def stop(self):
        self.server.shutdown()
