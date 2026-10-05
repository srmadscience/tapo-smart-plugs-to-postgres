from pathlib import Path

from tapo_watcher.sinks import failed_parts, heal, sink_names

ROOT = Path(__file__).resolve().parent.parent


def test_sink_names_match_connect_configs():
    import json

    files = sorted((ROOT / "connect").glob("tapo-sink-*-postgres.json"))
    assert sorted(json.loads(f.read_text())["name"] for f in files) == sorted(sink_names())


def test_failed_parts():
    assert failed_parts({"connector": {"state": "RUNNING"},
                         "tasks": [{"id": 0, "state": "RUNNING"}]}) == []
    assert failed_parts({"connector": {"state": "RUNNING"},
                         "tasks": [{"id": 0, "state": "FAILED"}]}) == ["task 0"]
    assert failed_parts({"connector": {"state": "FAILED"}, "tasks": []}) == ["connector"]


class _Resp:
    def __init__(self, code, body=None):
        self.status_code, self._body = code, body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def test_heal_restarts_only_failed(monkeypatch):
    import requests

    names = sink_names()
    states = {names[0]: "FAILED", names[1]: "RUNNING"}
    posts = []

    def get(url, timeout):
        name = url.split("/")[-2]
        if name not in states:
            return _Resp(404)
        return _Resp(200, {"connector": {"state": "RUNNING"},
                           "tasks": [{"id": 0, "state": states[name]}]})

    def post(url, params, timeout):
        posts.append((url.split("/")[-2], params))
        return _Resp(204)

    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(requests, "post", post)
    logs = []
    assert heal("http://connect:8083/", log=logs.append) == 1
    assert posts == [(names[0], {"includeTasks": "true", "onlyFailed": "true"})]
    assert sum("not registered" in m for m in logs) == 2
