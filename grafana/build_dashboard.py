"""Generate grafana/tapo.json (classic dashboard JSON, imports into Grafana 10-13).

    .venv/bin/python grafana/build_dashboard.py > grafana/tapo.json

Panel SQL lives in queries.py. Every computed number is cast to float8:
Grafana silently drops NUMERIC columns (see linksys-velop-watcher).
Verified rendering against Grafana 13.2.3 on 2026-10-05.
"""
import json, sys
from queries import Q

DS = {"type": "grafana-postgresql-datasource", "uid": "${DS_TAPO_POSTGRES}"}
_id = 0
def nid():
    global _id; _id += 1; return _id

def target(sql, fmt):
    return [{"refId": "A", "datasource": DS, "editorMode": "code", "format": fmt,
             "rawQuery": True, "rawSql": sql}]

def row(title, y):
    return {"type": "row", "id": nid(), "title": title, "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []}

def panel(kind, title, sql, fmt, pos, *, desc="", unit=None, options=None,
          defaults=None, overrides=None, custom=None):
    d = {"unit": unit} if unit else {}
    if custom: d["custom"] = custom
    d.update(defaults or {})
    return {"type": kind, "id": nid(), "title": title, "description": desc,
            "datasource": DS, "gridPos": dict(zip("xywh", pos)),
            "targets": target(sql, fmt),
            "fieldConfig": {"defaults": d, "overrides": overrides or []},
            "options": options or {}}

def thresholds(*steps):
    return {"mode": "absolute",
            "steps": [{"color": c, "value": v} for v, c in steps]}

def ts_custom(draw="line", stack=False, fill=10):
    c = {"drawStyle": draw, "lineWidth": 1, "fillOpacity": fill,
         "showPoints": "never", "spanNulls": False,
         "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    if draw == "bars": c.update(fillOpacity=80, barAlignment=1)  # timestamps are interval starts
    return c

LEGEND = {"legend": {"displayMode": "table", "placement": "right",
                     "calcs": ["lastNotNull", "max", "mean"], "showLegend": True},
          "tooltip": {"mode": "multi", "sort": "desc"}}
PER_ROW = {"reduceOptions": {"values": True, "calcs": [], "fields": ""}}

panels = [
    row("Now", 0),
    panel("bargauge", "Power now", Q["current_power"], "table", (0, 1, 12, 8),
          desc="Latest reading per plug (polled every 15 min). Plugs not heard from in the last hour are omitted.",
          unit="watt",
          options={**PER_ROW, "orientation": "horizontal", "displayMode": "gradient",
                   "showUnfilled": True, "minVizHeight": 16, "namePlacement": "left"},
          defaults={"displayName": "${__data.fields.metric}", "min": 0, "decimals": 1,
                    "color": {"mode": "continuous-GrYlRd"},
                    "thresholds": thresholds((None, "green"))}),
    panel("stat", "Energy today", Q["today_energy"], "table", (12, 1, 6, 4),
          desc="Sum of each plug's own today_energy counter (plug-local day).",
          unit="kwatth", options={"colorMode": "none", "graphMode": "none", "textMode": "value",
                                  **{"reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""}}},
          defaults={}),
    panel("stat", "Energy in selected range", Q["range_energy"], "table", (18, 1, 6, 4),
          desc="From the plugs' hourly energy history, so it includes hours backfilled after outages.",
          unit="kwatth", options={"colorMode": "none", "graphMode": "none", "textMode": "value",
                                  **{"reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""}}},
          defaults={}),
    panel("stat", "Minutes since last good reading", Q["freshness"], "table", (12, 5, 12, 4),
          desc="Feed staleness per enabled plug. Runs every 15 min, so above ~20 means a missed poll; above 35, two.",
          unit="m",
          options={**PER_ROW, "colorMode": "background", "graphMode": "none",
                   "textMode": "value_and_name", "justifyMode": "center", "orientation": "vertical"},
          defaults={"displayName": "${__data.fields.metric}", "decimals": 0,
                    "noValue": "never",
                    "thresholds": thresholds((None, "green"), (20, "orange"), (35, "red"))}),
    row("Power and energy", 9),
    panel("timeseries", "Power (5-minute average)", Q["power_ts"], "time_series", (0, 10, 24, 9),
          desc="tapo.power_5min: the plug's own 5-minute average power. Gaps are slots the plug had no data for.",
          unit="watt", custom=ts_custom(), options=LEGEND, defaults={"min": 0}),
    panel("timeseries", "Energy per hour", Q["hourly_ts"], "time_series", (0, 19, 12, 9),
          desc="tapo.energy_hourly, stacked by plug.", unit="watth",
          custom=ts_custom("bars", stack=True),
          options={**LEGEND, "legend": {**LEGEND["legend"], "calcs": ["sum"]}},
          defaults={"min": 0}),
    panel("timeseries", "Energy per day", Q["daily_ts"], "time_series", (12, 19, 12, 9),
          desc="Hourly energy summed per Europe/Dublin calendar day, stacked by plug.", unit="kwatth",
          custom=ts_custom("bars", stack=True),
          options={**LEGEND, "legend": {**LEGEND["legend"], "calcs": ["sum"]}},
          defaults={"min": 0}),
    row("Health", 28),
    panel("table", "Plug status", Q["status_table"], "table", (0, 29, 24, 7),
          desc="Latest poll per plug. RSSI: below -80 dBm is unreliable (the washing machine plug dropped out at -98).",
          options={"showHeader": True, "cellHeight": "sm"},
          defaults={"custom": {"align": "auto", "cellOptions": {"type": "auto"}}},
          overrides=[
              {"matcher": {"id": "byName", "options": "Status"},
               "properties": [{"id": "custom.cellOptions", "value": {"type": "color-background"}},
                              {"id": "mappings", "value": [{"type": "value", "options": {
                                  "ok": {"color": "green", "index": 0},
                                  "error": {"color": "red", "index": 1},
                                  "auth_backoff": {"color": "orange", "index": 2}}}]}]},
              {"matcher": {"id": "byName", "options": "RSSI (dBm)"},
               "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                              {"id": "thresholds", "value": thresholds((None, "red"), (-80, "orange"), (-70, "green"))}]},
              {"matcher": {"id": "byName", "options": "Last poll"},
               "properties": [{"id": "unit", "value": "dateTimeFromNow"}]},
              {"matcher": {"id": "byName", "options": "Power (W)"},
               "properties": [{"id": "decimals", "value": 1}]},
              {"matcher": {"id": "byName", "options": "Firmware"},
               "properties": [{"id": "custom.width", "value": 240}]},
              {"matcher": {"id": "byName", "options": "Last error"},
               "properties": [{"id": "custom.width", "value": 400}]},
          ]),
    panel("timeseries", "Wi-Fi signal (RSSI)", Q["rssi_ts"], "time_series", (0, 36, 12, 8),
          desc="Below -80 dBm (red zone) a plug is likely to drop off Wi-Fi.", unit="suffix: dBm",
          # Plugs share poll timestamps only approximately; bridge gaps up to
          # 30 min (one missed poll) so lines don't fragment, but show outages.
          custom={**ts_custom(fill=0), "thresholdsStyle": {"mode": "area"},
                  "spanNulls": 1800000},
          options=LEGEND,
          defaults={"max": -30, "min": -100,
                    "thresholds": thresholds((None, "red"), (-80, "transparent"))}),
    panel("timeseries", "Failed polls per hour", Q["failures_ts"], "time_series", (12, 36, 12, 8),
          desc="Readings with status error or auth_backoff. 4 per hour means the plug was unreachable all hour.",
          custom=ts_custom("bars", stack=True),
          options={**LEGEND, "legend": {**LEGEND["legend"], "calcs": ["sum"]}},
          defaults={"min": 0, "decimals": 0}),
]

dash = {
    "__inputs": [{"name": "DS_TAPO_POSTGRES", "label": "PostgreSQL (endowment_db)",
                  "description": "PostgreSQL holding the tapo.* tables",
                  "type": "datasource", "pluginId": "grafana-postgresql-datasource",
                  "pluginName": "PostgreSQL"}],
    "__requires": [{"type": "datasource", "id": "grafana-postgresql-datasource",
                    "name": "PostgreSQL", "version": "1.0.0"}],
    "uid": "tapo-plugs", "title": "Tapo smart plugs", "tags": ["tapo", "energy"],
    "timezone": "browser", "editable": True, "graphTooltip": 1,
    "time": {"from": "now-24h", "to": "now"}, "refresh": "5m",
    "schemaVersion": 39, "version": 1,
    "templating": {"list": [{
        "name": "plug", "label": "Plug", "type": "query", "datasource": DS,
        "query": "SELECT plug_name FROM tapo.plug WHERE enabled ORDER BY 1",
        "definition": "SELECT plug_name FROM tapo.plug WHERE enabled ORDER BY 1",
        "refresh": 1, "multi": True, "includeAll": True, "sort": 0,
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]}}]},
    "annotations": {"list": []},
    "panels": panels,
}
json.dump(dash, sys.stdout, indent=2)
print()
