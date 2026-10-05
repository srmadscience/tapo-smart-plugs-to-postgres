"""Panel SQL for grafana/tapo.json (used by build_dashboard.py)."""
# Grafana macros: $__timeFilter(col), $plug (multi), $__timeFrom()/$__timeTo().
PLUG = "plug_name IN ($plug)"
Q = {
"current_power": f"""SELECT DISTINCT ON (plug_name) plug_name AS metric, (current_power_mw / 1000.0)::float8 AS watts
FROM tapo.reading
WHERE status = 'ok' AND {PLUG} AND fetched_at > now() - interval '1 hour'
ORDER BY plug_name, fetched_at DESC""",
"today_energy": f"""SELECT COALESCE(SUM(today_energy_wh), 0)::float8 / 1000 AS "Today (kWh)"
FROM (SELECT DISTINCT ON (plug_name) today_energy_wh
      FROM tapo.reading
      WHERE status = 'ok' AND {PLUG} AND fetched_at > now() - interval '1 hour'
      ORDER BY plug_name, fetched_at DESC) latest""",
"range_energy": f"""SELECT COALESCE(SUM(energy_wh), 0)::float8 / 1000 AS "Selected range (kWh)"
FROM tapo.energy_hourly
WHERE $__timeFilter(interval_start) AND {PLUG}""",
"freshness": f"""SELECT p.plug_name AS metric,
       (EXTRACT(EPOCH FROM now() - MAX(r.fetched_at)) / 60)::float8 AS minutes
FROM tapo.plug p
LEFT JOIN tapo.reading r ON r.plug_name = p.plug_name AND r.status = 'ok'
WHERE p.enabled AND p.{PLUG}
GROUP BY p.plug_name ORDER BY p.plug_name""",
"power_ts": f"""SELECT interval_start AS time, plug_name AS metric, power_w::float8 AS value
FROM tapo.power_5min
WHERE $__timeFilter(interval_start) AND {PLUG}
ORDER BY 1""",
"hourly_ts": f"""SELECT interval_start AS time, plug_name AS metric, energy_wh::float8 AS value
FROM tapo.energy_hourly
WHERE $__timeFilter(interval_start) AND {PLUG}
ORDER BY 1""",
"daily_ts": f"""SELECT (date_trunc('day', interval_start AT TIME ZONE 'Europe/Dublin') AT TIME ZONE 'Europe/Dublin') AS time,
       plug_name AS metric, (SUM(energy_wh) / 1000.0)::float8 AS value
FROM tapo.energy_hourly
WHERE $__timeFilter(interval_start) AND {PLUG}
GROUP BY 1, 2 ORDER BY 1""",
"status_table": f"""SELECT DISTINCT ON (r.plug_name)
       r.plug_name AS "Plug", r.status AS "Status", r.fetched_at AS "Last poll",
       r.device_on AS "On", (r.current_power_mw / 1000.0)::float8 AS "Power (W)",
       r.rssi AS "RSSI (dBm)", r.signal_level AS "Signal", d.model AS "Model",
       d.fw_ver AS "Firmware", r.ip AS "IP", r.error AS "Last error"
FROM tapo.reading r
LEFT JOIN tapo.device d ON d.plug_name = r.plug_name
WHERE r.{PLUG}
ORDER BY r.plug_name, r.fetched_at DESC""",
"rssi_ts": f"""SELECT fetched_at AS time, plug_name AS metric, rssi::float8 AS value
FROM tapo.reading
WHERE $__timeFilter(fetched_at) AND {PLUG} AND rssi IS NOT NULL
ORDER BY 1""",
"failures_ts": f"""SELECT date_trunc('hour', fetched_at) AS time, plug_name AS metric,
       (COUNT(*) FILTER (WHERE status <> 'ok'))::float8 AS value
FROM tapo.reading
WHERE $__timeFilter(fetched_at) AND {PLUG}
GROUP BY 1, 2 ORDER BY 1""",
}
