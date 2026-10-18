"""Panel SQL for grafana/tapo.json (used by build_dashboard.py)."""
# Grafana macros: $__timeFilter(col), $plug (multi), $__timeFrom()/$__timeTo().
PLUG = "plug_name IN ($plug)"
# $location narrows the $plug dropdown; plugs with no location show as 'unset'.
LOCATION = "COALESCE(plug_location, 'unset')"
VAR_LOCATION = f"SELECT DISTINCT {LOCATION} FROM tapo.plug WHERE enabled ORDER BY 1"
VAR_PLUG = ("SELECT plug_name FROM tapo.plug "
            f"WHERE enabled AND {LOCATION} IN ($location) ORDER BY 1")
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
# Whole house per day, long format (metric = series): ESB meters (bottom/top
# MPRN) with the Tesla charger, dishwasher and each plug broken out. Plugs come off the meter named by their
# plug_location (BOTTOM/TOP); unset ones aren't subtracted.
"house_daily": """WITH plug_day AS (
    SELECT date_trunc('day', h.interval_start AT TIME ZONE 'Europe/Dublin') AS day,
           h.plug_name, UPPER(p.plug_location) AS loc, SUM(h.energy_wh) / 1000.0 AS kwh
    FROM tapo.energy_hourly h
    JOIN tapo.plug p ON p.plug_name = h.plug_name
    -- Whole days only: house rows start at the first midnight after $__timeFrom().
    WHERE h.interval_start >= $__timeFrom()::timestamptz - interval '1 day'
    GROUP BY 1, 2, 3),
loc_day AS (
    SELECT day, SUM(kwh) FILTER (WHERE loc = 'BOTTOM') AS bottom_kwh,
                SUM(kwh) FILTER (WHERE loc = 'TOP') AS top_kwh
    FROM plug_day GROUP BY 1),
house AS (
    SELECT b.read_day AS day,
           SUM(b.read_value) - SUM(c.usage_wh) / 1000.0 - SUM(d.kwh) - SUM(COALESCE(pl.bottom_kwh, 0)) AS bottom,
           SUM(t.read_value) - SUM(COALESCE(pl.top_kwh, 0)) AS top,
           SUM(c.usage_wh) / 1000.0 AS tesla, SUM(d.kwh) AS dishwasher
    FROM esb.house_kwh_by_day b
    JOIN esb.house_kwh_by_day t ON t.read_day = b.read_day AND t.mprn = 10001759014
    JOIN tesla.kwh_by_day c ON c.ts1 = b.read_day
    JOIN homeconnect.kwh_per_day d ON d.wash_day = b.read_day
    LEFT JOIN loc_day pl ON pl.day = b.read_day
    WHERE b.mprn = 10001759556 AND $__timeFilter(b.read_day)
    GROUP BY b.read_day
    HAVING SUM(b.read_value) - SUM(c.usage_wh) / 1000.0 > 0)
SELECT day AS time, metric, value::float8 AS value
FROM house, LATERAL (VALUES ('Bottom', bottom), ('Top', top),
                            ('Tesla', tesla), ('Dishwasher', dishwasher)) v(metric, value)
UNION ALL
SELECT pd.day, pd.plug_name, pd.kwh::float8
FROM plug_day pd JOIN house ON house.day = pd.day
ORDER BY 1, 2""",
"status_table": f"""SELECT DISTINCT ON (r.plug_name)
       r.plug_name AS "Plug", p.plug_location AS "Location", r.status AS "Status", r.fetched_at AS "Last poll",
       r.device_on AS "On", (r.current_power_mw / 1000.0)::float8 AS "Power (W)",
       r.rssi AS "RSSI (dBm)", r.signal_level AS "Signal", d.model AS "Model",
       d.fw_ver AS "Firmware", r.ip AS "IP", r.error AS "Last error"
FROM tapo.reading r
LEFT JOIN tapo.device d ON d.plug_name = r.plug_name
LEFT JOIN tapo.plug p ON p.plug_name = r.plug_name
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
