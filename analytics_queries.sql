-- analytics_queries.sql
-- SQLite queries for reporting / analytics


/* =========================================================
   1) Daily Active Users (DAU) over the last 30 days
   - DAU = distinct users who generated at least one event per day
   ========================================================= */

SELECT
  date(event_ts) AS event_date,
  COUNT(DISTINCT user_id) AS dau
FROM events
WHERE event_ts IS NOT NULL
  AND user_id IS NOT NULL
  AND date(event_ts) >= date('now', '-29 days')
GROUP BY 1
ORDER BY 1;


/* =========================================================
   2) Average session duration by user (using session_id)
   - Uses the real session_id from the app logs
   - Duration = max(event_ts) - min(event_ts) per (user_id, session_id)
   - Output: average session duration (seconds) per user
   ========================================================= */

WITH sessions AS (
  SELECT
    user_id,
    session_id,
    MIN(event_ts) AS session_start_ts,
    MAX(event_ts) AS session_end_ts,
    CAST((julianday(MAX(event_ts)) - julianday(MIN(event_ts))) * 86400 AS INTEGER) AS session_duration_seconds
  FROM events
  WHERE user_id IS NOT NULL
    AND session_id IS NOT NULL
    AND event_ts IS NOT NULL
  GROUP BY user_id, session_id
)
SELECT
  user_id,
  AVG(session_duration_seconds) AS avg_session_duration_seconds,
  COUNT(*) AS session_count
FROM sessions
GROUP BY user_id
ORDER BY avg_session_duration_seconds DESC;


/* =========================================================
   3) Top 10 most edited documents
   - Based on count of 'document_edit' events
   ========================================================= */

SELECT
  document_id,
  COUNT(*) AS edit_events
FROM events
WHERE event_type = 'document_edit'
  AND document_id IS NOT NULL
GROUP BY document_id
ORDER BY edit_events DESC
LIMIT 10;


/* =========================================================
   4) Number of shared documents per user
   - Returns BOTH:
     A) distinct documents shared by the user
     B) total share events by the user
   ========================================================= */

SELECT
  user_id,
  COUNT(DISTINCT document_id) AS distinct_documents_shared,
  COUNT(*) AS total_share_events
FROM events
WHERE event_type = 'document_shared'
  AND user_id IS NOT NULL
  AND document_id IS NOT NULL
GROUP BY user_id
ORDER BY distinct_documents_shared DESC;


/* =========================================================
   5) Anomaly detection + explanation
   We'll provide TWO useful anomalies:
   (A) DAU spike days compared to last 30 days average
       - A day is flagged if DAU > mean + 2*stddev
       - stddev computed via variance = avg(x^2) - avg(x)^2
   (B) "Shares without edits" documents (shared but never edited)
       - Might indicate spam, automation, or logging issues
   ========================================================= */


/* 5A) DAU spikes over the last 30 days (mean + 2*stddev threshold) */
WITH dau_daily AS (
  SELECT
    date(event_ts) AS event_date,
    COUNT(DISTINCT user_id) AS dau
  FROM events
  WHERE event_ts IS NOT NULL
    AND user_id IS NOT NULL
    AND date(event_ts) >= date('now', '-29 days')
  GROUP BY 1
),
stats AS (
  SELECT
    AVG(dau) AS mean_dau,
    (AVG(dau * dau) - (AVG(dau) * AVG(dau))) AS variance_dau
  FROM dau_daily
),
threshold AS (
  SELECT
    mean_dau,
    CASE
      WHEN variance_dau < 0 THEN 0
      ELSE sqrt(variance_dau)
    END AS stddev_dau
  FROM stats
)
SELECT
  d.event_date,
  d.dau,
  t.mean_dau,
  t.stddev_dau,
  (t.mean_dau + 2 * t.stddev_dau) AS spike_threshold
FROM dau_daily d
CROSS JOIN threshold t
WHERE d.dau > (t.mean_dau + 2 * t.stddev_dau)
ORDER BY d.event_date;


/* 5B) Documents shared but never edited (possible anomaly) */
WITH edited_docs AS (
  SELECT DISTINCT document_id
  FROM events
  WHERE event_type = 'document_edit'
    AND document_id IS NOT NULL
),
shared_docs AS (
  SELECT DISTINCT document_id
  FROM events
  WHERE event_type = 'document_shared'
    AND document_id IS NOT NULL
)
SELECT
  s.document_id
FROM shared_docs s
LEFT JOIN edited_docs e
  ON s.document_id = e.document_id
WHERE e.document_id IS NULL
ORDER BY s.document_id;


/* Optional: users who share a lot but never edit */
WITH user_edits AS (
  SELECT user_id, COUNT(*) AS edit_events
  FROM events
  WHERE event_type = 'document_edit' AND user_id IS NOT NULL
  GROUP BY user_id
),
user_shares AS (
  SELECT user_id, COUNT(*) AS share_events
  FROM events
  WHERE event_type = 'document_shared' AND user_id IS NOT NULL
  GROUP BY user_id
)
SELECT
  sh.user_id,
  sh.share_events,
  COALESCE(ed.edit_events, 0) AS edit_events
FROM user_shares sh
LEFT JOIN user_edits ed
  ON sh.user_id = ed.user_id
WHERE sh.share_events >= 5
  AND COALESCE(ed.edit_events, 0) = 0
ORDER BY sh.share_events DESC;