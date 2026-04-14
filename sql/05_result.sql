-- ================================================================================
-- PHASE 5. 결과 — 최종 지표 요약
-- BigQuery 기준 (ds-ysy.ab_test)
-- ================================================================================


-- ── 최종 결과 한 번에 요약 ──────────────────────────────────────────────────────

WITH
-- 첫 경험자 user_id 목록
first_users AS (
  SELECT user_id
  FROM `ds-ysy.ab_test.users`
  WHERE is_first_fractional_exp = TRUE
),

-- CVR (전체)
cvr_all AS (
  SELECT
    experiment_group,
    COUNT(DISTINCT CASE WHEN event_name = 'view_fractional_purchase_page'  THEN user_id END) AS entry_uv,
    COUNT(DISTINCT CASE WHEN event_name = 'complete_fractional_purchase'   THEN user_id END) AS complete_uv
  FROM `ds-ysy.ab_test.events`
  WHERE experiment_group IN ('A', 'B')
    AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY experiment_group
),

-- CVR (첫 경험자)
cvr_first AS (
  SELECT
    e.experiment_group,
    COUNT(DISTINCT CASE WHEN e.event_name = 'view_fractional_purchase_page' THEN e.user_id END) AS entry_uv,
    COUNT(DISTINCT CASE WHEN e.event_name = 'complete_fractional_purchase'  THEN e.user_id END) AS complete_uv
  FROM `ds-ysy.ab_test.events` e
  JOIN first_users f ON e.user_id = f.user_id
  WHERE e.experiment_group IN ('A', 'B')
    AND DATE(e.event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY e.experiment_group
),

-- Decision Velocity (첫 경험자, Winsorized)
dv_raw AS (
  SELECT
    i.user_id,
    i.experiment_group,
    TIMESTAMP_DIFF(c.event_start_timestamp, i.event_start_timestamp, MILLISECOND) AS dv_ms
  FROM (
    SELECT user_id, session_id, experiment_group, event_start_timestamp
    FROM `ds-ysy.ab_test.events`
    WHERE event_name = 'click_amount_input_field'
      AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  ) i
  JOIN (
    SELECT user_id, session_id, event_start_timestamp
    FROM `ds-ysy.ab_test.events`
    WHERE event_name = 'click_purchase_confirm'
      AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  ) c ON i.user_id = c.user_id AND i.session_id = c.session_id
  JOIN first_users f ON i.user_id = f.user_id
  WHERE TIMESTAMP_DIFF(c.event_start_timestamp, i.event_start_timestamp, MILLISECOND) > 0
),
p95 AS (
  SELECT APPROX_QUANTILES(dv_ms, 100)[OFFSET(95)] AS p95_val FROM dv_raw
),
dv_first AS (
  SELECT
    r.experiment_group,
    APPROX_QUANTILES(r.dv_ms, 100)[OFFSET(50)] AS median_dv_ms
  FROM dv_raw r, p95 p
  WHERE r.dv_ms <= p.p95_val
  GROUP BY r.experiment_group
),

-- ATV (소수점 구매 성공)
atv AS (
  SELECT
    u.experiment_group,
    COUNT(*)                         AS order_count,
    ROUND(AVG(o.purchase_amount_krw)) AS avg_atv_krw
  FROM `ds-ysy.ab_test.orders` o
  JOIN `ds-ysy.ab_test.users` u ON o.user_id = u.user_id
  WHERE o.order_type = 'fractional'
    AND o.status = 'success'
    AND u.experiment_group IN ('A', 'B')
  GROUP BY u.experiment_group
),

-- 퍼널 잔존율 (입력 클릭 기준)
funnel AS (
  SELECT
    experiment_group,
    COUNT(DISTINCT CASE WHEN event_name = 'view_fractional_purchase_page' THEN user_id END) AS page_view_uv,
    COUNT(DISTINCT CASE WHEN event_name = 'click_amount_input_field'      THEN user_id END) AS input_click_uv
  FROM `ds-ysy.ab_test.events`
  WHERE experiment_group IN ('A', 'B')
    AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY experiment_group
)

-- ── 최종 결과 출력 ──────────────────────────────────────────────────────────────
SELECT
  ca.experiment_group,

  -- CVR (전체)
  ROUND(ca.complete_uv / ca.entry_uv * 100, 2)            AS cvr_all_pct,

  -- CVR (첫 경험자)
  ROUND(cf.complete_uv / cf.entry_uv * 100, 2)            AS cvr_first_pct,

  -- Decision Velocity 중앙값 (첫 경험자, 초)
  ROUND(dv.median_dv_ms / 1000, 1)                        AS dv_median_sec,

  -- ATV
  a.avg_atv_krw,

  -- 입력 클릭 잔존율
  ROUND(f.input_click_uv / f.page_view_uv * 100, 1)       AS input_click_rate_pct

FROM cvr_all ca
JOIN cvr_first cf  USING (experiment_group)
JOIN dv_first  dv  USING (experiment_group)
JOIN atv       a   USING (experiment_group)
JOIN funnel    f   USING (experiment_group)
ORDER BY ca.experiment_group;
