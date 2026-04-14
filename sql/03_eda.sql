-- ================================================================================
-- PHASE 3. EDA
-- BigQuery 기준 (ds-ysy.ab_test)
-- ================================================================================


-- ── EDA 1. 기초 요약 (CVR + Decision Velocity median) ──────────────────────────

WITH
-- 소수점 퍼널 진입 UV (분모)
entry AS (
  SELECT experiment_group, COUNT(DISTINCT user_id) AS entry_uv
  FROM `ds-ysy.ab_test.events`
  WHERE event_name = 'view_fractional_purchase_page'
    AND experiment_group IN ('A', 'B')
    AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY experiment_group
),
-- 구매 완료 UV (분자)
complete AS (
  SELECT experiment_group, COUNT(DISTINCT user_id) AS complete_uv
  FROM `ds-ysy.ab_test.events`
  WHERE event_name = 'complete_fractional_purchase'
    AND experiment_group IN ('A', 'B')
    AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY experiment_group
),
-- Decision Velocity: 입력 클릭 ~ 구매 확인 클릭 구간 (ms)
dv AS (
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
  WHERE TIMESTAMP_DIFF(c.event_start_timestamp, i.event_start_timestamp, MILLISECOND) > 0
),
-- DV 상위 5% Winsorizing 기준값
dv_p95 AS (
  SELECT APPROX_QUANTILES(dv_ms, 100)[OFFSET(95)] AS p95
  FROM dv
),
dv_filtered AS (
  SELECT d.user_id, d.experiment_group, d.dv_ms
  FROM dv d, dv_p95 p
  WHERE d.dv_ms <= p.p95
)

SELECT
  e.experiment_group,
  e.entry_uv,
  c.complete_uv,
  ROUND(c.complete_uv / e.entry_uv * 100, 2)          AS cvr_pct,
  ROUND(APPROX_QUANTILES(dv.dv_ms, 100)[OFFSET(50)] / 1000, 1) AS dv_median_sec  -- 중앙값(초)
FROM entry e
JOIN complete c    USING (experiment_group)
JOIN dv_filtered dv USING (experiment_group)
GROUP BY e.experiment_group, e.entry_uv, c.complete_uv
ORDER BY e.experiment_group;


-- ── EDA 2. 시간대별 트래픽 히트맵 ──────────────────────────────────────────────

SELECT
  EXTRACT(HOUR FROM event_start_timestamp)            AS hour,
  FORMAT_DATE('%a', DATE(event_start_timestamp))      AS day_of_week,
  COUNT(DISTINCT user_id)                             AS uv
FROM `ds-ysy.ab_test.events`
WHERE event_name = 'view_fractional_purchase_page'
  AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
GROUP BY hour, day_of_week
ORDER BY hour, day_of_week;
-- 결과를 Python에서 pivot → seaborn heatmap으로 시각화


-- ── EDA 3. 마이크로 퍼널 잔존율 (4단계) ────────────────────────────────────────

WITH funnel AS (
  SELECT
    experiment_group,
    COUNTIF(event_name = 'view_fractional_purchase_page') AS step1_page_view,
    COUNTIF(event_name = 'click_amount_input_field')      AS step2_input_click,
    COUNTIF(event_name = 'input_amount_complete')         AS step3_amount_entered
  FROM `ds-ysy.ab_test.events`
  WHERE experiment_group IN ('A', 'B')
    AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY experiment_group
),
-- 구매 성공: orders 테이블 join (status = 'success'만)
purchase_success AS (
  SELECT
    u.experiment_group,
    COUNT(DISTINCT o.user_id) AS step4_purchase_success
  FROM `ds-ysy.ab_test.orders` o
  JOIN `ds-ysy.ab_test.users` u ON o.user_id = u.user_id
  WHERE o.status = 'success'
    AND o.order_type = 'fractional'
    AND u.experiment_group IN ('A', 'B')
  GROUP BY u.experiment_group
)

SELECT
  f.experiment_group,
  f.step1_page_view,
  f.step2_input_click,
  f.step3_amount_entered,
  p.step4_purchase_success,
  -- 잔존율 계산
  ROUND(f.step2_input_click    / f.step1_page_view * 100, 1) AS rate_step2_pct,
  ROUND(f.step3_amount_entered / f.step1_page_view * 100, 1) AS rate_step3_pct,
  ROUND(p.step4_purchase_success / f.step1_page_view * 100, 1) AS rate_step4_pct
FROM funnel f
JOIN purchase_success p USING (experiment_group)
ORDER BY f.experiment_group;


-- ── EDA 4. ATV 분포 (A vs B) ────────────────────────────────────────────────────

SELECT
  u.experiment_group,
  COUNT(*)                                AS order_count,
  ROUND(AVG(o.purchase_amount_krw))       AS avg_atv,
  ROUND(APPROX_QUANTILES(o.purchase_amount_krw, 100)[OFFSET(50)]) AS median_atv,
  ROUND(MIN(o.purchase_amount_krw))       AS min_atv,
  ROUND(MAX(o.purchase_amount_krw))       AS max_atv
FROM `ds-ysy.ab_test.orders` o
JOIN `ds-ysy.ab_test.users` u ON o.user_id = u.user_id
WHERE o.order_type = 'fractional'
  AND o.status = 'success'
  AND u.experiment_group IN ('A', 'B')
GROUP BY u.experiment_group
ORDER BY u.experiment_group;


-- ── EDA 5. 연령대별 CVR ─────────────────────────────────────────────────────────

WITH
entry_age AS (
  SELECT u.experiment_group, u.age_group, COUNT(DISTINCT e.user_id) AS entry_uv
  FROM `ds-ysy.ab_test.events` e
  JOIN `ds-ysy.ab_test.users` u ON e.user_id = u.user_id
  WHERE e.event_name = 'view_fractional_purchase_page'
    AND u.experiment_group IN ('A', 'B')
    AND DATE(e.event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY u.experiment_group, u.age_group
),
complete_age AS (
  SELECT u.experiment_group, u.age_group, COUNT(DISTINCT e.user_id) AS complete_uv
  FROM `ds-ysy.ab_test.events` e
  JOIN `ds-ysy.ab_test.users` u ON e.user_id = u.user_id
  WHERE e.event_name = 'complete_fractional_purchase'
    AND u.experiment_group IN ('A', 'B')
    AND DATE(e.event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY u.experiment_group, u.age_group
)

SELECT
  ea.experiment_group,
  ea.age_group,
  ea.entry_uv,
  ca.complete_uv,
  ROUND(ca.complete_uv / ea.entry_uv * 100, 2) AS cvr_pct
FROM entry_age ea
JOIN complete_age ca USING (experiment_group, age_group)
ORDER BY ea.experiment_group, ea.age_group;


-- ── EDA 6. 경험 여부별 Decision Velocity 분포 ──────────────────────────────────

WITH dv AS (
  SELECT
    i.user_id,
    i.experiment_group,
    u.is_first_fractional_exp,
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
  JOIN `ds-ysy.ab_test.users` u ON i.user_id = u.user_id
  WHERE TIMESTAMP_DIFF(c.event_start_timestamp, i.event_start_timestamp, MILLISECOND) > 0
),
p95 AS (
  SELECT APPROX_QUANTILES(dv_ms, 100)[OFFSET(95)] AS p95_val FROM dv
)

SELECT
  d.experiment_group,
  d.is_first_fractional_exp,
  COUNT(*)                                                                   AS n,
  ROUND(APPROX_QUANTILES(d.dv_ms, 100)[OFFSET(50)] / 1000, 1)               AS median_dv_sec,
  ROUND(AVG(d.dv_ms) / 1000, 1)                                             AS avg_dv_sec
FROM dv d, p95 p
WHERE d.dv_ms <= p.p95_val  -- Winsorizing 상위 5% 제거
GROUP BY d.experiment_group, d.is_first_fractional_exp
ORDER BY d.experiment_group, d.is_first_fractional_exp DESC;
