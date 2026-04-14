-- ================================================================================
-- PHASE 4. 통계 검정
-- BigQuery 기준 (ds-ysy.ab_test)
-- 참고: Mann-Whitney U / Chi-square p-value 계산은 BigQuery 미지원
--       → 이 파일은 검정에 필요한 데이터를 추출하고,
--         실제 p-value 계산은 Python(scipy)에서 수행
-- ================================================================================


-- ── 1. Decision Velocity 데이터 추출 (Mann-Whitney U 입력값) ───────────────────

WITH dv_raw AS (
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
  SELECT APPROX_QUANTILES(dv_ms, 100)[OFFSET(95)] AS p95_val
  FROM dv_raw
)

-- 전체 요약 (전체 유저)
SELECT
  r.experiment_group,
  r.is_first_fractional_exp,
  COUNT(*)                                                         AS n,
  ROUND(APPROX_QUANTILES(r.dv_ms, 100)[OFFSET(50)] / 1000, 1)     AS median_dv_sec,
  ROUND(APPROX_QUANTILES(r.dv_ms, 100)[OFFSET(25)] / 1000, 1)     AS q1_sec,
  ROUND(APPROX_QUANTILES(r.dv_ms, 100)[OFFSET(75)] / 1000, 1)     AS q3_sec
FROM dv_raw r, p95 p
WHERE r.dv_ms <= p.p95_val  -- Winsorizing 상위 5% 제거
GROUP BY r.experiment_group, r.is_first_fractional_exp
ORDER BY r.experiment_group, r.is_first_fractional_exp DESC;


-- ── 2. CVR 분할표 데이터 추출 (Chi-square 입력값) ──────────────────────────────

-- 2-1. 전체 유저 CVR 분할표
WITH
entry AS (
  SELECT experiment_group, COUNT(DISTINCT user_id) AS entry_uv
  FROM `ds-ysy.ab_test.events`
  WHERE event_name = 'view_fractional_purchase_page'
    AND experiment_group IN ('A', 'B')
    AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY experiment_group
),
complete AS (
  SELECT experiment_group, COUNT(DISTINCT user_id) AS complete_uv
  FROM `ds-ysy.ab_test.events`
  WHERE event_name = 'complete_fractional_purchase'
    AND experiment_group IN ('A', 'B')
    AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY experiment_group
)

SELECT
  e.experiment_group,
  e.entry_uv,
  c.complete_uv                              AS converted,       -- 전환O
  e.entry_uv - c.complete_uv                AS not_converted,   -- 전환X
  ROUND(c.complete_uv / e.entry_uv * 100, 2) AS cvr_pct
FROM entry e
JOIN complete c USING (experiment_group)
ORDER BY e.experiment_group;


-- 2-2. 첫 경험자만 CVR 분할표
WITH
first_users AS (
  SELECT user_id
  FROM `ds-ysy.ab_test.users`
  WHERE is_first_fractional_exp = TRUE
),
entry_first AS (
  SELECT experiment_group, COUNT(DISTINCT e.user_id) AS entry_uv
  FROM `ds-ysy.ab_test.events` e
  JOIN first_users f ON e.user_id = f.user_id
  WHERE event_name = 'view_fractional_purchase_page'
    AND experiment_group IN ('A', 'B')
    AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY experiment_group
),
complete_first AS (
  SELECT experiment_group, COUNT(DISTINCT e.user_id) AS complete_uv
  FROM `ds-ysy.ab_test.events` e
  JOIN first_users f ON e.user_id = f.user_id
  WHERE event_name = 'complete_fractional_purchase'
    AND experiment_group IN ('A', 'B')
    AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
  GROUP BY experiment_group
)

SELECT
  e.experiment_group,
  e.entry_uv,
  c.complete_uv                               AS converted,
  e.entry_uv - c.complete_uv                 AS not_converted,
  ROUND(c.complete_uv / e.entry_uv * 100, 2)  AS cvr_pct
FROM entry_first e
JOIN complete_first c USING (experiment_group)
ORDER BY e.experiment_group;


-- ── 3. 성공 조건 판정용 요약 ────────────────────────────────────────────────────
-- 아래 쿼리 결과를 보고 판정 기준 충족 여부 확인
-- (p-value는 Python scipy에서 계산 후 판단)

WITH
dv_summary AS (
  SELECT
    i.experiment_group,
    u.is_first_fractional_exp,
    APPROX_QUANTILES(
      TIMESTAMP_DIFF(c.event_start_timestamp, i.event_start_timestamp, MILLISECOND),
      100
    )[OFFSET(50)] AS median_dv_ms
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
  GROUP BY i.experiment_group, u.is_first_fractional_exp
)

SELECT
  experiment_group,
  is_first_fractional_exp,
  ROUND(median_dv_ms / 1000, 1) AS median_dv_sec
FROM dv_summary
ORDER BY is_first_fractional_exp DESC, experiment_group;
