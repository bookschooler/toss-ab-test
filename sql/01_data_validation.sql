-- ================================================================================
-- PHASE 1. 데이터셋 검증
-- BigQuery 기준 (ds-ysy.ab_test)
-- ================================================================================


-- ── 1. 테이블별 행 수 확인 ──────────────────────────────────────────────────────

SELECT 'users'    AS table_name, COUNT(*) AS row_count FROM `ds-ysy.ab_test.users`
UNION ALL
SELECT 'events',                  COUNT(*)              FROM `ds-ysy.ab_test.events`
UNION ALL
SELECT 'orders',                  COUNT(*)              FROM `ds-ysy.ab_test.orders`
UNION ALL
SELECT 'baseline',                COUNT(*)              FROM `ds-ysy.ab_test.baseline`;


-- ── 2. 실험 기간 확인 ───────────────────────────────────────────────────────────

SELECT
  MIN(DATE(event_start_timestamp)) AS min_date,  -- 가장 이른 날짜
  MAX(DATE(event_start_timestamp)) AS max_date   -- 가장 늦은 날짜
FROM `ds-ysy.ab_test.events`;
-- 기대값: min=2025-03-01 / max=2025-03-07


-- ── 3. SRM 검사 (Sample Ratio Mismatch) ────────────────────────────────────────

SELECT
  experiment_group,
  COUNT(*)                                          AS user_count,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4)        AS ratio  -- 전체 대비 비율
FROM `ds-ysy.ab_test.users`
WHERE experiment_group IN ('A', 'B')
GROUP BY experiment_group
ORDER BY experiment_group;
-- 기대값: A/B 각각 0.5000 (50:50). 0.45~0.55 범위 벗어나면 SRM 의심


-- ── 4. 결측치 확인 ──────────────────────────────────────────────────────────────

-- users 테이블 주요 컬럼 결측치
SELECT
  COUNTIF(user_id            IS NULL) AS user_id_null,
  COUNTIF(experiment_group   IS NULL) AS exp_group_null,
  COUNTIF(is_first_fractional_exp IS NULL) AS first_exp_null
FROM `ds-ysy.ab_test.users`;

-- events 테이블 주요 컬럼 결측치
SELECT
  COUNTIF(event_id           IS NULL) AS event_id_null,
  COUNTIF(user_id            IS NULL) AS user_id_null,
  COUNTIF(event_name         IS NULL) AS event_name_null,
  COUNTIF(event_start_timestamp IS NULL) AS timestamp_null
FROM `ds-ysy.ab_test.events`;


-- ── 5. A/B 그룹별 소수점 퍼널 진입 UV ─────────────────────────────────────────

SELECT
  experiment_group,
  COUNT(DISTINCT user_id) AS entry_uv  -- 고유 유저 수 (UV)
FROM `ds-ysy.ab_test.events`
WHERE event_name = 'view_fractional_purchase_page'
  AND experiment_group IN ('A', 'B')
  AND DATE(event_start_timestamp) BETWEEN '2025-03-01' AND '2025-03-07'
GROUP BY experiment_group
ORDER BY experiment_group;
