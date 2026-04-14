-- ================================================================================
-- PHASE 2. ERD 도식화 — 테이블 구조 및 관계 확인
-- BigQuery 기준 (ds-ysy.ab_test)
-- ================================================================================


-- ── 1. users 테이블 샘플 확인 ───────────────────────────────────────────────────

SELECT *
FROM `ds-ysy.ab_test.users`
LIMIT 5;


-- ── 2. events 테이블 샘플 확인 ──────────────────────────────────────────────────

SELECT *
FROM `ds-ysy.ab_test.events`
LIMIT 5;


-- ── 3. orders 테이블 샘플 확인 ──────────────────────────────────────────────────

SELECT *
FROM `ds-ysy.ab_test.orders`
LIMIT 5;


-- ── 4. PK 유일성 검증 (중복 PK 있으면 스키마 이상) ─────────────────────────────

-- users PK 검증
SELECT user_id, COUNT(*) AS cnt
FROM `ds-ysy.ab_test.users`
GROUP BY user_id
HAVING cnt > 1;  -- 결과가 없어야 정상

-- events PK 검증
SELECT event_id, COUNT(*) AS cnt
FROM `ds-ysy.ab_test.events`
GROUP BY event_id
HAVING cnt > 1;

-- orders PK 검증
SELECT order_id, COUNT(*) AS cnt
FROM `ds-ysy.ab_test.orders`
GROUP BY order_id
HAVING cnt > 1;


-- ── 5. FK 무결성 검증 (orphan 레코드 확인) ──────────────────────────────────────

-- events에 있는 user_id가 users에 없는 경우
SELECT COUNT(*) AS orphan_count
FROM `ds-ysy.ab_test.events` e
LEFT JOIN `ds-ysy.ab_test.users` u ON e.user_id = u.user_id
WHERE u.user_id IS NULL;  -- 0이어야 정상

-- orders에 있는 user_id가 users에 없는 경우
SELECT COUNT(*) AS orphan_count
FROM `ds-ysy.ab_test.orders` o
LEFT JOIN `ds-ysy.ab_test.users` u ON o.user_id = u.user_id
WHERE u.user_id IS NULL;


-- ── 6. 이벤트 종류 목록 확인 ────────────────────────────────────────────────────

SELECT
  event_name,
  COUNT(*)                AS event_count,
  COUNT(DISTINCT user_id) AS uv
FROM `ds-ysy.ab_test.events`
GROUP BY event_name
ORDER BY event_count DESC;
