"""
PHASE 1. 데이터셋 검증
=======================
- 테이블별 행 수 확인
- 실험 기간 확인 (2025-03-01 ~ 2025-03-07)
- SRM (Sample Ratio Mismatch) 검사
- 결측치 확인
"""

import pandas as pd

# ── 데이터 로드 ────────────────────────────────────────────────────────────────
users   = pd.read_csv('users.csv')
events  = pd.read_csv('events.csv')
orders  = pd.read_csv('orders.csv')
baseline = pd.read_csv('baseline.csv')

# ── 1. 행 수 확인 ──────────────────────────────────────────────────────────────
print("=== 테이블별 행 수 ===")
print(f"users   : {len(users):,}행")
print(f"events  : {len(events):,}행")
print(f"orders  : {len(orders):,}행")
print(f"baseline: {len(baseline):,}행")

# ── 2. 실험 기간 확인 ──────────────────────────────────────────────────────────
print("\n=== 실험 기간 확인 ===")
events['event_start_timestamp'] = pd.to_datetime(events['event_start_timestamp'])
print(f"events 최소 날짜: {events['event_start_timestamp'].min().date()}")
print(f"events 최대 날짜: {events['event_start_timestamp'].max().date()}")

# ── 3. SRM 검사 ────────────────────────────────────────────────────────────────
print("\n=== SRM (Sample Ratio Mismatch) 검사 ===")
group_counts = users['experiment_group'].value_counts()
print(group_counts)
ratio = group_counts['A'] / group_counts['B']
print(f"A:B 비율: {ratio:.4f} (1.0000이 이상적)")
if 0.95 <= ratio <= 1.05:
    print("✓ SRM 없음. 정상 배분.")
else:
    print("✗ SRM 의심. 배분 로직 확인 필요.")

# ── 4. 결측치 확인 ─────────────────────────────────────────────────────────────
print("\n=== 결측치 확인 ===")
for name, df in [("users", users), ("events", events), ("orders", orders)]:
    missing = df.isnull().sum()
    missing = missing[missing > 0]
    if missing.empty:
        print(f"{name}: 결측치 없음 ✓")
    else:
        print(f"{name}:\n{missing}")

# ── 5. A/B 그룹별 소수점 퍼널 진입 UV ─────────────────────────────────────────
print("\n=== A/B 그룹별 소수점 퍼널 진입 UV ===")
df_frac = events[
    (events['event_name'] == 'view_fractional_purchase_page') &
    (events['experiment_group'].isin(['A', 'B']))
]
print(df_frac.groupby('experiment_group')['user_id'].nunique())
