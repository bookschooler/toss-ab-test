"""
PHASE 2. ERD 도식화
====================
- 테이블 간 PK/FK 관계 출력
- dbdiagram.io용 DBML 파일은 erd.dbml 참고
"""

import pandas as pd

users   = pd.read_csv('users.csv')
events  = pd.read_csv('events.csv')
orders  = pd.read_csv('orders.csv')
baseline = pd.read_csv('baseline.csv')

print("=== 테이블 스키마 요약 ===")
for name, df in [("users", users), ("events", events), ("orders", orders), ("baseline", baseline)]:
    print(f"\n[{name}]")
    print(f"  행 수: {len(df):,}")
    print(f"  컬럼: {list(df.columns)}")

print("""
=== PK / FK 관계 ===
users.user_id (PK)
    ├── events.user_id (FK)
    └── orders.user_id (FK)

events.event_id (PK)
    └── orders.event_id (FK)

※ ERD 시각화: erd.dbml → dbdiagram.io 에서 확인
""")
