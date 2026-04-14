"""
PHASE 4. 통계 검정
===================
1. Mann-Whitney U test — Decision Velocity (전체 / 첫 경험자)
2. Chi-square test — CVR
3. 성공 조건 판정
"""

import pandas as pd
from scipy import stats

# ── 데이터 로드 ────────────────────────────────────────────────────────────────
events = pd.read_csv('events.csv')
users  = pd.read_csv('users.csv')

events['event_start_timestamp'] = pd.to_datetime(events['event_start_timestamp'])
df_frac = events[events['experiment_group'].isin(['A', 'B'])]

# ── Decision Velocity 계산 ─────────────────────────────────────────────────────
df_input = (
    df_frac[df_frac['event_name'] == 'click_amount_input_field']
    [['user_id', 'session_id', 'event_start_timestamp']]
    .rename(columns={'event_start_timestamp': 'input_ts'})
)
df_confirm = (
    df_frac[df_frac['event_name'] == 'click_purchase_confirm']
    [['user_id', 'session_id', 'event_start_timestamp']]
    .rename(columns={'event_start_timestamp': 'confirm_ts'})
)
df_dv = (
    df_input.merge(df_confirm, on=['user_id', 'session_id'])
    .merge(users[['user_id', 'experiment_group', 'is_first_fractional_exp']], on='user_id')
)
df_dv['dv_ms'] = (pd.to_datetime(df_dv['confirm_ts']) - pd.to_datetime(df_dv['input_ts'])).dt.total_seconds() * 1000
df_dv = df_dv[df_dv['dv_ms'] > 0]

# Winsorizing: 상위 5% 제거
p95    = df_dv['dv_ms'].quantile(0.95)
df_dv_w = df_dv[df_dv['dv_ms'] <= p95]


# ── 1. Mann-Whitney U test (전체) ──────────────────────────────────────────────
print("=== Mann-Whitney U Test: Decision Velocity (전체) ===")
a_all = df_dv_w[df_dv_w['experiment_group'] == 'A']['dv_ms']
b_all = df_dv_w[df_dv_w['experiment_group'] == 'B']['dv_ms']

stat, p = stats.mannwhitneyu(a_all, b_all, alternative='greater')
print(f"A군 중앙값: {a_all.median()/1000:.1f}초")
print(f"B군 중앙값: {b_all.median()/1000:.1f}초")
print(f"U statistic: {stat:,.0f}")
print(f"p-value: {p:.4f}")
print("→ 주의: 경험자 희석 효과로 전체 중앙값 차이 작음\n")


# ── 2. Mann-Whitney U test (첫 경험자만) ──────────────────────────────────────
print("=== Mann-Whitney U Test: Decision Velocity (첫 경험자만) ===")
a_first = df_dv_w[(df_dv_w['experiment_group'] == 'A') & (df_dv_w['is_first_fractional_exp'] == True)]['dv_ms']
b_first = df_dv_w[(df_dv_w['experiment_group'] == 'B') & (df_dv_w['is_first_fractional_exp'] == True)]['dv_ms']

stat, p_dv = stats.mannwhitneyu(a_first, b_first, alternative='greater')
print(f"A군 중앙값: {a_first.median()/1000:.1f}초")
print(f"B군 중앙값: {b_first.median()/1000:.1f}초")
print(f"U statistic: {stat:,.0f}")
print(f"p-value: {p_dv:.4f}")
if p_dv < 0.05:
    print("✓ p < 0.05 → A군 DV가 B군보다 유의미하게 길다. B군 문구 효과 확인.\n")
else:
    print("✗ p >= 0.05 → 유의미한 차이 없음.\n")


# ── 3. Chi-square test (CVR) ───────────────────────────────────────────────────

# 3-1. 전체 유저 CVR
print("=== Chi-square Test: CVR (전체 유저) ===")
entry    = df_frac[df_frac['event_name'] == 'view_fractional_purchase_page'].groupby('experiment_group')['user_id'].nunique()
complete = df_frac[df_frac['event_name'] == 'complete_fractional_purchase'].groupby('experiment_group')['user_id'].nunique()

a_entry, b_entry       = entry['A'], entry['B']
a_complete, b_complete = complete['A'], complete['B']

contingency = [
    [a_complete, a_entry - a_complete],
    [b_complete, b_entry - b_complete],
]
chi2, p_cvr, dof, _ = stats.chi2_contingency(contingency)

print(f"A군 CVR: {a_complete/a_entry*100:.2f}%  ({a_complete}/{a_entry})")
print(f"B군 CVR: {b_complete/b_entry*100:.2f}%  ({b_complete}/{b_entry})")
print(f"Chi2 statistic: {chi2:.4f}")
print(f"p-value: {p_cvr:.4f}")
if p_cvr < 0.05:
    print("✓ p < 0.05 → CVR 차이 유의미.\n")
else:
    print("✗ p >= 0.05 → 유의미한 차이 없음.\n")


# 3-2. 첫 경험자만 CVR (DV와 동일 세그먼트 기준)
print("=== Chi-square Test: CVR (첫 경험자만) ===")
first_users = users[users['is_first_fractional_exp'] == True]['user_id']

df_frac_first = df_frac[df_frac['user_id'].isin(first_users)]

entry_first    = df_frac_first[df_frac_first['event_name'] == 'view_fractional_purchase_page'].groupby('experiment_group')['user_id'].nunique()
complete_first = df_frac_first[df_frac_first['event_name'] == 'complete_fractional_purchase'].groupby('experiment_group')['user_id'].nunique()

a_entry_f, b_entry_f       = entry_first['A'], entry_first['B']
a_complete_f, b_complete_f = complete_first['A'], complete_first['B']

contingency_first = [
    [a_complete_f, a_entry_f - a_complete_f],
    [b_complete_f, b_entry_f - b_complete_f],
]
chi2_f, p_cvr_first, dof_f, _ = stats.chi2_contingency(contingency_first)

print(f"A군 CVR: {a_complete_f/a_entry_f*100:.2f}%  ({a_complete_f}/{a_entry_f})")
print(f"B군 CVR: {b_complete_f/b_entry_f*100:.2f}%  ({b_complete_f}/{b_entry_f})")
print(f"Chi2 statistic: {chi2_f:.4f}")
print(f"p-value: {p_cvr_first:.4f}")
if p_cvr_first < 0.05:
    print("✓ p < 0.05 → 첫 경험자 CVR 차이 유의미. B군 전환율 개선 확인.\n")
else:
    print("✗ p >= 0.05 → 유의미한 차이 없음.\n")


# ── 4. 성공 조건 판정 ──────────────────────────────────────────────────────────
print("=== 성공 조건 판정 ===")
dv_success  = p_dv < 0.05 and a_first.median() > b_first.median()
cvr_success = p_cvr_first < 0.05 and b_complete_f / b_entry_f > a_complete_f / a_entry_f
# DV와 동일하게 | 첫 경험자 기준으로 | CVR 성공 여부 판정

print(f"DV 단축 (첫 경험자): {'✓' if dv_success else '✗'}")
print(f"CVR non-inferior (첫 경험자): {'✓' if cvr_success else '✗'}")

if dv_success and cvr_success:
    print("\n✅ 성공 조건 충족 → B군 문구 채택 권고")
else:
    print("\n❌ 성공 조건 미충족")
