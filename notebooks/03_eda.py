"""
PHASE 3. 기본 EDA
==================
1. 기초 요약 (CVR, Decision Velocity median)
2. 시간대별 트래픽 히트맵
3. 마이크로 퍼널 단계별 잔존율 (4단계)
4. ATV 히스토그램 (A vs B)
5. 연령대별 CVR
6. 경험 여부별 Decision Velocity 분포
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import koreanize_matplotlib

# ── 데이터 로드 ────────────────────────────────────────────────────────────────
events  = pd.read_csv('events.csv')
users   = pd.read_csv('users.csv')
orders  = pd.read_csv('orders.csv')

events['event_start_timestamp'] = pd.to_datetime(events['event_start_timestamp'])
df_frac = events[events['experiment_group'].isin(['A', 'B'])]


# ── EDA 1. 기초 요약 ───────────────────────────────────────────────────────────
print("=== EDA 1. 기초 요약 ===")

entry    = df_frac[df_frac['event_name'] == 'view_fractional_purchase_page'].groupby('experiment_group')['user_id'].nunique()
complete = df_frac[df_frac['event_name'] == 'complete_fractional_purchase'].groupby('experiment_group')['user_id'].nunique()
cvr      = (complete / entry * 100).round(2)

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
df_dv = df_input.merge(df_confirm, on=['user_id', 'session_id'])
df_dv['dv_sec'] = (pd.to_datetime(df_dv['confirm_ts']) - pd.to_datetime(df_dv['input_ts'])).dt.total_seconds()
df_dv = df_dv[df_dv['dv_sec'] > 0].merge(events[['user_id', 'experiment_group']].drop_duplicates(), on='user_id')
dv_median = df_dv.groupby('experiment_group')['dv_sec'].median().round(1)

print(f"{'':20} {'A군':>10} {'B군':>10}")
print(f"{'CVR (%)':20} {cvr['A']:>10} {cvr['B']:>10}")
print(f"{'DV 중앙값 (초)':20} {dv_median['A']:>10} {dv_median['B']:>10}")


# ── EDA 2. 시간대별 트래픽 히트맵 ─────────────────────────────────────────────
print("\n=== EDA 2. 시간대별 트래픽 히트맵 ===")

df_page = df_frac[df_frac['event_name'] == 'view_fractional_purchase_page'].copy()
df_page['hour']       = df_page['event_start_timestamp'].dt.hour
df_page['day_of_week'] = df_page['event_start_timestamp'].dt.strftime('%a')

df_heatmap = (
    df_page.groupby(['hour', 'day_of_week'])['user_id']
    .nunique().reset_index()
)
df_heatmap.columns = ['hour', 'day_of_week', 'uv']

day_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
pivot = df_heatmap.pivot(index='hour', columns='day_of_week', values='uv').reindex(columns=day_order)

fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(pivot, annot=True, fmt='.0f', cmap='YlOrRd', ax=ax)
ax.set_title('시간대 × 요일별 소수점 구매 페이지 진입 UV')
ax.set_xlabel('요일')
ax.set_ylabel('시간 (시)')
plt.tight_layout()
plt.savefig('chart_heatmap.png', dpi=150, bbox_inches='tight')
plt.show()


# ── EDA 3. 마이크로 퍼널 잔존율 (4단계) ───────────────────────────────────────
print("\n=== EDA 3. 마이크로 퍼널 잔존율 ===")

steps_map = {
    'view_fractional_purchase_page': 'step1_page_view',
    'click_amount_input_field':      'step2_input_click',
    'input_amount_complete':         'step3_amount_entered',
}
df_steps = (
    df_frac[df_frac['event_name'].isin(steps_map.keys())]
    .groupby(['experiment_group', 'event_name'])['user_id']
    .nunique().reset_index()
)
df_steps['step'] = df_steps['event_name'].map(steps_map)
df_pivot = df_steps.pivot(index='experiment_group', columns='step', values='user_id').reset_index()

df_success = (
    orders[orders['status'] == 'success']
    .merge(events[['user_id', 'experiment_group']].drop_duplicates(), on='user_id')
    [lambda x: x['experiment_group'].isin(['A', 'B'])]
    .groupby('experiment_group')['user_id'].nunique().reset_index()
    .rename(columns={'user_id': 'step4_purchase_success'})
)
df_funnel = df_pivot.merge(df_success, on='experiment_group')

steps = ['페이지 진입', '금액 입력 클릭', '금액 입력 완료', '구매 완료(성공)']
cols  = ['step1_page_view', 'step2_input_click', 'step3_amount_entered', 'step4_purchase_success']

fig, ax = plt.subplots(figsize=(10, 5))
for _, row in df_funnel.iterrows():
    values = [row[c] for c in cols]
    rates  = [v / values[0] * 100 for v in values]
    ax.plot(steps, rates, marker='o', label=f"{row['experiment_group']}군")
    for s, r in zip(steps, rates):
        ax.annotate(f'{r:.1f}%', (s, r), textcoords='offset points', xytext=(0, 8), ha='center', fontsize=9)

ax.set_title('퍼널 단계별 잔존율 (A vs B)')
ax.set_ylabel('잔존율 (%)')
ax.set_ylim(0, 110)
ax.legend()
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('chart_funnel.png', dpi=150, bbox_inches='tight')
plt.show()


# ── EDA 4. ATV 히스토그램 ─────────────────────────────────────────────────────
print("\n=== EDA 4. ATV 히스토그램 ===")

df_atv = (
    orders[(orders['order_type'] == 'fractional') & (orders['status'] == 'success')]
    .merge(users[['user_id', 'experiment_group']], on='user_id')
)

fig, ax = plt.subplots(figsize=(10, 5))
for group, color in [('A', '#4C72B0'), ('B', '#DD8452')]:
    data = df_atv[df_atv['experiment_group'] == group]['purchase_amount_krw']
    ax.hist(data, bins=50, alpha=0.6, color=color, label=f'{group}군 (평균 {data.mean():,.0f}원)')

ax.set_title('A vs B군 구매 금액 분포 (ATV)')
ax.set_xlabel('구매 금액 (원)')
ax.set_ylabel('빈도')
ax.legend()
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('chart_atv.png', dpi=150, bbox_inches='tight')
plt.show()


# ── EDA 5. 연령대별 CVR ───────────────────────────────────────────────────────
print("\n=== EDA 5. 연령대별 CVR ===")

df_entry_age = (
    events[events['event_name'] == 'view_fractional_purchase_page']
    .merge(users[['user_id', 'age_group']], on='user_id')
    .groupby(['experiment_group', 'age_group'])['user_id'].nunique().reset_index()
    .rename(columns={'user_id': 'entry_uv'})
)
df_complete_age = (
    events[events['event_name'] == 'complete_fractional_purchase']
    .merge(users[['user_id', 'age_group']], on='user_id')
    .groupby(['experiment_group', 'age_group'])['user_id'].nunique().reset_index()
    .rename(columns={'user_id': 'complete_uv'})
)
df_cvr_age = df_entry_age.merge(df_complete_age, on=['experiment_group', 'age_group'])
df_cvr_age['cvr'] = df_cvr_age['complete_uv'] / df_cvr_age['entry_uv'] * 100

age_order = ['20대', '30대', '40대', '50대이상']
df_cvr_age['age_group'] = pd.Categorical(df_cvr_age['age_group'], categories=age_order, ordered=True)
df_cvr_age = df_cvr_age.sort_values('age_group')

fig, ax = plt.subplots(figsize=(10, 5))
x = range(len(age_order))
width = 0.35
for i, (group, color) in enumerate([('A', '#4C72B0'), ('B', '#DD8452')]):
    data = df_cvr_age[df_cvr_age['experiment_group'] == group].set_index('age_group').reindex(age_order)
    bars = ax.bar([xi + i * width for xi in x], data['cvr'], width=width, color=color, alpha=0.8, label=f'{group}군')
    for bar, val in zip(bars, data['cvr']):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3, f'{val:.1f}%', ha='center', fontsize=9)

ax.set_title('연령대별 CVR (A vs B)')
ax.set_ylabel('CVR (%)')
ax.set_xticks([xi + width / 2 for xi in x])
ax.set_xticklabels(age_order)
ax.legend()
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('chart_cvr_age.png', dpi=150, bbox_inches='tight')
plt.show()


# ── EDA 6. 경험 여부별 Decision Velocity 분포 ─────────────────────────────────
print("\n=== EDA 6. 경험 여부별 Decision Velocity 분포 ===")

df_dv_full = df_dv.merge(users[['user_id', 'is_first_fractional_exp']], on='user_id')

fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
subgroups = [
    (True,  '첫 경험 유저 (is_first_fractional_exp = True)'),
    (False, '경험자 (is_first_fractional_exp = False)'),
]
for ax, (is_first, title) in zip(axes, subgroups):
    for group, color in [('A', '#4C72B0'), ('B', '#DD8452')]:
        data = df_dv_full[
            (df_dv_full['experiment_group'] == group) &
            (df_dv_full['is_first_fractional_exp'] == is_first)
        ]['dv_sec']
        ax.hist(data, bins=40, alpha=0.6, color=color, label=f'{group}군 (중앙값 {data.median():.1f}초)')
    ax.set_title(title)
    ax.set_xlabel('Decision Velocity (초)')
    ax.set_ylabel('빈도')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

fig.suptitle('경험 여부별 Decision Velocity 분포 (A vs B)', fontsize=13, y=1.02)
plt.tight_layout()
plt.savefig('chart_dv_experience.png', dpi=150, bbox_inches='tight')
plt.show()
