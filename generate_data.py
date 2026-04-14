"""
generate_data.py
================
토스 소수점 구매 A/B 테스트 — 가상 데이터 생성 스크립트

실험: "얼마나 구매할까요?" (A) vs "얼마치 구매할까요?" + 예상 주수 표시 (B)
산출물:
    - users.csv              (35,000행)
    - events.csv             (정수 + 소수점 퍼널 포함)
    - orders.csv             (fractional + integer)
    - baseline.csv           (7행, 실험 전 베이스라인)

Random seed 42 고정 → 재현 가능
"""

import random
import math
import csv
import uuid
from datetime import datetime, timedelta, date

SEED = 42
random.seed(SEED)

# ── 실험 기간 ─────────────────────────────────────────────────────────────────
EXP_START = datetime(2025, 3, 1, 0, 0, 0)
EXP_END   = datetime(2025, 3, 7, 23, 59, 59)
PRE_START = date(2025, 2, 22)   # 실험 전 7일 베이스라인

# ── 규모 ──────────────────────────────────────────────────────────────────────
N_USERS        = 35_000          # 총 유저
DAILY_UV       = 5_000           # 소수점 페이지 일별 UV
EXP_DAYS       = 7

# ── CVR / ATV / 이탈율 ────────────────────────────────────────────────────────
CVR            = {"A": 0.098, "B": 0.122}
BOUNCE_RATE    = {"A": 0.714, "B": 0.546}
ATV_MEAN       = {"A": 17_800, "B": 21_600}
ATV_STD        = {"A": 13_700, "B": 14_800}
ATV_MIN        = 1_000

# ── 정수 구매 CVR / 선택 비율 ──────────────────────────────────────────────────
INTEGER_PURCHASE_RATE  = 0.817   # 상세보기에서 정수 구매 선택 비율
FRACTIONAL_PURCHASE_RATE = 0.183 # 상세보기에서 소수점 구매 선택 비율
INTEGER_CVR    = 0.312           # 정수 구매 전환율

# ── 세그먼트 비율 ──────────────────────────────────────────────────────────────
GENDER_DIST    = [("M", 0.55), ("F", 0.45)]
AGE_DIST       = [("20대", 0.35), ("30대", 0.40), ("40대", 0.15), ("50대이상", 0.10)]
DEVICE_DIST    = [("ios", 0.55), ("android", 0.45)]
INVEST_TYPE    = [
    ("공격투자형", 0.20), ("적극투자형", 0.30), ("위험중립형", 0.25),
    ("안정추구형", 0.15), ("안정형", 0.10),
]
ACQ_CHANNEL    = [("organic", 0.50), ("ad", 0.30), ("search", 0.20)]

# ── 종목 비중 ──────────────────────────────────────────────────────────────────
TICKERS = [
    ("TSLA", 0.30), ("NVDA", 0.18), ("AAPL", 0.12), ("PLTR", 0.10),
    ("MSFT", 0.08), ("TQQQ", 0.06), ("QQQ",  0.05), ("GOOGL", 0.05),
    ("IONQ", 0.04), ("TSLL", 0.02),
]

# ── 시간대 비중 ────────────────────────────────────────────────────────────────
# (시작시, 종료시(포함), 비중)
HOUR_DIST = [
    (7,  8,  0.15),
    (12, 12, 0.12),
    (22, 23, 0.28),
    # 나머지 45% → 나머지 시간대에 균등 분배
]

# ── 유입 경로 ─────────────────────────────────────────────────────────────────
SOURCE_PAGE = [("home", 0.40), ("search", 0.35), ("stock_detail", 0.25)]

# ── 퍼널 이벤트 (소수점) ──────────────────────────────────────────────────────
FUNNEL_FRAC = [
    "view_fractional_purchase_page",
    "click_amount_input_field",
    "input_amount_complete",
    "click_fractional_buy_button",
    "click_purchase_confirm",
    "complete_fractional_purchase",
]

# ── 퍼널 이벤트 (일반/정수) ───────────────────────────────────────────────────
FUNNEL_INT = [
    "view_integer_purchase_page",
    "click_quantity_input_field",
    "input_quantity_complete",
    "click_integer_buy_button",
    "click_purchase_confirm",
    "complete_integer_purchase",
]


# ─────────────────────────────────────────────────────────────────────────────
# 유틸 함수
# ─────────────────────────────────────────────────────────────────────────────

def weighted_choice(choices):
    """[(value, weight), ...] 에서 하나 샘플링"""
    vals, weights = zip(*choices)
    r = random.random()
    cumul = 0.0
    for v, w in zip(vals, weights):
        cumul += w
        if r < cumul:
            return v
    return vals[-1]


def sample_hour():
    """시간대 비중에 따라 시간(int) 샘플링"""
    remaining_hours = list(range(0, 24))
    for start, end, _ in HOUR_DIST:
        for h in range(start, end + 1):
            if h in remaining_hours:
                remaining_hours.remove(h)

    defined_total = sum(w for _, _, w in HOUR_DIST)
    remaining_weight = 1.0 - defined_total
    remaining_per_hour = remaining_weight / len(remaining_hours) if remaining_hours else 0

    dist = []
    for start, end, w in HOUR_DIST:
        count = end - start + 1
        per_hour = w / count
        for h in range(start, end + 1):
            dist.append((h, per_hour))
    for h in remaining_hours:
        dist.append((h, remaining_per_hour))

    return weighted_choice(dist)


def random_datetime(base_date: date) -> datetime:
    """해당 날짜 내에서 시간대 분포에 맞게 datetime 생성"""
    hour   = sample_hour()
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(base_date.year, base_date.month, base_date.day, hour, minute, second)


def lognormal_atv(group: str) -> int:
    """로그정규분포로 ATV 샘플링 (최소 1,000원)"""
    mu    = ATV_MEAN[group]
    sigma = ATV_STD[group]
    # 로그정규 파라미터 변환
    ln_mean  = math.log(mu**2 / math.sqrt(sigma**2 + mu**2))
    ln_sigma = math.sqrt(math.log(1 + (sigma / mu)**2))
    val = random.lognormvariate(ln_mean, ln_sigma)
    return max(ATV_MIN, round(val / 1000) * 1000)   # 1,000원 단위


# ── 이벤트별 duration_ms 범위 (ms) ────────────────────────────────────────────
# 페이지 뷰/체류: 수초~수십초, 클릭: 수백ms, 입력: 수초~수십초
COMMISSION_RATE = 0.0025   # 토스증권 해외주식 수수료율 0.25% (정상가 기준, 2026년)

DURATION_MS = {
    "view_stock_detail_page":          (3_000,  20_000),
    "click_purchase_button":           (100,    500),    # 종목 상세보기 내 구매하기 버튼
    "click_fractional_purchase":       (100,    600),
    "click_integer_purchase":          (100,    600),
    "view_fractional_purchase_page":   (3_000,  30_000),
    "click_amount_input_field":        (200,    800),
    "input_amount_complete":           (5_000,  20_000),
    "click_fractional_buy_button":     (100,    500),
    "click_purchase_confirm":          (500,    3_000),
    "complete_fractional_purchase":    (500,    2_000),
    "exit_fractional_purchase_page":   (100,    500),
    "view_integer_purchase_page":      (3_000,  25_000),
    "click_quantity_input_field":      (200,    800),
    "input_quantity_complete":         (5_000,  15_000),
    "click_integer_buy_button":        (100,    500),
    "complete_integer_purchase":       (500,    2_000),
    "exit_integer_purchase_page":      (100,    500),
}
DEFAULT_DURATION_MS = (200, 2_000)

# ── Decision Velocity 구간 duration_ms 범위 (그룹별 분리) ─────────────────────
# A군: "얼마나" 문구 → 주수/금액 혼동으로 인지 부하 → 각 단계에서 더 오래 체류
# B군: "얼마치" + 예상 주수 표시 → 명확 → 더 빠르게 결정
# Decision Velocity = click_amount_input_field ~ click_purchase_confirm 구간
# A군 목표 합계 ~30,000ms / B군 목표 합계 ~20,000ms
DURATION_MS_BY_GROUP = {
    # A군 + 비경험자: "얼마나" 문구 → 주수/금액 혼동 → 망설임 발생 → 평균 약 24초
    "A_first": {
        "click_amount_input_field":    (3_000,  8_000),   # 입력창 탭 전 망설임 (주수? 금액?)
        "input_amount_complete":       (6_000,  12_000),  # 금액 타이핑 중 재확인
        "click_fractional_buy_button": (1_500,  3_500),   # 버튼 누르기 전 재확인
        "click_purchase_confirm":      (2_000,  5_000),   # 최종 확인 전 망설임
    },
    # A군 + 경험자: 문구 혼동 없이 기계적으로 진행 → 평균 약 6초
    "A_exp": {
        "click_amount_input_field":    (300,    800),     # 익숙해서 바로 탭
        "input_amount_complete":       (2_000,  5_000),   # 빠른 타이핑
        "click_fractional_buy_button": (200,    500),     # 바로 클릭
        "click_purchase_confirm":      (300,    800),     # 바로 확인
    },
    # B군 + 비경험자: "얼마치" + 예상 주수 → 명확 → 고민 없이 진행 → 평균 약 13초
    "B_first": {
        "click_amount_input_field":    (600,    1_800),   # 명확한 문구로 바로 탭
        "input_amount_complete":       (4_500,  9_000),   # 금액 타이핑
        "click_fractional_buy_button": (400,    1_000),   # 망설임 없이 클릭
        "click_purchase_confirm":      (600,    1_800),   # 바로 확인
    },
    # B군 + 경험자: 문구 명확 + 이미 익숙 → 가장 빠름 → 평균 약 5초
    "B_exp": {
        "click_amount_input_field":    (200,    600),     # 가장 빠르게 탭
        "input_amount_complete":       (1_800,  4_500),   # 빠른 타이핑
        "click_fractional_buy_button": (150,    400),     # 즉시 클릭
        "click_purchase_confirm":      (200,    600),     # 즉시 확인
    },
}


def gen_id(prefix=""):
    return prefix + uuid.uuid4().hex[:12]


def make_event(event_name, start_ts, group="", dv_key="", **kwargs):
    """이벤트 딕셔너리 생성. duration/end/client/server timestamp 자동 계산."""
    # Decision Velocity 구간 이벤트는 경험 여부 반영한 dv_key로 duration 적용
    group_dur = DURATION_MS_BY_GROUP.get(dv_key, {})
    lo, hi = group_dur.get(event_name) or DURATION_MS.get(event_name, DEFAULT_DURATION_MS)
    duration_ms = random.randint(lo, hi)
    end_ts = start_ts + timedelta(milliseconds=duration_ms)

    # client_timestamp: start 기준 ±0~2초 오차
    client_offset_ms = random.randint(-2_000, 2_000)
    client_ts = start_ts + timedelta(milliseconds=client_offset_ms)

    # server_timestamp: client 기준 +50~500ms 네트워크 지연
    server_delay_ms = random.randint(50, 500)
    server_ts = client_ts + timedelta(milliseconds=server_delay_ms)

    # engagement_time_msec: 실제 앱 포커스 시간 (백그라운드 전환 시간 제외)
    # 비경험 A군: 환전 계산 등으로 다른 앱 다녀오는 경우 있음 → 포커스 비율 낮음
    # 나머지: 명확한 문구 또는 경험자 → 포커스 비율 높음
    # 페이지 뷰/입력 이벤트에서만 의미 있고, 클릭 이벤트는 거의 100%
    if event_name in ("view_fractional_purchase_page", "input_amount_complete",
                      "click_amount_input_field"):
        focus_ratio = random.uniform(0.5, 0.85) if dv_key == "A_first" else random.uniform(0.85, 1.0)
    else:
        focus_ratio = random.uniform(0.92, 1.0)
    engagement_time_msec = round(duration_ms * focus_ratio)

    fmt = "%Y-%m-%d %H:%M:%S.%f"
    row = {
        "event_id":               gen_id("EVT_"),
        "event_name":             event_name,
        "event_start_timestamp":  start_ts.strftime(fmt),
        "event_end_timestamp":    end_ts.strftime(fmt),
        "duration_ms":            duration_ms,
        "engagement_time_msec":   engagement_time_msec,
        "client_timestamp":       client_ts.strftime(fmt),
        "server_timestamp":       server_ts.strftime(fmt),
    }
    row.update(kwargs)
    return row, end_ts


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1-1. users.csv
# ─────────────────────────────────────────────────────────────────────────────

def generate_users(n=N_USERS):
    print(f"[users] 생성 중... ({n:,}행)")
    rows = []
    for _ in range(n):
        device = weighted_choice(DEVICE_DIST)
        age    = weighted_choice(AGE_DIST)

        # 첫 거래일: 1년 전 ~ 실험 시작 하루 전
        days_ago = random.randint(1, 365)
        ftd = (EXP_START - timedelta(days=days_ago)).date()

        # 소수점 거래 경험: 40대 이상은 경험 비율 낮음
        frac_exp_rate = 0.30 if age in ("40대", "50대이상") else 0.55
        is_first = random.random() > frac_exp_rate   # True = 경험 없음(첫 경험)

        total_trades = random.randint(1, 200)

        rows.append({
            "user_id":                gen_id("USR_"),
            "experiment_group":       "A" if _ < n // 2 else "B",
            "gender":                 weighted_choice(GENDER_DIST),
            "age_group":              age,
            "device_type":            device,
            "investment_type":        weighted_choice(INVEST_TYPE),
            "acquisition_channel":    weighted_choice(ACQ_CHANNEL),
            "first_trade_date":       ftd.isoformat(),
            "total_trade_count":      total_trades,
            "is_first_fractional_exp": is_first,
        })

    return rows



# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1-3 & 1-4. events.csv + orders.csv
# ─────────────────────────────────────────────────────────────────────────────

def generate_events_and_orders(users):
    """
    유저 1인당 7일 중 하루 1회 해외주식 상세보기 방문.
    상세보기에서 정수(81.7%) / 소수점(18.3%) 구매로 분기.
    """
    print("[events + orders] 생성 중...")

    events = []
    orders = []

    price_map = {
        "TSLA": 280_000, "NVDA": 900_000, "AAPL": 250_000,
        "PLTR": 80_000,  "MSFT": 520_000, "TQQQ": 75_000,
        "QQQ":  580_000, "GOOGL": 240_000, "IONQ": 40_000,
        "TSLL": 15_000,
    }

    session_seq = {}  # session_id → 현재 sequence 카운터

    def add(name, t, uid, session_id, ticker, source, group="", dv_key="", amount=""):
        seq = session_seq.get(session_id, 0) + 1
        session_seq[session_id] = seq
        row, end_t = make_event(
            name, t,
            group=group,
            dv_key=dv_key,
            user_id=uid,
            session_id=session_id,
            event_sequence=seq,
            experiment_group=group,
            ticker=ticker,
            source_page=source,
            input_amount_krw=amount,
        )
        events.append(row)
        return end_t  # 다음 이벤트 시작 시각으로 사용

    for u in users:
        uid      = u["user_id"]
        group    = u["experiment_group"]
        is_first = u["is_first_fractional_exp"]  # True = 소수점 첫 경험자

        # 경험 여부에 따라 Decision Velocity duration 키 결정
        dv_key = f"{group}_{'first' if is_first else 'exp'}"

        # 7일 중 하루 랜덤 방문
        day_idx  = random.randint(0, EXP_DAYS - 1)
        cur_date = (EXP_START + timedelta(days=day_idx)).date()

        session_id = gen_id("SES_")
        ticker     = weighted_choice(TICKERS)
        source     = weighted_choice(SOURCE_PAGE)
        t          = random_datetime(cur_date)

        # 1. 해외주식 상세보기 진입
        t = add("view_stock_detail_page", t, uid, session_id, ticker, source)
        t += timedelta(seconds=random.randint(5, 20))

        # 2. 구매하기 버튼 클릭 (상세보기 → 구매 페이지 진입 전)
        t = add("click_purchase_button", t, uid, session_id, ticker, source)
        t += timedelta(seconds=random.randint(1, 3))

        # 3. 정수 / 소수점 구매 분기
        is_fractional = random.random() < FRACTIONAL_PURCHASE_RATE

        if is_fractional:
            # ── 소수점 구매 퍼널 ──────────────────────────────────────────
            t = add("click_fractional_purchase", t, uid, session_id, ticker, source, group, dv_key)
            t += timedelta(seconds=random.randint(1, 3))
            t = add("view_fractional_purchase_page", t, uid, session_id, ticker, source, group, dv_key)

            bounced   = random.random() < BOUNCE_RATE[group]
            converted = (not bounced) and (random.random() < CVR[group] / (1 - BOUNCE_RATE[group]))

            if bounced:
                t += timedelta(seconds=random.randint(5, 30))
                add("exit_fractional_purchase_page", t, uid, session_id, ticker, source, group, dv_key)
                continue

            t += timedelta(seconds=random.randint(3, 10))
            t = add("click_amount_input_field", t, uid, session_id, ticker, source, group, dv_key)

            if not converted:
                t += timedelta(seconds=random.randint(10, 60))
                add("exit_fractional_purchase_page", t, uid, session_id, ticker, source, group, dv_key)
                continue

            amount = lognormal_atv(group)
            # 경험자: 빠르게 입력 / 비경험자: 고민 후 입력
            input_wait = (1, 3) if not is_first else ((3, 8) if group == "A" else (1, 3))
            t += timedelta(seconds=random.randint(*input_wait))
            t = add("input_amount_complete", t, uid, session_id, ticker, source, group, dv_key, amount)

            # 경험자: 바로 클릭 / 비경험자 A군: 재확인 후 클릭
            btn_wait = (1, 2) if not is_first else ((2, 4) if group == "A" else (1, 2))
            t += timedelta(seconds=random.randint(*btn_wait))
            t = add("click_fractional_buy_button", t, uid, session_id, ticker, source, group, dv_key)

            confirm_wait = (1, 2) if not is_first else ((2, 4) if group == "A" else (1, 2))
            t += timedelta(seconds=random.randint(*confirm_wait))
            t = add("click_purchase_confirm", t, uid, session_id, ticker, source, group, dv_key)

            success = random.random() < 0.95
            status  = "success" if success else random.choice(["fail", "canceled"])
            error   = "" if success else random.choice(["잔고부족", "네트워크오류", "한도초과"])

            t += timedelta(seconds=random.randint(1, 3))
            seq = session_seq.get(session_id, 0) + 1
            session_seq[session_id] = seq
            row, _ = make_event(
                "complete_fractional_purchase", t,
                group=group,
                dv_key=dv_key,
                user_id=uid, session_id=session_id,
                event_sequence=seq,
                experiment_group=group, ticker=ticker,
                source_page=source, input_amount_krw="",
            )
            complete_event_id = row["event_id"]
            events.append(row)

            price_krw   = price_map.get(ticker, 200_000)
            frac_shares = round(amount / price_krw, 4)
            commission  = round(amount * COMMISSION_RATE)
            orders.append({
                "order_id":            gen_id("ORD_"),
                "user_id":             uid,
                "session_id":          session_id,
                "event_id":            complete_event_id,
                "order_type":          "fractional",
                "ticker":              ticker,
                "purchase_amount_krw": amount,
                "price_per_share_krw": price_krw,
                "fractional_shares":   frac_shares,
                "integer_shares":      "",
                "commission_krw":      commission,
                "status":              status,
                "error_code":          error,
                "order_timestamp":     row["server_timestamp"],
            })

        else:
            # ── 정수 구매 퍼널 ──────────────────────────────────────────
            t = add("click_integer_purchase", t, uid, session_id, ticker, source)
            t += timedelta(seconds=random.randint(1, 3))
            t = add("view_integer_purchase_page", t, uid, session_id, ticker, source)

            converted_int = random.random() < INTEGER_CVR

            if not converted_int:
                t += timedelta(seconds=random.randint(5, 40))
                add("exit_integer_purchase_page", t, uid, session_id, ticker, source)
                continue

            t += timedelta(seconds=random.randint(3, 10))
            t = add("click_quantity_input_field", t, uid, session_id, ticker, source)

            qty = random.randint(1, 10)
            t += timedelta(seconds=random.randint(5, 15))
            t = add("input_quantity_complete", t, uid, session_id, ticker, source)

            t += timedelta(seconds=random.randint(2, 5))
            t = add("click_integer_buy_button", t, uid, session_id, ticker, source)

            t += timedelta(seconds=random.randint(2, 5))
            t = add("click_purchase_confirm", t, uid, session_id, ticker, source)

            success = random.random() < 0.95
            status  = "success" if success else random.choice(["fail", "canceled"])
            error   = "" if success else random.choice(["잔고부족", "네트워크오류", "한도초과"])

            t += timedelta(seconds=random.randint(1, 3))
            seq = session_seq.get(session_id, 0) + 1
            session_seq[session_id] = seq
            row, _ = make_event(
                "complete_integer_purchase", t,
                group="",
                user_id=uid, session_id=session_id,
                event_sequence=seq,
                experiment_group="", ticker=ticker,
                source_page=source, input_amount_krw="",
            )
            complete_event_id = row["event_id"]
            events.append(row)

            price_krw      = price_map.get(ticker, 200_000)
            int_amount     = price_krw * qty
            commission     = round(int_amount * COMMISSION_RATE)
            orders.append({
                "order_id":            gen_id("ORD_"),
                "user_id":             uid,
                "session_id":          session_id,
                "event_id":            complete_event_id,
                "order_type":          "integer",
                "ticker":              ticker,
                "purchase_amount_krw": int_amount,
                "price_per_share_krw": price_krw,
                "fractional_shares":   "",
                "integer_shares":      qty,
                "commission_krw":      commission,
                "status":              status,
                "error_code":          error,
                "order_timestamp":     row["server_timestamp"],
            })

    print(f"  → events: {len(events):,}행 / orders: {len(orders):,}행")
    return events, orders


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1-5. baseline.csv (실험 전 7일 베이스라인)
# ─────────────────────────────────────────────────────────────────────────────

def generate_baseline():
    print("[baseline] 생성 중... (7행)")
    rows = []
    for i in range(7):
        d = PRE_START + timedelta(days=i)
        overseas_uv        = random.randint(4_800, 5_200)
        purchase_btn_uv    = round(overseas_uv * random.uniform(0.55, 0.65))  # 구매하기 클릭 UV
        int_cvr            = round(random.uniform(0.18, 0.22), 4)
        frac_cvr           = round(random.uniform(0.085, 0.110), 4)
        avg_dv_ms          = random.randint(22_000, 28_000)   # 실험 전 평균 Decision Velocity
        rows.append({
            "date":                      d.isoformat(),
            "overseas_stock_uv":         overseas_uv,
            "purchase_button_click_uv":  purchase_btn_uv,
            "integer_purchase_cvr":      int_cvr,
            "fractional_purchase_cvr":   frac_cvr,
            "avg_decision_velocity_ms":  avg_dv_ms,
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# CSV 저장
# ─────────────────────────────────────────────────────────────────────────────

def save_csv(rows, filepath, fieldnames):
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  [OK] {filepath}  ({len(rows):,} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    import os
    out_dir = os.path.dirname(os.path.abspath(__file__))

    print("=" * 60)
    print(" Toss Fractional Purchase A/B Test - Data Generation")
    print("=" * 60)

    # 1. users
    users = generate_users()
    save_csv(users, os.path.join(out_dir, "users.csv"), [
        "user_id", "experiment_group", "gender", "age_group",
        "device_type", "investment_type",
        "acquisition_channel", "first_trade_date",
        "total_trade_count", "is_first_fractional_exp",
    ])

    # 2. events + 3. orders
    events, orders = generate_events_and_orders(users)
    save_csv(events, os.path.join(out_dir, "events.csv"), [
        "event_id", "user_id", "session_id", "event_sequence", "event_name",
        "event_start_timestamp", "event_end_timestamp", "duration_ms",
        "engagement_time_msec",
        "client_timestamp", "server_timestamp",
        "experiment_group", "ticker", "source_page", "input_amount_krw",
    ])
    save_csv(orders, os.path.join(out_dir, "orders.csv"), [
        "order_id", "user_id", "session_id", "event_id",
        "order_type", "ticker", "purchase_amount_krw",
        "price_per_share_krw", "fractional_shares", "integer_shares",
        "commission_krw", "status", "error_code", "order_timestamp",
    ])

    # 5. baseline
    baseline = generate_baseline()
    save_csv(baseline, os.path.join(out_dir, "baseline.csv"), [
        "date", "overseas_stock_uv", "purchase_button_click_uv",
        "integer_purchase_cvr", "fractional_purchase_cvr",
        "avg_decision_velocity_ms",
    ])

    print("\n완료!")


if __name__ == "__main__":
    main()
