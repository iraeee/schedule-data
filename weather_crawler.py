"""
weather_crawler.py - 기상청 날씨 수집 통합 크롤러
Version: 2.0.0
Created: 2026-03-25

기능:
1. ASOS 일자료 (과거~전일) — 실측 확정 데이터
2. 단기예보 (오늘~3일) — 예보, 매번 최신화
3. 중기예보 (4~10일) — 예보, 매번 최신화

규칙:
- ASOS 확정 데이터가 있으면 예보로 덮어쓰지 않음
- 예보 데이터는 매번 최신값으로 갱신
- 날씨 4단계 분류: 맑음/흐림/비/눈
"""

import requests
import sqlite3
import os
import sys
import json
from datetime import datetime, timedelta


# ============================================================================
# 설정
# ============================================================================
API_KEY = "7c936cc1f21809dd469728d43991be4136170741ec967c2d774628d0e3bcca52"

ASOS_URL = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"
SHORT_FCST_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
MID_TA_URL = "https://apis.data.go.kr/1360000/MidFcstInfoService/getMidTa"
MID_LAND_URL = "https://apis.data.go.kr/1360000/MidFcstInfoService/getMidLandFcst"

ASOS_STN = "108"
SHORT_NX, SHORT_NY = 60, 127
MID_TA_REG = "11B10101"
MID_LAND_REG = "11B00000"

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'schedule.db')
BACKFILL_START = "20240701"


# ============================================================================
# DB
# ============================================================================
def init_weather_table(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            date TEXT PRIMARY KEY,
            avg_temp REAL,
            max_temp REAL,
            min_temp REAL,
            temp_range REAL,
            rainfall REAL,
            snowfall TEXT,
            avg_cloud REAL,
            avg_humidity REAL,
            sunshine_hours REAL,
            weather_event TEXT,
            sky_condition TEXT,
            source TEXT DEFAULT 'ASOS',
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_hourly (
            date TEXT,
            hour INTEGER,
            temp REAL,
            sky TEXT,
            updated_at TEXT,
            PRIMARY KEY (date, hour)
        )
    """)
    # 기존 DB에 source 컬럼 없으면 추가
    try:
        conn.execute("SELECT source FROM weather LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE weather ADD COLUMN source TEXT DEFAULT 'ASOS'")
        conn.execute("UPDATE weather SET source = 'ASOS' WHERE source IS NULL")
        print("📌 source 컬럼 추가")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_weather_sky ON weather(sky_condition)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_weather_source ON weather(source)")
    conn.commit()
    conn.close()
    print("✅ weather 테이블 준비 완료")


# ============================================================================
# 날씨 분류 (8단계)
# ☀️맑음 / ☁️흐림 / 🌦️이슬비 / 🌧️비 / ⛈️폭우 / 🌨️눈조금 / ❄️눈 / ⛄폭설
# ============================================================================
def classify_sky(iscs, sum_rn, dd_mes, avg_tca):
    """ASOS 확정 데이터 → 8단계 분류"""
    iscs = iscs or ""
    snowfall = 0
    if dd_mes and dd_mes.strip():
        try:
            snowfall = float(dd_mes)
        except:
            snowfall = 0
    
    # 1) 눈 계열 (적설 기준)
    if snowfall >= 10:
        return "폭설"
    if snowfall >= 3:
        return "눈"
    if snowfall > 0 or "{눈}" in iscs or "눈날림" in iscs:
        return "눈조금"
    
    # 2) 비 계열 (강수량 기준)
    if sum_rn >= 30:
        return "폭우"
    if sum_rn >= 1:
        return "비"
    if sum_rn > 0 or "{비}" in iscs:
        return "이슬비"
    
    # 3) 맑음/흐림 (운량 기준)
    if avg_tca <= 5:
        return "맑음"
    return "흐림"


def classify_forecast(sky_code, pty_code=0):
    """단기/중기 예보 → 8단계 중 가능한 분류"""
    if isinstance(sky_code, str):
        s = sky_code
        if '폭설' in s:
            return '폭설'
        if '눈' in s:
            return '눈'
        if '소나기' in s:
            return '비'
        if '비' in s:
            return '비'
        if '맑' in s:
            return '맑음'
        return '흐림'
    pty = int(pty_code) if pty_code else 0
    sky = int(sky_code) if sky_code else 1
    if pty in (2, 3):
        return '눈'
    if pty in (1, 4):
        return '비'
    if sky == 1:
        return '맑음'
    return '흐림'


def _has_precip(sky_text):
    """비/눈 여부 판단"""
    if not sky_text:
        return False, None
    s = sky_text
    if '눈' in s:
        return True, '눈'
    if '비' in s or '소나기' in s:
        return True, '비'
    return False, None


def _classify_ampm(sky_am, sky_pm):
    """오전/오후 예보를 비/눈 시간대 구분하여 분류
    반환: '오전비', '오후비', '비', '오전눈', '오후눈', '눈', '맑음', '흐림' 등
    """
    am_precip, am_type = _has_precip(sky_am)
    pm_precip, pm_type = _has_precip(sky_pm)
    
    if am_precip and pm_precip:
        # 둘 다 강수 → 눈+비 혼합이면 비 우선
        ptype = am_type if am_type == pm_type else '비'
        return ptype  # '비' 또는 '눈' (종일)
    elif am_precip:
        return f'오전{am_type}'  # '오전비' 또는 '오전눈'
    elif pm_precip:
        return f'오후{pm_type}'  # '오후비' 또는 '오후눈'
    else:
        # 강수 없음 → PM 기준 (흐림/구름많음/맑음)
        return classify_forecast(sky_pm or sky_am or '맑음')


# ============================================================================
# 1. ASOS 과거 데이터
# ============================================================================
def fetch_asos(start_dt, end_dt):
    try:
        resp = requests.get(ASOS_URL, params={
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
            'dataType': 'JSON', 'dataCd': 'ASOS', 'dateCd': 'DAY',
            'startDt': start_dt, 'endDt': end_dt, 'stnIds': ASOS_STN,
        }, timeout=30)
        if resp.status_code != 200:
            print(f"❌ ASOS: HTTP {resp.status_code}")
            return []
        data = resp.json()
        if data['response']['header']['resultCode'] != '00':
            print(f"❌ ASOS: {data['response']['header']['resultMsg']}")
            return []
        items = data['response']['body']['items']['item']
        print(f"📡 ASOS: {len(items)}건 ({start_dt}~{end_dt})")
        return items
    except Exception as e:
        print(f"❌ ASOS 실패: {e}")
        return []


def save_asos(items, db_path=DB_PATH):
    if not items:
        return 0
    conn = sqlite3.connect(db_path)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    saved = 0
    for item in items:
        try:
            dt = item.get('tm', '')
            conn.execute("""
                INSERT OR REPLACE INTO weather
                (date, avg_temp, max_temp, min_temp, temp_range, rainfall,
                 snowfall, avg_cloud, avg_humidity, sunshine_hours,
                 weather_event, sky_condition, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ASOS', ?)
            """, (
                dt,
                float(item.get('avgTa', 0) or 0),
                float(item.get('maxTa', 0) or 0),
                float(item.get('minTa', 0) or 0),
                round(float(item.get('maxTa', 0) or 0) - float(item.get('minTa', 0) or 0), 1),
                float(item.get('sumRn', 0) or 0),
                item.get('ddMes', '') or '',
                float(item.get('avgTca', 0) or 0),
                float(item.get('avgRhm', 0) or 0),
                float(item.get('sumSsHr', 0) or 0),
                item.get('iscs', '') or '',
                classify_sky(
                    item.get('iscs', ''), float(item.get('sumRn', 0) or 0),
                    item.get('ddMes', ''), float(item.get('avgTca', 0) or 0)
                ), now
            ))
            saved += 1
        except Exception as e:
            print(f"⚠️ ASOS 저장 실패 ({item.get('tm','?')}): {e}")
    conn.commit()
    conn.close()
    return saved


# ============================================================================
# 2. 단기예보 (오늘~3일)
# ============================================================================
def fetch_short_forecast():
    today = datetime.now()
    base_date = today.strftime('%Y%m%d')
    hour = today.hour
    base_times = [23, 20, 17, 14, 11, 8, 5, 2]
    base_time = '0200'
    for bt in base_times:
        if hour >= bt + 1:
            base_time = f'{bt:02d}00'
            break
    if hour < 3:
        base_date = (today - timedelta(days=1)).strftime('%Y%m%d')
        base_time = '2300'

    try:
        resp = requests.get(SHORT_FCST_URL, params={
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '1000',
            'dataType': 'JSON', 'base_date': base_date, 'base_time': base_time,
            'nx': SHORT_NX, 'ny': SHORT_NY,
        }, timeout=30)
        if resp.status_code != 200:
            print(f"⚠️ 단기예보: HTTP {resp.status_code} (API 미활성화 가능)")
            return {}
        data = resp.json()
        if data['response']['header']['resultCode'] != '00':
            print(f"⚠️ 단기예보: {data['response']['header']['resultMsg']}")
            return {}
        items = data['response']['body']['items']['item']

        daily = {}
        for item in items:
            fd = item['fcstDate']
            cat = item['category']
            val = item['fcstValue']
            if fd not in daily:
                daily[fd] = {'max_t': [], 'min_t': [], 'temps': [], 'sky': [], 'pty': []}
            if cat == 'TMX':
                daily[fd]['max_t'].append(float(val))
            elif cat == 'TMN':
                daily[fd]['min_t'].append(float(val))
            elif cat == 'TMP':
                daily[fd]['temps'].append(float(val))
            elif cat == 'SKY':
                daily[fd]['sky'].append(int(val))
            elif cat == 'PTY':
                daily[fd]['pty'].append(int(val))

        result = {}
        hourly = {}  # {(date_fmt, hour): {'temp': x, 'sky': 'x'}}
        
        for fd, d in daily.items():
            dt_fmt = fd[:4] + '-' + fd[4:6] + '-' + fd[6:8]
            mx = max(d['max_t']) if d['max_t'] else (max(d['temps']) if d['temps'] else None)
            mn = min(d['min_t']) if d['min_t'] else (min(d['temps']) if d['temps'] else None)
            if mx is not None and mn is not None:
                worst_sky = max(d['sky']) if d['sky'] else 1
                worst_pty = max(d['pty']) if d['pty'] else 0
                result[dt_fmt] = {
                    'max_temp': mx, 'min_temp': mn,
                    'sky': classify_forecast(worst_sky, worst_pty),
                }
        
        # 시간별 데이터 추출
        hourly_raw = {}  # {(fcstDate, fcstTime): {TMP, SKY, PTY}}
        for item in items:
            fd = item['fcstDate']
            ft = item['fcstTime']
            cat = item['category']
            val = item['fcstValue']
            key = (fd, ft)
            if key not in hourly_raw:
                hourly_raw[key] = {}
            if cat in ('TMP', 'SKY', 'PTY'):
                hourly_raw[key][cat] = val
        
        for (fd, ft), vals in hourly_raw.items():
            if 'TMP' in vals:
                dt_fmt = fd[:4] + '-' + fd[4:6] + '-' + fd[6:8]
                hr = int(ft[:2])
                sky_code = int(vals.get('SKY', 1))
                pty_code = int(vals.get('PTY', 0))
                hourly[(dt_fmt, hr)] = {
                    'temp': float(vals['TMP']),
                    'sky': classify_forecast(sky_code, pty_code),
                }
        
        print('📡 단기예보: ' + str(len(result)) + '일, 시간별 ' + str(len(hourly)) + '건')
        return result, hourly
    except Exception as e:
        print('⚠️ 단기예보 실패: ' + str(e))
        return {}, {}


# ============================================================================
# 3. 중기예보 (4~10일)
# ============================================================================
def fetch_mid_forecast():
    today = datetime.now()
    if today.hour >= 18:
        tmFc = today.strftime('%Y%m%d') + '1800'
        base_date = today
    elif today.hour >= 6:
        tmFc = today.strftime('%Y%m%d') + '0600'
        base_date = today
    else:
        tmFc = (today - timedelta(days=1)).strftime('%Y%m%d') + '1800'
        base_date = today - timedelta(days=1)  # 발표일 기준!

    result = {}
    ta_data = {}
    land_data = {}

    # 중기기온
    try:
        resp = requests.get(MID_TA_URL, params={
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '10',
            'dataType': 'JSON', 'regId': MID_TA_REG, 'tmFc': tmFc,
        }, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data['response']['header']['resultCode'] == '00':
                item = data['response']['body']['items']['item'][0]
                for d in range(3, 11):
                    dt = (base_date + timedelta(days=d)).strftime('%Y-%m-%d')
                    mk, nk = f'taMax{d}', f'taMin{d}'
                    if mk in item and nk in item:
                        ta_data[dt] = {'max_temp': float(item[mk]), 'min_temp': float(item[nk])}
        else:
            print(f"⚠️ 중기기온: HTTP {resp.status_code} (API 미활성화 가능)")
    except Exception as e:
        print(f"⚠️ 중기기온 실패: {e}")

    # 중기육상
    try:
        resp = requests.get(MID_LAND_URL, params={
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '10',
            'dataType': 'JSON', 'regId': MID_LAND_REG, 'tmFc': tmFc,
        }, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data['response']['header']['resultCode'] == '00':
                item = data['response']['body']['items']['item'][0]
                for d in range(3, 11):
                    dt = (base_date + timedelta(days=d)).strftime('%Y-%m-%d')
                    if d <= 7:
                        # AM/PM 둘 다 확인하여 비/눈 시간대 구분
                        sky_am = item.get(f'wf{d}Am', '')
                        sky_pm = item.get(f'wf{d}Pm', '')
                        sky_str = _classify_ampm(sky_am, sky_pm)
                    else:
                        sky_str = item.get(f'wf{d}', '')
                    if sky_str:
                        land_data[dt] = sky_str
        else:
            print(f"⚠️ 중기육상: HTTP {resp.status_code} (API 미활성화 가능)")
    except Exception as e:
        print(f"⚠️ 중기육상 실패: {e}")

    for dt in ta_data:
        sky_val = land_data.get(dt, '맑음')
        # AM/PM 구분된 값은 이미 분류됨 (오전비, 오후비 등), 그 외는 classify
        if sky_val.startswith('오전') or sky_val.startswith('오후') or sky_val in ('맑음','흐림','비','눈','폭우','폭설','이슬비','눈조금'):
            sky_final = sky_val
        else:
            sky_final = classify_forecast(sky_val)
        result[dt] = {
            'max_temp': ta_data[dt]['max_temp'],
            'min_temp': ta_data[dt]['min_temp'],
            'sky': sky_final,
        }
    if result:
        print(f"📡 중기예보: {len(result)}일 ({min(result.keys())}~{max(result.keys())})")
    else:
        print("⚠️ 중기예보: 데이터 없음")
    return result


# ============================================================================
# 예보 저장 (ASOS 확정은 보호)
# ============================================================================
def save_forecast(forecast, source, db_path=DB_PATH):
    if not forecast:
        return 0
    conn = sqlite3.connect(db_path)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    saved = 0
    for dt, info in forecast.items():
        try:
            cur = conn.execute("SELECT source FROM weather WHERE date = ?", (dt,))
            row = cur.fetchone()
            if row and row[0] == 'ASOS':
                continue
            mx = info['max_temp']
            mn = info['min_temp']
            conn.execute("""
                INSERT OR REPLACE INTO weather
                (date, avg_temp, max_temp, min_temp, temp_range, rainfall,
                 snowfall, avg_cloud, avg_humidity, sunshine_hours,
                 weather_event, sky_condition, source, updated_at)
                VALUES (?, ?, ?, ?, ?, -1, '', -1, -1, -1, '', ?, ?, ?)
            """, (dt, round((mx+mn)/2, 1), mx, mn, round(mx-mn, 1), info['sky'], source, now))
            saved += 1
        except Exception as e:
            print(f"⚠️ 예보 저장 실패 ({dt}): {e}")
    conn.commit()
    conn.close()
    return saved


# ============================================================================
# 시간별 예보 저장
# ============================================================================
def save_hourly(hourly, db_path=DB_PATH):
    """시간별 예보 저장 — 항상 최신값으로 덮어씀"""
    if not hourly:
        return 0
    conn = sqlite3.connect(db_path)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    saved = 0
    for (dt, hr), info in hourly.items():
        try:
            conn.execute(
                "INSERT OR REPLACE INTO weather_hourly (date, hour, temp, sky, updated_at) VALUES (?,?,?,?,?)",
                (dt, hr, info['temp'], info['sky'], now)
            )
            saved += 1
        except:
            pass
    conn.commit()
    conn.close()
    return saved


# ============================================================================
# 명령어
# ============================================================================
def backfill(db_path=DB_PATH):
    init_weather_table(db_path)
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    print(f"🔄 백필: {BACKFILL_START} ~ {yesterday}")
    items = fetch_asos(BACKFILL_START, yesterday)
    saved = save_asos(items, db_path)
    print(f"✅ 백필 완료: {saved}건")
    _print_summary(db_path)


def update(db_path=DB_PATH):
    init_weather_table(db_path)
    print("=" * 50)
    print(f"🔄 날씨 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

    # ASOS 최근 3일
    d3 = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
    d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    a = save_asos(fetch_asos(d3, d1), db_path)
    print(f"  ASOS: {a}건")

    # 단기예보 + 시간별
    short_daily, short_hourly = fetch_short_forecast()
    s = save_forecast(short_daily, 'SHORT_FCST', db_path)
    h = save_hourly(short_hourly, db_path)
    print(f"  단기예보: {s}건, 시간별: {h}건")

    # 중기예보
    m = save_forecast(fetch_mid_forecast(), 'MID_FCST', db_path)
    print(f"  중기예보: {m}건")

    print(f"\n✅ 완료: ASOS {a} + 단기 {s} + 시간별 {h} + 중기 {m}")
    _print_summary(db_path)


def forecast_only(db_path=DB_PATH):
    init_weather_table(db_path)
    short_daily, short_hourly = fetch_short_forecast()
    s = save_forecast(short_daily, 'SHORT_FCST', db_path)
    h = save_hourly(short_hourly, db_path)
    m = save_forecast(fetch_mid_forecast(), 'MID_FCST', db_path)
    print(f"✅ 예보: 단기 {s} + 시간별 {h} + 중기 {m}건")


def _print_summary(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT COUNT(*), MIN(date), MAX(date) FROM weather")
    cnt, mn, mx = c.fetchone()
    c.execute("SELECT source, COUNT(*) FROM weather GROUP BY source")
    src = c.fetchall()
    c.execute("SELECT sky_condition, COUNT(*) FROM weather GROUP BY sky_condition ORDER BY COUNT(*) DESC")
    sky = c.fetchall()
    conn.close()
    print(f"\n📊 총 {cnt}건 | {mn} ~ {mx}")
    print(f"   소스: {' / '.join(f'{s}:{c}건' for s,c in src)}")
    emoji_map = {'맑음':'☀️','흐림':'☁️','비':'🌧️','눈':'❄️'}
    sky_parts = []
    for s, c in sky:
        em = emoji_map.get(s, '?')
        sky_parts.append(f"{em}{s} {c}일")
    print(f"   날씨: {' / '.join(sky_parts)}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "update"
    db = sys.argv[2] if len(sys.argv) > 2 else DB_PATH
    
    if cmd == "backfill":
        backfill(db)
    elif cmd == "update":
        update(db)
    elif cmd == "forecast":
        forecast_only(db)
    elif cmd == "init":
        init_weather_table(db)
    else:
        print("사용법: python weather_crawler.py [backfill|update|forecast|init] [db_path]")
        print("  backfill  - 전체 과거 데이터 수집")
        print("  update    - ASOS + 단기예보 + 중기예보 (기본)")
        print("  forecast  - 예보만 업데이트")
