"""
홈쇼핑 스케줄 데이터 수집 스크립트
POST API 방식 지원 버전
"""

from __future__ import annotations
import argparse
import datetime as _dt
import json
import logging
import os
import sys
from typing import Any, Dict, List

# 상위 디렉토리의 utils 폴더를 경로에 추가
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
utils_dir = os.path.join(parent_dir, 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

try:
    import requests
except ImportError as exc:
    raise SystemExit(
        "The 'requests' library is required to run this script. "
        "Please install it with 'pip install requests' and try again."
    ) from exc

from schedule_analytics import ScheduleDB

logger = logging.getLogger(__name__)

def protect_revenue_data(new_records, db_path, date_str, debug=False):
    """
    매출 데이터 보호 로직
    - 기존 매출이 있는데 새 매출이 0이면 기존 값 유지
    - 매출이 급감하면 기존 값 유지
    """
    if not os.path.exists(db_path):
        return new_records
    
    # 날짜 형식 변환 (YYMMDD -> YYYY-MM-DD)
    if len(date_str) == 6:  # YYMMDD
        date_obj = _dt.datetime.strptime(date_str, "%y%m%d")
    else:  # YYYYMMDD
        date_obj = _dt.datetime.strptime(date_str, "%Y%m%d")
    formatted_date = date_obj.strftime("%Y-%m-%d")
    
    # 기존 데이터 로드
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 해당 날짜의 기존 데이터 조회 (broadcast_id가 없으므로 time, platform, broadcast로 매칭)
    cursor.execute("""
        SELECT time, platform, broadcast, revenue, units_sold 
        FROM schedule 
        WHERE date = ?
    """, (formatted_date,))
    
    existing_data = {}
    for row in cursor.fetchall():
        key = f"{row[0]}_{row[1]}_{row[2]}"  # time_platform_broadcast
        existing_data[key] = {
            'revenue': row[3] or 0,
            'units_sold': row[4] or 0
        }
    conn.close()
    
    if not existing_data:
        if debug:
            print("[보호] 기존 데이터 없음 - 보호 로직 스킵")
        return new_records
    
    # 보호 로직 적용
    protected_count = 0
    increased_count = 0
    
    for record in new_records:
        # broadcast_id가 없으므로 time, platform, broadcast로 키 생성
        key = f"{record.get('time')}_{record.get('platform')}_{record.get('broadcast')}"
        new_revenue = record['revenue']
        
        if key in existing_data:
            old_revenue = existing_data[key]['revenue']
            old_units = existing_data[key]['units_sold']
            
            # 보호 규칙 1: 기존 매출이 있는데 새로 0원이면 기존 값 유지
            if old_revenue > 0 and new_revenue == 0:
                if debug:
                    print(f"  💾 매출 보호: {record['time']} {record['platform']} - {old_revenue:,}원 유지 (새값: 0)")
                record['revenue'] = old_revenue
                record['units_sold'] = old_units
                protected_count += 1
                
            # 보호 규칙 2: 매출이 70% 이상 감소하면 의심스러움 - 기존 값 유지
            elif old_revenue > 1000000 and new_revenue < old_revenue * 0.3:
                if debug:
                    print(f"  💾 급감 보호: {record['time']} {record['platform']} - {old_revenue:,}원 유지 (새값: {new_revenue:,})")
                record['revenue'] = old_revenue
                record['units_sold'] = old_units
                protected_count += 1
                
            # 매출 증가는 정상 반영
            elif new_revenue > old_revenue:
                if debug and old_revenue > 0:
                    print(f"  📈 매출 증가: {record['time']} {record['platform']} - {new_revenue:,}원 (기존: {old_revenue:,})")
                increased_count += 1
    
    if protected_count > 0:
        print(f"\n💾 매출 보호 적용: {protected_count}개 항목의 기존 매출 유지")
    if increased_count > 0:
        print(f"📈 매출 증가 반영: {increased_count}개 항목")
    
    return new_records

def fetch_schedule_json(
    date_str: str = None,
    url: str = "https://live.ecomm-data.com/schedule/list_hs",
    *,
    json_file: str | None = None,
    debug: bool = False,
) -> Dict[str, Any]:
    """POST 방식으로 스케줄 데이터 가져오기 (세션 초기화 포함)"""
    
    if json_file:
        with open(json_file, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload
    
    # 날짜 처리 - 기본값은 오늘
    if date_str is None:
        today = _dt.datetime.now()
        date_str = today.strftime("%y%m%d")  # YYMMDD 형식
    
    # 세션 생성 (쿠키 유지)
    session = requests.Session()
    
    cookie_str = "_ga=GA1.1.1148900813.1753071738; _gcl_au=1.1.2127562858.1753071789.734155396.1753071810.1753071813; _fwb=8206MdvNQcDXiuEel5llWx.1753071736391; sales2=eyJoaXN0b3J5IjpbNTAwMDAwMDNdLCJsYWJhbmdfb2JqIjp7fSwicGFzdF9rZXl3b3JkMiI6Iu2DgO2IrOq3uOumrOuqqCIsInVzZXIiOnsidXNlcl9pZCI6IjlqOTE3YldXdHktQ29FSU9Qa2wzTiIsIm5pY2tuYW1lIjoiaXJhZSIsInNlc3NfaWQiOiI4MTBTbkFuMXl0SEktLV9pRGEtRDYiLCJ1c2VyX3R5cGUiOjAsInZvdWNoZXIiOjAsInByZWZlciI6MX19; sales2.sig=lz9-0bjYr4MEirSNAA8JCeqAwYo; _ga_VN7F3DELDK=GS2.1.s1759100069$o2$g1$t1759100902$j53$l0$h0; _ga_NLGYGNTN3F=GS2.1.s1759100069$o2$g1$t1759100902$j53$l0$h0"
    
    headers = {
        'Host': 'live.ecomm-data.com',
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        'Accept': '*/*',
        'Content-Type': 'application/json',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'sec-ch-ua-platform': '"Windows"',
        'Origin': 'https://live.ecomm-data.com',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://live.ecomm-data.com/schedule/hs',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cookie': cookie_str
    }
    
    # 세션 초기화 - 메인 페이지 먼저 방문 (중요!)
    if debug:
        print(f"[세션] 메인 페이지 방문 중...")
    try:
        main_resp = session.get(
            'https://live.ecomm-data.com/schedule/hs',
            headers={
                'User-Agent': headers['User-Agent'],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Cookie': cookie_str
            },
            timeout=10
        )
        if debug:
            print(f"[세션] 메인 페이지 응답: {main_resp.status_code}")
    except:
        pass  # 세션 초기화 실패해도 계속 진행
    
    # POST 요청 데이터
    post_data = {"date": date_str}
    
    print(f"[API] POST 요청: {url}")
    print(f"[API] 날짜: {date_str}")
    
    try:
        response = session.post(
            url, 
            headers=headers, 
            json=post_data,  # JSON으로 전송
            timeout=30
        )
        
        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch schedule: HTTP {response.status_code}: {response.text}")
        
        payload = response.json()
        
        if debug:
            # 응답 구조 디버깅
            print(f"[DEBUG] 응답 키: {list(payload.keys())}")
            if isinstance(payload, list) and len(payload) > 0:
                print(f"[DEBUG] 첫 번째 항목 키: {list(payload[0].keys())}")
                sample = payload[0]
                print(f"[DEBUG] 샘플 데이터:")
                for key, value in sample.items():
                    if key in ['hsshow_title', 'platform_name', 'sales_cnt', 'sales_amt', 'item_cnt']:
                        print(f"  - {key}: {value}")
            elif isinstance(payload, dict):
                if "list" in payload and payload["list"]:
                    sample = payload["list"][0]
                    print(f"[DEBUG] 첫 번째 항목 키: {list(sample.keys())}")
                    print(f"[DEBUG] 샘플 데이터:")
                    for key, value in sample.items():
                        if key in ['hsshow_title', 'platform_name', 'sales_cnt', 'sales_amt', 'item_cnt']:
                            print(f"  - {key}: {value}")
        
        return payload
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API 요청 실패: {e}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"JSON 파싱 실패: {e}")

def parse_records(data: Any, date_str: str, debug: bool = False) -> List[Dict[str, Any]]:
    """API 응답 데이터 파싱"""
    
    # 엑셀 파일에서 방송사별 시간대별 비용 로드 (평일/주말 구분)
    def load_cost_from_excel(path="방송사별 방송정액비.xlsx"):
        try:
            import pandas as pd
            
            # 엑셀 파일 읽기 - 헤더 없이
            df = pd.read_excel(path, header=None)
            
            weekday_costs = {}
            weekend_costs = {}
            
            # 평일 방송사 리스트 (Excel 3~18행 = pandas index 2~17)
            weekday_platforms = [
                (2, "현대홈쇼핑"),
                (3, "GS홈쇼핑"),      
                (4, "롯데홈쇼핑"),
                (5, "CJ온스타일"),
                (6, "홈앤쇼핑"),
                (7, "NS홈쇼핑"),      
                (8, "공영쇼핑"),
                (9, "GS홈쇼핑 마이샵"),
                (10, "CJ온스타일 플러스"),
                (11, "현대홈쇼핑 플러스샵"),
                (12, "SK스토아"),      
                (13, "신세계쇼핑"),
                (14, "KT알파쇼핑"),    
                (15, "NS홈쇼핑 샵플러스"),
                (16, "쇼핑엔티"),
                (17, "롯데원티비")
            ]
            
            # 주말 방송사 리스트 (Excel 23~38행 = pandas index 22~37)
            weekend_platforms = [
                (22, "현대홈쇼핑"),
                (23, "GS홈쇼핑"),
                (24, "롯데홈쇼핑"),
                (25, "CJ온스타일"),
                (26, "홈앤쇼핑"),
                (27, "NS홈쇼핑"),
                (28, "공영쇼핑"),
                (29, "GS홈쇼핑 마이샵"),
                (30, "CJ온스타일 플러스"),
                (31, "현대홈쇼핑 플러스샵"),
                (32, "SK스토아"),
                (33, "신세계쇼핑"),
                (34, "KT알파쇼핑"),
                (35, "NS홈쇼핑 샵플러스"),
                (36, "쇼핑엔티"),
                (37, "롯데원티비")
            ]
            
            # 평일 데이터 로드
            for idx, platform in weekday_platforms:
                weekday_hourly = {}
                for hour in range(24):
                    try:
                        col_idx = hour + 1  # B열(1)이 0시, M열(12)이 11시
                        val = df.iloc[idx, col_idx]
                        
                        if pd.notnull(val):
                            if isinstance(val, (int, float)):
                                cost = int(val)
                            else:
                                val_str = str(val).replace(',', '').replace('원', '').strip()
                                if val_str.isdigit():
                                    cost = int(val_str)
                                else:
                                    cost = 0
                        else:
                            cost = 0
                    except:
                        cost = 0
                    weekday_hourly[hour] = cost
                
                # 모든 변형 저장
                weekday_costs[platform] = weekday_hourly
                weekday_costs[platform.lower()] = weekday_hourly
                weekday_costs[platform.upper()] = weekday_hourly
                
                # 추가 변형 처리
                if "GS홈쇼핑" in platform and "마이샵" not in platform:
                    weekday_costs["gs홈쇼핑"] = weekday_hourly
                elif "GS홈쇼핑 마이샵" in platform:
                    weekday_costs["gs홈쇼핑 마이샵"] = weekday_hourly
                    weekday_costs["GS홈쇼핑마이샵"] = weekday_hourly
                elif "CJ온스타일" in platform and "플러스" not in platform:
                    weekday_costs["cj온스타일"] = weekday_hourly
                elif "NS홈쇼핑" in platform and "샵플러스" not in platform:
                    weekday_costs["ns홈쇼핑"] = weekday_hourly
            
            # 주말 데이터 로드
            for idx, platform in weekend_platforms:
                weekend_hourly = {}
                for hour in range(24):
                    try:
                        col_idx = hour + 1
                        val = df.iloc[idx, col_idx]
                        
                        if pd.notnull(val):
                            if isinstance(val, (int, float)):
                                cost = int(val)
                            else:
                                val_str = str(val).replace(',', '').replace('원', '').strip()
                                if val_str.isdigit():
                                    cost = int(val_str)
                                else:
                                    cost = 0
                        else:
                            cost = 0
                    except:
                        cost = 0
                    weekend_hourly[hour] = cost
                
                # 모든 변형 저장
                weekend_costs[platform] = weekend_hourly
                weekend_costs[platform.lower()] = weekend_hourly
                weekend_costs[platform.upper()] = weekend_hourly
                
                # 추가 변형 처리
                if "GS홈쇼핑" in platform and "마이샵" not in platform:
                    weekend_costs["gs홈쇼핑"] = weekend_hourly
                elif "GS홈쇼핑 마이샵" in platform:
                    weekend_costs["gs홈쇼핑 마이샵"] = weekend_hourly
                    weekend_costs["GS홈쇼핑마이샵"] = weekend_hourly
                elif "CJ온스타일" in platform and "플러스" not in platform:
                    weekend_costs["cj온스타일"] = weekend_hourly
                elif "NS홈쇼핑" in platform and "샵플러스" not in platform:
                    weekend_costs["ns홈쇼핑"] = weekend_hourly
            
            print(f"[정액비] 엑셀에서 평일 {len(weekday_platforms)}개, 주말 {len(weekend_platforms)}개 방송사 비용 로드")
            
            return {
                'weekday': weekday_costs,
                'weekend': weekend_costs
            }
        except Exception as e:
            print(f"[경고] 엑셀 파일 로드 실패: {e}")
            return None

    # 방송사명으로 시간대별 비용 찾기
    def get_cost_from_excel(platform_name: str, hour: int, cost_table: dict) -> int:
        if not cost_table:
            return 0
        
        # 1. 정확한 매칭
        if platform_name in cost_table:
            return cost_table[platform_name].get(hour, 0)
        
        # 2. 대소문자 변형
        variations = [
            platform_name.lower(),
            platform_name.upper(),
            platform_name.replace(" ", ""),
            platform_name.replace(" ", "").lower(),
        ]
        
        for variant in variations:
            if variant in cost_table:
                return cost_table[variant].get(hour, 0)
        
        # 3. 부분 매칭
        platform_lower = platform_name.lower()
        for key in cost_table.keys():
            if key.lower() == platform_lower:
                return cost_table[key].get(hour, 0)
        
        return 0

    # 엑셀에서 비용 정보 로드
    excel_cost_table = load_cost_from_excel()
    
    result: List[Dict[str, Any]] = []
    
    # 날짜 파싱 (YYMMDD 형식)
    try:
        if len(date_str) == 6:  # YYMMDD
            date_obj = _dt.datetime.strptime(date_str, "%y%m%d")
        else:  # YYYYMMDD
            date_obj = _dt.datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        print(f"[오류] 날짜 파싱 실패: {date_str}")
        raise
    
    # 응답이 리스트인지 딕셔너리인지 확인
    if isinstance(data, list):
        shows = data
    elif isinstance(data, dict) and "list" in data:
        shows = data.get("list", [])
    else:
        print("[오류] 예상치 못한 응답 구조")
        return []
    
    print(f"[파싱] {len(shows)}개 방송 데이터 처리 중...")
    
    # 필드명 변경 가능성 체크를 위한 샘플 출력
    if debug and shows:
        print("\n[DEBUG] 첫 번째 방송 데이터 구조:")
        sample = shows[0]
        for key, value in sample.items():
            print(f"  {key}: {value}")
        print()

    for idx, show in enumerate(shows):
        try:
            broadcast_id = show.get("hsshow_id") or ""
            start_raw = show.get("hsshow_datetime_start", "")
            
            if not start_raw:
                print(f"[경고] {idx+1}번째 항목: 시작 시간 없음")
                continue
            
            try:
                start_dt = _dt.datetime.strptime(start_raw, "%Y%m%d%H%M")
            except ValueError:
                print(f"[경고] {idx+1}번째 항목: 시간 파싱 실패 - {start_raw}")
                continue
            
            broadcast_date = start_dt.strftime("%Y-%m-%d")
            broadcast_time = start_dt.strftime("%H:%M")
            title = (show.get("hsshow_title") or "").strip()
            platform = (show.get("platform_name") or "").strip()
            cat = show.get("cat") or {}
            category = (cat.get("cat_name") or "").strip()
            
            # 매출 관련 필드 확인 - 다양한 필드명 시도
            sales_count = 0
            sales_amount = 0
            
            # 판매수량 필드 체크
            for field in ['sales_cnt', 'salesCnt', 'sales_count', 'salesCount', 'sale_cnt']:
                if field in show:
                    val = show.get(field)
                    if val is not None:
                        try:
                            sales_count = int(val)
                            if debug and sales_count > 0:
                                print(f"  [{idx+1}] 판매수량 필드 '{field}': {sales_count}")
                            break
                        except (ValueError, TypeError):
                            pass
            
            # 매출액 필드 체크 - 중요!
            for field in ['sales_amt', 'salesAmt', 'sales_amount', 'salesAmount', 'sale_amt', 'revenue']:
                if field in show:
                    val = show.get(field)
                    if val is not None:
                        try:
                            sales_amount = int(val)
                            if debug and sales_amount > 0:
                                print(f"  [{idx+1}] 매출액 필드 '{field}': {sales_amount:,}원")
                            break
                        except (ValueError, TypeError):
                            pass
            
            # 매출액이 0인 경우 디버깅
            if sales_amount == 0 and debug:
                print(f"  [{idx+1}] ⚠️ 매출액 0원 - {platform} / {title[:30]}")
                print(f"    가능한 필드들: {[k for k in show.keys() if 'sale' in k.lower() or 'amt' in k.lower()]}")
            
            product_count = int(show.get("item_cnt") or 0)
            
            # 메이저 방송사 여부
            MAJOR_PLATFORMS = {"GS홈쇼핑", "현대홈쇼핑", "CJ온스타일", "롯데홈쇼핑"}
            is_major = 1 if platform in MAJOR_PLATFORMS else 0
            
            # 시간대 추출
            hour = start_dt.hour
            
            # 요일 확인 (0=월, 1=화, ... 5=토, 6=일)
            weekday = start_dt.weekday()
            is_weekend = weekday >= 5  # 토(5), 일(6)은 주말
            
            # 비용 계산 - 정확한 평일/주말 구분
            cost = 0
            if excel_cost_table:
                if is_weekend:
                    cost = get_cost_from_excel(platform, hour, excel_cost_table.get('weekend', {}))
                else:
                    cost = get_cost_from_excel(platform, hour, excel_cost_table.get('weekday', {}))
            
            # ROI 계산
            roi = 0.0
            if cost > 0:
                roi = float(sales_amount) / float(cost)
            
            record = {
                "date": broadcast_date,
                "time": broadcast_time,
                "broadcast": title,
                "platform": platform,
                "category": category,
                "units_sold": max(sales_count, 0),
                "revenue": max(sales_amount, 0),
                "product_count": max(product_count, 0),
                "cost": cost,
                "roi": roi,
                "is_major": is_major,
            }
            result.append(record)
            
        except Exception as e:
            print(f"[오류] {idx+1}번째 항목 처리 실패: {e}")
            if debug:
                import traceback
                traceback.print_exc()
    
    # 결과 통계
    if result:
        total_revenue = sum(r['revenue'] for r in result)
        zero_revenue_count = sum(1 for r in result if r['revenue'] == 0)
        
        print(f"\n[파싱 완료]")
        print(f"  - 총 {len(result)}개 데이터")
        print(f"  - 총 매출액: {total_revenue:,}원")
        print(f"  - 매출 0원 항목: {zero_revenue_count}개 ({zero_revenue_count/len(result)*100:.1f}%)")
        
        # 샘플 출력
        print(f"\n[샘플 데이터] 상위 5개:")
        for i, r in enumerate(sorted(result, key=lambda x: x['revenue'], reverse=True)[:5]):
            weekday = _dt.datetime.strptime(r['date'], "%Y-%m-%d").weekday()
            weekday_str = "주말" if weekday >= 5 else "평일"
            print(f"  {i+1}. {r['date']}({weekday_str}) {r['platform']} {r['time']}")
            print(f"     방송: {r['broadcast'][:40]}")
            print(f"     매출: {r['revenue']:,}원, 비용: {r['cost']:,}원, ROI: {r['roi']:.2f}")
    
    return result

def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="홈쇼핑 스케줄 데이터 수집")
    parser.add_argument("--db", dest="db_path", metavar="PATH", default="schedule.db")
    parser.add_argument("--date", dest="date", default=None, help="날짜 (YYMMDD 형식, 예: 250820)")
    parser.add_argument("--json-file", dest="json_file", default=None, help="로컬 JSON 파일 경로")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug", action="store_true", help="디버깅 모드 활성화")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    db_path = args.db_path
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    # 날짜 처리
    if args.date:
        date_str = args.date
    else:
        date_str = _dt.datetime.now().strftime("%y%m%d")
    
    print(f"\n{'='*60}")
    print(f"홈쇼핑 데이터 수집")
    print(f"날짜: {date_str}")
    print(f"API: POST 방식")
    print(f"디버그 모드: {'ON' if args.debug else 'OFF'}")
    print(f"{'='*60}\n")

    print("[스케줄] 스케줄 데이터 요청 중...")
    try:
        data = fetch_schedule_json(
            date_str=date_str,
            json_file=args.json_file,
            debug=args.debug
        )
    except Exception as exc:
        print(f"[오류] API 요청 실패: {str(exc).encode('utf-8', errors='replace').decode('utf-8')}")
        return

    try:
        records = parse_records(data, date_str, debug=args.debug)
    except Exception as exc:
        print(f"[오류] JSON 파싱 실패: {exc}")
        if args.debug:
            import traceback
            traceback.print_exc()
        return

    if not records:
        print("[오류] 스케줄 데이터 없음")
        return

    print(f"\n[완료] 총 {len(records)}건 데이터 파싱됨")

    # 매출 보호 로직 적용
    protected_records = protect_revenue_data(records, db_path, date_str, args.debug)

    # DB 저장
    db = ScheduleDB(db_path)
    db.create_table()
    db.insert_records(protected_records)
    db.close()

    print(f"[저장] {len(protected_records)}개 방송 데이터를 {db_path}에 저장 완료")
    
    # 매출 0원 항목 경고
    zero_count = sum(1 for r in records if r['revenue'] == 0)
    if zero_count > len(records) * 0.5:  # 50% 이상이 0원이면 경고
        print(f"\n⚠️ 경고: 매출 0원 항목이 {zero_count}개 ({zero_count/len(records)*100:.1f}%)입니다.")
        print("API 응답 구조가 변경되었을 가능성이 있습니다.")
        print("--debug 옵션으로 상세 정보를 확인하세요.")

if __name__ == "__main__":
    main()