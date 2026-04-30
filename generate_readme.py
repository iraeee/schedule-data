#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
README.md 생성 - revenue 컬럼 사용
"""

import sqlite3
import os
from datetime import datetime
import json

try:
    import pytz
    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)
    time_str = now_kst.strftime('%Y-%m-%d %H:%M:%S KST')
except:
    time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')

print("="*50)
print("README 생성 시작")
print(f"시간: {time_str}")
print("="*50)

# DB 찾기
db_file = None
if os.path.exists('schedule.db'):
    db_file = 'schedule.db'
    print("✅ schedule.db 발견")
elif os.path.exists('schedule.db.zst'):
    print("📦 압축 DB 발견, 해제 중...")
    try:
        import zstandard as zstd
        with open('schedule.db.zst', 'rb') as f:
            dctx = zstd.ZstdDecompressor()
            with open('schedule.db', 'wb') as out:
                out.write(dctx.decompress(f.read()))
        db_file = 'schedule.db'
        print("✅ 압축 해제 성공")
    except Exception as e:
        print(f"❌ 압축 해제 실패: {e}")

if not db_file:
    print("❌ DB 파일 없음")
    exit()

# 통계 수집
stats = {
    'time': time_str,
    'current_revenue': 0,
    'previous_revenue': 0,
    'total_records': 0,
    'zero_count': 0,
    'latest_date': 'N/A'
}

# 이전 기록 읽기
if os.path.exists('last_stats.json'):
    try:
        with open('last_stats.json', 'r') as f:
            last = json.load(f)
            stats['previous_revenue'] = last.get('current_revenue', 0)
            print(f"이전 매출: {stats['previous_revenue']:,}원")
    except:
        print("이전 기록 없음")

# DB 읽기
try:
    print("\nDB 읽기 시작...")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # 최신 날짜 찾기
    cursor.execute("SELECT MAX(date) FROM schedule")
    latest_date = cursor.fetchone()[0]
    
    if latest_date:
        stats['latest_date'] = latest_date
        print(f"최신 날짜: {latest_date}")
        
        # 해당 날짜 레코드 수
        cursor.execute("SELECT COUNT(*) FROM schedule WHERE date = ?", (latest_date,))
        stats['total_records'] = cursor.fetchone()[0] or 0
        print(f"레코드 수: {stats['total_records']}개")
        
        # 매출 합계 (revenue 컬럼 사용)
        cursor.execute("""
            SELECT SUM(revenue)
            FROM schedule 
            WHERE date = ?
            AND revenue IS NOT NULL 
            AND revenue > 0
        """, (latest_date,))
        
        result = cursor.fetchone()
        if result and result[0]:
            stats['current_revenue'] = result[0]
            print(f"매출 합계: {stats['current_revenue']:,}원")
        
        # 0원 매출 카운트
        cursor.execute("""
            SELECT COUNT(*) 
            FROM schedule 
            WHERE date = ? 
            AND (revenue = 0 OR revenue IS NULL)
        """, (latest_date,))
        stats['zero_count'] = cursor.fetchone()[0] or 0
        print(f"0원 매출: {stats['zero_count']}개")
    
    conn.close()
    print("✅ DB 읽기 성공")
    
except Exception as e:
    print(f"❌ DB 읽기 실패: {e}")

# 현재 상태 저장
with open('last_stats.json', 'w') as f:
    json.dump(stats, f)
    print("상태 저장 완료")

# 포맷팅
def format_money(num):
    if num >= 100000000:
        return f"{num/100000000:.1f}억원"
    elif num >= 10000000:
        return f"{num/10000000:.0f}천만원"
    elif num >= 10000:
        return f"{num/10000:.0f}만원"
    elif num > 0:
        return f"{num:,}원"
    else:
        return "0원"

# 상태 결정
if stats['current_revenue'] > stats['previous_revenue']:
    status = "정상"
    badge = "![크롤링](https://img.shields.io/badge/크롤링-정상-green)"
    icon = "✅"
    change = f"📈 +{format_money(stats['current_revenue'] - stats['previous_revenue'])}"
elif stats['current_revenue'] == stats['previous_revenue'] and stats['previous_revenue'] > 0:
    status = "점검필요"
    badge = "![크롤링](https://img.shields.io/badge/크롤링-점검필요-yellow)"
    icon = "⚠️"
    change = "➡️ 변화없음"
else:
    status = "확인필요"
    badge = "![크롤링](https://img.shields.io/badge/크롤링-확인필요-orange)"
    icon = "🔍"
    if stats['previous_revenue'] == 0:
        change = "첫 실행"
    else:
        change = f"📉 -{format_money(stats['previous_revenue'] - stats['current_revenue'])}"

# README 생성
readme = f"""# 📊 Media Commerce Analytics Platform

{badge}

## {icon} 실시간 현황 ({stats['latest_date']})

### 📍 최종 업데이트
- **시간**: {stats['time']}
- **상태**: {status}

### 💰 매출 현황
- **현재 총 매출**: **{format_money(stats['current_revenue'])}**
- **이전 총 매출**: {format_money(stats['previous_revenue'])}
- **매출 변화**: {change}
- **데이터 건수**: {stats['total_records']}개
- **0원 매출**: {stats['zero_count']}개

### 🔍 모니터링 포인트
"""

if status == "점검필요":
    readme += """
⚠️ **매출 변화 없음**
- 크롤링 동작 확인 필요
"""
elif stats['current_revenue'] == 0:
    readme += """
❌ **매출 데이터 없음**
- 데이터 수집 확인 필요
"""
else:
    readme += f"""
✅ **정상 수집 중**
- 정상 매출: {stats['total_records'] - stats['zero_count']}개
- 0원 매출: {stats['zero_count']}개
"""

readme += f"""

## 📈 실행 기록

| 구분 | 매출 | 데이터수 |
|------|------|----------|
| 현재 | {format_money(stats['current_revenue'])} | {stats['total_records']}개 |
| 이전 | {format_money(stats['previous_revenue'])} | - |
| 변화 | {change} | - |

---

## 🔗 바로가기

- [⚙️ Actions](../../actions)
- [📝 실행 로그](../../actions/workflows/daily_scraping.yml)

---

*자동 업데이트: 매 시간*
"""

# 파일 쓰기
with open('README.md', 'w', encoding='utf-8') as f:
    f.write(readme)

print("\n✅ README.md 생성 완료!")
print(f"- 현재: {format_money(stats['current_revenue'])}")
print(f"- 이전: {format_money(stats['previous_revenue'])}")
print(f"- 변화: {change}")

# 정리
if os.path.exists('schedule.db') and os.path.exists('schedule.db.zst'):
    os.remove('schedule.db')
    print("임시 DB 삭제 완료")

print("="*50)
