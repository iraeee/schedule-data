"""
데이터 검증 및 쿠키 상태 체크
기존 schedule.db 구조와 호환
GitHub Actions 환경변수 업데이트
"""

import sqlite3
import json
import os
from datetime import datetime

def check_data_quality():
    """오늘 데이터 품질 체크"""
    
    # DB 파일 확인
    if not os.path.exists('schedule.db'):
        print("⚠️ schedule.db 파일이 없습니다. 빈 DB 생성...")
        conn = sqlite3.connect('schedule.db')
        cursor = conn.cursor()
        
        # 테이블 생성
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            time TEXT,
            broadcast TEXT,
            platform TEXT,
            category TEXT,
            units_sold INTEGER,
            revenue INTEGER,
            product_count INTEGER,
            cost INTEGER,
            roi REAL,
            is_major INTEGER
        );
        """)
        conn.commit()
    else:
        conn = sqlite3.connect('schedule.db')
        cursor = conn.cursor()
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 오늘 데이터 확인
    try:
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN revenue = 0 OR revenue IS NULL THEN 1 END) as zero_count,
                AVG(revenue) as avg_revenue,
                MAX(revenue) as max_revenue,
                MIN(revenue) as min_revenue
            FROM schedule 
            WHERE date = ?
        """, (today,))
        
        result = cursor.fetchone()
        total, zero_count, avg_revenue, max_revenue, min_revenue = result
    except Exception as e:
        print(f"❌ DB 조회 실패: {e}")
        total = 0
        zero_count = 0
        avg_revenue = 0
        max_revenue = 0
        min_revenue = 0
    
    conn.close()
    
    # 상태 판단
    if total == 0:
        status = "NO_DATA"
        message = "❌ 데이터 없음"
        action = "SCRAPE_NOW"
    elif zero_count == total and total > 50:
        status = "CRITICAL"
        message = f"🚨 모든 데이터가 0원 ({total}개) - 쿠키 만료"
        action = "UPDATE_COOKIE"
    elif zero_count > total * 0.7:
        status = "WARNING"
        message = f"⚠️ 0원 매출 {zero_count}/{total}개 ({zero_count/total*100:.1f}%)"
        action = "CHECK_COOKIE"
    elif zero_count > total * 0.3:
        status = "CAUTION"
        message = f"⚠️ 0원 매출 다소 많음: {zero_count}개 ({zero_count/total*100:.1f}%)"
        action = "MONITOR"
    else:
        status = "OK"
        message = f"✅ 정상: {total}개 레코드, 0원 {zero_count}개"
        action = "NONE"
    
    # 결과 저장
    result = {
        'date': today,
        'time': datetime.now().strftime('%H:%M:%S'),
        'total': total,
        'zero_count': zero_count,
        'zero_ratio': (zero_count/total*100) if total > 0 else 0,
        'avg_revenue': avg_revenue or 0,
        'max_revenue': max_revenue or 0,
        'status': status,
        'message': message,
        'action': action
    }
    
    # JSON으로 저장
    with open('data_check.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    # 콘솔 출력
    print(message)
    print(f"총 레코드: {total}개")
    print(f"0원 매출: {zero_count}개")
    
    # GitHub Actions 환경변수 (새로운 방식)
    if os.environ.get('GITHUB_ACTIONS'):
        github_output = os.environ.get('GITHUB_OUTPUT')
        if github_output:
            with open(github_output, 'a') as f:
                f.write(f"status={status}\n")
                f.write(f"zero_ratio={zero_count/total*100 if total > 0 else 0:.1f}\n")
                f.write(f"total_records={total}\n")
    
    # 종료 코드 (CRITICAL이면 1)
    return 0 if status != "CRITICAL" else 1

if __name__ == "__main__":
    import sys
    sys.exit(check_data_quality())
