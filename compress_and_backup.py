#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
compress_and_backup.py - 총매출 감소 감지 버전
매출이 감소하거나 0원이면 백업 생략하고 경고
"""

import os
import sqlite3
from datetime import datetime

try:
    import zstandard as zstd
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "zstandard"])
    import zstandard as zstd

def get_total_revenue_today(db_path='schedule.db'):
    """오늘 총매출 확인"""
    if not os.path.exists(db_path):
        return 0
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 오늘 날짜 (YYYY-MM-DD 형식)
        today = datetime.now().strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_count,
                SUM(revenue) as total_revenue,
                SUM(CASE WHEN revenue > 0 THEN 1 ELSE 0 END) as with_revenue
            FROM schedule
            WHERE date = ?
        """, (today,))
        
        result = cursor.fetchone()
        total_count = result[0] or 0
        total_revenue = result[1] or 0
        with_revenue = result[2] or 0
        
        print(f"📊 오늘 데이터: {total_count}개 방송, {with_revenue}개 매출 있음")
        print(f"💰 오늘 총매출: {total_revenue:,}원")
        
        return total_revenue
        
    except Exception as e:
        print(f"❌ 매출 확인 실패: {e}")
        return 0
    finally:
        conn.close()

def get_last_backup_revenue():
    """마지막 백업의 총매출 확인"""
    backup_file = 'backups/backup_latest.db.zst'
    
    if not os.path.exists(backup_file):
        print("📁 이전 백업 없음 (첫 실행)")
        return 0
    
    try:
        # 백업 압축 해제
        with open(backup_file, 'rb') as f:
            dctx = zstd.ZstdDecompressor()
            decompressed = dctx.decompress(f.read())
        
        # 임시 DB 파일로 저장
        temp_db = 'temp_backup_check.db'
        with open(temp_db, 'wb') as f:
            f.write(decompressed)
        
        # 매출 확인
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT SUM(revenue) as total_revenue
            FROM schedule
            WHERE date = ?
        """, (today,))
        
        result = cursor.fetchone()
        last_revenue = result[0] if result[0] else 0
        
        conn.close()
        os.remove(temp_db)
        
        print(f"📁 이전 백업 매출: {last_revenue:,}원")
        return last_revenue
        
    except Exception as e:
        print(f"⚠️ 백업 확인 실패: {e}")
        return 0

def compress_file(input_file, output_file, level=3):
    """파일을 zstandard로 압축"""
    print(f"📦 압축 중: {input_file} → {output_file}")
    
    with open(input_file, 'rb') as f_in:
        data = f_in.read()
    
    cctx = zstd.ZstdCompressor(level=level)
    compressed = cctx.compress(data)
    
    with open(output_file, 'wb') as f_out:
        f_out.write(compressed)
    
    original_size = os.path.getsize(input_file) / (1024 * 1024)
    compressed_size = os.path.getsize(output_file) / (1024 * 1024)
    ratio = (1 - compressed_size / original_size) * 100
    
    print(f"✅ 압축 완료: {original_size:.1f}MB → {compressed_size:.1f}MB ({ratio:.1f}% 감소)")
    return compressed_size

def create_warning_file(current_revenue, last_revenue):
    """경고 파일 생성"""
    warning_file = 'DATA_WARNING.txt'
    
    with open(warning_file, 'w', encoding='utf-8') as f:
        f.write(f"⚠️ 매출 데이터 이상 감지\n")
        f.write(f"="*50 + "\n")
        f.write(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"이전 매출: {last_revenue:,}원\n")
        f.write(f"현재 매출: {current_revenue:,}원\n")
        f.write(f"감소액: {last_revenue - current_revenue:,}원\n")
        f.write(f"="*50 + "\n")
        f.write(f"조치사항:\n")
        f.write(f"1. 쿠키 업데이트 확인\n")
        f.write(f"2. API 상태 확인\n")
        f.write(f"3. 백업 파일에서 복구 필요\n")
    
    print(f"⚠️ 경고 파일 생성: {warning_file}")

def main():
    print("="*50)
    print("🚀 DB 압축 및 백업 (매출 보호 버전)")
    print("="*50)
    
    # schedule.db 확인
    if not os.path.exists('schedule.db'):
        print("⚠️ schedule.db 없음")
        if os.path.exists('schedule.db.zst'):
            print("✅ 압축본만 존재 (정상)")
        return
    
    print("\n📊 매출 데이터 확인 중...")
    
    # 1. 현재 총매출 확인
    current_revenue = get_total_revenue_today()
    
    # 2. 이전 백업 매출 확인
    last_revenue = get_last_backup_revenue()
    
    # 3. 매출 비교 및 판단
    print("\n🔍 매출 변화 분석...")
    
    should_backup = True
    
    if last_revenue > 0:  # 이전 백업이 있는 경우
        if current_revenue == 0:
            print(f"❌ 매출이 0원으로 변경됨! ({last_revenue:,}원 → 0원)")
            should_backup = False
            create_warning_file(current_revenue, last_revenue)
            
        elif current_revenue < last_revenue:
            decrease_ratio = (last_revenue - current_revenue) / last_revenue * 100
            print(f"⚠️ 매출 감소: {last_revenue:,}원 → {current_revenue:,}원 ({decrease_ratio:.1f}% 감소)")
            
            # 50% 이상 감소하면 백업 안 함
            if decrease_ratio > 50:
                print(f"❌ 매출이 50% 이상 감소! 백업 생략")
                should_backup = False
                create_warning_file(current_revenue, last_revenue)
            else:
                print(f"✅ 허용 범위 내 감소 (백업 진행)")
                
        else:
            print(f"✅ 매출 증가 또는 유지: {last_revenue:,}원 → {current_revenue:,}원")
    
    elif current_revenue == 0:
        # 첫 백업인데 매출이 0원
        print("⚠️ 첫 백업인데 매출이 0원입니다")
        should_backup = False
        create_warning_file(current_revenue, 0)
    
    # 4. 백업 처리
    if should_backup:
        print("\n💾 백업 생성 중...")
        if not os.path.exists('backups'):
            os.makedirs('backups')
        
        # 백업 생성 (압축)
        backup_path = 'backups/backup_latest.db.zst'
        compress_file('schedule.db', backup_path)
        print(f"✅ 백업 완료: {backup_path}")
    else:
        print("\n⚠️ 매출 이상으로 백업 생략")
        print("💡 이전 백업 파일을 유지합니다")
    
    # 5. 메인 DB 압축 (항상 수행)
    print("\n📦 메인 DB 압축...")
    size = compress_file('schedule.db', 'schedule.db.zst')
    
    # GitHub 용량 체크
    if size > 95:
        print(f"⚠️ 압축 파일이 {size:.1f}MB입니다. GitHub 제한(100MB)에 근접!")
    
    # 6. 원본 삭제
    print("🗑️ 원본 DB 삭제...")
    os.remove('schedule.db')
    print("✅ 원본 삭제 완료")
    
    # 7. 최종 상태
    print("\n📊 최종 상태:")
    if os.path.exists('schedule.db.zst'):
        size = os.path.getsize('schedule.db.zst') / (1024 * 1024)
        print(f"✅ schedule.db.zst: {size:.1f}MB")
    
    if os.path.exists('backups/backup_latest.db.zst'):
        size = os.path.getsize('backups/backup_latest.db.zst') / (1024 * 1024)
        print(f"✅ 백업: {size:.1f}MB")
    
    if os.path.exists('DATA_WARNING.txt'):
        print(f"⚠️ 경고 파일이 생성되었습니다!")
    
    print("\n✅ 완료!")

if __name__ == "__main__":
    main()
