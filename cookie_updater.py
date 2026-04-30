"""
쿠키 업데이트 헬퍼
기존 scrape_schedule.py의 쿠키를 환경변수에서 업데이트
"""

import os
import re
import sys

def update_cookie_in_file():
    """scrape_schedule.py의 쿠키를 환경변수에서 업데이트"""
    
    # 환경변수에서 쿠키 가져오기
    new_cookie = os.environ.get('LABANGBA_COOKIE')
    
    if not new_cookie:
        print("환경변수 LABANGBA_COOKIE가 없습니다. 기존 쿠키 사용.")
        return False
    
    try:
        # scrape_schedule.py 읽기
        with open('scrape_schedule.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Cookie 라인 찾기 (59번째 라인 근처)
        pattern = r'"Cookie":\s*"[^"]*"'
        replacement = f'"Cookie": "{new_cookie}"'
        
        # 쿠키 교체
        new_content = re.sub(pattern, replacement, content)
        
        if new_content != content:
            # 파일 업데이트
            with open('scrape_schedule.py', 'w', encoding='utf-8') as f:
                f.write(new_content)
            print("✅ 쿠키 업데이트 완료")
            return True
        else:
            print("⚠️ 쿠키 패턴을 찾을 수 없습니다")
            return False
            
    except Exception as e:
        print(f"❌ 쿠키 업데이트 실패: {e}")
        return False

if __name__ == "__main__":
    # GitHub Actions에서만 실행
    if os.environ.get('GITHUB_ACTIONS'):
        update_cookie_in_file()
    else:
        print("로컬 환경에서는 실행하지 않습니다.")
