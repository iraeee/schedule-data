"""
소스 API 상태 체크 모듈
API 변경, 쿠키 만료, 데이터 수집 이상 감지
"""

import requests
import json
import sqlite3
from datetime import datetime, timedelta
import re

class HealthChecker:
    """API 상태 체크 클래스"""
    
    def __init__(self):
        self.api_url = "https://live.ecomm-data.com/schedule/list_hs"
        self.headers = {
            "accept": "*/*",
            "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/json",
            "origin": "https://live.ecomm-data.com",
            "referer": "https://live.ecomm-data.com/schedule/hs",
            "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            "Cookie": "_ga=GA1.1.1148900813.1753071738; _gcl_au=1.1.2127562858.1753071789.734155396.1753071810.1753071813; _fwb=8206MdvNQcDXiuEel5llWx.1753071736391; sales2=eyJoaXN0b3J5IjpbNTAwMDAwMDNdLCJsYWJhbmdfb2JqIjp7fSwicGFzdF9rZXl3b3JkMiI6Iu2DgO2IrOq3uOumrOuqqCIsInVzZXIiOnsidXNlcl9pZCI6IjlqOTE3YldXdHktQ29FSU9Qa2wzTiIsIm5pY2tuYW1lIjoiaXJhZSIsInNlc3NfaWQiOiI4MTBTbkFuMXl0SEktLV9pRGEtRDYiLCJ1c2VyX3R5cGUiOjAsInZvdWNoZXIiOjAsInByZWZlciI6MX19; sales2.sig=lz9-0bjYr4MEirSNAA8JCeqAwYo; _ga_VN7F3DELDK=GS2.1.s1759100069$o2$g1$t1759100902$j53$l0$h0; _ga_NLGYGNTN3F=GS2.1.s1759100069$o2$g1$t1759100902$j53$l0$h0"
        }
        self.issues = []
        self.warnings = []
        
        # 매출 0원 임계값 설정 (50%로 상향)
        self.ZERO_REVENUE_THRESHOLD = 50  # 심각한 문제로 판단하는 기준
        self.ZERO_REVENUE_WARNING = 40    # 경고 수준
    
    def check_api_health(self):
        """API 상태 종합 체크 - 메인 메서드"""
        is_healthy = True
        
        # 1. API 응답 체크
        if not self.check_api_response():
            is_healthy = False
            
        # 2. 데이터 품질 체크 (DB가 있는 경우만) - 수정: 인자 제거
        import os
        if os.path.exists('schedule.db'):
            try:
                # check_data_quality는 내부에서 DB를 직접 읽음
                self.check_data_quality()
            except Exception as e:
                self.warnings.append(f"데이터 품질 체크 실패: {e}")
        
        # 3. 결과 출력
        if self.issues:
            print("\n❌ 발견된 문제:")
            for issue in self.issues:
                print(f"  - {issue}")
        
        if self.warnings:
            print("\n⚠️ 경고 사항:")
            for warning in self.warnings:
                print(f"  - {warning}")
        
        if is_healthy and not self.issues:
            print("\n✅ API 상태 정상")
        
        return is_healthy
    
    def check_api_response(self):
        """API 응답 체크 (세션 초기화 포함)"""
        print("🔍 API 응답 테스트...")
        
        try:
            # 세션 생성
            session = requests.Session()
            
            # 메인 페이지 먼저 방문 (세션 초기화) - 중요!
            try:
                main_resp = session.get(
                    'https://live.ecomm-data.com/schedule/hs',
                    headers={
                        'User-Agent': self.headers['user-agent'], 
                        'Cookie': self.headers['Cookie']
                    },
                    timeout=5
                )
            except:
                pass  # 세션 초기화 실패해도 계속 진행
            
            # 오늘 날짜로 테스트
            date_str = datetime.now().strftime("%y%m%d")
            post_data = {"date": date_str}
            
            response = session.post(
                self.api_url,
                headers=self.headers,
                json=post_data,
                timeout=10
            )
            
            if response.status_code != 200:
                self.issues.append(f"API 응답 코드 이상: {response.status_code}")
                return False
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                self.issues.append("API 응답이 JSON 형식이 아님")
                return False
            
            # 데이터 구조 확인
            if isinstance(data, dict):
                if "list" in data:
                    actual_data = data["list"]
                    print(f"  ✅ API 응답 정상 ({len(actual_data)}개 데이터)")
                    return actual_data
                else:
                    self.issues.append("예상치 못한 API 응답 구조 - list 필드 없음")
                    return False
            elif isinstance(data, list):
                if len(data) == 0:
                    self.issues.append("API가 빈 리스트 반환")
                    return False
                print(f"  ✅ API 응답 정상 ({len(data)}개 데이터)")
                return data
            else:
                self.issues.append("예상치 못한 API 응답 타입")
                return False
            
        except requests.exceptions.Timeout:
            self.issues.append("API 응답 시간 초과 (10초)")
            return False
        except requests.exceptions.RequestException as e:
            self.issues.append(f"API 요청 실패: {str(e)}")
            return False
    
    def check_data_quality(self, data, debug=False):
        """데이터 품질 체크 - 현재 시간 이전 방송만 체크"""
        print(f"🔍 데이터 품질 검사... [임계값: {self.ZERO_REVENUE_THRESHOLD}%]")
        
        if not data:
            return False
        
        current_time = datetime.now()
        current_hour = current_time.hour
        current_minute = current_time.minute
        
        # 현재 시간을 분 단위로 변환
        current_minutes = current_hour * 60 + current_minute
        
        # 샘플 데이터 체크
        past_broadcasts = []
        future_broadcasts = []
        zero_revenue_past = 0
        
        for idx, item in enumerate(data):
            # 시작 시간 파싱
            start_time_str = item.get('hsshow_datetime_start', '')
            if not start_time_str:
                continue
                
            try:
                # YYYYMMDDHHMM 형식 파싱
                start_dt = datetime.strptime(start_time_str, "%Y%m%d%H%M")
                broadcast_hour = start_dt.hour
                broadcast_minute = start_dt.minute
                broadcast_minutes = broadcast_hour * 60 + broadcast_minute
                
                # 현재 시간 이전 방송인지 확인
                if broadcast_minutes < current_minutes:
                    past_broadcasts.append(item)
                    
                    # 매출 확인
                    revenue = 0
                    revenue_fields = ['sales_amt', 'salesAmt', 'sales_amount', 'salesAmount', 'sale_amt', 'revenue']
                    
                    for field in revenue_fields:
                        if field in item:
                            val = item.get(field)
                            if val is not None and val != '' and val != 0:
                                try:
                                    revenue = int(val)
                                    break
                                except:
                                    pass
                    
                    if revenue == 0:
                        zero_revenue_past += 1
                else:
                    future_broadcasts.append(item)
                    
            except ValueError:
                continue
        
        print(f"  ℹ️ 현재 시간: {current_hour:02d}:{current_minute:02d}")
        print(f"  - 과거 방송: {len(past_broadcasts)}개")
        print(f"  - 미래 방송: {len(future_broadcasts)}개")
        
        # 과거 방송 중 매출 0원 비율 체크
        if past_broadcasts:
            zero_ratio = (zero_revenue_past / len(past_broadcasts)) * 100
            print(f"  - 과거 방송 중 매출 0원: {zero_revenue_past}개 ({zero_ratio:.1f}%)")
            
            if zero_ratio > self.ZERO_REVENUE_THRESHOLD:
                self.issues.append(f"과거 방송 매출 0원 비율이 {zero_ratio:.1f}%로 너무 높음 (기준: {self.ZERO_REVENUE_THRESHOLD}%)")
            elif zero_ratio > self.ZERO_REVENUE_WARNING:
                self.warnings.append(f"과거 방송 매출 0원 비율이 {zero_ratio:.1f}%로 높음")
        
        return True
    
    def check_cookie_validity(self):
        """쿠키 유효성 검사"""
        print("🔍 쿠키 유효성 검사...")
        
        cookie = self.headers.get("Cookie", "")
        
        # 필수 쿠키 확인
        required = ['sales2', 'sales2.sig', '_ga']
        missing = []
        
        for req in required:
            if req not in cookie:
                missing.append(req)
        
        if missing:
            self.issues.append(f"필수 쿠키 누락: {', '.join(missing)}")
            return False
        
        print("  ✅ 쿠키 형식 정상")
        return True
    
    def check_past_data(self):
        """과거 데이터와 비교"""
        print("🔍 과거 데이터 비교...")
        
        try:
            conn = sqlite3.connect('schedule.db')
            cursor = conn.cursor()
            
            # 최근 7일 데이터 통계
            cursor.execute("""
                SELECT 
                    COUNT(*) as cnt,
                    AVG(revenue) as avg_revenue
                FROM schedule
                WHERE date >= date('now', '-7 days')
                    AND date < date('now')
            """)
            
            cnt, avg_revenue = cursor.fetchone()
            
            if cnt and avg_revenue:
                print(f"  ℹ️ 최근 7일: {cnt}건, 평균 매출: {avg_revenue:,.0f}원")
                
                if avg_revenue < 1000000:  # 평균 매출 100만원 미만
                    self.warnings.append("최근 7일 평균 매출이 낮음")
            
            # 오늘 데이터 확인
            cursor.execute("""
                SELECT COUNT(*) 
                FROM schedule 
                WHERE date = date('now')
            """)
            
            today_count = cursor.fetchone()[0]
            
            if today_count == 0:
                self.warnings.append("오늘 수집된 데이터 없음")
            
            conn.close()
            print("  ✅ 과거 데이터 비교 완료")
            return True
            
        except Exception as e:
            self.warnings.append(f"DB 접근 실패: {str(e)}")
            return False
    
    def check_all(self):
        """모든 체크 수행"""
        print("="*60)
        print("🏥 소스 API 상태 진단 시작")
        print("⚙️ 매출 0원 임계값: {}%".format(self.ZERO_REVENUE_THRESHOLD))
        print("="*60)
        
        # API 응답 체크
        api_data = self.check_api_response()
        
        # 쿠키 유효성 체크
        self.check_cookie_validity()
        
        # 데이터 품질 체크
        if api_data:
            self.check_data_quality(api_data)
        
        # 과거 데이터 비교
        self.check_past_data()
        
        # 결과 종합
        print("="*60)
        print("📊 진단 결과")
        print("="*60)
        
        if self.issues:
            print("❌ 심각한 문제:")
            for issue in self.issues:
                print(f"  - {issue}")
        
        if self.warnings:
            print("⚠️ 경고 사항:")
            for warning in self.warnings:
                print(f"  - {warning}")
        
        if not self.issues and not self.warnings:
            print("✅ 모든 항목 정상")
        
        print("="*60)
        
        # 상태 결정
        if self.issues:
            status = 'CRITICAL'
        elif self.warnings:
            status = 'WARNING'
        else:
            status = 'OK'
        
        # 권장 조치사항
        recommendations = []
        
        if '매출 0원 비율' in str(self.issues):
            recommendations.append("세션 초기화가 제대로 되지 않았을 수 있습니다")
            recommendations.append("브라우저에서 실제 매출이 보이는지 확인하세요")
        
        if '쿠키 누락' in str(self.issues):
            recommendations.append("브라우저에서 새 쿠키를 추출하세요")
            recommendations.append("extract_cookies.js 사용 또는 F12 → Network → Cookie 복사")
        
        return {
            'status': status,
            'issues': self.issues,
            'warnings': self.warnings,
            'recommendations': recommendations
        }

if __name__ == "__main__":
    checker = HealthChecker()
    result = checker.check_all()
    
    if result['status'] == 'CRITICAL':
        print("\n🚨 크롤링을 진행하기 전에 문제를 해결하세요!")
        exit(1)
    elif result['status'] == 'WARNING':
        print("\n⚠️ 경고가 있지만 크롤링은 가능합니다.")
        exit(0)
    else:
        print("\n✅ 크롤링 준비 완료!")
        exit(0)
