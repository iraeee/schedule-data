# labangba-data

라방바 (홈쇼핑 편성표) 크롤링 데이터 저장 repo.

기존 `iraeee/media-commerce-insights` repo 의 누적 히스토리 (~2.3GB) 가 GitHub push 거부 임계에 도달해 신규 repo 로 분리. 동일한 GitHub Actions 워크플로 (lab2 출처) 가 매시간 라방바 + 날씨 데이터를 수집해 `schedule.db.zst` 로 압축 push.

## 데이터

- **`schedule.db.zst`** — `schedule.db` (~105MB) 의 zstandard 압축본 (~30MB).
  - 매 시각 GitHub Actions 가 갱신.
  - 다른 도구가 `https://raw.githubusercontent.com/iraeee/labangba-data/main/schedule.db.zst` 로 다운로드.

## 워크플로

| 워크플로 | 일정 | 역할 |
|---|---|---|
| `daily_scraping.yml` | 매시 (KST 09·12·16·21·22·22:55·23:20·23:45·23:56) + workflow_dispatch | 라방바 + 날씨 크롤링 → DB 갱신 → 압축 push |
| `weekly_maintenance.yml` | 매주 일요일 03:00 KST | DB VACUUM + 월별 백업 + 통계 |

## 시크릿

GitHub Actions 가 사용하는 secret:

| 이름 | 용도 | 출처 |
|---|---|---|
| `GITHUB_TOKEN` | repo push (자체 제공, 수동 설정 불요) | GitHub 자동 |
| `LABANGBA_COOKIE` | 라방바 사이트 인증 쿠키 (cookie_updater.py 가 scrape_schedule.py 의 Cookie 헤더 갱신) | 사용자 추출 — 기존 `iraeee/media-commerce-insights` Settings → Secrets → Actions 에서 동일 값 복사 |

## 소스

`C:\Users\tjwng\Desktop\라방바분석\lab2_extracted\github_actions\` 의 스크립트를 그대로 복사. lab2 코드는 read-only 보존, 본 repo 는 동일 동작 유지.

## 데이터 소비자

- **lab2 로컬 도구** (Streamlit, http://localhost:8501) — `run_and_backup_and_dashboard.py` 의 다운로드 URL 6 곳을 본 repo URL 로 갱신 후 동작.
- **더수오 허브 broadcast 모듈** (Flask, http://localhost:5000/broadcast/) — `modules/broadcast/sync.py` 가 `database/schedule.db` 로 받아 사용.
