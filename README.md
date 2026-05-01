# schedule-data

홈쇼핑 편성·매출 스냅샷을 매시간 갱신하는 데이터 저장소.

## 데이터

- **`schedule.db.zst`** — SQLite DB 의 zstandard 압축본 (약 24MB).
  - GitHub Actions 가 매시간 갱신.
  - 소비자는 `https://raw.githubusercontent.com/iraeee/schedule-data/main/schedule.db.zst` 로 다운로드.

## 워크플로

| 워크플로 | 일정 |
|---|---|
| `daily_scraping.yml` | 매시 + 수동 트리거 (편성·날씨 갱신) |
| `weekly_maintenance.yml` | 매주 일요일 03:00 KST (VACUUM + 월별 백업) |

## 시크릿

| 이름 | 용도 |
|---|---|
| `GITHUB_TOKEN` | repo push (GitHub 자동 제공) |
| `SOURCE_COOKIE` | 소스 사이트 인증 쿠키 (cookie_updater.py 가 헤더 갱신) |
