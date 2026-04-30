import sqlite3

class ScheduleDB:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()

    def create_table(self):
        self.cur.execute("""
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
        # 중복 체크용 복합 인덱스 (크롤링 성능 핵심)
        self.cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_schedule_lookup 
            ON schedule(date, time, platform, broadcast)
        """)
        self.conn.commit()

    def insert_records(self, records):
        for r in records:
            self.cur.execute("""
                SELECT id, revenue, units_sold, roi
                FROM schedule
                WHERE date = ? AND time = ? AND broadcast = ? AND platform = ?
            """, (r["date"], r["time"], r["broadcast"], r["platform"]))
            existing = self.cur.fetchone()

            if existing:
                same = (
                    int(existing["revenue"]) == int(r["revenue"]) and
                    int(existing["units_sold"]) == int(r["units_sold"]) and
                    float(existing["roi"]) == float(r["roi"])
                )
                if same:
                    continue  # 변경 없으면 패스

                self.cur.execute("""
                    UPDATE schedule
                    SET revenue = ?, units_sold = ?, roi = ?, cost = ?
                    WHERE id = ?
                """, (r["revenue"], r["units_sold"], r["roi"], r["cost"], existing["id"]))
            else:
                self.cur.execute("""
                    INSERT INTO schedule
                    (date, time, broadcast, platform, category,
                     units_sold, revenue, product_count, cost, roi, is_major)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    r["date"], r["time"], r["broadcast"], r["platform"], r["category"],
                    r["units_sold"], r["revenue"], r["product_count"], r["cost"],
                    r["roi"], r["is_major"]
                ))

        self.conn.commit()

    def close(self):
        self.conn.close()