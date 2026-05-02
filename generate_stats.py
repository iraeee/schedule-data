import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import os

if os.path.exists('schedule.db'):
    conn = sqlite3.connect('schedule.db')
    
    query = '''
    SELECT 
        substr(date, 1, 7) as month,
        COUNT(*) as records,
        COUNT(DISTINCT date) as days,
        COUNT(DISTINCT platform) as platforms,
        AVG(revenue) as avg_revenue,
        SUM(revenue) as total_revenue
    FROM schedule
    WHERE date >= date('now', '-3 months')
    GROUP BY month
    ORDER BY month DESC
    '''
    
    df = pd.read_sql_query(query, conn)
    
    print("📊 월별 통계")
    print(df.to_string(index=False))
    
    df.to_csv('monthly_stats.csv', index=False)
    
    conn.close()
else:
    print("DB 파일이 없습니다")
