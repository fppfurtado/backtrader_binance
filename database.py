import sqlite3
from backtrader import num2date

conn = sqlite3.connect('chicken_bot.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS tb_trade (
        id_trade INTEGER PRIMARY KEY AUTOINCREMENT,
        dt_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        dt_execution TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ds_side TEXT,
        ticker TEXT,
        vl_exec_price REAL,
        vl_quantity REAL,
        vl_comission REAL,
        ds_custody TEXT
    )
''')
conn.commit()

def save_trade(dt_execution, side, ticker, exec_price, quantity, comission, custody):
    cursor.execute('''
        INSERT INTO tb_trade (
            dt_execution,
            ds_side,
            ticker,
            vl_exec_price,
            vl_quantity,
            vl_comission,
            ds_custody
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
            dt_execution,
            side,
            ticker,
            exec_price,
            quantity,
            comission,
            custody
        )
    )
    conn.commit()