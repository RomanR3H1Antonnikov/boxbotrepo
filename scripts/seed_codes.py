import os
from sqlalchemy.orm import Session
from db.repo import make_engine
from db.init_db import init_db, seed_data
from db.repo import bulk_insert_redeem_codes

# Путь к БД (тот же, что в боте)
DB_PATH = os.getenv("DB_PATH", "app.sqlite3")

# Твои коды (все 40)
ANXIETY_CODES = [
    "1002", "1347", "2589", "3761", "4923", "5178", "6354", "7490", "8632", "9714",
    "1286", "2439", "3591", "4725", "5863", "6917", "7048", "8251", "9376", "1432",
    "2567", "3789", "4910", "5123", "6345", "7578", "8790", "9012", "1234", "3456",
    "5678", "7890", "1023", "2345", "4567", "6789", "8901", "3210", "5432", "7654"
]

if __name__ == "__main__":
    engine = make_engine(DB_PATH)
    init_db(engine)  # на всякий случай создаст таблицы

    with Session(engine) as sess:
        # Создаём/получаем продукт "anxiety"
        from db.repo import ensure_product
        anxiety = ensure_product(
            sess,
            code="anxiety",
            title="Коробочка «Отпусти тревогу»",
            description="Комплект для снижения тревожности",
            price_kop=299000
        )

        # Заливаем коды
        inserted = bulk_insert_redeem_codes(sess, anxiety.id, ANXIETY_CODES)
        print(f"Заливка завершена: добавлено {inserted} новых кодов (дубликаты пропущены)")

        sess.commit()

    print("Готово! Коды в таблице redeem_codes.")