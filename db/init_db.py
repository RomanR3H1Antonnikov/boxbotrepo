from sqlalchemy.orm import Session
import logging

from .models import Base
from .repo import ensure_product, bulk_insert_redeem_codes

logger = logging.getLogger("box_bot")


def init_db(engine):
    logger.info("Начало пересоздания БД: drop_all + create_all")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    logger.info("Таблицы пересозданы успешно")

def seed_data(session: Session, anxiety_codes: list[str] | None = None):
    try:
        # Основная коробочка
        anxiety = ensure_product(
            session,
            code="anxiety",
            title="Коробочка «Отпусти тревогу»",
            description="Комплект для снижения тревожности с практиками и физическими предметами.",
            price_kop=599000  # 5990.00 руб
        )

        if anxiety_codes:
            bulk_insert_redeem_codes(session, anxiety.id, anxiety_codes)

        session.commit()
        logger.info("seed_data завершено успешно.")
    except Exception as e:
        logger.error(f"Ошибка в seed_data: {e}")
        session.rollback()