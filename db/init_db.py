from sqlalchemy.orm import Session

from .models import Base
from .repo import ensure_product, bulk_insert_redeem_codes


def init_db(engine):
    Base.metadata.create_all(engine)


def seed_data(session: Session, anxiety_codes: list[str] | None = None):
    # Основная коробочка
    anxiety = ensure_product(
        session,
        code="anxiety",
        title="Коробочка «Отпусти тревогу»",
        description="Комплект для снижения тревожности с практиками и физическими предметами.",
        price_kop=299000  # 2990.00 руб
    )

    if anxiety_codes:
        bulk_insert_redeem_codes(session, anxiety.id, anxiety_codes)