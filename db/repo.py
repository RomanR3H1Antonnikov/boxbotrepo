from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from typing import List

from .models import User, Product, Order, Access, RedeemCode, RedeemUse


def make_engine(db_path: str = "app.sqlite3"):
    return create_engine(f"sqlite:///{db_path}", echo=False, future=True)


def get_or_create_user(session: Session, telegram_id: int, username: str | None = None) -> User:
    user = session.get(User, telegram_id)
    if user is None:
        user = User(
            telegram_id=telegram_id,
            username=username,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        session.add(user)
        session.flush()  # чтобы получить ID если понадобится
        # создаём access по умолчанию
        session.add(Access(user_id=telegram_id))
    else:
        if username and user.username != username:
            user.username = username
        user.updated_at = datetime.now(timezone.utc)
    session.refresh(user)
    return user


def ensure_product(session: Session, code: str, title: str, description: str = "", price_kop: int = 0) -> Product:
    stmt = select(Product).where(Product.code == code)
    product = session.scalar(stmt)
    if product is None:
        product = Product(
            code=code,
            title=title,
            description=description,
            price_kop=price_kop,
            is_active=True
        )
        session.add(product)
        session.flush()
    return product


def bulk_insert_redeem_codes(session: Session, product_id: int, codes: list[str]):
    inserted = 0
    for code in codes:
        code = code.strip()
        if not code:
            continue
        exists = session.scalar(
            select(RedeemCode.id).where(RedeemCode.product_id == product_id, RedeemCode.code == code)
        )
        if not exists:
            session.add(RedeemCode(product_id=product_id, code=code))
            inserted += 1
    return inserted


def update_user_state(session: Session, user: User):
    session.merge(user)
    session.commit()


def get_user_by_id(session: Session, telegram_id: int) -> User | None:
    user = session.get(User, telegram_id)
    if user:
        session.refresh(user)
    return user


def create_order_db(session: Session, user_id: int, **kwargs) -> Order:
    order = Order(user_id=user_id, **kwargs)
    session.add(order)
    session.commit()
    return order


def get_user_orders_db(session: Session, user_id: int) -> List[Order]:
    return list(
        session.scalars(
            select(Order)
            .where(Order.user_id == user_id)
            .order_by(Order.id.desc())
        ).all()
    )


def mark_code_used(session: Session, code: str, user_id: int):
    exists = session.query(RedeemUse).filter_by(redeem_code_id=code).first()
    if not exists:
        session.add(RedeemUse(redeem_code_id=code, user_id=user_id))
        session.commit()