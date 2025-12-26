from __future__ import annotations
from sqlalchemy import JSON
from datetime import datetime, timezone
from sqlalchemy import text

from sqlalchemy import (
    BigInteger, Integer, String, Text, Boolean, DateTime,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class User(Base):
    __tablename__ = "users"

    telegram_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    full_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    email: Mapped[str | None] = mapped_column(String(128), nullable=True)
    gallery_viewed: Mapped[bool] = mapped_column(Boolean, default=False)
    team_viewed: Mapped[bool] = mapped_column(Boolean, default=False)
    practices_access: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    # Связи
    orders: Mapped[list["Order"]] = relationship(back_populates="user", lazy="selectin")
    access: Mapped["Access"] = relationship(back_populates="user", uselist=False, lazy="joined")

    is_authorized: Mapped[bool] = mapped_column(Boolean, default=False)  # вместо проверки full_name/phone/email
    practices: Mapped[list[str]] = mapped_column(JSON, default=list)  # для практик
    temp_pvz_list: Mapped[list[dict] | None] = mapped_column(JSON, nullable=True, default=None)
    temp_selected_pvz: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # временные данные
    extra_data: Mapped[dict] = mapped_column(JSON, default=dict, server_default=text("'{}'"), nullable=False)
    awaiting_gift_message: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("0"), nullable=False)
    pvz_for_order_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    awaiting_pvz_address: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("0"), nullable=False)
    awaiting_manual_pvz: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("0"), nullable=False)
    awaiting_manual_track: Mapped[bool] = mapped_column(Boolean, default=False)
    temp_order_id_for_track: Mapped[int | None] = mapped_column(Integer, nullable=True)


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)  # "anxiety"
    title: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[Text] = mapped_column(Text, nullable=False, default="")
    price_kop: Mapped[int] = mapped_column(Integer, nullable=False)  # в копейках

    has_channel: Mapped[bool] = mapped_column(Boolean, default=False)
    has_practices: Mapped[bool] = mapped_column(Boolean, default=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )

    # Связи
    orders: Mapped[list["Order"]] = relationship(back_populates="product")
    redeem_codes: Mapped[list["RedeemCode"]] = relationship(back_populates="product", cascade="all, delete-orphan")


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.telegram_id"), nullable=False)
    product_id: Mapped[int] = mapped_column(Integer, ForeignKey("products.id"), nullable=False)

    status: Mapped[str] = mapped_column(String(32), default="created", nullable=False)
    payment_stage: Mapped[str | None] = mapped_column(String(32), nullable=True)  # full / prepay / remainder

    total_price_kop: Mapped[int] = mapped_column(Integer, nullable=False)
    delivery_cost_kop: Mapped[int] = mapped_column(Integer, default=0)

    address: Mapped[str] = mapped_column(Text, nullable=False, default="")
    shipping_method: Mapped[str] = mapped_column(String(32), default="cdek_pvz")
    track: Mapped[str | None] = mapped_column(String(64), nullable=True)
    cdek_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    # Связи
    user: Mapped["User"] = relationship(back_populates="orders")
    product: Mapped["Product"] = relationship(back_populates="orders")
    payments: Mapped[list["Payment"]] = relationship(back_populates="order", cascade="all, delete-orphan")
    redeem_use: Mapped["RedeemUse | None"] = relationship(back_populates="order", uselist=False)

    extra_data: Mapped[dict] = mapped_column(JSON, default=dict)  # для pvz_code, delivery_period и т.д.
    payment_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)  # "full", "pre", "remainder"

    @property
    def remainder_amount(self) -> int:
        prepay = (self.total_price_kop * 30) // 100
        return max(self.total_price_kop - prepay, 0) // 100  # в рублях

    @property
    def prepay_amount(self) -> int:
        return (self.total_price_kop * 30 // 100) // 100  # в рублях

    @property
    def total_price(self) -> int:
        return self.total_price_kop // 100  # в рублях


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(Integer, ForeignKey("orders.id"), nullable=False)

    yookassa_payment_id: Mapped[str] = mapped_column(String(128), nullable=False)
    amount_kop: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    order: Mapped["Order"] = relationship(back_populates="payments")

    __table_args__ = (
        UniqueConstraint("yookassa_payment_id", name="uq_payment_yookassa_id"),
        Index("ix_payments_order_id", "order_id"),
    )


class Access(Base):
    __tablename__ = "access"

    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.telegram_id"), primary_key=True)

    channel_access: Mapped[bool] = mapped_column(Boolean, default=False)
    practices_access: Mapped[bool] = mapped_column(Boolean, default=False)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    user: Mapped["User"] = relationship(back_populates="access")


class RedeemCode(Base):
    __tablename__ = "redeem_codes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(Integer, ForeignKey("products.id"), nullable=False)
    code: Mapped[str] = mapped_column(String(16), nullable=False)

    is_used: Mapped[bool] = mapped_column(Boolean, default=False)
    used_by: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("users.telegram_id"), nullable=True)
    used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    product: Mapped["Product"] = relationship(back_populates="redeem_codes")

    __table_args__ = (
        UniqueConstraint("product_id", "code", name="uq_redeem_product_code"),
        Index("ix_redeem_code", "code"),
    )


class RedeemUse(Base):
    __tablename__ = "redeem_uses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    redeem_code_id: Mapped[int] = mapped_column(Integer, ForeignKey("redeem_codes.id"), unique=True, nullable=False)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.telegram_id"), nullable=False)
    order_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("orders.id"), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )

    order: Mapped["Order | None"] = relationship(back_populates="redeem_use")