import os
import re
import asyncio
import logging
import logging.config
import sys
import requests
from pathlib import Path
from collections import defaultdict
from typing import Optional, Dict, List
from enum import Enum
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified
from db.init_db import init_db, seed_data
from db.repo import (
    make_engine, get_or_create_user,
    get_user_by_id,
    create_order_db, get_user_orders_db
)
from db.models import Order
from yookassa import Configuration, Payment
from yookassa.domain.notification import WebhookNotification
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from aiogram.types import Update
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy import inspect


LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
        },
    },
    'handlers': {
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'bot.log',
            'maxBytes': 10 * 1024 * 1024,
            'backupCount': 5,
            'formatter': 'standard',
            'level': 'DEBUG',  # ← временно DEBUG, чтобы видеть всё
        },
        'console': {
            'class': 'logging.StreamHandler',
            'stream': sys.stdout,
            'formatter': 'standard',
            'level': 'DEBUG',
        },
    },
    'loggers': {
        '': {  # root
            'handlers': ['file', 'console'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'box_bot': {
            'handlers': ['file', 'console'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'uvicorn': {
            'handlers': ['console'],  # uvicorn пусть пишет только в stdout
            'level': 'INFO',
            'propagate': False,
        },
        'aiogram': {
            'level': 'WARNING',
        },
    }
}

logging.config.dictConfig(LOG_CONFIG)

# Переопределяем logger сразу
logger = logging.getLogger("box_bot")
logger.setLevel(logging.DEBUG)
logger.debug("=== Logging initialized with DEBUG level ===")
app = FastAPI()


@app.get("/test")
async def test_endpoint():
    logger.info("Test endpoint hit!")
    return {"status": "ok", "message": "Server alive"}


# ========== CONFIG ==========
USE_WEBHOOK = True
load_dotenv(dotenv_path=Path(__file__).parent / '.env')

# === PAYMENT LOCKS (защита от повторных нажатий) ===
_payment_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

def get_payment_lock(order_id: int) -> asyncio.Lock:
    """
    Возвращает asyncio.Lock для конкретного заказа.
    Гарантирует, что оплата обрабатывается строго один раз.
    """
    return _payment_locks[order_id]


# ============DATABASE===========
def get_order_by_id(order_id: int, user_id: int) -> Optional[Order]:
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if order and order.user_id == user_id:
            return order
        return None


def get_all_orders_by_status(status: str) -> list[Order]:
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        stmt = select(Order).where(Order.status == status)
        return list(sess.scalars(stmt).all())


# ==============DATA=============
STREET_KEYWORDS = [
    "ул", "ул.", "улица",
    "проезд", "пр-д", "пр-зд", "пр-д.", "пр-зд.",
    "проспект", "просп.", "пр.", "пр-т", "пр-кт",
    "пер.", "переулок",
    "шоссе",
    "бульвар", "бул.", "б-р.", "бульв.",
    "пл.", "площадь",
    "наб.", "набережная",
    "тракт",
    "аллея",
]

# --- CDEK TEST CREDENTIALS ---
CDEK_ACCOUNT = os.getenv("CDEK_ACCOUNT")
CDEK_SECURE_PASSWORD = os.getenv("CDEK_SECURE_PASSWORD")

logging.getLogger("aiogram.event").setLevel(logging.WARNING)
logging.getLogger("uvicorn").setLevel(logging.WARNING)  # Меньше uvicorn spam

prod_account = os.getenv("CDEK_PROD_ACCOUNT") or ""
prod_password = os.getenv("CDEK_PROD_PASSWORD") or ""
logger.info(f"CDEK_PROD_ACCOUNT загружен: {'Да (непустой)' if prod_account.strip() else 'НЕТ или пустой'} | Длина: {len(prod_account)}")
logger.info(f"CDEK_PROD_PASSWORD загружен: {'Да (непустой)' if prod_password.strip() else 'НЕТ или пустой'} | Длина: {len(prod_password)}")
Configuration.account_id = os.getenv("YOOKASSA_SHOP_ID")
Configuration.secret_key = os.getenv("YOOKASSA_SECRET_KEY")


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        logger.info(f"Incoming request: {request.method} {request.url} from IP {request.client.host}")
        logger.debug(f"Headers: {request.headers}")
        response = await call_next(request)
        logger.info(f"Response status: {response.status_code}")
        return response


app.add_middleware(LoggingMiddleware)

# Проверяем, что ключи загрузились
if not Configuration.account_id or not Configuration.secret_key:
    logger.critical("!!! ЮKassa ключи НЕ ЗАГРУЗИЛИСЬ !!! Проверь .env")
else:
    logger.info(f"ЮKassa подключена: shopId = {Configuration.account_id[:6]}...")


# ========== CDEK: Получение токена ==========
async def get_cdek_token() -> Optional[str]:
    """Получает токен из тестовой среды СДЭК."""
    if not CDEK_ACCOUNT or not CDEK_SECURE_PASSWORD:
        logger.error("CDEK_ACCOUNT или CDEK_SECURE_PASSWORD не заданы в .env!")
        return None

    url = "https://api.edu.cdek.ru/v2/oauth/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CDEK_ACCOUNT,
        "client_secret": CDEK_SECURE_PASSWORD
    }

    try:
        response = await asyncio.to_thread(requests.post, url, data=data, timeout=15)
        response.raise_for_status()
        token = response.json().get("access_token")
        if token:
            logger.info(f"СДЭК токен успешно получен: {token[:20]}...")
            return token
        else:
            logger.error("Токен не пришёл в ответе СДЭК")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса к СДЭК: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Ответ сервера: {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"Неизвестная ошибка при получении токена СДЭК: {e}")
        return None


async def get_cdek_prod_token() -> Optional[str]:
    account = os.getenv("CDEK_PROD_ACCOUNT") or ""
    password = os.getenv("CDEK_PROD_PASSWORD") or ""
    if not account.strip() or not password.strip():  # .strip() для игнора пробелов
        logger.error("CDEK_PROD ключи пустые или отсутствуют!")
        return None
    url = "https://api.cdek.ru/v2/oauth/token"  # прод!
    data = {"grant_type": "client_credentials", "client_id": account, "client_secret": password}
    response = None
    try:
        response = await asyncio.to_thread(requests.post, url, data=data, timeout=15)
        if response.status_code == 200:
            return response.json().get("access_token")
    except Exception as e:
        logger.error(f"Ошибка получения прод-токена: {e}")
        if response:
            logger.error(f"Ответ: {response.status_code} {response.text}")
        return None


async def calculate_cdek_delivery_cost(pvz_code: str) -> Optional[dict]:
    """Возвращает dict: {'cost': int, 'period_min': int, 'period_max': int}"""
    token = await get_cdek_prod_token()
    if not token:
        return None

    url = "https://api.cdek.ru/v2/calculator/tariff"
    payload = {
        "type": 1,
        "tariff_code": 136,
        "from_location": {"code": Config.CDEK_FROM_CITY_CODE},
        "to_location": {"code": pvz_code},
        "packages": [{
            "weight": Config.PACKAGE_WEIGHT_G,
            "length": Config.PACKAGE_LENGTH_CM,
            "width": Config.PACKAGE_WIDTH_CM,
            "height": Config.PACKAGE_HEIGHT_CM,
        }]
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        r = await asyncio.to_thread(requests.post, url, json=payload, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            cost = int(data.get("delivery_sum", 0))
            period_min = data.get("calendar_min", 0) or data.get("period_min", 0)
            period_max = data.get("calendar_max", 0) or data.get("period_max", 0)
            logger.info(f"СДЭК: до {pvz_code} → {cost}₽, срок {period_min}–{period_max} дн.")
            return {
                "cost": cost,
                "period_min": period_min,
                "period_max": period_max
            }
        else:
            logger.warning(f"Ошибка тарифа: {r.status_code} {r.text}")
    except Exception as e:
        logger.error(f"Исключение при расчёте тарифа: {e}")
    return None


async def get_cdek_order_status(cdek_uuid: str) -> Optional[str]:
    """Получает статус заказа по UUID"""
    token = await get_cdek_prod_token()
    if not token or not cdek_uuid:
        return None

    url = f"https://api.cdek.ru/v2/orders/{cdek_uuid}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        r = await asyncio.to_thread(requests.get, url, headers=headers, timeout=15)
        if r.status_code == 200:
            status_code = r.json().get("status", {}).get("code")
            # переводим самые важные статусы
            mapping = {
                "CREATED": "Создан",
                "ACCEPTED": "Принят на склад",
                "IN_PROGRESS": "В пути",
                "DELIVERED": "Доставлен в ПВЗ",
                "RECEIVED": "Выдан клиенту",
            }
            return mapping.get(status_code, status_code)
    except:
        pass
    return None


async def get_cdek_order_info(cdek_uuid: str) -> Optional[dict]:
    """Полная инфа по заказу в СДЭК по UUID"""
    token = await get_cdek_prod_token()
    if not token or not cdek_uuid:
        return None

    url = f"https://api.cdek.ru/v2/orders/{cdek_uuid}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        r = await asyncio.to_thread(requests.get, url, headers=headers, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.error(f"Ошибка получения полной инфы по заказу {cdek_uuid}: {e}")
    return None


# ========== ENUMS & CONFIG ==========
class CallbackData(Enum):
    MENU = "menu"
    GALLERY = "gallery"
    CABINET = "cabinet"
    PRACTICES = "practices"
    ORDERS = "orders"
    HELP = "help"
    FAQ = "faq"
    TEAM = "team"
    REDEEM_START = "redeem:start"
    CHECKOUT_START = "checkout:start"
    SHIP_CDEK = "ship:cdek"
    CHANGE_CONTACT_YES = "change_contact:yes"
    CHANGE_CONTACT_NO = "change_contact:no"
    AUTH_START = "auth:start"
    ADMIN_PANEL = "admin:panel"
    ADMIN_ORDERS_PREPAID = "admin:orders_prepaid"
    ADMIN_ORDERS_READY = "admin:orders_ready"
    ADMIN_ORDERS_SHIPPED = "admin:orders_shipped"
    ADMIN_ORDERS_ARCHIVED = "admin:orders_archived"
    ADMIN_ORDERS_TO_SHIP = "admin:orders_to_ship"  # Новое: "Готовые к отправке"
    ADMIN_SET_ASSEMBLED = "admin:set_assembled"   # Переименуй старый set_ready
    ADMIN_SET_SHIPPED = "admin:set_shipped"        # Новое: для отправки
    ADMIN_SET_READY = "admin:set_ready"
    ADMIN_SET_ARCHIVED = "admin:set_archived"
    ADMIN_SET_TRACK = "admin:set_track"

class OrderStatus(Enum):
    NEW = "new"
    PAID_PARTIALLY = "paid_partially"  # После предоплаты 30%
    PAID_FULL = "paid_full"            # После полной оплаты или дооплаты
    ASSEMBLED = "assembled"           # Собран админом
    SHIPPED = "shipped"                # Отправлен (CDEK создан)
    ARCHIVED = "archived"              # Завершён
    ABANDONED = "abandoned"            # Отменён
    PENDING_PAYMENT = "pending_payment"  # Заказ ждет подтверждения оплаты

class Config:
    TOKEN = os.getenv("BOT_TOKEN")
    GREETING_NOTE_FILE_ID = os.getenv("GREETING_NOTE_FILE_ID")
    VIDEO1_ID = os.getenv("GALLERY_VIDEO1_ID")
    VIDEO2_ID = os.getenv("GALLERY_VIDEO2_ID")
    VIDEO3_ID = os.getenv("GALLERY_VIDEO3_ID")
    VIDEO4_ID = os.getenv("GALLERY_VIDEO4_ID")
    VIDEO5_ID = os.getenv("GALLERY_VIDEO5_ID")
    DB_PATH = os.getenv("DB_PATH", "app.sqlite3")
    PRACTICE_NOTES: dict[int, Optional[str]] = {}
    EXPERTS: dict[str, dict] = {
        "anna": {"name": "Анна Большакова", "video_note_id": os.getenv("EXPERT_ANNA_NOTE_ID")},
        "maria": {"name": "Мария Горелко", "video_note_id": os.getenv("EXPERT_MARIA_NOTE_ID")},
        "alena": {"name": "Алёна Махонина", "video_note_id": os.getenv("EXPERT_ALENA_NOTE_ID")},
        "alexey": {"name": "Алексей Большаков", "video_note_id": os.getenv("EXPERT_ALEXEY_NOTE_ID")},
        "alexander": {"name": "Александр Верховский", "video_note_id": os.getenv("EXPERT_ALEXANDER_NOTE_ID")},
    }
    PRICE_RUB = 5990
    PREPAY_PERCENT = 30
    ADMIN_HELP_NICK = "@anbolshakowa"
    CODES_POOL = set()
    DEFAULT_PRACTICES = [
        "Дыхательная практика", "Зеркало", "Снять тревогу с тревоги",
        "Внутренний ребенок", "Антихрупкость", "Созидать жизнь", "Спокойный сон",
    ]
    PRACTICE_PERFORMERS = [
        "Алексей Большаков",  # 0
        "Анна Большакова",  # 1
        "Мария Горелко",  # 2
        "Алёна Махонина",  # 3
        "Алексей Большаков",  # 4
        "Алексей Большаков",  # 5
        "Александр Верховский",  # 6
    ]
    PRACTICE_DETAILS = [
        {"duration": 34, "desc": "Единственное в своем теле, что ты можешь контролировать - это дыхание. Ты даешь своему телу сигнал «я здесь главная, расслабься, ты в моих любящих и заботливых руках, все хорошо»"},
        {"duration": 15, "desc": "Когда ты есть у себя, когда ты чувствуешь опору в себе - любая задача решается с интересом и последующим ростом."},
        {"duration": 6, "desc": "Теория тревожного состояния простым языком расслабит ум, даст ясность и уверенность."},
        {"duration": 16, "desc": "Когда восстанавливается связь с внутренним ребенком - игра возвращается в жизнь. Это очень приятно."},
        {"duration": 17, "desc": "Перестать убегать от неопределенности жизни в тревогу, сделав её своей супер силой."},
        {"duration": 13, "desc": "Энергию, расходовавшуюся на тревогу, направляем на улучшение своей жизни."},
        {"duration": 16, "desc": "Отправляясь в царство Морфея в спокойнейшим состоянии, пробуждение утром будет радостным и полным энергии."},
    ]
    PRACTICE_AUDIO_IDS = [
        os.getenv("AUDIO1_ID"),
        os.getenv("AUDIO2_ID"),
        None,
        os.getenv("AUDIO4_ID"),
        os.getenv("AUDIO5_ID"),
        None,
        os.getenv("AUDIO7_ID"),
    ]
    PRACTICE_BONUS_AUDIO = [
        None, None, None, None, None,
        os.getenv("AUDIO6_BONUS_ID"),  # только для "Созидать жизнь"
        None
    ]

    PRACTICE_VIDEO_IDS = [  # только для тех, где есть видео
        os.getenv("VIDEO_PRACTICE1_ID"), None, os.getenv("VIDEO_PRACTICE3_ID"), None, None, None, None
    ]
    WELCOME_TEXT = ("Привет! Я тебе очень и очень рада. Меня зовут Анна Большакова, но"
                    " сейчас я буду говорить от имени коробочки. Я создана для тебя, чтобы тебе всегда"
                    " была доступна качественная помощь, а забота о себе ассоциировалась отныне с красотой,"
                    " с неповторимостью и с огромной ценностью. Располагайся поуютнее, здесь ты найдешь всю"
                    " необходимую тебе информацию. Знакомься и до встречи!")
    GALLERY_TEXT = (
        "Коробочка «Отпусти тревогу»\n\n"
        "Внутри ты найдёшь:\n"
        "1. Путеводитель на пути к равновесию\n"
        "2. 7 видео и аудио практик\n"
        "3. Баночка поддерживающих посланий\n"
        "4. Маска для практики со льном и лавандой\n"
        "5. Чай «Глоток тепла и спокойствия»\n"
        "6. Маркер для зеркала\n"
        "7. Личные послания от экспертов\n"
        "8. Вдохновляющее письмо в конверте\n"
        f"\nЦена: {PRICE_RUB} ₽\n"
        "\nО проекте:\n"
        "• 7 практик + физ. содержимое в коробочке\n"
        "• Доступ навсегда\n"
        "• Поддержка в чате"
    )
    PAYMENT_TIMEOUT_SEC = 600

    # Склад в СДЭК (код города). Москва = 44, СПб = 137, Екат = 195 и т.д.
    CDEK_FROM_CITY_CODE = os.getenv("CDEK_FROM_CITY_CODE", "44")  # по умолчанию Москва
    CDEK_SHIPMENT_POINT_CODE = "MSK2296"

    # Вес и габариты коробки (можно вынести в .env)
    PACKAGE_WEIGHT_G = 750  # грамм
    PACKAGE_LENGTH_CM = 26
    PACKAGE_WIDTH_CM = 19
    PACKAGE_HEIGHT_CM = 8

    # CHANEL
    CLOSED_CHANNEL_LINK = "https://t.me/+n85Qa4GPd1s5Yzgy"
    CLOSED_CHANNEL_ID = -1003556936442

    USE_WEBHOOK = os.getenv("USE_WEBHOOK", "True") == "True"  # True по default, False для polling


# ========== ADMIN ==========
ADMIN_USERNAMES = {"@RE_HY",
                   "@anbolshakowa",
                   "@dmitrieva_live",
                   }
MAIN_ADMIN_IDS = {1049170524}
ADMIN_ID = 1049170524

# ========== BOOTSTRAP ==========
bot = Bot(
    Config.TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()
r = Router()
dp.include_router(r)

CODE_RE = re.compile(r"^\d{3}$")


class NoTGWebhookFilter(logging.Filter):
    def filter(self, record):
        # Не логируем каждый TG webhook (только errors)
        if "TG webhook attempt" in record.msg and record.levelno < logging.WARNING:
            return False
        return True


logging.basicConfig(
    level=logging.WARNING,  # Изменить на WARNING (меньше info)
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            "bot.log", maxBytes=10*1024*1024, backupCount=5
        ),
        logging.StreamHandler()
    ]
)

logger.addFilter(NoTGWebhookFilter())


async def create_yookassa_payment(order: Order, amount_rub: int, description: str, return_url: str, kind: Optional[str] = None) -> dict:
    lock = get_payment_lock(order.id)
    async with lock:
        try:
            engine = make_engine(Config.DB_PATH)
            with Session(engine) as sess:
                user = get_user_by_id(sess, order.user_id)
                if not user:
                    raise ValueError("User not found for receipt")

            # ───────────────────────────────────────────────
            # Динамический выбор НДС и payment_mode
            # ───────────────────────────────────────────────
            is_prepayment = "pre" in description.lower()  # предоплата 30%

            if is_prepayment:
                vat_code = 6          # расчётная 20/120
                payment_mode = "full_prepayment"
            else:
                vat_code = 4          # обычная 20%
                payment_mode = "full_payment"

            # Формируем receipt
            receipt = {
                "customer": {
                    "email": user.email or "noemail@example.com",
                    "phone": user.phone.replace("+", "") if user.phone else None  # type: ignore
                },
                "items": [
                    {
                        "description": "Коробочка «Отпусти тревогу»",
                        "quantity": "1.00",
                        "amount": {
                            "value": f"{amount_rub}.00",
                            "currency": "RUB"
                        },
                        "vat_code": vat_code,
                        "payment_mode": payment_mode,
                        "payment_subject": "commodity"  # физический товар
                    }
                ]
            }

            payment = Payment.create({
                "amount": {
                    "value": f"{amount_rub}.00",
                    "currency": "RUB"
                },
                "confirmation": {
                    "type": "redirect",
                    "return_url": return_url
                },
                "capture": True,
                "description": description,
                "metadata": {
                    "order_id": str(order.id),
                    "user_id": str(order.user_id),
                    "payment_kind": kind or "unknown"
                },
                "receipt": receipt
            })

            logger.info(
                f"Создан платёж ЮKassa #{payment.id} для заказа #{order.id} на {amount_rub}₽ "
                f"({description}) → vat_code={vat_code}, mode={payment_mode}"
            )

            return {
                "payment_id": payment.id,
                "confirmation_url": payment.confirmation.confirmation_url,
                "status": payment.status
            }

        except Exception as e:
            logger.exception(f"Ошибка создания платежа ЮKassa для заказа #{order.id}")
            await notify_admin(f"❌ Ошибка ЮKassa при создании платежа для заказа #{order.id}\n{e}")
            return None


async def create_cdek_order(order_id: int) -> bool:
    token = await get_cdek_prod_token()
    if not token:
        logger.error("Нет токена СДЭК")
        return False

    engine = make_engine(Config.DB_PATH)

    # ================== 1. Загружаем заказ и пользователя ==================
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if not order:
            logger.error(f"Заказ #{order_id} не найден")
            return False

        # FIX: Refresh to ensure attached
        sess.refresh(order)

        pvz_code = order.extra_data.get("pvz_code")
        if not pvz_code:
            logger.error(f"Нет pvz_code для заказа #{order.id}")
            return False

        user = get_user_by_id(sess, order.user_id)
        if not user or not user.full_name or not user.phone:
            logger.error(f"Нет данных пользователя для заказа #{order.id}")
            return False

        address = order.address or "ПВЗ СДЭК"
        postal_code = order.extra_data.get("postal_code", "000000")

    # ================== 2. Формируем payload ==================
    payload = {
        "type": 2,
        "number": f"BOX{order_id}",
        "tariff_code": 136,
        "comment": f"Заказ из бота «ТВОЯ КОРОБОЧКА» #{order_id}",
        "shipment_point": Config.CDEK_SHIPMENT_POINT_CODE,

        "delivery_recipient_cost": {"value": 0},

        "to_location": {
            "code": str(pvz_code),
            "address": address,
            "postal_code": postal_code,
        },

        "sender": {
            "company": "ИП Большаков А. М.",
            "name": "Алексей",
            "phones": [{"number": "+79651051779"}],
        },

        "recipient": {
            "name": user.full_name,
            "phones": [{
                "number": user.phone.replace("+", "").replace(" ", "").replace("-", "")  # type: ignore[attr-defined]
            }],
        },

        "packages": [{
            "number": f"BOX{order_id}",
            "weight": Config.PACKAGE_WEIGHT_G,
            "length": Config.PACKAGE_LENGTH_CM,
            "width": Config.PACKAGE_WIDTH_CM,
            "height": Config.PACKAGE_HEIGHT_CM,
            "comment": "Подарочная коробочка с антистресс-набором",
            "items": [{
                "name": "Коробочка «Отпусти тревогу»",
                "ware_key": f"BOX{order_id}",
                "payment": {"value": 0},
                "cost": Config.PRICE_RUB,
                "weight": Config.PACKAGE_WEIGHT_G,
                "amount": 1,
            }],
        }],

        "services": [
            {"code": "INSURANCE", "parameter": Config.PRICE_RUB}
        ],
    }

    import json
    logger.info(
        f"\n=== ОТПРАВЛЯЕМ В СДЭК ЗАКАЗ #{order_id} ===\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
        f"{'=' * 50}"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    url = "https://api.cdek.ru/v2/orders"

    # ================== 3. HTTP-запрос ==================
    try:
        r = await asyncio.to_thread(
            requests.post,
            url,
            json=payload,
            headers=headers,
            timeout=30,
        )

        logger.info(f"СДЭК ответил: {r.status_code}\n{r.text[:2000]}")

        if r.status_code not in (200, 201, 202):
            await notify_admin(
                f"❌ СДЭК ошибка для заказа #{order_id}\n"
                f"{r.status_code}\n{r.text[:1000]}"
            )
            return False

        data = r.json()
        uuid = data.get("entity", {}).get("uuid")

        if not uuid:
            logger.error(f"СДЭК не вернул uuid для заказа #{order_id}")
            return False

    except Exception as e:
        logger.exception(f"Исключение при создании заказа СДЭК #{order_id}")
        await notify_admin(f"❌ Исключение при создании заказа СДЭК #{order_id}\n{e}")
        return False

    # ================== 4. СОХРАНЯЕМ UUID В БД ==================
    with Session(engine) as sess:  # Новая сессия для записи
        order = sess.get(Order, order_id)  # Перезагружаем свежий объект
        if not order:
            return False
        if order.extra_data is None:
            order.extra_data = {}
        order.extra_data["cdek_uuid"] = uuid
        flag_modified(order, "extra_data")  # Маркируем как изменённый
        order.track = uuid
        order.status = OrderStatus.SHIPPED.value
        sess.commit()  # Коммитим в этой сессии

    logger.info(f"СДЭК: ЗАКАЗ #{order_id} ПРИНЯТ | UUID: {uuid}")

    await notify_admin(
        f"🚚 Заказ #{order_id} успешно принят СДЭК\n"
        f"UUID: {uuid}\n"
        f"Трек-номер придёт автоматически."
    )

    return True



def validate_data(full_name: str, phone: str, email: str) -> tuple[bool, str]:
    if not full_name or not full_name.strip():
        return False, "Отсутствуют имя и фамилия."
    if not re.match(r"^[А-ЯЁ][а-яё]+(\s+[А-ЯЁ][а-яё]+)+$", full_name.strip()):
        return False, "Имя и Фамилия с заглавной буквы, без отчества и лишних пробелов."
    if not phone or not phone.strip():
        return False, "Отсутствует телефон."
    phone = phone.strip().replace(" ", "").replace("-", "")
    if not re.match(r"^\+7\d{10}$", phone):
        return False, "Телефон: только +7 и 10 цифр (например, +79161234567)."
    if not email or not email.strip():
        return False, "Отсутствует email."
    if not re.match(r"^[^@]+@[^@]+\.[a-zA-Z]{2,}$", email.strip()):
        return False, "Некорректный email."
    return True, "Данные валидны."

def validate_address(address: str) -> tuple[bool, str]:
    address = address.strip()
    if not address or len(address) < 4:
        return False, "Адрес слишком короткий. Укажите улицу и номер дома."
    return True, "Адрес валиден."


def reset_states(user, session: Session = None):
    """
    session — опционально, если передана — используем её, иначе создаём новую
    """
    close_session = False
    if session is None:
        engine = make_engine(Config.DB_PATH)
        session = Session(engine)
        close_session = True

    try:
        # Сбрасываем флаги
        user.awaiting_redeem_code = False
        user.awaiting_auth = False
        user.awaiting_gift_message = False
        user.awaiting_pvz_address = False
        user.awaiting_manual_pvz = False
        user.awaiting_manual_track = False
        user.pvz_for_order_id = None
        user.temp_gift_order_id = None
        user.temp_pvz_list = None
        user.temp_selected_pvz = None
        user.temp_order_id_for_track = None

        # Abandon unfinished NEW orders — используем ту же сессию!
        orders = get_user_orders_db(session, user.telegram_id)
        for o in orders:
            if o.status == OrderStatus.NEW.value:
                o = session.merge(o)
                o.status = OrderStatus.ABANDONED.value

        session.commit()
        logger.info(f"Состояния пользователя {user.telegram_id} сброшены")

    finally:
        if close_session:
            session.close()


# ======== ADMIN HELPERS ========
def get_order_admin(order_id: int) -> Optional[Order]:
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        return sess.get(Order, order_id)


async def is_admin(message_or_callback: Message | CallbackQuery) -> bool:
    if isinstance(message_or_callback, Message):
        user = message_or_callback.from_user
    else:  # CallbackQuery
        user = message_or_callback.from_user

    uid = user.id
    username = user.username

    # 1. Проверка по ID (самая надёжная)
    if uid in MAIN_ADMIN_IDS:
        logger.info(f"Доступ разрешён по ID: {uid}")
        return True

    # 2. Проверка по username (удобно для команды)
    if username and f"@{username}" in ADMIN_USERNAMES:
        logger.info(f"Доступ разрешён по username: @{username}")
        return True

    logger.info(f"Доступ запрещён: uid={uid}, username=@{username or 'нет'}")

    # Сообщение пользователю
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer("Доступ запрещён. Только для администраторов.")
    elif isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.answer("Доступ запрещён", show_alert=True)

    return False

async def notify_admin(text: str):
    for admin_id in MAIN_ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception as e:
            logger.error(f"Admin notify failed for {admin_id}: {e}")

async def notify_admins_payment_started(order: Order):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"🔔 Новый заказ #{order.id}\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Тип оплаты: {order.payment_kind}\n"
        f"Адрес: {order.address or '—'}\n"
        f"Статус: {order.status}"
    )

async def notify_admins_payment_success(order_id: int):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if not order:
            return
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"✅ Предоплата #{order_id} получена\n"
        f"Пользователь: {full_name} ({order.user_id})\n"
        f"Статус: {order.status}"
    )

async def notify_admins_order_ready(order_id: int):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        from sqlalchemy.orm import joinedload  # импортируйте в начале файла, если нет
        order = sess.query(Order).options(joinedload(Order.user)).get(order_id)
        if not order:
            return
        full_name = order.user.full_name if order.user else "Неизвестно"
    await notify_admin(
        f"📦 Заказ #{order_id} собран\n"
        f"Пользователь: {full_name} ({order.user_id})\n"
        f"Статус: {order.status}"
    )

async def notify_admins_payment_remainder(order_id: int):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if not order:
            return
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"💸 Заказ #{order_id} полностью оплачен\n"
        f"Пользователь: {full_name} ({order.user_id})\n"
        f"Статус: {order.status}"
    )

async def notify_admins_order_shipped(order_id: int):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if not order:
            return
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"🚚 Заказ #{order_id} отправлен\n"
        f"Пользователь: {full_name} ({order.user_id})\n"
        f"Трек: {order.track}\n"
        f"Статус: {order.status}"
    )

async def notify_admins_order_archived(order_id: int):   # ← теперь принимает order_id
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if not order:
            logger.warning(f"Заказ {order_id} не найден при уведомлении админа")
            return
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"🗄 Заказ #{order_id} заархивирован\n"
        f"Пользователь: {full_name} ({order.user_id})\n"
        f"Статус: {order.status}"
    )


async def notify_admins_order_address_changed(order: Order):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"!! Обновлён адрес ПВЗ для заказа #{order.id}\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Новый адрес: {order.address or '—'}"
    )


async def notify_client_order_assembled(order_id: int, message: Message):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if order is None:
            logger.warning(f"Заказ {order_id} не найден при уведомлении клиента")
            return

    text = format_client_order_info(order)
    await message.answer(
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=kb_ready_message(order)
    )


async def notify_client_order_shipped(order_id: int, message: Message):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if order is None:
            logger.warning(f"Заказ {order_id} не найден при уведомлении об отправке")
            return

    text = format_client_order_info(order)
    await message.answer(
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=kb_order_status(order)
    )


async def notify_client_order_abandoned(order: Order, message: Message):
    await message.answer(
        f"Ваш заказ #{order.id} был отменён из-за отсутствия оплаты в течение 10 минут.",
        reply_markup=kb_main()
    )

# ======== SEND UTILS ========
async def edit_or_send(
    msg: Message,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    *,
    force_new: bool = False,
    edit_only: bool = False,
    parse_mode: str | None = "HTML",
    disable_web_page_preview: bool = True
):
    common_kwargs = {
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
        "reply_markup": reply_markup
    }

    if force_new:
        return await msg.answer(text, **common_kwargs)

    if edit_only:
        try:
            await msg.edit_text(text, **common_kwargs)
            return
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
                logger.warning(f"Edit failed (edit_only): {e}")
            return

    try:
        await msg.edit_text(text, **common_kwargs)
    except TelegramBadRequest:
        await msg.answer(text, **common_kwargs)

# ========== КОМАНДА ТЕСТА СДЭК (РАБОЧАЯ!) ==========
@r.message(Command("test_cdek_token"))
async def cmd_test_cdek_token(message: Message):
    if not await is_admin(message):
        await message.answer("Только админ может использовать эту команду.")
        return

    await message.answer("Запрашиваю токен у СДЭК (тестовая среда)...")
    token = await get_cdek_token()
    if token:
        await message.answer(
            f"<b>Успех!</b>\n\nТокен получен:\n<code>{token}</code>",
            parse_mode="HTML"
        )
    else:
        await message.answer("Не удалось получить токен. Смотри логи.")

# ======== HANDLERS: helpers ========
async def send_greeting_circle(message: Message):
    if Config.GREETING_NOTE_FILE_ID:
        try:
            await message.answer_video_note(
                video_note=Config.GREETING_NOTE_FILE_ID,
                reply_markup=ReplyKeyboardRemove()
            )
        except Exception as e:
            logger.error(f"Failed to send video note: {e}")
    await message.answer(Config.WELCOME_TEXT)

async def send_practice_intro(message: Message, idx: int, title: str):
    details = Config.PRACTICE_DETAILS[idx]
    descr = f"<b>{title}</b>\n⏰ {details['duration']} мин\n\n{details['desc']}"
    await message.answer(descr)

# ========== KEYBOARDS ==========
def create_inline_keyboard(buttons: List[List[dict]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(**btn) for btn in row] for row in buttons
    ])

MAIN_KB = create_inline_keyboard([
    [
        {"text": "Заказать", "callback_data": CallbackData.CHECKOUT_START.value},
        {"text": "Знакомство", "callback_data": CallbackData.GALLERY.value},
        {"text": "Личный кабинет", "callback_data": CallbackData.CABINET.value},
    ]
])

def kb_main() -> InlineKeyboardMarkup:
    return MAIN_KB

def kb_empty_practices() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Оформить заказ", "callback_data": CallbackData.CHECKOUT_START.value}],
        [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
    ])

def kb_practices_list(titles: List[str]) -> InlineKeyboardMarkup:
    rows = [[{"text": f"{i + 1}. {t}", "callback_data": f"practice:{i}"}] for i, t in enumerate(titles)]
    rows.append([{"text": "В меню", "callback_data": CallbackData.MENU.value}])
    return create_inline_keyboard(rows)


def kb_back_to_practices() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Назад к списку практик", "callback_data": CallbackData.PRACTICES.value}]
    ])


def kb_practice_card(idx: int) -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Начать", "callback_data": f"practice:play:{idx}"}],
        [{"text": "Назад к списку", "callback_data": CallbackData.PRACTICES.value}],
    ])

def kb_cabinet() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Мои практики", "callback_data": CallbackData.PRACTICES.value}],
        [{"text": "Мои заказы", "callback_data": CallbackData.ORDERS.value}],
        [{"text": "Активировать код", "callback_data": CallbackData.REDEEM_START.value}],
        [{"text": "Помощь", "callback_data": CallbackData.HELP.value}],
        [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
    ])

def kb_cabinet_unauth() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Заказать", "callback_data": CallbackData.CHECKOUT_START.value}],
        [{"text": "Авторизоваться", "callback_data": CallbackData.AUTH_START.value}],
        [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
    ])

def kb_gallery() -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "Хочу заказать", "callback_data": CallbackData.CHECKOUT_START.value}],
        [{"text": "Команда коробочки", "callback_data": CallbackData.TEAM.value}],
        [{"text": "FAQ", "callback_data": CallbackData.FAQ.value}],
        [{"text": "Назад", "callback_data": CallbackData.MENU.value}],
    ]
    return create_inline_keyboard(buttons)

def kb_shipping() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "СДЭК ПВЗ", "callback_data": CallbackData.SHIP_CDEK.value}],
        [{"text": "Назад", "callback_data": CallbackData.GALLERY.value}],
    ])

def kb_review(order: Optional[Order]) -> InlineKeyboardMarkup:
    prepay = (Config.PRICE_RUB * Config.PREPAY_PERCENT + 99) // 100
    return create_inline_keyboard([
        [{"text": f"Оплатить 100% ({Config.PRICE_RUB} ₽)", "callback_data": f"pay:full:{0 if not order else order.id}"}],
        [{"text": f"Предоплата {Config.PREPAY_PERCENT}% ({prepay} ₽)", "callback_data": f"pay:pre:{0 if not order else order.id}"}],
        [{"text": "Назад", "callback_data": CallbackData.GALLERY.value}],
    ])

def kb_ready_message(order: Order) -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Оплатить остаток", "callback_data": f"pay:rem:{order.id}"}],
        [{"text": "Статус заказа", "callback_data": f"order:{order.id}"}],
    ])


def kb_order_status(order: Order) -> InlineKeyboardMarkup:
    buttons = []

    # Отслеживание
    if order.track:
        buttons.append([{
            "text": "Отследить посылку",
            "url": f"https://www.cdek.ru/ru/tracking?order_id={order.track}"
        }])

    # Дооплата — только если предоплата и собран
    if order.status == OrderStatus.ASSEMBLED.value and order.payment_kind == "pre":
        remainder_rub = (order.total_price_kop // 100) - (order.total_price_kop * Config.PREPAY_PERCENT // 10000)
        buttons.append([{
            "text": f"Оплатить остаток ({remainder_rub} ₽)",
            "callback_data": f"pay:rem:{order.id}"
        }])

    buttons.append([{"text": "Информация о заказе", "callback_data": f"order:{order.id}"}])
    buttons.append([{"text": "В меню", "callback_data": CallbackData.MENU.value}])

    return create_inline_keyboard(buttons)


def kb_orders_list(order_ids: List[int]) -> InlineKeyboardMarkup:
    rows = [[{"text": f"Заказ #{oid}", "callback_data": f"order:{oid}"}] for oid in order_ids]
    rows.append([
        {"text": "Оформить заказ", "callback_data": CallbackData.CHECKOUT_START.value}
    ])
    rows.append([{"text": "В меню", "callback_data": CallbackData.MENU.value}])
    return create_inline_keyboard(rows)

def kb_change_contact(back_to: str = CallbackData.GALLERY.value) -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Да", "callback_data": CallbackData.CHANGE_CONTACT_YES.value}],
        [{"text": "Нет", "callback_data": CallbackData.CHANGE_CONTACT_NO.value}],
        [{"text": "Назад", "callback_data": back_to}],
    ])

def kb_admin_panel() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Заказы для сборки", "callback_data": CallbackData.ADMIN_ORDERS_PREPAID.value}],
        [{"text": "Заказы, ожидающие дооплаты", "callback_data": CallbackData.ADMIN_ORDERS_READY.value}],
        [{"text": "Заказы готовые к отправке", "callback_data": CallbackData.ADMIN_ORDERS_TO_SHIP.value}],
        [{"text": "Отправленные заказы", "callback_data": CallbackData.ADMIN_ORDERS_SHIPPED.value}],
        [{"text": "Архив заказов", "callback_data": CallbackData.ADMIN_ORDERS_ARCHIVED.value}],
        [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
    ])

def kb_admin_orders(orders: List[Order]) -> InlineKeyboardMarkup:
    rows = []
    for order in orders:
        rows.append([
            {
                "text": f"Заказ #{order.id} ({order.status}) {'full' if order.payment_kind == 'full' else 'pre' if order.payment_kind == 'pre' else ''}",
                "callback_data": f"admin:order:{order.id}"}        ])
    rows.append([{"text": "Назад", "callback_data": CallbackData.ADMIN_PANEL.value}])
    return create_inline_keyboard(rows)


def kb_admin_order_actions(order: Order) -> InlineKeyboardMarkup:
    buttons = []
    # Для сборки (если PAID_PARTIALLY или PAID_FULL)
    if order.status in [OrderStatus.PAID_PARTIALLY.value, OrderStatus.PAID_FULL.value]:
        buttons.append([{"text": "Собран", "callback_data": f"{CallbackData.ADMIN_SET_ASSEMBLED.value}:{order.id}"}])
    # Для отправки (если ASSEMBLED и PAID_FULL, no track)
    if order.status == OrderStatus.ASSEMBLED.value and order.payment_kind in ["full", "remainder"] and not order.track:
        buttons.append([{"text": "Отправить", "callback_data": f"{CallbackData.ADMIN_SET_SHIPPED.value}:{order.id}"}])
        if order.extra_data.get("manual_pvz", False):
            buttons.append([{"text": "Ввести трек вручную", "callback_data": f"{CallbackData.ADMIN_SET_TRACK.value}:{order.id}"}])
    # Для архива (SHIPPED)
    elif order.status == OrderStatus.SHIPPED.value:
        buttons.append([{"text": "Архивировать", "callback_data": f"{CallbackData.ADMIN_SET_ARCHIVED.value}:{order.id}"}])
    buttons.append([{"text": "Назад", "callback_data": CallbackData.ADMIN_PANEL.value}])
    return create_inline_keyboard(buttons)


# ========== UTILS ==========
def format_order_review(order: Order) -> str:
    return (
        f'<b>Заказ:</b>\n• Коробочка "Отпусти тревогу" — {Config.PRICE_RUB} руб.\n'
        f"• Доставка: ПВЗ СДЭК\n"
        f"• Адрес: {order.address or '—'}\n\n"
        f"<b>Предоплата:</b> {Config.PREPAY_PERCENT}% = {order.prepay_amount} ₽\n"
        f"<b>Остаток:</b> {order.remainder_amount} ₽"
    )

def format_order_admin(order: Order) -> str:
    # Assume order attached (from caller sess)
    full_name = order.user.full_name if order.user else "Неизвестно"
    pvz_code = (order.extra_data or {}).get("pvz_code", "—")
    gift = (order.extra_data or {}).get("gift_message", "").strip()
    gift_text = f"Послание в подарок:\n{gift or '—'}\n\n"
    return (
        f"Заказ #{order.id}\n"
        f"Пользователь: {full_name} ({order.user_id})\n"
        f"Статус: {order.status}\n"
        f"ПВЗ код: {pvz_code}\n"
        f"Адрес: {order.address or '—'}\n"
        f"Трек: {order.track or '—'}\n"
        f"Тип оплаты: {order.payment_kind or '—'}\n\n"
        f"{gift_text}"
    )


def format_client_order_info(order: Order) -> str:
    status_map = {
        OrderStatus.NEW.value: "🆕 Новый заказ",
        OrderStatus.PAID_PARTIALLY.value: "✅ Предоплачен (30%), ждём сборки",
        OrderStatus.PAID_FULL.value: "💳 Полностью оплачен, ждём сборки",
        OrderStatus.ASSEMBLED.value: "📦 Собран - ждём дооплату" if order.payment_kind == "pre" else "📦 Собран - скоро отправим",
        OrderStatus.SHIPPED.value: "🚚 Отправлен",
        OrderStatus.ARCHIVED.value: "✅ Доставлен и завершён",
        OrderStatus.ABANDONED.value: "❌ Отменён",
    }
    status_text = status_map.get(order.status, f"Статус: {order.status}")

    lines = [
        f"<b>Заказ #{order.id}</b>",
        f"<b>{status_text}</b>",
        "",
        "📦 <b>Товар:</b> Коробочка «Отпусти тревогу»",
        f"💰 <b>Цена:</b> {Config.PRICE_RUB} ₽",
    ]

    # Доставка
    delivery_cost = (order.extra_data or {}).get("delivery_cost", 0)
    period = (order.extra_data or {}).get("delivery_period", "3–7")
    lines += [
        "",
        "🚚 <b>Доставка:</b> ПВЗ СДЭК",
        f"💸 Стоимость доставки: <b>{delivery_cost} ₽</b>",
        f"⏳ Срок доставки: ≈ <b>{period} дн.</b>",
        f"📍 <b>Адрес ПВЗ:</b>\n{order.address}",
    ]

    # Послание
    gift = (order.extra_data or {}).get("gift_message")
    lines += [
        "",
        "💌 <b>Личное послание в подарок:</b>",
        f"<i>{gift if gift else '—'}</i>",
    ]

    # Оплата — подробнее
    total = order.total_price_kop // 100  # предполагаем, что total_price теперь в рублях (не копейках)
    prepay_amount = (total * Config.PREPAY_PERCENT + 99) // 100
    remainder = total - prepay_amount

    lines += ["", "💳 <b>Оплата:</b>"]

    if order.status == OrderStatus.NEW.value:
        lines += [
            f"К оплате: <b>{total} ₽</b>",
            f"   • Предоплата {Config.PREPAY_PERCENT}% ({prepay_amount} ₽)",
            f"   • Полная оплата ({total} ₽)",
        ]
    elif order.status == OrderStatus.PAID_PARTIALLY.value:
        lines += [
            f"✅ Предоплата получена: {prepay_amount} ₽",
            f"🔄 Остаток к оплате: <b>{remainder} ₽</b>",
        ]
    elif order.status == OrderStatus.ASSEMBLED.value:
        if order.payment_kind == "pre":
            lines += [
                f"✅ Предоплата: {prepay_amount} ₽",
                f"Ожидаем дооплату: <b>{remainder} ₽</b>",
            ]
        else:
            lines += [f"✅ Полностью оплачено: {total} ₽"]
    elif order.status in [OrderStatus.PAID_FULL.value, OrderStatus.SHIPPED.value, OrderStatus.ARCHIVED.value]:
        lines += [f"✅ Полностью оплачено: {total} ₽"]
    else:
        lines += [f"Сумма: {total} ₽"]

    # Трек
    if order.track and order.track not in ("—", None, ""):
        lines += [
            "",
            f"📮 <b>Трек-номер:</b> <code>{order.track}</code>",
            f'<a href="https://www.cdek.ru/ru/tracking?order_id={order.track}">Отследить посылку</a>',
        ]

    return "\n".join(lines)


# ========== START / MENU ==========
@r.message(CommandStart())
async def on_start(message: Message):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        get_or_create_user(sess, message.from_user.id, message.from_user.username)
        sess.commit()
    await send_greeting_circle(message)
    await message.answer("Выбери действие:", reply_markup=kb_main())

@r.message(Command("grab_id"))
async def grab_id(message: Message):
    src = message.reply_to_message
    if not src:
        await message.answer("Сделайте /grab_id ответом на сообщение с медиа.")
        return

    if src.video or src.video_note:
        file_id = src.video.file_id if src.video else src.video_note.file_id
        await message.answer(f"file_id видео/кружочка: {file_id}")
    elif src.audio:
        await message.answer(f"file_id аудио: {src.audio.file_id}")
    elif src.voice:
        await message.answer(f"file_id голосового: {src.voice.file_id}")
    elif src.document:
        await message.answer(f"file_id документа (аудио?): {src.document.file_id}")
    else:
        await message.answer("Нет поддерживаемого медиа в ответе.")

@r.message(Command("menu"))
async def cmd_menu(message: Message):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, message.from_user.id)
        if user:
            reset_states(user, sess)
            await message.answer("Все черновики заказов отменены. Если был незавершённый заказ - он отменён. Оплаченные заказы вы можете найти в Личном кабинете")
    await message.answer("Выбери действие:", reply_markup=kb_main())

@r.message(Command("admin_panel"))
async def cmd_admin_panel(message: Message):
    if not await is_admin(message):
        return
    await message.answer("Панель администратора:", reply_markup=kb_admin_panel())


@r.callback_query(F.data == CallbackData.MENU.value)
async def cb_menu(cb: CallbackQuery):
    logger.info(f"Menu callback: user_id={cb.from_user.id}, data={cb.data}")

    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await edit_or_send(cb.message, "Выбери действие:", kb_main())
            await cb.answer()
            return

        # Проверяем, есть ли незавершённый процесс оформления
        has_active_process = any([
            user.awaiting_redeem_code,
            user.awaiting_auth,
            user.awaiting_gift_message,
            user.awaiting_pvz_address,
            user.awaiting_manual_pvz,
            user.awaiting_manual_track,
            user.pvz_for_order_id is not None,
            user.temp_gift_order_id is not None,
        ])

        if has_active_process:
            # Предупреждаем, но НЕ сбрасываем автоматически
            await cb.message.answer(
                "У вас сейчас активный процесс (ввод кода, оформление заказа и т.д.).\n\n"
                "Если вернуться в меню сейчас - незавершённый заказ будет отменён.\n"
                "Хотите продолжить или всё-таки отменить и вернуться?",
                reply_markup=create_inline_keyboard([
                    [{"text": "Продолжить оформление", "callback_data": "noop"}],  # просто закрыть
                    [{"text": "Отменить всё и в меню", "callback_data": "force_menu_reset"}],
                ])
            )
            await cb.answer("Есть активный процесс!")
            return

        # Если ничего активного нет — спокойно сбрасываем и идём в меню
        reset_states(user)
        await cb.message.answer("Все черновики (если были) отменены.")

    await edit_or_send(cb.message, "Выбери действие:", kb_main())
    await cb.answer()


@r.callback_query(F.data == "force_menu_reset")
async def cb_force_menu_reset(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if user:
            reset_states(user)  # здесь уже force не нужен, т.к. пользователь явно согласился
            await cb.message.edit_text("Всё отменено. Возвращаемся в главное меню.")
            await cb.message.answer("Выбери действие:", reply_markup=kb_main())
    await cb.answer("Сброс выполнен")


# ========== CABINET ==========
@r.callback_query(F.data == CallbackData.CABINET.value)
async def cb_cabinet(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return
    name = cb.from_user.first_name or "друг"
    if not user.is_authorized:
        await edit_or_send(cb.message, f"Добро пожаловать, {name}!\nВы не авторизованы.", kb_cabinet_unauth())
    else:
        await edit_or_send(cb.message, f"Добро пожаловать, {name}!\nВы авторизованы как {user.full_name}.", kb_cabinet())
    await cb.answer()

@r.callback_query(F.data == CallbackData.HELP.value)
async def cb_help(cb: CallbackQuery):
    # reset_waiting_flags(ustate(cb.from_user.id))
    await edit_or_send(cb.message, f"При ошибке обращайтесь: {Config.ADMIN_HELP_NICK}",
                       create_inline_keyboard([[{"text": "В меню", "callback_data": CallbackData.MENU.value}]]))
    await cb.answer()

# ========== AUTH ==========
@r.callback_query(F.data == CallbackData.AUTH_START.value)
async def cb_auth_start(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        user.awaiting_auth = True
        sess.commit()

    await cb.message.answer(
        "Введите данные в 3 строки:\n"
        "Имя Фамилия\n"
        "+7XXXXXXXXXX\n"
        "email@example.com",
        reply_markup=create_inline_keyboard([
            [{"text": "Отмена", "callback_data": CallbackData.MENU.value}]
        ])
    )
    await cb.answer()


# ========== GALLERY + FAQ + TEAM ==========
@r.callback_query(F.data == CallbackData.GALLERY.value)
async def cb_gallery(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        sess.refresh(user)

        if user.gallery_viewed:
            await cb.message.answer(Config.GALLERY_TEXT, reply_markup=kb_gallery())
            await cb.answer()
            return

        try:
            await cb.message.answer("Загружаю видео знакомства...")
            await cb.message.answer_document(document=Config.VIDEO1_ID, caption="Видео 1")
            await cb.message.answer_document(document=Config.VIDEO2_ID, caption="Видео 2")
            await cb.message.answer_document(document=Config.VIDEO3_ID, caption="Видео 3 - Часть 1")
            await cb.message.answer_document(document=Config.VIDEO4_ID, caption="Видео 4 - Часть 2")
            await cb.message.answer_document(document=Config.VIDEO5_ID, caption="Видео 5 - Часть 3")
        except Exception as e:
            logger.error(f"Failed to send gallery videos: {e}")
            await cb.message.answer("Ошибка при загрузке видео. Свяжитесь с администратором.")

        await cb.message.answer(Config.GALLERY_TEXT, reply_markup=kb_gallery())

        user.gallery_viewed = True
        sess.commit()
    await cb.answer()

@r.callback_query(F.data == CallbackData.FAQ.value)
async def cb_faq(cb: CallbackQuery):
    faq_text = "<b>Частые вопросы:</b>\n\n"
    faq_items = [
        "1. Что такое коробочка?\nЭто комплект заботы о себе. Внутри - предметы, практики и маленькие сюрпризы, которые помогают снизить тревогу, восстановить ресурс и почувствовать опору.\n",
        "2. Чем коробочка отличается от консультации психолога?\nЭто не замена терапии, а мягкая поддержка. Консультация - это работа в диалоге со специалистом. А коробочка - ваш личный набор «здесь и сейчас», чтобы помочь себе в нужный момент.\n",
        "3. Для кого подходит коробочка?\nДля тех, кто чувствует тревогу, усталость, потерю энергии, перегрузку делами. Подойдёт и тем, кто просто хочет ввести новые ритуалы заботы о себе.\n",
        "4. Что внутри коробочки?\n1. Путеводитель на пути к равновесию\n2. 7 видео и аудио практик\n3. Баночка поддерживающих посланий\n4. Маска для практик со льном и лавандой\n5. Чай “Глоток тепла и спокойствия”\n6. Маркер для зеркала\n7. Личные послания от экспертов\n8. Вдохновляющее письмо в конверте\n",
        "5. Кто создаёт практики для коробочки?\nПрактики разработаны пятью практикующими психологами. Каждый из них, используя собственный уникальный стиль, помогает супер объемно и результативно подойти к решению.\n",
        "6. Как пользоваться коробочкой?\nОткройте её в момент тревоги или когда хочется тепла. Выбирайте ритуал, заваривайте чай, доставайте фразу или выполняйте практику. Всё — в своём темпе.\n",
        "7. Можно ли использовать коробочку несколько раз?\nДа! Практики и предметы рассчитаны на многократное использование. А баночка с фразами - это как маленькое объятие словами, к которой можно возвращаться.\n",
        "8. Сколько времени занимает работа с коробочкой?\nОт 2 минут (например, достать фразу поддержки) до 15–20 минут (практика или ритуал). Всё зависит от того, сколько у вас ресурса сейчас.\n",
        "9. Сколько стоит коробочка?\nАктуальную цену можно посмотреть в разделе Инфо.\n",
        "10. Как заказать коробочку?\nНажмите кнопку «Заказать», бот поможет оформить заказ и доставку.\n",
        "11. Сколько ждать доставку?\nВ среднем 3–7 дней, в зависимости от региона и службы доставки.\n",
        "12. Можно ли заказать коробочку в подарок?\nКонечно. В коробочку можно добавить послание для получателя, текст послания вы пишите в поле для комментариев.\n",
        "13. А если я потерял доступ к онлайн-практикам?\nНапишите в бот или свяжитесь с нами, мы восстановим доступ.\n",
        "14. Будут ли новые коробочки?\nДа! Уже готовим сезонные коллекции: новогоднюю, к 14 февраля, 23 февраля и 8 марта. Каждая со своей темой.\n",
        "15. Можно ли купить несколько коробочек сразу?\nКонечно, можно. Они часто становятся отличным подарком близким.\n",
        "16. Чем коробочка отличается от обычного подарочного набора?\nОбычные наборы - это вещи. Наша коробочка - это опыт, смыслы, ответы. Она создана так, чтобы вы не просто получили предметы, а прожили поддержку, заботу и практику.\n",
        "17. Есть ли доставка за пределы России?\nПока доставка работает только по России. В будущем планируем расширение.\n",
        "18. Где я смогу увидеть результаты других и поделиться своими?\nТебя ждёт продолжение путешествие в закрытом чате (здесь нужна ссылка на чат), где в бессрочном доступе будут поддерживающая атмосфера, эфиры от мастеров и возможность делиться своими успехами и вдохновляться результатами близких по духу людей.\n",
        "19. Что делать, если у меня остались вопросы?\nНапишите в Telegram: @anbolshakowa и @dmitrieva_live, мы ответим вам с 10:00 до 20:00 (gmt+3) в рабочие дни с понедельника по пятницу.\n",
    ]
    faq_text += "\n\n".join(faq_items)

    await edit_or_send(
        cb.message,
        faq_text,
        create_inline_keyboard([
            [{"text": "Назад к товару", "callback_data": CallbackData.GALLERY.value}]
        ])
    )
    await cb.answer()


@r.callback_query(F.data == CallbackData.TEAM.value)
async def cb_team(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        sess.refresh(user)
        if not user:
            await cb.answer("Ошибка", show_alert=True)
            return

        if user.team_viewed:
            await cb.message.answer(
                "Ты уже знаком с командой коробочки - смотри кружочки выше!",
                reply_markup=kb_gallery()
            )
            await cb.answer()
            return

        await cb.message.answer("Знакомься с командой коробочки!")

        experts_order = ["anna", "maria", "alena", "alexey", "alexander"]
        for key in experts_order:
            info = Config.EXPERTS[key]
            name = info["name"]
            video_id = info.get("video_note_id")
            if video_id:
                try:
                    await cb.message.answer_video_note(video_id)
                except Exception as e:
                    logger.error(f"Team video error ({key}): {e}")
                    await cb.message.answer("Ошибка загрузки видео")
            await cb.message.answer(f"<b>{name}</b>", parse_mode=ParseMode.HTML)
            await asyncio.sleep(0.6)
        user.team_viewed = True
        sess.commit()

        await cb.message.answer(
            "Теперь ты знаешь команду, приятно познакомиться!))",
            reply_markup=kb_gallery()
        )
    await cb.answer()

# ========== PRACTICES ==========
@r.callback_query(F.data == CallbackData.PRACTICES.value)
async def cb_practices_list(cb: CallbackQuery):
    logger.info(f"[PRACTICES_LIST] Начало обработки | user_id={cb.from_user.id} | data={cb.data}")

    engine = make_engine(Config.DB_PATH)
    try:
        with Session(engine) as sess:
            user = get_user_by_id(sess, cb.from_user.id)
            if not user:
                logger.error(f"[PRACTICES_LIST] Пользователь не найден | user_id={cb.from_user.id}")
                await cb.answer("Ошибка доступа", show_alert=True)
                return

            logger.info(
                f"[PRACTICES_LIST] Пользователь найден | is_authorized={user.is_authorized} | practices_count={len(user.practices if user.practices else [])}")

            if not user.is_authorized:
                logger.warning(f"[PRACTICES_LIST] Не авторизован → редирект на авторизацию")
                await edit_or_send(
                    cb.message,
                    "Чтобы увидеть практики — авторизуйтесь в личном кабинете.",
                    kb_cabinet_unauth()
                )
                await cb.answer()
                return

            if not user.practices:
                logger.info(f"[PRACTICES_LIST] У пользователя нет практик")
                await edit_or_send(
                    cb.message,
                    "У вас пока нет практик.\nАктивируйте код или закажите коробочку!\nЕсли вы получили коробочку в подарок, просто активируйте код в личном кабинете",
                    kb_empty_practices()
                )
                await cb.answer()
                return

            logger.info(f"[PRACTICES_LIST] Успешно → показываем список из {len(user.practices)} практик")
            await edit_or_send(
                cb.message,
                "Твои практики:",
                kb_practices_list(user.practices)
            )
            await cb.answer()

    except Exception as e:
        logger.exception(
            f"[PRACTICES_LIST] Критическая ошибка при обработке списка практик | user_id={cb.from_user.id}")
        await notify_admin(f"Паника в PRACTICES_LIST!\nUser: {cb.from_user.id}\nError: {e}")
        await cb.answer("Произошла ошибка при загрузке практик 😔\nПопробуйте позже или напишите в поддержку",
                        show_alert=True)


@r.callback_query(F.data.startswith("practice:"))
async def cb_single_practice(cb: CallbackQuery):
    logger.info(f"[PRACTICE_SINGLE] Начало | user_id={cb.from_user.id} | callback_data={cb.data}")

    engine = make_engine(Config.DB_PATH)
    try:
        with Session(engine) as sess:
            user = get_user_by_id(sess, cb.from_user.id)
            if not user:
                logger.error(f"[PRACTICE_SINGLE] Пользователь не найден | user_id={cb.from_user.id}")
                await cb.answer("Ошибка доступа", show_alert=True)
                return

            logger.info(f"[PRACTICE_SINGLE] Пользователь найден | authorized={user.is_authorized}")

            data = cb.data
            if not data.startswith("practice:"):
                await cb.answer("Неверный формат команды", show_alert=True)
                return

            parts = data.split(":", 2)  # делим максимум на 3 части
            # Возможные варианты:
            # "practice:5"          → ["practice", "5"]          → просмотр карточки
            # "practice:play:3"     → ["practice", "play", "3"]  → запуск практики

            if len(parts) == 2:
                # Просто номер → открываем карточку
                action = None
                idx_str = parts[1]
            elif len(parts) == 3 and parts[1] == "play":
                # play + номер → запускаем
                action = "play"
                idx_str = parts[2]
            else:
                logger.warning(f"[PRACTICE_SINGLE] Некорректный callback_data: {data}")
                await cb.answer("Ошибка формата команды", show_alert=True)
                return

            if not idx_str.isdigit():
                logger.warning(f"[PRACTICE_SINGLE] Номер практики не число: {idx_str}")
                await cb.answer("Ошибка формата команды", show_alert=True)
                return

            idx = int(idx_str)
            logger.info(f"[PRACTICE_SINGLE] Запрошена практика №{idx} | action={action}")
            if not (0 <= idx < len(user.practices)):
                logger.warning(f"Неверный idx практики: {idx} для user {user.telegram_id}")
                await cb.answer("Практика не найдена", show_alert=True)
                return

            if not (user.is_authorized and 0 <= idx < len(user.practices)):
                logger.warning(
                    f"[PRACTICE_SINGLE] Доступ запрещён | authorized={user.is_authorized} | idx={idx} | practices_len={len(user.practices)}")
                await cb.answer("Доступ ограничен", show_alert=True)
                return

            title = user.practices[idx]
            logger.info(f"[PRACTICE_SINGLE] Практика: {title} (idx={idx}) | action={action}")

            if action == "play":
                # 1. Вступительное видео/кружочек
                note_id = Config.PRACTICE_NOTES.get(idx)
                if note_id:
                    try:
                        logger.info(f"[PRACTICE_SINGLE] Отправляем вступительное видео_note {note_id}")
                        await cb.message.answer_video_note(note_id)
                    except Exception as e:
                        logger.error(f"[PRACTICE_SINGLE] Ошибка вступительного видео_note {idx}: {e}")

                # 2. Описание
                try:
                    await send_practice_intro(cb.message, idx, title)
                except Exception as e:
                    logger.error(f"[PRACTICE_SINGLE] Ошибка отправки описания практики {idx}: {e}")

                # 3. Основное видео с кнопкой назад
                video_id = None
                if idx < len(Config.PRACTICE_VIDEO_IDS):
                    video_id = Config.PRACTICE_VIDEO_IDS[idx]
                if video_id:
                    try:
                        logger.info(f"[PRACTICE_SINGLE] Отправляем основное видео_note {video_id}")
                        await cb.message.answer_video_note(video_id)
                        await cb.message.answer("Практика запущена ↓", reply_markup=kb_back_to_practices())
                    except Exception as e:
                        logger.error(f"[PRACTICE_SINGLE] Ошибка основного видео {idx}: {e}")

                # 4. Бонус-аудио с кнопкой назад
                bonus_audio = None
                if idx < len(Config.PRACTICE_BONUS_AUDIO):
                    bonus_audio = Config.PRACTICE_BONUS_AUDIO[idx]
                if bonus_audio:
                    try:
                        logger.info(f"[PRACTICE_SINGLE] Отправляем бонус-аудио для {idx}")
                        await cb.message.answer_audio(
                            audio=bonus_audio,
                            title=f"{title} — Бонус",
                            performer=Config.PRACTICE_PERFORMERS[idx],
                            duration=300,
                            reply_markup=kb_back_to_practices()
                        )
                        await asyncio.sleep(1.5)
                    except Exception as e:
                        logger.error(f"[PRACTICE_SINGLE] Ошибка бонус-аудио {idx}: {e}")

                # 5. Основное аудио с кнопкой назад
                audio_id = None
                if idx < len(Config.PRACTICE_AUDIO_IDS):
                    audio_id = Config.PRACTICE_AUDIO_IDS[idx]
                if audio_id:
                    try:
                        duration_minutes = Config.PRACTICE_DETAILS[idx]["duration"]
                        logger.info(
                            f"[PRACTICE_SINGLE] Отправляем основное аудио {audio_id} (длительность ~{duration_minutes} мин)")
                        await cb.message.answer_audio(
                            audio=audio_id,
                            title=title,
                            performer=Config.PRACTICE_PERFORMERS[idx],
                            duration=duration_minutes * 60,
                            reply_markup=kb_back_to_practices()
                        )
                    except Exception as e:
                        logger.error(f"[PRACTICE_SINGLE] Ошибка основного аудио {idx}: {e}")
                        await cb.message.answer("Не удалось загрузить основное аудио 😔")

                # Завершение (как раньше, но можно добавить кнопку если нужно)
                try:
                    await cb.message.answer(
                        "Практика завершена! ✨\n\nХочешь повторить или перейти к следующей?",
                        reply_markup=kb_practices_list(user.practices)
                    )
                except Exception as e:
                    logger.error(f"[PRACTICE_SINGLE] Ошибка финального сообщения: {e}")

                await cb.answer("Практика началась!")

            else:  # Просто открытие карточки
                logger.info(f"[PRACTICE_SINGLE] Открываем карточку практики {idx}")
                try:
                    await send_practice_intro(cb.message, idx, title)
                    await cb.message.answer(
                        f"<b>{title}</b>\n\nГотовы приступить к практике?",
                        reply_markup=kb_practice_card(idx)
                    )
                except Exception as e:
                    logger.error(f"[PRACTICE_SINGLE] Ошибка при показе карточки {idx}: {e}")
                    await cb.message.answer("Не удалось показать описание практики 😔")

            await cb.answer()

    except Exception as e:
        logger.exception(f"[PRACTICE_SINGLE] Критическая ошибка обработки практики | user_id={cb.from_user.id} | data={cb.data}")
        await notify_admin(f"Паника в PRACTICE_SINGLE!\nUser: {cb.from_user.id}\nData: {cb.data}\nError: {e}")
        await cb.answer("Произошла ошибка при работе с практикой 😔\nПопробуйте позже", show_alert=True)


# ========== REDEEM ==========
@r.callback_query(F.data == CallbackData.REDEEM_START.value)
async def cb_redeem_start(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)

    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        if not user.is_authorized:
            await cb.message.answer("Сначала авторизуйтесь.", reply_markup=kb_cabinet_unauth())
            await cb.answer()
            return

        # Самое важное — меняем и коммитим ВНУТРИ with-блока
        user.awaiting_redeem_code = True
        logger.info(f"Пользователь {user.telegram_id} начал ввод кода → awaiting_redeem_code = True")

        sess.commit()  # ← сохраняем немедленно

        # Дополнительная защита — ждём чуть-чуть, чтобы диск точно успел
        await asyncio.sleep(0.3)  # ← 300 мс обычно хватает даже на Pi

        # Только после успешного сохранения отправляем сообщение
        await cb.message.answer(
            "Введите <b>код с карточки</b> (несколько волшебных цифр):",
            reply_markup=create_inline_keyboard([
                [{"text": "Отменить", "callback_data": "redeem:cancel"}],
                [{"text": "Назад в кабинет", "callback_data": CallbackData.CABINET.value}]
            ])
        )

    await cb.answer()


@r.callback_query(F.data == "redeem:cancel")
async def cb_redeem_cancel(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if user:
            user.awaiting_redeem_code = False
            sess.commit()
    await cb.message.edit_text("Ввод кода отменён.", reply_markup=kb_cabinet())
    await cb.answer()

# ========== CHECKOUT ==========
@r.callback_query(F.data == CallbackData.CHECKOUT_START.value)
async def cb_checkout_start(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        # Abandon any unfinished
        orders = get_user_orders_db(sess, cb.from_user.id)
        for o in orders:
            if o.status == OrderStatus.NEW.value:
                o = sess.merge(o)
                o.status = OrderStatus.ABANDONED.value
        sess.commit()
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        user.pvz_for_order_id = None
        user.temp_selected_pvz = None
        user.temp_pvz_list = None
        user.awaiting_gift_message = False
        user.awaiting_auth = False

        sess.commit()

        if user.is_authorized:
            await cb.message.answer(
                f"Проверьте данные:\n• Имя и фамилия: {user.full_name}\n• Телефон: {user.phone}\n• Email: {user.email}\n\nХотите изменить?",
                reply_markup=kb_change_contact(CallbackData.MENU.value)
            )
        else:
            await cb.message.answer(
                "❗ Вы не авторизованы.\n\n"
                "Пожалуйста, пройдите авторизацию в личном кабинете, "
                "чтобы оформить заказ.",
                reply_markup=kb_cabinet_unauth()
            )
    await cb.answer()


@r.callback_query(F.data.startswith("change_contact:"))
async def cb_change_contact(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return
        # Определяем, куда вести кнопку "Назад"
        back_callback = (
            CallbackData.MENU.value
            if user.is_authorized
            else CallbackData.GALLERY.value
        )
        if cb.data == CallbackData.CHANGE_CONTACT_YES.value:
            # ДОБАВИТЬ ЗДЕСЬ: Установка флага awaiting_auth
            user.awaiting_auth = True
            sess.commit()  # Сохраняем немедленно
            await cb.message.answer(
                "Введите новые данные:\nИмя Фамилия\n+7XXXXXXXXXX\nemail@example.com",
                reply_markup=create_inline_keyboard([[
                    {"text": "Назад", "callback_data": back_callback}
                ]])
            )
        else:  # "Нет" → продолжаем оформление заказа
            user.awaiting_pvz_address = True
            sess.add(user)
            sess.commit()
            await cb.message.answer(
                "Введите адрес или код ПВЗ (строго в формате «Москва, ул. Барклая, 15» или «MSK126»):",
                reply_markup=create_inline_keyboard([[
                    {"text": "Назад", "callback_data": back_callback}
                ]])
            )
    await cb.answer()


# ========== УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК "НАЗАД" И ПРОСТЫХ НАВИГАЦИОННЫХ КНОПОК ==========
@r.callback_query(F.data.in_(["menu", "gallery", "cabinet", "faq", "team", "practices", "orders"]))
async def cb_simple_navigation(cb: CallbackQuery):
    data = cb.data
    try:
        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            user = get_user_by_id(sess, cb.from_user.id)
            if user:
                reset_states(user, sess)
        if data == "menu":
            await edit_or_send(cb.message, "Выбери действие:", kb_main())
        elif data == "gallery":
            await cb_gallery(cb)
        elif data == "cabinet":
            await cb_cabinet(cb)
        elif data == "faq":
            await cb_faq(cb)
        elif data == "team":
            await cb_team(cb)
        elif data == "orders":
            await cb_orders_list(cb)
    except Exception as e:
        logger.error(f"Navigation error for {data}: {e}")
        await cb.answer("Ошибка навигации. Попробуйте заново.", show_alert=True)
    await cb.answer()


@r.callback_query(F.data == CallbackData.SHIP_CDEK.value)
async def cb_shipping_cdek(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        if not user.is_authorized:
            await cb.message.answer("Сначала авторизуйтесь.", reply_markup=kb_cabinet_unauth())
            await cb.answer()
            return

        user.pvz_for_order_id = None
        user.awaiting_pvz_address = True
        sess.add(user)
        sess.commit()

    await cb.message.answer(
        "Введите адрес или код ПВЗ в формате «Москва, ул. Профсоюзная,83» или «MSK89»:",
        reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.GALLERY.value}]])
    )
    await cb.answer()

async def show_review(msg: Message, order: Order):
    await edit_or_send(msg, format_order_review(order), kb_review(order))

# ========== PAYMENT ==========
@r.callback_query(F.data.startswith("pay:"))
async def cb_pay(cb: CallbackQuery):
    parts = cb.data.split(":")
    if len(parts) != 3:
        await cb.answer("Ошибка", show_alert=True)
        return

    kind = parts[1]
    try:
        order_id = int(parts[2])
    except:
        await cb.answer("Неверный ID заказа", show_alert=True)
        return

    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if not order or order.user_id != cb.from_user.id:
            await cb.answer("Заказ не найден", show_alert=True)
            return

        # В cb_pay, после получения order
        if order.status not in (OrderStatus.NEW.value, OrderStatus.PAID_PARTIALLY.value) and \
                not (kind == "rem" and order.status == OrderStatus.ASSEMBLED.value and order.payment_kind == "pre"):
            await cb.answer("Оплата уже завершена или невозможна", show_alert=True)
            return

    await send_payment_keyboard(cb.message, order, kind=kind)
    await cb.answer()


# ========== ORDER STATUS ==========
@r.callback_query(F.data.startswith("order:"))
async def cb_order_status(cb: CallbackQuery):
    try:
        oid = int(cb.data.split(":")[1])
        order = get_order_by_id(oid, cb.from_user.id)
        if not order or order.user_id != cb.from_user.id:
            await cb.answer("Заказ не найден", show_alert=True)
            return

        text = format_client_order_info(order)

        await edit_or_send(
            cb.message,
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=kb_order_status(order)
        )
        await cb.answer()
    except Exception as e:
        logger.error(f"Order status error: {e}")
        await cb.answer("Ошибка", show_alert=True)


@r.callback_query(F.data == CallbackData.ORDERS.value)
async def cb_orders_list(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        if not user.is_authorized:
            await edit_or_send(cb.message, "Пожалуйста, авторизуйтесь.", kb_cabinet_unauth())
            await cb.answer()
            return

        orders = get_user_orders_db(sess, cb.from_user.id)
        ids = [o.id for o in orders]

    if not ids:
        await edit_or_send(
            cb.message,
            "У вас пока нет заказов.",
            create_inline_keyboard([
                [{"text": "Оформить заказ", "callback_data": CallbackData.CHECKOUT_START.value}],
                [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
            ])
        )
    else:
        await edit_or_send(cb.message, "Ваши заказы:", kb_orders_list(ids))

    await cb.answer()

@r.callback_query(F.data.startswith("change_addr:"))
async def cb_change_addr(cb: CallbackQuery):
    oid = None
    try:
        oid = int(cb.data.split(":")[1])

        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            user = get_user_by_id(sess, cb.from_user.id)
            if not user:
                await cb.answer("Ошибка доступа", show_alert=True)
                return

            order = sess.get(Order, oid)
            if not order or order.user_id != cb.from_user.id:
                await cb.answer("Заказ не найден", show_alert=True)
                return
            user.pvz_for_order_id = oid
            sess.commit()

        await cb.message.answer(
            "Введите новый адрес ПВЗ (Строго в формате «Екатеринбург, Профсоюзная, 93»):",
            reply_markup=create_inline_keyboard([
                [{"text": "Статус заказа", "callback_data": f"order:{oid}"}]
            ])
        )
        await cb.answer()
    except Exception as e:
        logger.error(f"Change addr error: {e}")
        await notify_admin(f"❌ Ошибка изменения адреса заказа #{oid}")
        await cb.answer("Ошибка", show_alert=True)



# ========== ADMIN PANEL ==========
@r.callback_query(F.data == CallbackData.ADMIN_PANEL.value)
async def cb_admin_panel(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID and f"@{cb.from_user.username or ''}" not in ADMIN_USERNAMES:
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    logger.info(f"Admin panel callback: user_id={cb.from_user.id}, data={cb.data}")
    if not await is_admin(cb):
        logger.info("Admin access denied")
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    await edit_or_send(cb.message, "Панель администратора:", kb_admin_panel())
    await cb.answer()


@r.callback_query(F.data == CallbackData.ADMIN_ORDERS_PREPAID.value)
async def cb_admin_orders_prepaid(cb: CallbackQuery):
    logger.info(f"Orders prepaid callback: user_id={cb.from_user.id}, data={cb.data}")
    if not await is_admin(cb):
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    # Заказы для сборки: PAID_PARTIALLY или PAID_FULL
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        stmt = select(Order).where(Order.status.in_([OrderStatus.PAID_PARTIALLY.value, OrderStatus.PAID_FULL.value]))
        orders = list(sess.scalars(stmt).all())
    if not orders:
        await edit_or_send(cb.message, "Нет заказов для сборки.", kb_admin_panel())
    else:
        await edit_or_send(cb.message, "Заказы для сборки:", kb_admin_orders(orders))
    await cb.answer()


@r.callback_query(F.data == CallbackData.ADMIN_ORDERS_READY.value)
async def cb_admin_orders_ready(cb: CallbackQuery):
    logger.info(f"Orders ready callback: user_id={cb.from_user.id}, data={cb.data}")
    if not await is_admin(cb):
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    # Ожидающие дооплаты: ASSEMBLED и payment_kind == "pre" (PAID_PARTIALLY)
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        stmt = select(Order).where(
            Order.status == OrderStatus.ASSEMBLED.value,
            Order.payment_kind == "pre"
        )
        orders = list(sess.scalars(stmt).all())
    if not orders:
        await edit_or_send(cb.message, "Нет заказов, ожидающих дооплаты.", kb_admin_panel())
    else:
        await edit_or_send(cb.message, "Заказы, ожидающие дооплаты:", kb_admin_orders(orders))
    await cb.answer()


@r.callback_query(F.data == CallbackData.ADMIN_ORDERS_SHIPPED.value)
async def cb_admin_orders_shipped(cb: CallbackQuery):
    logger.info(f"Orders shipped callback: user_id={cb.from_user.id}, data={cb.data}")
    if not await is_admin(cb):
        logger.info("Admin access denied")
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    orders = get_all_orders_by_status(OrderStatus.SHIPPED.value)
    if not orders:
        await edit_or_send(cb.message, "Нет отправленных заказов.", kb_admin_panel())
    else:
        await edit_or_send(cb.message, "Отправленные заказы:", kb_admin_orders(orders))
    await cb.answer()


@r.callback_query(F.data == CallbackData.ADMIN_ORDERS_TO_SHIP.value)
async def cb_admin_orders_to_ship(cb: CallbackQuery):
    logger.info(f"Orders to ship callback: user_id={cb.from_user.id}, data={cb.data}")
    if not await is_admin(cb):
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    # Заказы ASSEMBLED и PAID_FULL
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        stmt = select(Order).where(
            Order.status == OrderStatus.ASSEMBLED.value,
            Order.payment_kind.in_(['full', 'remainder'])  # full or after rem
        )
        orders = list(sess.scalars(stmt).all())
    if not orders:
        await edit_or_send(cb.message, "Нет заказов готовых к отправке.", kb_admin_panel())
    else:
        await edit_or_send(cb.message, "Заказы готовые к отправке:", kb_admin_orders(orders))
    await cb.answer()


@r.callback_query(F.data == CallbackData.ADMIN_ORDERS_ARCHIVED.value)
async def cb_admin_orders_archived(cb: CallbackQuery):
    logger.info(f"Orders archived callback: user_id={cb.from_user.id}, data={cb.data}")
    if not await is_admin(cb):
        logger.info("Admin access denied")
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    orders = get_all_orders_by_status(OrderStatus.ARCHIVED.value)
    if not orders:
        await edit_or_send(cb.message, "Архив пуст.", kb_admin_panel())
    else:
        await edit_or_send(cb.message, "Архив заказов:", kb_admin_orders(orders))
    await cb.answer()

@r.callback_query(F.data.startswith("admin:order:"))
async def cb_admin_order_details(cb: CallbackQuery):
    logger.info(f"Order details callback: user_id={cb.from_user.id}, data={cb.data}")
    oid = None
    try:
        oid = int(cb.data.split(":")[2])

        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            order = sess.get(Order, oid)
            if not order:
                await cb.answer("Заказ не найден", show_alert=True)
                return
            # FIX: Load user relationship
            sess.expunge(order)  # Detach to avoid issues
            order = sess.merge(order, load=True)
            sess.refresh(order)
            if order.user:
                sess.refresh(order.user)  # Load user

        if not await is_admin(cb):
            await cb.answer("Доступ запрещён", show_alert=True)
            return

        text = format_order_admin(order)
        await edit_or_send(cb.message, text, kb_admin_order_actions(order))
        await cb.answer()
    except Exception as e:
        logger.error(f"Admin order details error: {e}")
        await notify_admin(f"❌ Ошибка просмотра заказа #{oid if 'oid' in locals() else 'неизвестный'}")
        await cb.answer("Ошибка просмотра заказа", show_alert=True)


@r.callback_query(F.data.startswith(CallbackData.ADMIN_SET_ASSEMBLED.value))
async def cb_admin_set_assembled(cb: CallbackQuery):
    logger.info(f"Set assembled callback: user_id={cb.from_user.id}, data={cb.data}")
    oid = None
    try:
        oid = int(cb.data.split(":")[2])

        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            order = sess.get(Order, oid)
            if not order or order.status not in [OrderStatus.PAID_PARTIALLY.value, OrderStatus.PAID_FULL.value]:
                await cb.answer("Нельзя собрать этот заказ", show_alert=True)
                return

            if not await is_admin(cb):
                await cb.answer("Доступ запрещён", show_alert=True)
                return

            order.status = OrderStatus.ASSEMBLED.value
            sess.commit()

        # Уведомление клиенту о сборке
        await notify_client_order_assembled(oid, cb.message)  # Переименуй функцию на notify_client_order_assembled если хочешь
        await edit_or_send(cb.message, f"Заказ #{oid} собран.", kb_admin_panel())
        await cb.answer()

    except Exception as e:
        logger.error(f"Admin set assembled error: {e}")
        await notify_admin(f"❌ Ошибка сборки заказа #{oid if 'oid' in locals() else 'неизвестный'}")
        await cb.answer("Ошибка", show_alert=True)


@r.callback_query(F.data.startswith(CallbackData.ADMIN_SET_SHIPPED.value))
async def cb_admin_set_shipped(cb: CallbackQuery):
    logger.info(f"Set shipped callback: user_id={cb.from_user.id}, data={cb.data}")
    oid = None
    try:
        oid = int(cb.data.split(":")[2])

        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            order = sess.get(Order, oid)
            if not order or order.status != OrderStatus.ASSEMBLED.value or order.payment_kind not in ["full", "remainder"]:
                await cb.answer("Нельзя отправить этот заказ", show_alert=True)
                return

            if not await is_admin(cb):
                await cb.answer("Доступ запрещён", show_alert=True)
                return

            # Создаём CDEK
            success = await create_cdek_order(oid)
            if not success:
                await cb.answer("Ошибка создания заказа в CDEK", show_alert=True)
                return

            # Reload fresh after create (which sets SHIPPED)
            order = sess.get(Order, oid)

        await notify_client_order_shipped(order.id, cb.message)
        await edit_or_send(cb.message, f"Заказ #{oid} отправлен.", kb_admin_panel())
        await cb.answer()

    except Exception as e:
        logger.error(f"Admin set shipped error: {e}")
        await notify_admin(f"❌ Ошибка отправки заказа #{oid if 'oid' in locals() else 'неизвестный'}")
        await cb.answer("Ошибка", show_alert=True)


@r.callback_query(F.data.startswith(CallbackData.ADMIN_SET_ARCHIVED.value))
async def cb_admin_set_archived(cb: CallbackQuery):
    logger.info(f"Set archived callback: user_id={cb.from_user.id}, data={cb.data}")
    oid = None  # Initialize
    try:
        oid = int(cb.data.split(":")[2])

        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            order = sess.get(Order, oid)
            if not order or order.status != OrderStatus.SHIPPED.value:
                await cb.answer("Нельзя архивировать заказ", show_alert=True)
                return

            if not await is_admin(cb):
                await cb.answer("Доступ запрещён", show_alert=True)
                return

            order.status = OrderStatus.ARCHIVED.value
            sess.commit()

        # Передаём только ID, а не detached объект
        await notify_admins_order_archived(oid)   # ← изменили на oid

        await edit_or_send(cb.message, f"Заказ #{oid} заархивирован.", kb_admin_panel())
        await cb.answer()
    except Exception as e:
        logger.error(f"Admin set archived error: {e}")
        await notify_admin(f"❌ Ошибка архивирования заказа #{oid if oid else 'неизвестный'}")
        await cb.answer("Ошибка", show_alert=True)


@r.callback_query(F.data.startswith(CallbackData.ADMIN_SET_TRACK.value))
async def cb_admin_set_track(cb: CallbackQuery):
    if not await is_admin(cb):
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    try:
        oid = int(cb.data.split(":")[2])
        order = get_order_admin(oid)
        if not order:
            await cb.answer("Заказ не найден")
            return
        # Сохраняем, что админ ждёт трек для этого заказа
        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            user = get_user_by_id(sess, cb.from_user.id)
            if not user:
                await cb.answer("Ошибка доступа", show_alert=True)
                return
        user.awaiting_manual_track = True
        user.temp_order_id_for_track = oid
        sess.commit()
        await cb.message.answer(
            f"Введите трек-номер для заказа #{oid}:",
            reply_markup=create_inline_keyboard(
                [[{"text": "Отмена", "callback_data": CallbackData.ADMIN_PANEL.value}]])
        )
        await cb.answer()
    except Exception as e:
        logger.error(f"Set track error: {e}")
        await cb.answer("Ошибка")


@r.callback_query(F.data == "pvz_reenter")
async def cb_pvz_reenter(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        user.awaiting_pvz_address = True
        user.temp_pvz_list = None
        user.temp_selected_pvz = None
        sess.commit()

    await cb.message.edit_text(
        "Введите адрес ПВЗ ещё раз (Строга в формате: Москва, пр. 6-й Рощинский, 1с4):",
        reply_markup=create_inline_keyboard([
            [{"text": "Отмена", "callback_data": CallbackData.MENU.value}]
        ])
    )
    await cb.answer()


@r.callback_query(F.data == "pvz_backlist")
async def cb_pvz_backlist(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

    if not user.temp_pvz_list:
        await cb.answer("Список устарел, введите адрес заново", show_alert=True)
        return

    await edit_or_send(
        cb.message,
        "Выбери нужный ПВЗ:",
        kb_pvz_list(user.temp_pvz_list)
    )
    await cb.answer()


@r.callback_query(lambda c: (c.data or "").startswith("pvz_sel:"))
async def cb_pvz_select(cb: CallbackQuery):
    try:
        parts = (cb.data or "").split(":")
        if len(parts) != 3:
            await cb.answer("Ошибка выбора ПВЗ", show_alert=True)
            return
        _, old_code, idx_str = parts
        idx = int(idx_str)
    except (ValueError, IndexError):
        await cb.answer("Ошибка выбора ПВЗ - попробуйте заново", show_alert=True)
        return

    engine = make_engine(Config.DB_PATH)

    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        if not user.temp_pvz_list or not (0 <= idx < len(user.temp_pvz_list)):
            await cb.answer("Список ПВЗ устарел - введите адрес заново", show_alert=True)
            return

        pvz = user.temp_pvz_list[idx]

        current_code = pvz.get("code")
        if str(current_code) != str(old_code):
            await cb.answer("Эта кнопка устарела — выберите ПВЗ заново", show_alert=True)
            return

        if user.pvz_for_order_id is not None:
            await cb.answer("ПВЗ уже выбран. Продолжайте оформление.", show_alert=True)
            return

        raw_code = pvz.get("code")
        if isinstance(raw_code, str):
            # Поддерживаем любой региональный префикс: MSK, YAR, KZN, NN, SPB, EKB и т.д.
            prefix_match = re.match(r'^([A-Z]{2,5})(\d+)', raw_code.upper())
            if prefix_match:
                real_code = int(prefix_match.group(2))
            else:
                # Если без префикса — считаем весь код числом
                try:
                    real_code = int(raw_code)
                except ValueError:
                    await cb.answer("Ошибка формата кода ПВЗ", show_alert=True)
                    return
        elif isinstance(raw_code, int):
            real_code = raw_code
        else:
            await cb.answer("Некорректный код ПВЗ от СДЭК", show_alert=True)
            return

        city_code = pvz.get("location", {}).get("code") or Config.CDEK_FROM_CITY_CODE
        city_code = str(city_code)

        full_address = pvz["location"]["address_full"]
        work_time = pvz.get("work_time") or "Пн–Пт 10:00–20:00, Сб–Вс 10:00–18:00"

        user.temp_selected_pvz = {
            "code": real_code,
            "city_code": city_code,
            "address": full_address,
            "work_time": work_time
        }

        await cb.message.answer("Считаю стоимость доставки…")

        delivery_info = await calculate_cdek_delivery_cost(city_code)

        delivery_cost = delivery_info["cost"] if delivery_info else 590
        period_text = "3–7"
        if delivery_info:
            mn = delivery_info["period_min"]
            mx = delivery_info["period_max"] or mn + 2
            period_text = f"{mn}" if mn == mx else f"{mn}–{mx}"

        total = Config.PRICE_RUB + delivery_cost
        prepay = (total * Config.PREPAY_PERCENT + 99) // 100

        order = create_order_db(
            sess,
            user_id=cb.from_user.id,
            product_id=1,
            status=OrderStatus.NEW.value,
            shipping_method="cdek_pvz",
            address=full_address,
            total_price_kop=(total * 100),
            delivery_cost_kop=(delivery_cost * 100),
            extra_data={
                "pvz_code": real_code,
                "city_code": city_code,
                "delivery_cost": delivery_cost,
                "delivery_period": period_text,
            }
        )

        order_id = order.id

        user.pvz_for_order_id = order_id
        user.awaiting_gift_message = False
        user.temp_gift_order_id = order_id

        sess.commit()

        user.awaiting_pvz_address = False
        user.temp_pvz_list = None
        user.temp_selected_pvz = None

    # UI outside
    await edit_or_send(
        cb.message,
        f"<b>ПВЗ сохранён!</b>\n\n"
        f"{full_address}\n"
        f"Время работы пункта: {work_time}\n\n"
        f"Доставка: <b>{delivery_cost} ₽</b>\n"
        f"Срок: <b>≈ {period_text} дн.</b>\n\n"
        f"<b>Итого: {total} ₽</b>"
    )

    await cb.answer("Готово!")

    # Gift question
    await cb.message.answer(
        "Хотите добавить личное послание в подарок получателю?\n"
        "(Текст будет вложен в коробочку)",
        reply_markup=create_inline_keyboard([
            [{"text": "Да, добавить", "callback_data": "gift:yes"}],
            [{"text": "Нет, без послания", "callback_data": "gift:no"}],
        ])
    )


@r.callback_query(F.data == "gift:yes")
async def cb_gift_yes(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)

    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        orders = get_user_orders_db(sess, cb.from_user.id)
        order = next((o for o in reversed(orders or []) if o.status == OrderStatus.NEW.value), None)
        if not order:
            await cb.answer("Нет активного заказа", show_alert=True)
            return

        order = sess.merge(order)

        if not order or order.status != OrderStatus.NEW.value:
            await cb.answer("Заказ устарел. Начните оформление заново.", show_alert=True)
            return

        user.awaiting_pvz_address = False
        user.awaiting_manual_pvz = False

        # Устанавливаем новый флаг
        user.awaiting_gift_message = True

        sess.commit()

    await cb.message.edit_text(
        "✍️ Напишите текст послания (до 300 символов):",
        reply_markup=create_inline_keyboard([[{"text": "Отмена", "callback_data": "gift:cancel"}]])
    )
    await cb.answer()
    

@r.callback_query(F.data == "gift:no")
async def cb_gift_no(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)

    order_id = None

    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        orders = get_user_orders_db(sess, cb.from_user.id)
        order = next((o for o in reversed(orders or []) if o.status == OrderStatus.NEW.value), None)

        if order:
            order = sess.merge(order)
            if order.extra_data is None:
                order.extra_data = {}
            if "gift_message" not in order.extra_data:
                order.extra_data["gift_message"] = "Без послания"
            flag_modified(order, "extra_data")

            order_id = order.id

        user.awaiting_gift_message = False
        sess.commit()

    await cb.message.answer("Ок, без послания. Переходим к оплате...")

    if order_id:
        await send_payment_keyboard(cb.message, order_or_id=order_id, kind=None)
    else:
        await cb.message.answer(
            "Заказ не найден. Начните оформление заново.",
            reply_markup=kb_main()
        )

    await cb.answer()


async def send_payment_keyboard(msg: Message, order_or_id: Order | int, kind: str | None = None):
    engine = make_engine(Config.DB_PATH)

    with Session(engine) as sess:
        # Приводим к объекту Order в любом случае
        if isinstance(order_or_id, int):
            order = sess.get(Order, order_or_id)
            if not order:
                await msg.answer("Заказ не найден. Попробуйте начать заново.")
                return
        else:
            order = order_or_id   # уже объект

        # Теперь order — всегда объект Order
        order_id = order.id

        if not order:
            await msg.answer("Заказ не найден. Попробуйте начать заново.")
            return

        total_rub = order.total_price_kop // 100
        prepay_rub = (total_rub * Config.PREPAY_PERCENT + 99) // 100
        remainder_rub = total_rub - prepay_rub

        # 1. Устанавливаем статус PENDING_PAYMENT (если нужно)
        if order.status != OrderStatus.PENDING_PAYMENT.value:
            order.status = OrderStatus.PENDING_PAYMENT.value
            if order.extra_data is None:
                order.extra_data = {}
            if "pending_payments" not in order.extra_data:
                order.extra_data["pending_payments"] = {}
            flag_modified(order, "extra_data")

        if order.status == OrderStatus.ABANDONED.value:
            await msg.answer(
                "Этот черновик заказа был отменён (возможно, из-за предыдущей попытки). Начните оформление заново.",
                reply_markup=kb_main()
            )
            return
        elif order.status not in (OrderStatus.NEW.value, OrderStatus.PENDING_PAYMENT.value):
            await msg.answer(
                f"Заказ #{order.id} находится в статусе '{order.status}' — оплата невозможна.",
                reply_markup=kb_main()
            )
            return

        # Получаем username бота ОДИН РАЗ внутри сессии
        bot_info = await bot.get_me()
        base_return_url = f"https://t.me/{bot_info.username}?start=payment_success&order_id={order.id}"

        buttons = []
        text_lines = []

        # ───────────────────────────────────────────────
        # Случай 1: kind is None → показываем обе кнопки
        # ───────────────────────────────────────────────
        if kind is None:
            # Полная оплата
            full_payment = await create_yookassa_payment(
                order=order,
                amount_rub=total_rub,
                description=f"Полная оплата заказа #{order.id} — Коробочка «Отпусти тревогу»",
                return_url=f"{base_return_url}&kind=full",
                kind="full"
            )
            if full_payment and full_payment["confirmation_url"]:
                buttons.append([{
                    "text": f"Оплатить 100% ({total_rub} ₽)",
                    "url": full_payment["confirmation_url"]
                }])
                order.extra_data["pending_payments"]["full"] = full_payment["payment_id"]
                flag_modified(order, "extra_data")

            # Предоплата
            pre_payment = await create_yookassa_payment(
                order=order,
                amount_rub=prepay_rub,
                description=f"Предоплата 30% заказа #{order.id} — Коробочка «Отпусти тревогу»",
                return_url=f"{base_return_url}&kind=pre",
                kind="pre"
            )
            if pre_payment and pre_payment["confirmation_url"]:
                buttons.append([{
                    "text": f"Предоплата 30% ({prepay_rub} ₽)",
                    "url": pre_payment["confirmation_url"]
                }])
                order.extra_data["pending_payments"]["pre"] = pre_payment["payment_id"]
                flag_modified(order, "extra_data")

            text_lines = [
                f"<b>Оплата заказа #{order.id}</b>\n",
                f"Итого: <b>{total_rub} ₽</b>",
                f"• Предоплата 30% = {prepay_rub} ₽",
                f"• Остаток = {remainder_rub} ₽\n",
                "Выберите способ оплаты ↓"
            ]

        # ───────────────────────────────────────────────
        # Случай 2: конкретный kind
        # ───────────────────────────────────────────────
        else:
            amount_rub = 0
            button_text = ""
            description = ""

            if kind == "full":
                amount_rub = total_rub
                button_text = f"Оплатить 100% ({amount_rub} ₽)"
                description = f"Полная оплата заказа #{order.id} — Коробочка «Отпусти тревогу»"
            elif kind == "pre":
                amount_rub = prepay_rub
                button_text = f"Предоплата 30% ({amount_rub} ₽)"
                description = f"Предоплата 30% заказа #{order.id} — Коробочка «Отпусти тревогу»"
            elif kind == "rem":
                amount_rub = remainder_rub
                button_text = f"Оплатить остаток ({amount_rub} ₽)"
                description = f"Дооплата заказа #{order.id} — Коробочка «Отпусти тревогу»"
            else:
                await msg.answer("Ошибка: неизвестный тип оплаты.")
                return

            payment = await create_yookassa_payment(
                order=order,
                amount_rub=amount_rub,
                description=description,
                return_url=f"{base_return_url}&kind={kind}",
                kind=kind
            )

            if payment and payment["confirmation_url"]:
                buttons.append([{
                    "text": button_text,
                    "url": payment["confirmation_url"]
                }])
                order.extra_data["pending_payments"][kind] = payment["payment_id"]
                flag_modified(order, "extra_data")

            text_lines = [
                f"<b>Оплата заказа #{order.id}</b>\n",
                f"К оплате: <b>{amount_rub} ₽</b>",
                f"После оплаты вернитесь в бот — статус обновится автоматически."
            ]

        # Общая кнопка "В меню"
        buttons.append([{"text": "В меню", "callback_data": CallbackData.MENU.value}])

        sess.commit()  # финальный коммит всех изменений

        user = get_user_by_id(sess, msg.chat.id)
        if user:
            reset_states(user, sess)
            logger.info(f"Состояния пользователя {user.telegram_id} сброшены перед показом клавиатуры оплаты заказа #{order.id}")

    # Отправка сообщения уже вне сессии
    text = "\n".join(text_lines)
    await msg.answer(
        text,
        reply_markup=create_inline_keyboard(buttons),
        parse_mode="HTML",
        disable_web_page_preview=True
    )


@r.callback_query(F.data == "gift:cancel")
async def cb_gift_cancel(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)

    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        user.awaiting_gift_message = False
        sess.commit()

    # Возвращаем к выбору
    await cb.message.edit_text(
        "Хотите добавить личное послание в подарок получателю?\n"
        "(Текст будет вложен в коробочку)",
        reply_markup=create_inline_keyboard([
            [{"text": "Да, добавить", "callback_data": "gift:yes"}],
            [{"text": "Нет, без послания", "callback_data": "gift:no"}],
        ])
    )

    await cb.answer("Отменено — выберите снова")



@r.callback_query(F.data == "pvz_manual")
async def cb_pvz_manual(cb: CallbackQuery):
    await cb.message.edit_text(
        "Вероятно, у вас возникли проблемы с адресом ПВЗ СДЭК.\n\n"
        "Обратитесь к @anbolshakowa или @dmitrieva_live - они помогут подобрать подходящий пункт выдачи и сделать заказ.",
        reply_markup=create_inline_keyboard([
            [{"text": "В меню", "callback_data": CallbackData.MENU.value}]
        ])
    )
    await cb.answer()


@r.callback_query(F.data == "pvz_back")
async def cb_pvz_back(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

    pvz_list = user.temp_pvz_list

    if not pvz_list:
        await cb.message.edit_text(
            "Список ПВЗ устарел.\nВведите адрес ПВЗ ещё раз (например: Барклая, 5А):",
            reply_markup=create_inline_keyboard([
                [{"text": "Отмена", "callback_data": CallbackData.MENU.value}]
            ])
        )
        await cb.answer()
        return

    query = user.extra_data.get("pvz_query", "выбранным адресом")

    await edit_or_send(
        cb.message,
        f"Нашёл {len(pvz_list)} ПВЗ рядом с «{query}» (Москва).\nВыбери нужный:",
        kb_pvz_list(pvz_list)
    )
    await cb.answer()


@r.callback_query(F.data == "pvz_confirm")
async def cb_pvz_confirm(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        if not user.temp_selected_pvz:
            await cb.answer("Ошибка выбора", show_alert=True)
            return

        pvz = user.temp_selected_pvz
        real_code = pvz["code"]
        full_address = pvz["address"]
        city_code = pvz.get("city_code", Config.CDEK_FROM_CITY_CODE)

        await cb.message.answer("Считаю стоимость доставки…")
        delivery_info = await calculate_cdek_delivery_cost(city_code)

        delivery_cost = delivery_info["cost"] if delivery_info else 590
        period_text = "3–7"
        if delivery_info:
            mn = delivery_info["period_min"]
            mx = delivery_info["period_max"] or mn + 2
            period_text = f"{mn}" if mn == mx else f"{mn}–{mx}"

        total = Config.PRICE_RUB + delivery_cost
        prepay = (total * Config.PREPAY_PERCENT + 99) // 100

        order = create_order_db(
            sess,
            user_id=cb.from_user.id,
            status=OrderStatus.NEW.value,
            shipping_method="cdek_pvz",
            address=full_address,
            total_price_kop=(total * 100),
            delivery_cost_kop=(delivery_cost * 100),
            extra_data={
                "pvz_code": real_code,
                "city_code": city_code,
                "delivery_cost": delivery_cost,
                "delivery_period": period_text,
            }
        )
        sess.commit()

        user.awaiting_pvz_address = False
        user.temp_pvz_list = None
        user.temp_selected_pvz = None
        reset_states(user, sess)

        await edit_or_send(
            cb.message,
            f"Отлично! ПВЗ сохранён:\n\n"
            f"{full_address}\n"
            f"Режим работы: {pvz.get('work_time', 'не указано')}\n\n"
            f"Доставка: <b>{delivery_cost} ₽</b>\n"
            f"Срок доставки: <b>≈ {period_text} дн.</b>\n"
            f"Итого: <b>{total} ₽</b>\n"
            f"• Предоплата {Config.PREPAY_PERCENT}% = {prepay} ₽\n"
            f"• Остаток = {total - prepay} ₽",
            reply_markup=None  # убираем кнопки, т.к. теперь вызовем send_payment_keyboard
        )
    await send_payment_keyboard(cb.message, order_id=order.id, kind=None)
    await cb.answer("Готово!")


@r.message()
async def on_message_router(message: Message):
    engine = make_engine(Config.DB_PATH)

    with Session(engine) as sess:
        user = get_user_by_id(sess, message.from_user.id)
        if not user:
            return

        sess.refresh(user)
        text = (message.text or "").strip()

        # ───────────────────────────────────────────────
        # САМЫЙ ВЕРХ — проверка активации кода (самый высокий приоритет!)
        # ───────────────────────────────────────────────
        logger.info(f"→ Проверка состояния перед обработкой: awaiting_redeem_code = {user.awaiting_redeem_code}")
        logger.info(
            f"Получено сообщение '{text}' от {user.telegram_id}, awaiting_redeem_code = {user.awaiting_redeem_code}")

        if user.awaiting_redeem_code:
            # Проверяем, что введено ровно 3 цифры
            if not CODE_RE.match(text):
                await message.answer("Код должен состоять из <b>3 цифр</b>. Попробуйте ещё раз.")
                return

            code = text.strip()

            # Проверяем код в базе данных
            from db.models import RedeemCode, RedeemUse

            redeem_code = sess.query(RedeemCode).filter(
                RedeemCode.code == code,
                RedeemCode.is_used == False
            ).first()

            if not redeem_code:
                await message.answer("❌ Код не найден или уже использован.")
                user.awaiting_redeem_code = False
                logger.info(
                    f"Пользователь {user.telegram_id} завершил/отменил ввод кода → awaiting_redeem_code = False")
                sess.commit()
                await message.answer("Вернитесь в кабинет:", reply_markup=kb_cabinet())
                return

            # Код найден и не использован → активируем
            redeem_code.is_used = True
            redeem_code.used_by = user.telegram_id
            redeem_code.used_at = datetime.now(timezone.utc)

            # Записываем факт использования
            sess.add(RedeemUse(
                redeem_code_id=redeem_code.id,
                user_id=user.telegram_id
            ))

            # Убираем флаг ожидания
            user.awaiting_redeem_code = False
            logger.info(f"Пользователь {user.telegram_id} успешно активировал код {code}")

            # Добавляем практики, если их ещё нет
            added_count = 0
            if not user.practices:
                user.practices = []

            for practice in Config.DEFAULT_PRACTICES:
                if practice not in user.practices:
                    user.practices.append(practice)
                    added_count += 1

            # Обновляем/создаём запись в таблице access
            from db.models import Access

            access = sess.query(Access).filter(Access.user_id == user.telegram_id).first()
            if not access:
                access = Access(user_id=user.telegram_id)
                sess.add(access)

            was_already_open = added_count == 0

            access.practices_access = True
            access.channel_access = True

            sess.commit()

            # Добавляем в закрытый канал
            try:
                await bot.unban_chat_member(
                    chat_id=Config.CLOSED_CHANNEL_ID,
                    user_id=user.telegram_id,
                    only_if_banned=True
                )
                logger.info(f"Пользователь {user.telegram_id} добавлен в канал {Config.CLOSED_CHANNEL_ID}")
            except Exception as e:
                logger.error(f"Ошибка добавления в канал {user.telegram_id}: {e}")
                await notify_admin(
                    f"⚠️ Не удалось добавить {user.telegram_id} (@{user.username or 'нет username'}) "
                    f"в канал после активации кода.\nОшибка: {e}"
                )

            # Формируем сообщение
            if was_already_open:
                text = (
                    "Этот код уже был активирован ранее (или все практики уже открыты).\n\n"
                    "У тебя уже есть доступ ко всем 7 практикам! ✨\n\n"
                    "Практики ты всегда сможешь найти в разделе «Мои практики» в личном кабинете.\n\n"
                    "Увидимся в нашем закрытом канале с поддержкой, живыми эфирами "
                    "и тёплой атмосферой заботы:\n"
                    f"👉 {Config.CLOSED_CHANNEL_LINK}"
                )
            else:
                text = (
                    "🎉 Код успешно активирован!\n\n"
                    f"Добавлено новых практик: {added_count}\n\n"
                    "Теперь у тебя есть полный доступ ко всем практикам навсегда ❤️\n\n"
                    "Практики ты всегда сможешь найти в разделе «Мои практики» в личном кабинете.\n\n"
                    "А ещё приглашаю тебя в наш закрытый канал с поддержкой, живыми эфирами "
                    "и тёплой атмосферой заботы:\n"
                    f"👉 {Config.CLOSED_CHANNEL_LINK}"
                )

            # Клавиатура после активации
            kb = create_inline_keyboard([
                [{"text": "Личный кабинет", "callback_data": CallbackData.CABINET.value}],
                [{"text": "В меню", "callback_data": CallbackData.MENU.value}]
            ])

            await message.answer(
                text,
                reply_markup=kb,
                disable_web_page_preview=True
            )

            return

        # ===== 1. ПОДАРОЧНОЕ ПОСЛАНИЕ =====
        if user.awaiting_gift_message:
            orders = get_user_orders_db(sess, message.from_user.id)
            order = next((o for o in reversed(orders or []) if o.status == OrderStatus.NEW.value), None)

            # FIX: Attach detached order
            if order:
                order = sess.merge(order)

            if not order:
                user.awaiting_gift_message = False
                sess.commit()
                await message.answer("Активный заказ не найден. Послание добавить нельзя.", reply_markup=kb_main())
                return

            if not text:
                await message.answer("Послание не может быть пустым.")
                return

            if len(text) > 300:
                await message.answer("Максимум 300 символов.")
                return

            if order.extra_data is None:
                order.extra_data = {}

            order.extra_data["gift_message"] = text.strip()
            # FIX: Mark modified for commit
            flag_modified(order, "extra_data")

            user.awaiting_gift_message = False
            sess.commit()

            await message.answer("💌 Послание сохранено!")
            await send_payment_keyboard(message, order.id)
            return


        if user.awaiting_manual_pvz:
            manual_address = text.strip()
            if not manual_address:
                await message.answer("Введите адрес или код ПВЗ.")
                return

            # Fallback
            real_code = 0  # Placeholder
            city_code = "44"
            full_address = manual_address
            work_time = "Не указано"

            delivery_cost = 590  # Fallback
            period_text = "3–7"

            total = Config.PRICE_RUB + delivery_cost
            prepay = (total * Config.PREPAY_PERCENT + 99) // 100

            order = create_order_db(
                sess,
                user_id=message.from_user.id,
                product_id=1,
                status=OrderStatus.NEW.value,
                shipping_method="cdek_pvz",
                address=full_address,
                total_price_kop=total * 100,
                delivery_cost_kop=delivery_cost * 100,
                extra_data={
                    "pvz_code": real_code,
                    "city_code": city_code,
                    "delivery_cost": delivery_cost,
                    "delivery_period": period_text,
                    "manual_pvz": True,
                    "manual_address": manual_address
                }
            )

            order_id = order.id

            user.awaiting_manual_pvz = False
            user.awaiting_pvz_address = False
            user.temp_pvz_list = None
            user.pvz_for_order_id = order_id
            user.temp_gift_order_id = order_id
            sess.commit()

            await message.answer(
                f"Введённый вами вручную адрес: {manual_address}\n"
                f"Стоимость доставки будет считаться уже после оплаты (ориентировочно {delivery_cost} ₽, ≈ {period_text} дн.)\n"
                f"Итого (ориентировочно): {total} ₽"
            )

            await message.answer(
                "Хотите добавить личное послание в подарок получателю?",
                reply_markup=create_inline_keyboard([
                    [{"text": "Да, добавить", "callback_data": "gift:yes"}],
                    [{"text": "Нет, без послания", "callback_data": "gift:no"}],
                ])
            )
            return


        # ===== ВВОД ТРЕК-НОМЕРА АДМИНОМ =====
        if user.awaiting_manual_track:
            order_id = user.temp_order_id_for_track
            if not order_id:
                user.awaiting_manual_track = False
                sess.commit()
                await message.answer("Нет активного заказа для трека.", reply_markup=kb_admin_panel())
                return

            order = sess.get(Order, order_id)
            if not order or order.status not in [OrderStatus.ASSEMBLED.value, OrderStatus.PAID_FULL.value]:
                user.awaiting_manual_track = False
                sess.commit()
                await message.answer("Заказ не готов к вводу трека.", reply_markup=kb_admin_panel())
                return

            track = text.strip()
            if not track or len(track) < 5:  # Простая валидация
                await message.answer("Некорректный трек-номер. Попробуйте заново.")
                return

            order.track = track
            order.status = OrderStatus.SHIPPED.value
            user.awaiting_manual_track = False
            user.temp_order_id_for_track = None
            sess.commit()

            await notify_client_order_shipped(order.id, message)
            await message.answer(f"Трек {track} сохранён для #{order.id}. Заказ отправлен!", reply_markup=kb_admin_panel())
            return


        # ===== 2. ВВОД АДРЕСА ПВЗ =====
        if user.awaiting_pvz_address:
            ok, msg = validate_address(text)
            if not ok:
                await message.answer(
                    f"Адрес не распознан: {msg}\n\n"
                    "Примеры:\n"
                    "• Профсоюзная, 93\n"
                    "• ул Василисы Кожиной, 14\n"
                    "• Барклая 5А\n"
                    "• Ленинский проспект, д12 к2"
                )
                return

            if not user.extra_data:
                user.extra_data = {}

            user.extra_data["pvz_query"] = text
            sess.commit()

            await message.answer("Ищу ближайшие ПВЗ СДЭК...")

            pvz_list = await find_best_pvz(text)
            if not pvz_list:
                await message.answer(
                    f"Не удалось точно найти ПВЗ по запросу «{text}» 😔\n\n"
                    "Попробуйте ввести адрес чуть иначе "
                    "(например, без города, или только улицу + номер дома), или введите код пункта выдачи.\n\n"
                    "Или напишите в поддержку @anbolshakowa — подберём вручную.",
                    reply_markup=create_inline_keyboard([
                        [{"text": "Ввести адрес ПВЗ вручную в поддержку", "callback_data": "pvz_manual"}],
                        [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
                    ])
                )
                return

            user.temp_pvz_list = pvz_list
            sess.commit()

            await message.answer(
                f"Нашёл {len(pvz_list)} ПВЗ рядом с «{text}».\nВыбери нужный:",
                reply_markup=kb_pvz_list(pvz_list)
            )
            return

        # ===== 3. АВТОРИЗАЦИЯ =====
        # ===== АВТОРИЗАЦИЯ (ТОЛЬКО ПО СОСТОЯНИЮ) =====
        if user.awaiting_auth:
            lines = [l.strip() for l in text.split("\n") if l.strip()]

            if len(lines) != 3:
                await message.answer(
                    "Введите данные в 3 строки:\n"
                    "Имя Фамилия\n"
                    "+7XXXXXXXXXX\n"
                    "email@example.com"
                )
                return

            full_name, phone, email = lines
            ok, msg = validate_data(full_name, phone, email)

            if not ok:
                await message.answer(f"Ошибка: {msg}")
                return

            user.full_name = full_name
            user.phone = phone
            user.email = email
            user.is_authorized = True
            user.awaiting_auth = False
            sess.commit()

            await message.answer(
                f"Спасибо, {full_name.split()[0]}! Данные сохранены.\n"
                "Теперь вы авторизованы.",
                reply_markup=kb_main()
            )
            return

    # ===== 4. ОБЫЧНЫЙ ТЕКСТ / ФОЛЛБЕК =====
    await on_text(message)


async def on_text(message: Message):
    text = (message.text or "").strip().lower()

    if text.startswith("/"):
        if text.startswith("/admin "):
            await handle_admin_command(message, text)
        return

    if text in {"меню", "/menu"}:
        await cmd_menu(message)
    elif text.lower() in {"мои практики", "практики", "практика"}:
            fake_cb = type("FakeCB", (), {
                "from_user": message.from_user,
                "message": message,
                "data": CallbackData.PRACTICES.value,
                "answer": lambda *a, **kw: None,
            })()
            await cb_practices_list(fake_cb)
            return
    elif text in {"личный кабинет", "кабинет"}:
        await cb_cabinet(type("obj", (), {"from_user": message.from_user, "message": message, "answer": lambda *a, **k: None})())
    elif text in {"заказать"}:
        await message.answer(
            "Для оформления заказа используйте кнопки меню 👇",
            reply_markup=kb_main()
        )
    else:
        await message.answer("Не понял запрос. Воспользуйтесь меню.", reply_markup=kb_main())


@r.callback_query()
async def catch_all_callbacks(cb: CallbackQuery):
    logger.info(f"Uncaught callback: user_id={cb.from_user.id}, data={cb.data}")
    await cb.answer("Команда не распознана", show_alert=True)

# ========== ADMIN COMMANDS ==========
async def handle_admin_command(message: Message, text: str):
    if not await is_admin(message):
        return

    parts = text.split()
    if len(parts) < 2:
        await message.answer(
            "Использование: /admin <действие> [order_id] [трек]\n"
            "Действия: list, assembled, shipped, archived"
        )
        return

    action = parts[1].lower()
    args = parts[2:]

    engine = make_engine(Config.DB_PATH)

    with Session(engine) as sess:
        if action == "list":
            all_orders = sess.scalars(select(Order)).all()
            if not all_orders:
                await message.answer("Нет заказов.")
                return

            def tag(o: Order) -> str:
                return {
                    OrderStatus.NEW.value: "new",
                    OrderStatus.PAID_PARTIALLY.value: "paid_partially",
                    OrderStatus.PAID_FULL.value: "paid_full",
                    OrderStatus.ASSEMBLED.value: "assembled",
                    OrderStatus.SHIPPED.value: "shipped",
                    OrderStatus.ARCHIVED.value: "archived",
                    OrderStatus.ABANDONED.value: "abandoned",
                }.get(o.status, o.status)

            rows = [f"#{o.id}: {tag(o)} | {o.address or '—'} | user_{o.user_id}" for o in all_orders]
            await message.answer("Заказы:\n" + "\n".join(rows[:50]))
            return

        # Все остальные действия требуют order_id
        if not args or not args[0].isdigit():
            await message.answer(f"Укажите order_id. Пример: /admin {action} 1")
            return

        order_id = int(args[0])
        order = sess.get(Order, order_id)

        if not order:
            await message.answer(f"Заказ #{order_id} не найден.")
            return

        if action == "assembled":
            # Собираем заказ (переводим в ASSEMBLED)
            if order.status not in [OrderStatus.PAID_PARTIALLY.value, OrderStatus.PAID_FULL.value]:
                await message.answer("Заказ можно собрать только если он оплачен частично или полностью.")
                return

            order.status = OrderStatus.ASSEMBLED.value
            sess.commit()

            # Уведомляем клиента
            await notify_client_order_assembled(order_id, message)
            await message.answer(f"Заказ #{order_id} собран и готов к отправке (или к дооплате).")

        elif action == "shipped":
            # Отправляем заказ (создаём в СДЭК)
            track = args[1] if len(args) > 1 else None

            if not track:
                await message.answer("Укажите трек-номер: /admin shipped 1 ТРЕК123")
                return

            if order.status != OrderStatus.ASSEMBLED.value:
                await message.answer("Заказ можно отправить только после сборки (статус assembled).")
                return

            if order.payment_kind not in ["full", "remainder"]:
                await message.answer("Заказ должен быть полностью оплачен.")
                return

            # Создаём заказ в СДЭК
            success = await create_cdek_order(order_id)
            if not success:
                await message.answer(f"Ошибка создания заказа в СДЭК для #{order_id}")
                return

            # Обновляем трек (create_cdek_order уже должен это сделать)
            sess.refresh(order)
            if order.track != track:
                order.track = track
                sess.commit()

            await notify_client_order_shipped(order.id, message)
            await message.answer(f"📦 Заказ #{order_id} отправлен! Трек: {track}")

        elif action == "archived":
            if order.status != OrderStatus.SHIPPED.value:
                await message.answer("Архивировать можно только отправленные заказы (shipped).")
                return

            order.status = OrderStatus.ARCHIVED.value
            sess.commit()

            await notify_admins_order_archived(order.id)
            await message.answer(f"🗄 Заказ #{order_id} заархивирован")

        else:
            await message.answer("Неизвестное действие. Доступно: list, assembled, shipped, archived")

# ========== НОВЫЕ ФУНКЦИИ СДЭК ==========
async def get_cdek_city_code(city_name: str) -> Optional[int]:
    token = await get_cdek_prod_token()
    if not token:
        return None

    url = "https://api.cdek.ru/v2/location/cities"
    params = {"city": city_name.strip()}

    try:
        r = await asyncio.to_thread(
            requests.get,
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=15
        )
        if r.status_code == 200:
            cities = r.json()
            if cities:
                code = cities[0].get('code')
                logger.info(f"Город '{city_name}' → code {code}")
                return code
        logger.warning(f"Ошибка поиска города '{city_name}': {r.status_code} {r.text}")
    except Exception as e:
        logger.error(f"Исключение при поиске города '{city_name}': {e}")

    return None


async def get_cdek_pvz_list(address_query: str, city_code: Optional[int] = None, limit: int = 50) -> List[dict]:
    # token = await get_cdek_token()
    token = await get_cdek_prod_token()
    if not token:
        logger.error("Нет прод токена для поиска ПВЗ - проверьте .env")
        return []

    url = "https://api.cdek.ru/v2/deliverypoints"
    params = {
        "type": "PVZ",
        "limit": limit
    }
    if city_code is not None:
        params["city_code"] = city_code
    # Убрали "address" - теперь ищем все PVZ в городе
    logger.info(f"Запрос ПВЗ: url={url}, params={params}")

    try:
        resp = await asyncio.to_thread(requests.get, url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if resp.status_code == 200:
            points = resp.json()
            logger.info(f"Найдено {len(points)} ПВЗ по запросу (city_code={city_code})")
            return points
        else:
            logger.warning(f"Ошибка поиска ПВЗ: {resp.status_code} {resp.text}")
            return []
    except Exception as e:
        logger.error(f"Исключение при поиске ПВЗ: {e}")
        return []


def _shorten_address(address: str) -> str:
    if not address:
        return "ПВЗ СДЭК"

    parts = [p.strip() for p in address.split(',') if p.strip()]

    # Найти индекс части с типом улицы (search anywhere in part)
    street_idx = -1
    for i, part in enumerate(parts):
        lower_part = part.lower()
        if any(re.search(r'(^|\b)' + re.escape(kw) + r'(\b|\.?$)', lower_part, re.I) for kw in STREET_KEYWORDS):
            street_idx = i
            break

    if street_idx == -1:
        # Fallback: последние 2-3 части как street + house
        if len(parts) >= 3:
            street = parts[-3]
            house = ', '.join(parts[-2:])
        elif len(parts) == 2:
            street = parts[0]
            house = parts[1]
        else:
            street = ' '.join(parts)
            house = ''
    else:
        # Street = найденная часть, house = всё после
        street = parts[street_idx]
        house_parts = parts[street_idx + 1:]
        house = ', '.join(house_parts).strip() if house_parts else ''

    # Очистка street: убрать тип только если в начале (сохраняя "2-й Проезд")
    street = re.sub(
        r'^(ул\.?|улица|пр\.?|проспект|пр-кт|пр-т|пр-д|проезд|б-р|бульвар|пер\.?|переулок|ш\.?|шоссе|наб\.?|набережная|пл\.?|площадь|тракт|аллея)\s+',
        '', street, flags=re.I).strip()

    # Для house: убрать "эт., оф., пом., цоколь, подвал" и после
    if house:
        house = re.sub(r'\s+(эт\.?|оф\.?|пом\.?|цоколь|подвал).*', '', house, flags=re.I).strip()

    short = f"{street}, {house}" if house else street

    short = re.sub(r'\s+', ' ', short).strip()

    if len(short) > 42:
        short = short[:39] + '…'

    return short or "ПВЗ СДЭК"


def _extract_street_house(addr: str) -> tuple[Optional[str], Optional[str]]:
    parts = [p.strip() for p in (addr or "").split(",") if p.strip()]
    if len(parts) < 2:
        return None, None
    return parts[-2], parts[-1]


def _addr_key(street: Optional[str], house: Optional[str]) -> Optional[str]:
    if not street or not house:
        return None
    norm = lambda s: re.sub(r"\s+", " ", s.lower().strip())
    return norm(street) + "|" + norm(house)


_ADDR_RE = re.compile(
    r"(?P<street>.+?)\s*,\s*(?P<house>\d+[^\s,]*)",
    re.IGNORECASE
)

def _normalize_address_variants(address_query: str) -> List[str]:
    """
    Генерирует все возможные варианты запроса:
    - как ввёл пользователь
    - с очищенным домом (без корпуса, строения, дроби)
    - только улица
    - с дробью как отдельным домом
    """
    address_query = address_query.strip()
    if not address_query:
        return []

    variants = {address_query}  # set чтобы не было дубликатов

    # Приводим к нижнему регистру для поиска паттернов
    lower = address_query.lower()

    # Ищем дом с возможной дробью: 7/1, 44к2, 5а к.3, 12 стр.5 и т.д.
    house_match = re.search(r"(\d+[^\s,]*[\/кк]\s*\d+|\d+[^\s,]*\s*[кк]\.?\s*\d+|\d+[^\s,]*\s*стр\.?\s*\d+|\d+[^\s,]*\s*корп?\.?\s*\d+|\d+[^\s,]*)", lower)
    if not house_match:
        return list(variants)

    raw_house = house_match.group(1)

    # Очищаем дом: оставляем только основную цифру
    clean_house = re.sub(r"[\/кк]\s*\d+.*$", "", raw_house)  # убираем /1, к2
    clean_house = re.sub(r"\s*[кк]\.?\s*\d+.*$", "", clean_house)
    clean_house = re.sub(r"\s*стр\.?\s*\d+.*$", "", clean_house)
    clean_house = re.sub(r"\s*корп?\.?\s*\d+.*$", "", clean_house)
    clean_house = re.sub(r"[^\d].*$", "", clean_house)  # оставляем только цифры в начале
    clean_house = clean_house.strip()

    # Находим улицу — всё до последнего запятого и дома
    before_house = address_query.rsplit(",", 1)[0] if "," in address_query else address_query
    street = before_house.strip()

    # Добавляем варианты
    if clean_house:
        variants.add(f"{street}, {clean_house}")
        variants.add(f"{street} {clean_house}")

    # Вариант только с улицей
    variants.add(street)

    # Вариант с дробью как отдельным домом (например, "Барклая 7" и "Барклая 1")
    if "/" in raw_house:
        parts = raw_house.split("/", 1)
        main_part = re.sub(r"[^\d].*$", "", parts[0]).strip()
        sub_part = re.sub(r"[^\d].*$", "", parts[1]).strip() if len(parts) > 1 else ""
        if main_part:
            variants.add(f"{street}, {main_part}")
            variants.add(f"{street} {main_part}")
        if sub_part:
            variants.add(f"{street}, {sub_part}")
            variants.add(f"{street} {sub_part}")

    return list(variants)


def _prepare_street_key(street_part: str) -> str:
    """Сохраняем дефисы и порядковые окончания для правильного матчинга"""
    # Не убираем дефисы и окончания типа 2-й, 1-я, 5-е
    street = street_part.lower().strip()
    # Только убираем тип улицы в начале/конце, но оставляем суть
    street = re.sub(r'^(ул\.?|улица|пр\.?|проспект|пр-кт|пр-т|б-р|бульвар|пер\.?|переулок|ш\.?|шоссе)\s+', '', street)
    street = re.sub(r'\s+(ул\.?|улица|пр\.?|проспект|пр-кт|пр-т|б-р|бульвар|пер\.?|переулок|ш\.?|шоссе)$', '', street)
    # Убираем лишние пробелы
    return re.sub(r'\s+', ' ', street).strip()


def _make_exact_matcher(address_query: str):
    """
    Улучшенный матчер, который лучше работает с «2-й», дефисами и сложными названиями
    """
    query = (address_query or "").strip().lower()

    logger.info(f"Начало работы матчера для запроса: '{query}'")

    # 1. Пытаемся убрать город в начале
    city_removed = False
    if ',' in query:
        parts = query.split(',', 1)
        if len(parts) > 1 and any(kw in parts[0] for kw in ["город", "г.", "г ", "г. ", "область", "край", "республика"]):
            query = parts[1].strip()
            city_removed = True
            logger.info(f"Убрали город/регион в начале → '{query}'")

    # 2. Нормализация сокращений (расширенный список)
    replacements = [
        ("пр-т", "проспект"), ("пр-кт", "проспект"), ("пр.", "проспект"), ("пр ", "проспект "),
        ("пр-д", "проезд"), ("пр-зд", "проезд"),
        ("ул.", "улица"), ("ул ", "улица "),
        ("пер.", "переулок"),
        ("ш.", "шоссе"),
        ("б-р", "бульвар"), ("бул.", "бульвар"),
        ("наб.", "набережная"),
        ("пл.", "площадь"),
    ]

    for old, new in replacements:
        query = query.replace(old, new)

    logger.info(f"После замены сокращений: '{query}'")

    # 3. Пробуем выделить улицу и дом несколькими способами
    variants = []

    # Вариант А: классический — запятая перед домом
    if ',' in query:
        parts = [p.strip() for p in query.split(',')]
        if len(parts) >= 2:
            street = ' '.join(parts[:-1])
            house_raw = parts[-1]
            variants.append((street, house_raw))

    # Вариант Б: без запятой — ищем дом в конце
    house_match = re.search(r'(\d+[а-яА-ЯёЁ0-9/кстркорп.-]*\s*[а-яА-ЯёЁ]?)', query)
    if house_match:
        house_raw = house_match.group(1).strip()
        street = query[:house_match.start()].strip()
        if street and house_raw:
            variants.append((street, house_raw))

    # Вариант В: просто берём всё как улицу + дом (если ничего выше не нашлось)
    if not variants and ' ' in query:
        parts = query.rsplit(' ', 1)
        street = parts[0]
        house_raw = parts[1]
        variants.append((street, house_raw))

    # Если всё ещё ничего — весь запрос как улица, дома нет
    if not variants:
        variants.append((query, None))

    logger.info(f"Варианты разбиения: {variants}")

    # Выбираем самый вероятный вариант (с домом предпочтительнее)
    street, house_raw = None, None
    for s, h in variants:
        if h and re.search(r'\d', h):  # в доме должна быть хотя бы одна цифра
            street, house_raw = s, h
            break
    if not street:
        street, house_raw = variants[0]

    street_key = _prepare_street_key(street)
    logger.info(f"Итоговый street_key = '{street_key}', house_raw = '{house_raw}'")

    def matcher(pvz: dict) -> bool:
        addr_full = (pvz.get("location", {}).get("address_full") or
                     pvz.get("location", {}).get("address") or "").lower()

        if not addr_full:
            return False

        # Очень лояльная проверка улицы
        street_ok = False
        if street_key:
            # Пробуем разные уровни строгости
            if street_key in addr_full:
                street_ok = True
            elif len(street_key) > 8 and street_key[:8] in addr_full:
                street_ok = True
            elif any(word in addr_full for word in street_key.split() if len(word) >= 5):
                street_ok = True

        # Проверка дома
        house_ok = False
        if house_raw:
            house_lower = house_raw.lower()
            addr_lower = addr_full.lower()

            # Базовое: полное совпадение подстроки
            if house_lower in addr_lower:
                house_ok = True
            else:
                # Для форм "44к2", "7/1", "5а" — ищем как word (границы)
                if re.match(r'\d+[а-яa-z/.-]*$', house_lower):
                    escaped = re.escape(house_lower)
                    if re.search(r'(?<![\w/.-])' + escaped + r'(?![\w/.-])', addr_lower):
                        house_ok = True

            # Проверка с корпусами, строениями, литерами и т.д.
            if not house_ok:
                # Пытаемся найти хотя бы часть house_raw
                house_parts = re.split(r'[\s,]+', house_raw)
                house_ok = any(part in addr_full for part in house_parts if len(part) >= 2)

        if not street_key:
            return house_ok

        return street_ok and (house_ok or not house_raw)

    return matcher

# ======== УМНЫЙ ПОИСК ЛУЧШИХ ПВЗ =========

def filter_pvz_by_distance(pvz_list: List[dict], max_distance_m: int = 6000) -> List[dict]:
    filtered = []
    for pvz in pvz_list:
        d = pvz.get("distance")
        if isinstance(d, (int, float)) and d > 0 and d <= max_distance_m:
            filtered.append(pvz)
        elif d is None:
            filtered.append(pvz)
    return filtered

async def find_best_pvz(address_query: str, limit: int = 12) -> List[dict]:
    """
    Основная функция поиска ПВЗ:
    - Пытается определить город через API
    - Если не получилось — fallback на Москву
    - Возвращает список ПВЗ в этом городе (до limit штук)
    """
    original_query = address_query.strip()
    logger.info(f"Поиск ПВЗ по запросу: {original_query!r}")

    # ───────────────────────────────────────────────
    # 1. Пользователь ввёл код ПВЗ напрямую (MSK123, SPB45 и т.п.)
    # ───────────────────────────────────────────────
    if re.fullmatch(r'[A-Z]{2,5}\d{2,6}', original_query.upper()):
        code = original_query.upper()
        logger.info(f"Обнаружен прямой ввод кода ПВЗ: {code}")

        all_points = await get_cdek_pvz_list("", city_code=None, limit=3000)
        exact = [p for p in all_points if str(p.get("code", "")).upper() == code]

        if exact:
            logger.info(f"Найден точный ПВЗ по коду {code}")
            return exact[:limit]
        else:
            logger.warning(f"Код {code} не найден даже по всей России")
            return []

    # ───────────────────────────────────────────────
    # 2. Пытаемся выделить название города из запроса
    # ───────────────────────────────────────────────
    city_name_candidate = None

    # Вариант А: есть запятая → берём всё до первой запятой
    if ',' in original_query:
        city_name_candidate = original_query.split(',', 1)[0].strip()

    # Вариант Б: нет запятой → берём первые 1–2 слова
    else:
        words = original_query.split()
        if len(words) >= 2:
            city_name_candidate = ' '.join(words[:2])
        elif words:
            city_name_candidate = words[0]

    city_code = None

    if city_name_candidate:
        city_code = await get_cdek_city_code(city_name_candidate)

        if city_code:
            logger.info(f"Успешно найден код города '{city_name_candidate}' → {city_code}")
        else:
            logger.warning(f"Не удалось найти код города по '{city_name_candidate}'")

    # ───────────────────────────────────────────────
    # 3. Если город не найден — fallback на Москву
    # ───────────────────────────────────────────────
    if city_code is None:
        city_code = 44
        logger.warning(
            f"Город не определён по запросу '{original_query}'. "
            f"Используем Москву (44) как fallback"
        )


    logger.info(f"Итоговый city_code для поиска ПВЗ: {city_code}")

    # ───────────────────────────────────────────────
    # 4. Получаем все ПВЗ в определённом городе
    # ───────────────────────────────────────────────
    pts = await get_cdek_pvz_list("", city_code=city_code, limit=1000)

    if not pts:
        logger.warning(f"Не найдено ни одного ПВЗ для city_code={city_code}")
        return []

    logger.info(f"Найдено {len(pts)} пунктов выдачи в городе с code={city_code}")

    # ───────────────────────────────────────────────
    # 5. Фильтруем по адресу (если запрос выглядит как адрес)
    # ───────────────────────────────────────────────
    matcher = _make_exact_matcher(original_query)
    filtered = [p for p in pts if matcher(p)]
    if not filtered:
        words = re.findall(r'\w+', original_query.lower())
        is_pure_city = (len(words) <= 2 and not any(re.match(r'\d', w) for w in words) and
                        city_code is not None and len(pts) > 0)
        if is_pure_city:
            logger.info(f"Чистый запрос на город '{original_query}' — возвращаем первые {limit} ПВЗ, отсортированные по code")
            sorted_pts = sorted(pts, key=lambda p: str(p.get('code', '')))
            filtered = sorted_pts[:limit]

    # ───────────────────────────────────────────────
    # КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ — поведение при неудаче
    # ───────────────────────────────────────────────
    # Если ничего не нашли по матчингу — показываем все ПВЗ города
    if not filtered:
        logger.info("Ни один точный ПВЗ не найден даже после всех попыток")

        # НОВОЕ ПОВЕДЕНИЕ
        if city_code != 44:  # Если это НЕ Москва
            logger.info(
                f"Город определён как {city_name_candidate or 'неизвестный'} (code={city_code}) → показываем все ПВЗ города")
            filtered = pts[:limit]  # показываем первые N ПВЗ города
        else:
            logger.info("Город — Москва или не определён → НЕ показываем все ПВЗ (чтобы не спамить)")
            return []  # пустой список → будет сообщение "не нашли"

        # Если всё же показываем - сортируем по алфавиту (или по distance, если есть)
        if any("distance" in p for p in filtered):
            filtered.sort(key=lambda p: p.get("distance") or 999999)
        else:
            filtered.sort(key=lambda p: p.get("code", ""))

    result = filtered[:limit]

    # Очень подробный лог того, что именно мы отдаём пользователю
    logger.info("═" * 60)
    logger.info(f"ФИНАЛЬНЫЙ РЕЗУЛЬТАТ — возвращаем {len(result)} ПВЗ:")
    for i, pvz in enumerate(result, 1):
        code = pvz.get("code", "—")
        addr = (pvz.get("location") or {}).get("address_full") or \
               (pvz.get("location") or {}).get("address") or "—"
        dist = pvz.get("distance", "—")
        dist_str = f"{int(dist)} м" if isinstance(dist, (int, float)) and dist > 0 else "—"
        logger.info(f"  {i:2}. {code:8} | {dist_str:>8} | {addr}")
    logger.info("═" * 60)

    logger.info(f"Итого возвращено пользователю: {len(result)} пунктов")
    return result


def format_pvz_button(pvz: dict, index: int) -> dict:
    code = pvz["code"]
    loc = pvz.get("location", {}) or {}
    address = loc.get("address_full") or loc.get("address") or ""

    short_addr = _shorten_address(address) or f"ПВЗ {code}"

    # Можно оставить расстояние, если оно есть и небольшое
    dist = pvz.get("distance")
    dist_text = f" · {int(dist)}м" if isinstance(dist, (int, float)) and 100 < dist < 5000 else ""

    text = f"{index + 1}. {short_addr.strip()}{dist_text}"

    # Обрезаем только если всё равно длиннее лимита Telegram
    if len(text) > 64:
        text = text[:61] + "…"

    return {
        "text": text,
        "callback_data": f"pvz_sel:{code}:{index}"
    }


def kb_pvz_list(pvz_list: List[dict]) -> InlineKeyboardMarkup:
    buttons = []

    for i, pvz in enumerate(pvz_list[:12]):
        buttons.append([format_pvz_button(pvz, i)])

    buttons.append([{"text": "Не вижу свой ПВЗ", "callback_data": "pvz_manual"}])
    buttons.append([{"text": "Ввести адрес заново", "callback_data": "pvz_reenter"}])
    buttons.append([{"text": "Назад в меню", "callback_data": CallbackData.MENU.value}])

    return create_inline_keyboard(buttons)


# Храним последний известный статус, чтобы не спамить
last_status_cache: Dict[int, str] = {}  # order_id → status_text

async def check_all_shipped_orders():
    engine = make_engine(Config.DB_PATH)  # свежий engine

    await asyncio.sleep(5)  # Дай 5 сек на init_db (если гонка)
    while True:
        try:
            # Проверяем наличие таблицы
            inspector = inspect(engine)
            if not inspector.has_table("orders"):
                logger.warning("Таблица orders не существует - ждём 60 сек")
                await asyncio.sleep(60)
                continue

            logger.info("Запуск проверки статусов СДЭК...")
            orders_to_check = get_all_orders_by_status(OrderStatus.SHIPPED.value)

            for detached_order in orders_to_check:
                with Session(engine) as sess:
                    order: Optional[Order] = sess.get(Order, detached_order.id)  # Reload fresh
                    if order is None:
                        continue

                    uuid = order.extra_data.get("cdek_uuid")
                    if not uuid:
                        continue

                    info = await get_cdek_order_info(uuid)
                    if not info:
                        continue

                    # Извлекаем актуальные данные
                    new_track = info.get("number") or info.get("cdek_number")
                    new_status = info.get("status", {}).get("description") or info.get("status", {}).get("code")

                    if not new_track and not new_status:
                        continue

                # === 1. Присылаем ТРЕК-НОМЕР (один раз!) ===
                if new_track and (not order.track or order.track.startswith("BOX")):
                    with Session(engine) as sess:  # New sess for write
                        order = sess.get(Order, detached_order.id)  # Reload again for safety
                        if order:
                            order.track = new_track
                            sess.commit()

                    # Красивое финальное сообщение клиенту
                    await bot.send_message(
                        order.user_id,
                        "Готово! Посылка отправлена! 🚀\n\n"
                        f"Трек-номер: <code>{new_track}</code>\n"
                        f"<a href=\"https://www.cdek.ru/ru/tracking?order_id={new_track}\">"
                        "Отслеживать посылку</a>",
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        reply_markup=kb_order_status(order)
                    )
                    logger.info(f"Трек-номер отправлен клиенту по заказу #{order.id}: {new_track}")

                    await notify_admin(
                        f"Трек-номер пришёл!\n"
                        f"Заказ #{order.id} → <code>{new_track}</code>\n"
                    )


                # === 2. Уведомления об изменении статуса (опционально, только важные) ===
                important_statuses = [
                    "Принят на склад отправителя",
                    "Выдан на доставку",
                    "Доставлен",
                    "Вручён",
                    "Возврат",
                    "Неудачная попытка вручения"
                ]

                current_status_desc = info.get("status", {}).get("description", "")
                if (current_status_desc in important_statuses and
                    current_status_desc != last_status_cache.get(order.id)):

                    last_status_cache[order.id] = current_status_desc

                    await bot.send_message(
                        order.user_id,
                        f"Обновление по заказу #{order.id}\n\n"
                        f"Статус: <b>{current_status_desc}</b>\n"
                        f"Трек: <code>{order.track}</code>\n"
                        f"<a href=\"https://www.cdek.ru/ru/tracking?order_id={order.track}\">"
                        "Отслеживать</a>",
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )

                    await notify_admin(
                        f"СДЭК: #{order.id} → {current_status_desc}\n"
                        f"Трек: {order.track}"
                    )
            logger.info(f"Проверка статусов завершена. Проверено заказов: {len(orders_to_check)}")
        except (ValueError, KeyError, requests.RequestException) as e:  # Уточнили broad except
            logger.exception(f"КРИТИЧНАЯ ошибка в check_all_shipped_orders: {e}")
            await notify_admin(f"ОШИБКА в фоновой задаче СДЭК:\n{e}")
        # Проверяем каждые 2-3 минуты в первые 2 часа после создания, потом реже - но пока просто 5 минут
        await asyncio.sleep(300)  # 5 минут - оптимально


async def check_pending_timeouts():
    while True:
        try:
            logger.info("Starting pending timeouts check")
            engine = make_engine(Config.DB_PATH)
            with Session(engine) as sess:
                pending_orders = sess.query(Order).filter(
                    Order.status == OrderStatus.PENDING_PAYMENT.value,
                    Order.created_at < datetime.now(timezone.utc) - timedelta(seconds=Config.PAYMENT_TIMEOUT_SEC)
                ).all()
                logger.info(f"Found {len(pending_orders)} pending orders: {[o.id for o in pending_orders]}")

                for order in pending_orders:
                    logger.info(f"Processing order #{order.id} (current status: {order.status})")
                    succeeded = False
                    pending_payments = order.extra_data.get("pending_payments", {})
                    logger.info(f"Pending payments for #{order.id}: {pending_payments}")

                    for k, pid in pending_payments.items():
                        logger.info(f"Checking payment {pid} for kind '{k}'")
                        try:
                            payment = Payment.find_one(pid)
                            logger.info(f"Payment {pid} status: {payment.status}")
                            if payment.status == "succeeded":
                                succeeded = True
                                if k == "full":
                                    order.payment_kind = "full"
                                    order.status = OrderStatus.PAID_FULL.value
                                elif k == "pre":
                                    order.payment_kind = "pre"
                                    order.status = OrderStatus.PAID_PARTIALLY.value
                                elif k == "rem":
                                    order.payment_kind = "remainder"
                                    order.status = OrderStatus.PAID_FULL.value
                                else:
                                    logger.warning(f"Unknown payment kind '{k}' for succeeded payment - skipping update")
                                    continue
                                logger.info(f"Updated order #{order.id} status to {order.status} (kind: {k})")
                                try:
                                    sess.commit()
                                    logger.info(f"Commit successful for order #{order.id}")
                                except Exception as commit_e:
                                    logger.error(f"Commit failed for order #{order.id}: {commit_e}")
                                    sess.rollback()
                                    await notify_admin(f"⚠️ Commit failed in timeouts for #{order.id}: {commit_e}")
                                try:
                                    await notify_admins_payment_success(order.id)
                                    logger.info(f"Notify sent for order #{order.id}")
                                except Exception as notify_e:
                                    logger.error(f"Notify failed for order #{order.id}: {notify_e}")
                                    await notify_admin(f"⚠️ Notify failed in timeouts for #{order.id}: {notify_e}")
                                break
                        except Exception as payment_e:
                            logger.error(f"Error checking payment {pid} for order #{order.id}: {payment_e}")

                    if not succeeded:
                        logger.info(f"No succeeded payments for #{order.id} - abandoning")
                        order.status = OrderStatus.ABANDONED.value
                        try:
                            sess.commit()
                            logger.info(f"Commit successful for abandoned #{order.id}")
                        except Exception as commit_e:
                            logger.error(f"Commit failed for abandoned #{order.id}: {commit_e}")
                            sess.rollback()
                        try:
                            await bot.send_message(order.user_id, f"Ваш заказ #{order.id} был отменён из-за отсутствия оплаты в течение 10 минут.")
                            logger.info(f"Abandon message sent to user for #{order.id}")
                        except Exception as msg_e:
                            logger.error(f"Abandon message failed for #{order.id}: {msg_e}")

            logger.info("Pending timeouts check completed")
            await asyncio.sleep(60)  # Проверять каждую минуту
        except Exception as e:
            logger.exception(f"Global error in check_pending_timeouts: {e}")
            await notify_admin(f"❌ Global error in timeouts task: {e}")


async def check_channel_permissions():
    try:
        member = await bot.get_chat_member(Config.CLOSED_CHANNEL_ID, bot.id)
        if member.status not in ("administrator", "creator"):
            logger.error("Бот НЕ является администратором канала! Добавление пользователей не сработает.")
            await notify_admin("⚠️ Критично: бот не админ в закрытом канале!")
    except Exception as e:
        logger.error(f"Не удалось проверить права бота в канале: {e}")

@r.message(CommandStart(deep_link=True))
async def handle_payment_success(message: Message):
    args = message.text.split(maxsplit=1)[1:] if len(message.text.split()) > 1 else []
    if not args or not args[0].startswith("payment_success"):
        await on_start(message)  # обычный старт
        return

    # Парсим параметры
    params_str = " ".join(args[1:]) if len(args) > 1 else ""
    params = dict(p.split('=', 1) for p in params_str.split('&') if '=' in p)

    order_id_str = params.get("order_id")
    kind = params.get("kind")

    if not order_id_str or not order_id_str.isdigit():
        await message.answer("Ошибка: некорректная ссылка после оплаты. Напишите в поддержку.")
        return

    order_id = int(order_id_str)

    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if not order or order.user_id != message.from_user.id:
            await message.answer("Заказ не найден или принадлежит другому пользователю.")
            return

        # Проверяем, не оплачен ли уже
        if order.status in (OrderStatus.PAID_FULL.value, OrderStatus.SHIPPED.value, OrderStatus.ARCHIVED.value):
            await message.answer("Заказ уже оплачен и обрабатывается ❤️")
            await message.answer(format_client_order_info(order), reply_markup=kb_order_status(order))
            return

        # Критично: проверяем реальный статус платежа в ЮKассе
        try:
            payment = Payment.find_one(order.extra_data.get("yookassa_payment_id"))
            if payment.status != "succeeded":
                await message.answer(
                    "Платёж ещё не подтверждён ЮKассой.\n"
                    "Пожалуйста, подождите 1–2 минуты и вернитесь по этой же ссылке снова.\n"
                    "Если проблема сохраняется — напишите в поддержку."
                )
                return

            # Платёж успешен — обновляем заказ
            if kind == "full":
                order.payment_kind = "full"
                order.status = OrderStatus.PAID_FULL.value
                await notify_admins_payment_success(order.id)
                text = f"Полная оплата получена! ❤️\nЗаказ <b>#{order.id}</b> принят в сборку."

            elif kind == "pre":
                order.payment_kind = "pre"
                order.status = OrderStatus.PAID_PARTIALLY.value
                await notify_admins_payment_success(order.id)
                text = f"Предоплата получена ❤️\nЗаказ <b>#{order.id}</b> принят в сборку."

            elif kind == "rem":
                order.payment_kind = "remainder"
                order.status = OrderStatus.PAID_FULL.value
                await notify_admins_payment_remainder(order.id)
                text = f"Дооплата получена ❤️\nЗаказ <b>#{order.id}</b> готов к отправке."

            else:
                text = "Оплата прошла, но тип оплаты неизвестен. Админ уже уведомлён."

            logger.info(f"Оплата #{order.id} ({kind}) прошла → использован vat_code = {6 if kind == 'pre' else 4}")
            # Сохраняем ID платежа (на будущее)
            if not order.extra_data:
                order.extra_data = {}
            order.extra_data["yookassa_payment_id"] = payment.id
            flag_modified(order, "extra_data")
            sess.commit()

            user = get_user_by_id(sess, message.from_user.id)
            if user:
                reset_states(user, sess)
                logger.info(
                    f"Состояния пользователя {user.telegram_id} "
                    f"сброшены после подтверждения оплаты заказа #{order.id}"
                )

        except Exception as e:
            logger.exception("Ошибка проверки статуса платежа в ЮKассе")
            await notify_admin(f"Ошибка проверки платежа заказа #{order_id}: {e}")
            await message.answer("Ошибка проверки оплаты. Админ уже уведомлён, скоро разберёмся.")

    await message.answer(text)
    await message.answer(format_client_order_info(order), reply_markup=kb_order_status(order))


# ───────────────────────────────────────────────
# WEBHOOK ОТ ЮKASSA (отдельный FastAPI сервер)
# ───────────────────────────────────────────────
from fastapi.responses import JSONResponse


@app.post("/webhook/yookassa")
async def yookassa_webhook(request: Request):
    client_ip = request.client.host
    logger.info(f"YooKassa webhook from IP: {client_ip} | Headers: {dict(request.headers)}")

    # Быстрая защита: принимаем только от официальных IP ЮKассы
    yookassa_ips = {"77.75.154.206", "77.75.153.78", "77.75.154.0/24", "77.75.153.0/24"}
    if client_ip not in yookassa_ips and not any(client_ip.startswith(ip.split('/')[0]) for ip in yookassa_ips):
        logger.warning(f"Webhook от неизвестного IP: {client_ip} — отклоняем")
        return JSONResponse(status_code=403, content={"ok": False})

    try:
        payload = await request.json()
        logger.debug(f"YooKassa payload: {payload}")

        notification = WebhookNotification(payload)
        event = notification.event
        payment = notification.object

        logger.info(f"Успешно распарсено уведомление: {event} | Payment ID: {payment.id}")

        if event == "payment.succeeded":
            order_id_str = payment.metadata.get("order_id")
            if not order_id_str:
                logger.error("В метаданных нет order_id!")
                return JSONResponse(status_code=200, content={"ok": True})

            try:
                order_id = int(order_id_str)
            except ValueError:
                logger.error(f"Некорректный order_id в метаданных: {order_id_str}")
                return JSONResponse(status_code=200, content={"ok": True})

            engine = make_engine(Config.DB_PATH)
            with Session(engine) as sess:
                order = sess.get(Order, order_id)
                if not order:
                    logger.error(f"Заказ #{order_id} не найден по webhook")
                    return JSONResponse(status_code=200, content={"ok": True})

                # Защита от повторной обработки
                if order.status in (OrderStatus.PAID_FULL.value, OrderStatus.SHIPPED.value, OrderStatus.ARCHIVED.value):
                    logger.info(f"Заказ #{order_id} уже обработан, пропускаем")
                    return JSONResponse(status_code=200, content={"ok": True})

                # Обновляем статус
                kind = payment.metadata.get("payment_kind", "unknown")
                if kind == "full":
                    order.payment_kind = "full"
                    order.status = OrderStatus.PAID_FULL.value
                elif kind == "pre":
                    order.payment_kind = "pre"
                    order.status = OrderStatus.PAID_PARTIALLY.value
                elif kind == "rem":
                    order.payment_kind = "remainder"
                    order.status = OrderStatus.PAID_FULL.value
                else:
                    logger.warning(f"Неизвестный payment_kind: {kind}")

                # Сохраняем payment_id
                if not order.extra_data:
                    order.extra_data = {}
                order.extra_data["yookassa_payment_id"] = payment.id
                flag_modified(order, "extra_data")

                sess.commit()

                # В конец функции yookassa_webhook (внутри try, после sess.commit() где обновляется статус заказа)
                # Сброс состояний пользователя после успешной оплаты
                user = get_user_by_id(sess, order.user_id)
                if user:
                    reset_states(user, sess)
                    logger.info(f"Состояния пользователя {user.telegram_id} сброшены после оплаты заказа #{order.id}")

                # Уведомления
                await notify_admins_payment_success(order.id)
                await bot.send_message(
                    order.user_id,
                    f"✅ Оплата прошла успешно! Заказ <b>#{order.id}</b> принят в обработку.\n\n"
                    f"Статус обновлён автоматически.",
                    parse_mode="HTML",
                    reply_markup=kb_order_status(order)
                )

                logger.info(f"Успешно обработан payment.succeeded для заказа #{order.id} → {order.status}")

        return JSONResponse(status_code=200, content={"ok": True})

    except Exception:
        logger.exception("Критическая ошибка в yookassa_webhook")
        return JSONResponse(status_code=200, content={"ok": True})


# Секретный токен (придумай свой длинный, 32+ символов, сохрани в .env или здесь)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "fallback_secret_if_not_set")

WEBHOOK_PATH = "/webhook/telegram"


@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    logger.info(f"TG webhook attempt from IP: {request.client.host} | Headers: {dict(request.headers)}")

    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        logger.warning("Invalid TG secret token!")
        raise HTTPException(status_code=403, detail="Invalid secret token")

    try:
        json_data = await request.json()
        logger.debug(f"TG webhook payload: {json_data}")

        update = Update(**json_data)
        await dp.feed_update(bot, update)
        logger.info("TG update processed successfully")
        return {"ok": True}
    except ValueError as ve:
        logger.error(f"TG webhook JSON parse error: {ve}")
        return {"ok": False}
    except Exception as e:
        logger.exception(f"TG webhook critical error: {e}")
        return {"ok": False}


@app.on_event("startup")
async def on_startup():
    logger.debug("on_startup started")
    logger.info("=== FastAPI Startup: начало инициализации ===")
    logger.debug("BOT VERSION MARK: 2026-01-29 FINAL (webhook)")

    retries = 3
    engine = None
    while retries > 0:
        try:
            logger.debug(f"Attempt {4-retries}/3 to create engine")
            engine = make_engine(Config.DB_PATH)
            logger.debug("Engine created")

            logger.debug("Calling init_db")
            init_db(engine)
            logger.info("init_db выполнен (drop_all + create_all)")

            inspector = inspect(engine)
            tables = inspector.get_table_names()
            logger.debug(f"Tables after init_db: {tables}")

            logger.debug("Starting seed_data session")
            with Session(engine) as sess:
                try:
                    with open("INFO_FOR_DB/PROMOCODES/promocodes.txt", "r", encoding="utf-8") as f:
                        codes = [line.strip() for line in f if line.strip().isdigit() and len(line.strip()) == 3]
                    logger.debug(f"Loaded {len(codes)} codes")
                except FileNotFoundError as e:
                    logger.error(f"promocodes.txt not found: {e}")
                    codes = []

                logger.debug("Calling seed_data")
                seed_data(sess, anxiety_codes=codes)
                logger.debug("seed_data done, committing")
                sess.commit()
                logger.info("seed_data + commit выполнен")

            # Финальный чек
            inspector = inspect(engine)
            if not inspector.has_table("orders"):
                logger.error("Таблица orders НЕ создана!")
                raise RuntimeError("Таблица orders не создана после init_db!")
            logger.info("DB проверена: все таблицы на месте.")
            break

        except Exception as e:
            retries -= 1
            logger.exception(f"Ошибка инициализации DB (осталось попыток: {retries}): {e}")
            await asyncio.sleep(5)

    if retries == 0 or engine is None:
        logger.critical("Не удалось инициализировать БД после 3 попыток!")
        await notify_admin("❌ Критическая ошибка: DB не инициализирована!")

    logger.debug("Starting background tasks")
    await asyncio.sleep(2)
    asyncio.create_task(check_all_shipped_orders())
    asyncio.create_task(check_pending_timeouts())
    await check_channel_permissions()

    logger.debug("Setting webhook")
    webhook_url = f"https://bot.rehy.ru{WEBHOOK_PATH}"
    await bot.set_webhook(
        url=webhook_url,
        secret_token=WEBHOOK_SECRET,
        allowed_updates=dp.resolve_used_update_types(),
        drop_pending_updates=True
    )
    webhook_info = await bot.get_webhook_info()
    logger.info(f"Webhook установлен: {webhook_url}")
    logger.debug(f"Webhook info: {webhook_info.dict() if webhook_info else 'None'}")

    logger.info("=== FastAPI Startup завершён успешно ===")


@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Telegram webhook удалён при остановке")