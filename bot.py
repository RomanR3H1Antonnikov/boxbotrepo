import os
import re
import asyncio
import logging
import requests
from collections import defaultdict
from typing import Optional, Dict, List
from enum import Enum
from sqlalchemy.orm import Session
from sqlalchemy import select
from db.init_db import init_db, seed_data
from db.repo import (
    make_engine, get_or_create_user,
    get_user_by_id,
    create_order_db, get_user_orders_db
)
from db.models import Order
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

# ========== CONFIG ==========
USE_WEBHOOK = False
load_dotenv()


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
    "проспект", "просп.", "пр.", "пр-т", "пр-кт",
    "пер.", "переулок",
    "шоссе",
    "бульвар", "бул.",
    "пл.", "площадь",
    "наб.", "набережная",
    "тракт",
    "аллея",
]

# --- CDEK TEST CREDENTIALS ---
CDEK_ACCOUNT = os.getenv("CDEK_ACCOUNT")
CDEK_SECURE_PASSWORD = os.getenv("CDEK_SECURE_PASSWORD")

# Логируем сразу при старте — чтобы видеть, загрузились ли ключи
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger("box_bot")
logging.getLogger("aiogram.event").setLevel(logging.WARNING)

logger.info(f"CDEK_ACCOUNT загружен: {'Да' if CDEK_ACCOUNT else 'НЕТ'}")
logger.info(f"CDEK_SECURE_PASSWORD загружен: {'Да' if CDEK_SECURE_PASSWORD else 'НЕТ'}")

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


async def calculate_cdek_delivery_cost(pvz_code: str) -> Optional[dict]:
    """Возвращает dict: {'cost': int, 'period_min': int, 'period_max': int}"""
    token = await get_cdek_token()
    if not token:
        return None

    url = "https://api.edu.cdek.ru/v2/calculator/tariff"
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
    token = await get_cdek_token()
    if not token or not cdek_uuid:
        return None

    url = f"https://api.edu.cdek.ru/v2/orders/{cdek_uuid}"
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
    token = await get_cdek_token()
    if not token or not cdek_uuid:
        return None

    url = f"https://api.edu.cdek.ru/v2/orders/{cdek_uuid}"
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
    ADMIN_SET_READY = "admin:set_ready"
    ADMIN_SET_SHIPPED = "admin:set_shipped"
    ADMIN_SET_ARCHIVED = "admin:set_archived"
    ADMIN_SET_TRACK = "admin:set_track"

class OrderStatus(Enum):
    NEW = "new"
    PENDING = "pending"
    PREPAID = "prepaid"
    READY = "ready"
    PAID = "paid"
    SHIPPED = "shipped"
    ARCHIVED = "archived"
    ABANDONED = "abandoned"

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
    PRICE_RUB = 2990
    PREPAY_PERCENT = 30
    ADMIN_HELP_NICK = "@anbolshakowa"
    CODES_POOL = {
        "1002", "1347", "2589", "3761", "4923", "5178", "6354", "7490", "8632", "9714",
        "1286", "2439", "3591", "4725", "5863", "6917", "7048", "8251", "9376", "1432",
        "2567", "3789", "4910", "5123", "6345", "7578", "8790", "9012", "1234", "3456",
        "5678", "7890", "1023", "2345", "4567", "6789", "8901", "3210", "5432", "7654"
    }
    DEFAULT_PRACTICES = [
        "Дыхательная практика", "Зеркало", "Снять тревогу с тревоги",
        "Внутренний ребенок", "Антихрупкость", "Созидать жизнь", "Спокойный сон",
    ]
    PRACTICE_DETAILS = [
        {"duration": 40, "desc": "Единственное в своем теле, что ты можешь контролировать - это дыхание..."},
        {"duration": 15, "desc": "Когда ты есть у себя, когда ты чувствуешь опору в себе..."},
        {"duration": 15, "desc": "Теория тревожного состояния простым языком..."},
        {"duration": 16, "desc": "Когда восстанавливается связь с внутренним ребенком..."},
        {"duration": 15, "desc": "Перестать убегать от неопределенности жизни..."},
        {"duration": 15, "desc": "Энергию, расходовавшуюся на тревогу, направляем..."},
        {"duration": 16, "desc": "Отправляясь в царство Морфея в спокойнейшем состоянии..."},
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
    FAQ_ANSWERS = {
        "faq:q1": "Это комплект заботы о себе. Внутри - предметы, практики и маленькие сюрпризы, которые помогают снизить тревогу, восстановить ресурс и почувствовать опору.",
        "faq:q2": "Это не замена терапии, а мягкая поддержка. Консультация - это работа в диалоге со специалистом. А коробочка - ваш личный набор «здесь и сейчас», чтобы помочь себе в нужный момент.",
        "faq:q3": "Для тех, кто чувствует тревогу, усталость, потерю энергии, перегрузку делами. Подойдёт и тем, кто просто хочет ввести новые ритуалы заботы о себе.",
        "faq:q4": "1. Путеводитель на пути к равновесию\n2. 7 видео и аудио практик\n3. Баночка поддерживающих посланий\n4. Маска для практик со льном и лавандой\n5. Чай “Глоток тепла и спокойствия”\n6. Маркер для зеркала\n7. Личные послания от экспертов\n8. Вдохновляющее письмо в конверте",
        "faq:q5": "Практики разработаны пятью практикующими психологами. Каждый из них, используя собственный уникальный стиль, помогает супер объемно и результативно подойти к решению.",
        "faq:q6": "Откройте её в момент тревоги или когда хочется тепла. Выбирайте ритуал, заваривайте чай, доставайте фразу или выполняйте практику. Всё — в своём темпе.",
        "faq:q7": "Да! Практики и предметы рассчитаны на многократное использование. А баночка с фразами - это как маленькое объятие словами, к которой можно возвращаться.",
        "faq:q8": "От 2 минут (например, достать фразу поддержки) до 15–20 минут (практика или ритуал). Всё зависит от того, сколько у вас ресурса сейчас.",
        "faq:q9": "Актуальную цену можно посмотреть в разделе Инфо.",
        "faq:q10": "Нажмите кнопку «Заказать», бот поможет оформить заказ и доставку.",
        "faq:q11": "В среднем 3–7 дней, в зависимости от региона и службы доставки.",
        "faq:q12": "Конечно. В коробочку можно добавить послание для получателя, текст послания вы пишите в поле для комментариев.",
        "faq:q13": "Напишите в бот или свяжитесь с нами, мы восстановим доступ",
        "faq:q14": "Да! Уже готовим сезонные коллекции: новогоднюю, к 14 февраля, 23 февраля и 8 марта. Каждая со своей темой.",
        "faq:q15": "Конечно, можно. Они часто становятся отличным подарком близким.",
        "faq:q16": "Обычные наборы - это вещи. Наша коробочка - это опыт, смыслы, ответы. Она создана так, чтобы вы не просто получили предметы, а прожили поддержку, заботу и практику.",
        "faq:q17": "Пока доставка работает только по России. В будущем планируем расширение.",
        "faq:q18": "Тебя ждёт продолжение путешествие в закрытом чате (здесь нужна ссылка на чат), где в бессрочном доступе будут поддерживающая атмосфера, эфиры от мастеров и возможность делиться своими успехами и вдохновляться результатами близких по духу людей",
        "faq:q19": "Напишите в Telegram: @abolshakowa и @dmitrieva_live, мы ответим вам с 10:00 до 20:00 (gmt+3) в рабочие дни с понедельника по пятницу"
    }
    PAYMENT_TIMEOUT_SEC = 600

    # Склад в СДЭК (код города). Москва = 44, СПб = 137, Екат = 195 и т.д.
    CDEK_FROM_CITY_CODE = os.getenv("CDEK_FROM_CITY_CODE", "44")  # по умолчанию Москва
    CDEK_SHIPMENT_POINT_CODE = "MSK2296"

    # Вес и габариты коробки (можно вынести в .env)
    PACKAGE_WEIGHT_G = 370  # грамм
    PACKAGE_LENGTH_CM = 19
    PACKAGE_WIDTH_CM = 26
    PACKAGE_HEIGHT_CM = 8

    # Популярные города (для быстрого выбора)
    POPULAR_CITIES = {
        "Москва": "44",
        "Санкт-Петербург": "137",
        "Екатеринбург": "195",
        "Новосибирск": "157",
        "Казань": "138",
    }

# ========== ADMIN ==========
ADMIN_USERNAMES = {"@RE_HY"}
ADMIN_ID = 1049170524

# ========== BOOTSTRAP ==========
bot = Bot(
    Config.TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()
r = Router()
dp.include_router(r)

CODE_RE = re.compile(r"^\d{4}$")


async def create_cdek_order(order_id: int) -> bool:
    token = await get_cdek_token()
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
            "company": "ИП Романов Р. А.",
            "name": "Роман",
            "phones": [{"number": "+79999999999"}],
        },

        "recipient": {
            "name": user.full_name,
            "phones": [{
                "number": user.phone.replace("+", "").replace(" ", "").replace("-", "")
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

    url = "https://api.edu.cdek.ru/v2/orders"

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
    with Session(engine) as sess:
        order = sess.get(Order, order_id)
        if not order:
            return False

        order.extra_data["cdek_uuid"] = uuid
        order.track = uuid  # временно используем UUID как трек
        order.status = OrderStatus.SHIPPED.value
        sess.commit()

    logger.info(f"СДЭК: ЗАКАЗ #{order_id} ПРИНЯТ | UUID: {uuid}")

    await notify_admin(
        f"🚚 Заказ #{order_id} успешно принят СДЭК\n"
        f"UUID: {uuid}\n"
        f"Трек-номер придёт автоматически."
    )

    return True



def validate_data(full_name: str, phone: str, email: str) -> tuple[bool, str]:
    if not full_name or not full_name.strip():
        return False, "Отсутствует ФИО."
    if not re.match(r"^[А-ЯЁ][а-яё]+(\s+[А-ЯЁ][а-яё]+)+$", full_name.strip()):
        return False, "ФИО: Имя и Фамилия с заглавной буквы, без отчества и лишних пробелов."
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
    if uid == bot.id:  # Игнорируем сообщения от самого бота
        return False
    username = user.username
    logger.info(f"Checking admin access: uid={uid}, username={username}")
    if username and f"@{username}" in ADMIN_USERNAMES:
        logger.info("Access granted via username")
        return True
    if uid == ADMIN_ID:
        logger.info("Access granted via ID")
        return True
    logger.info("Access denied")
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer("Доступ запрещён. Только для администраторов.")
    return False

async def notify_admin(text: str):
    try:
        await bot.send_message(ADMIN_ID, text)
    except Exception as e:
        logger.error(f"Admin notify failed: {e}")

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

async def notify_admins_payment_success(order: Order):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"✅ Предоплата #{order.id} получена\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Статус: {order.status}"
    )

async def notify_admins_order_ready(order: Order):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"📦 Заказ #{order.id} собран\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Статус: {order.status}"
    )

async def notify_admins_payment_remainder(order: Order):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"💸 Заказ #{order.id} полностью оплачен\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Статус: {order.status}"
    )

async def notify_admins_order_shipped(order: Order):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"🚚 Заказ #{order.id} отправлен\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Трек: {order.track}\n"
        f"Статус: {order.status}"
    )

async def notify_admins_order_archived(order: Order):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    await notify_admin(
        f"🗄 Заказ #{order.id} заархивирован\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
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



async def notify_client_order_ready(order: Order, message: Message):
    text = format_client_order_info(order)
    await message.answer(
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=kb_ready_message(order)
    )

async def notify_client_order_shipped(order: Order, message: Message):
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
    rows = [[{"text": f"{i+1}. {t}", "callback_data": f"practice:{i}"}] for i, t in enumerate(titles)]
    rows.append([{"text": "В меню", "callback_data": CallbackData.MENU.value}])
    return create_inline_keyboard(rows)

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
        [{"text": "Изменить адрес доставки", "callback_data": f"change_addr:{order.id}"}],
        [{"text": "Статус заказа", "callback_data": f"order:{order.id}"}],
    ])

def kb_order_status(order: Order) -> InlineKeyboardMarkup:
    buttons = []

    # Кнопка отслеживания (всегда, если есть трек)
    if order.track:
        buttons.append([{
            "text": "Отследить посылку",
            "url": f"https://www.cdek.ru/ru/tracking?order_id={order.track}"
        }])

    # Если заказ READY — показываем оплату остатка
    if order.status == OrderStatus.READY.value:
        buttons.append([{"text": "Оплатить остаток", "callback_data": f"pay:rem:{order.id}"}])

    buttons.append([{"text": "Обновить статус", "callback_data": f"order:{order.id}"}])
    buttons.append([{"text": "В меню", "callback_data": CallbackData.MENU.value}])

    return create_inline_keyboard(buttons)

def kb_orders_list(order_ids: List[int]) -> InlineKeyboardMarkup:
    rows = [[{"text": f"Заказ #{oid}", "callback_data": f"order:{oid}"}] for oid in order_ids]
    rows.append([
        {"text": "Оформить заказ", "callback_data": CallbackData.CHECKOUT_START.value}
    ])
    rows.append([{"text": "В меню", "callback_data": CallbackData.MENU.value}])
    return create_inline_keyboard(rows)

def kb_change_contact() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Да", "callback_data": CallbackData.CHANGE_CONTACT_YES.value}],
        [{"text": "Нет", "callback_data": CallbackData.CHANGE_CONTACT_NO.value}],
        [{"text": "Назад", "callback_data": CallbackData.GALLERY.value}],
    ])

def kb_admin_panel() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Заказы для сборки", "callback_data": CallbackData.ADMIN_ORDERS_PREPAID.value}],
        [{"text": "Заказы, ожидающие дооплаты", "callback_data": CallbackData.ADMIN_ORDERS_READY.value}],
        [{"text": "Отправленные заказы", "callback_data": CallbackData.ADMIN_ORDERS_SHIPPED.value}],
        [{"text": "Архив заказов", "callback_data": CallbackData.ADMIN_ORDERS_ARCHIVED.value}],
        [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
    ])

def kb_admin_orders(orders: List[Order]) -> InlineKeyboardMarkup:
    rows = []
    for order in orders:
        rows.append([
            {"text": f"Заказ #{order.id} ({order.status})", "callback_data": f"admin:order:{order.id}"}
        ])
    rows.append([{"text": "Назад", "callback_data": CallbackData.ADMIN_PANEL.value}])
    return create_inline_keyboard(rows)

def kb_admin_order_actions(order: Order) -> InlineKeyboardMarkup:
    buttons = []
    if order.status == OrderStatus.PREPAID.value:
        buttons.append([{"text": "Готов к отправке", "callback_data": f"{CallbackData.ADMIN_SET_READY.value}:{order.id}"}])
    if order.status in [OrderStatus.READY.value, OrderStatus.PAID.value] and not order.track:
        if order.extra_data.get("manual_pvz", False):  # ← ДОБАВИТЬ УСЛОВИЕ
            buttons.append([{"text": "Ввести трек вручную", "callback_data": f"{CallbackData.ADMIN_SET_TRACK.value}:{order.id}"}])
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
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        u = get_user_by_id(sess, order.user_id)
        full_name = u.full_name if u else "Неизвестно"
    pvz_code = order.extra_data.get("pvz_code", "—")
    gift = order.extra_data.get("gift_message", "—")
    gift_text = f"Послание в подарок:\n{gift}\n\n" if gift else ""
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
    # Русские названия статусов
    status_map = {
        OrderStatus.NEW.value: "🆕 Новый заказ",
        OrderStatus.PREPAID.value: "✅ Предоплачен (30%)",
        OrderStatus.READY.value: "📦 Готов к отправке — ждём дооплату",
        OrderStatus.PAID.value: "💳 Полностью оплачен",
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
    delivery_cost = order.extra_data.get("delivery_cost", 0)
    period = order.extra_data.get("delivery_period", "3–7")
    lines += [
        "",
        "🚚 <b>Доставка:</b> ПВЗ СДЭК",
        f"💸 Стоимость доставки: <b>{delivery_cost} ₽</b>",
        f"⏳ Срок доставки: ≈ <b>{period} дн.</b>",
        f"📍 <b>Адрес ПВЗ:</b>\n{order.address}",
    ]

    # Послание
    gift = order.extra_data.get("gift_message")
    if gift:
        lines += [
            "",
            "💌 <b>Личное послание в подарок:</b>",
            f"<i>{gift}</i>",
        ]

    # Оплата — подробнее
    total = order.total_price
    prepay_amount = (total * Config.PREPAY_PERCENT + 99) // 100
    remainder = total - prepay_amount

    lines += ["", "💳 <b>Оплата:</b>"]

    if order.status == OrderStatus.NEW.value:
        lines += [
            f"К оплате: <b>{total} ₽</b>",
            f"   • Вариант: предоплата {Config.PREPAY_PERCENT}% ({prepay_amount} ₽)",
            f"   • Вариант: полная оплата ({total} ₽)",
        ]
    elif order.status == OrderStatus.PREPAID.value:
        lines += [
            f"✅ Предоплата получена: {prepay_amount} ₽",
            f"🔄 Остаток к оплате: <b>{remainder} ₽</b>",
        ]
    elif order.status == OrderStatus.READY.value:
        lines += [
            f"✅ Предоплата: {prepay_amount} ₽",
            f"Ожидаем дооплату: <b>{remainder} ₽</b>",
        ]
    elif order.status in [OrderStatus.PAID.value, OrderStatus.SHIPPED.value, OrderStatus.ARCHIVED.value]:
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
    if src and src.video:
        await message.answer(f"file_id видео: {src.video.file_id}")
    else:
        await message.answer("Сделайте /grab_id ответом на видео.")

@r.message(Command("menu"))
async def cmd_menu(message: Message):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, message.from_user.id)
        if user:
            user.pvz_for_order_id = None
            sess.commit()
    await message.answer("Выбери действие:", reply_markup=kb_main())

@r.message(Command("admin_panel"))
async def cmd_admin_panel(message: Message):
    if not await is_admin(message):
        return
    await message.answer("Панель администратора:", reply_markup=kb_admin_panel())

@r.callback_query(F.data == CallbackData.MENU.value)
async def cb_menu(cb: CallbackQuery):
    logger.info(f"Menu callback: user_id={cb.from_user.id}, data={cb.data}")
    await edit_or_send(cb.message, "Выбери действие:", kb_main())
    await cb.answer()

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
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        if not user.is_authorized:
            await edit_or_send(cb.message, "Пожалуйста, авторизуйтесь.", kb_cabinet_unauth())
            await cb.answer()
            return

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
        "19. Что делать, если у меня остались вопросы?\nНапишите в Telegram: @abolshakowa и @dmitrieva_live, мы ответим вам с 10:00 до 20:00 (gmt+3) в рабочие дни с понедельника по пятницу.\n",
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
async def cb_practices(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return
        sess.commit()
    if not user.is_authorized:
        await edit_or_send(cb.message, "Пожалуйста, авторизуйтесь.", kb_cabinet_unauth())
        await cb.answer(); return
    if not user.practices:
        await edit_or_send(cb.message, "У вас нет практик.\nАктивируйте код или закажите коробочку.", kb_empty_practices())
        await cb.answer(); return
    await edit_or_send(cb.message, "Твои практики:", kb_practices_list(user.practices))
    await cb.answer()

@r.callback_query(F.data.startswith("practice:"))
async def cb_open_practice(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return
    parts = cb.data.split(":")
    if len(parts) >= 3 and parts[1] == "play":
        await cb.answer(); return
    try:
        idx = int(parts[1])
    except:
        await cb.message.answer("Ошибка.", reply_markup=kb_practices_list(user.practices))
        await cb.answer(); return
    if not (user.is_authorized and 0 <= idx < len(user.practices)):
        await cb.message.answer("Доступ ограничен.", reply_markup=kb_practices_list(user.practices))
        await cb.answer(); return
    title = user.practices[idx]
    note_id = Config.PRACTICE_NOTES.get(idx)
    if note_id:
        try:
            await cb.message.answer_video_note(note_id)
        except Exception as e:
            logger.error(f"Practice video error: {e}")
    await send_practice_intro(cb.message, idx, title)
    await cb.message.answer(f"<b>Практика:</b> {title}\n\nНачинаем?", reply_markup=kb_practice_card(idx))
    sess.commit()
    await cb.answer()

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
        await cb.answer(); return
    # user.awaiting_code = True
    await cb.message.answer("Введите <b>код с карточки</b>:",
                            reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.CABINET.value}]]))
    sess.commit()
    await cb.answer()

# ========== CHECKOUT ==========
@r.callback_query(F.data == CallbackData.CHECKOUT_START.value)
async def cb_checkout_start(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
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
                f"Проверьте данные:\n• ФИО: {user.full_name}\n• Телефон: {user.phone}\n• Email: {user.email}\n\nХотите изменить?",
                reply_markup=kb_change_contact()
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
    if cb.data == CallbackData.CHANGE_CONTACT_YES.value:
        await cb.message.answer(
            "Введите новые данные:\nИмя Фамилия\n+7XXXXXXXXXX\nemail@example.com",
            reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.GALLERY.value}]])
        )
    else:
        user.awaiting_pvz_address = True
        sess.add(user)
        sess.commit()
        await cb.message.answer(
            "Введите адрес ПВЗ (например: «Профсоюзная, 93»):",
            reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.GALLERY.value}]])
        )
    sess.commit()
    await cb.answer()


# ========== УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК "НАЗАД" И ПРОСТЫХ НАВИГАЦИОННЫХ КНОПОК ==========
@r.callback_query(F.data.in_(["menu", "gallery", "cabinet", "faq", "team", "practices", "orders"]))
async def cb_simple_navigation(cb: CallbackQuery):
    data = cb.data
    try:
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
        elif data == "practices":
            await cb_practices(cb)
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
        "Введите адрес ПВЗ (например: «Профсоюзная, 93»):",
        reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.GALLERY.value}]])
    )
    await cb.answer()

async def show_review(msg: Message, order: Order):
    await edit_or_send(msg, format_order_review(order), kb_review(order))

# ========== PAYMENT ==========
@r.callback_query(F.data.startswith("pay:"))
async def cb_pay(cb: CallbackQuery):
    parts = (cb.data or "").split(":")
    if len(parts) != 3:
        await cb.answer("Ошибка оплаты", show_alert=True)
        return

    kind = parts[1]   # full | pre | rem
    try:
        oid = int(parts[2])
    except ValueError:
        await cb.answer("Ошибка заказа", show_alert=True)
        return

    lock = get_payment_lock(oid)

    if lock.locked():
        await cb.answer("Оплата уже обрабатывается, подождите…", show_alert=True)
        return

    async with lock:
        try:
            engine = make_engine(Config.DB_PATH)
            need_cdek_create = False

            with Session(engine) as sess:
                order = sess.get(Order, oid)

                if not order or order.user_id != cb.from_user.id:
                    await cb.answer("Заказ не найден", show_alert=True)
                    return

                if order.status in (
                    OrderStatus.PAID.value,
                    OrderStatus.SHIPPED.value,
                    OrderStatus.ARCHIVED.value
                ):
                    await cb.answer("Этот заказ уже оплачен", show_alert=True)
                    return

                # Гарантия цены
                if order.total_price_kop == 0:
                    delivery_cost = (order.extra_data or {}).get("delivery_cost", 590)
                    total = Config.PRICE_RUB + delivery_cost
                    prepay = (total * Config.PREPAY_PERCENT + 99) // 100
                    order.prepay_amount = prepay * 100
                    order.remainder_amount = (total - prepay) * 100

                if kind == "full":
                    if order.status not in (OrderStatus.NEW.value, OrderStatus.PREPAID.value, OrderStatus.READY.value):
                        await cb.answer("Нельзя оплатить этот заказ", show_alert=True)
                        return

                    order.payment_kind = "full"
                    order.status = OrderStatus.PAID.value
                    need_cdek_create = True

                elif kind == "pre":
                    if order.status != OrderStatus.NEW.value:
                        await cb.answer("Предоплата уже внесена", show_alert=True)
                        return

                    order.payment_kind = "pre"
                    order.status = OrderStatus.PREPAID.value

                elif kind == "rem":
                    if order.status not in (OrderStatus.PREPAID.value, OrderStatus.READY.value):
                        await cb.answer("Этот заказ нельзя дооплатить", show_alert=True)
                        return

                    order.payment_kind = "remainder"
                    order.status = OrderStatus.PAID.value
                    need_cdek_create = True

                else:
                    await cb.answer("Ошибка типа оплаты", show_alert=True)
                    return

                sess.commit()

                # Уведомления сразу после коммита (order ещё валиден)
                if kind == "full":
                    await notify_admins_payment_success(order)
                    await cb.message.answer(
                        "Полная оплата получена! ❤️\n\n"
                        f"Заказ <b>#{order.id}</b> передаётся в СДЭК.",
                        reply_markup=kb_order_status(order)
                    )

                elif kind == "pre":
                    await notify_admins_payment_success(order)
                    await cb.message.answer(
                        "Предоплата получена ❤️\n\n"
                        f"Заказ <b>#{order.id}</b> принят в сборку.",
                        reply_markup=kb_order_status(order)
                    )

                elif kind == "rem":
                    await notify_admins_payment_remainder(order)
                    await cb.message.answer(
                        "Дооплата получена ❤️\n\n"
                        f"Заказ <b>#{order.id}</b> передаётся в СДЭК.",
                        reply_markup=kb_order_status(order)
                    )

            # Создание заказа в СДЭК — ВНЕ сессии
            if need_cdek_create:
                success = await create_cdek_order(oid)

                if success:
                    # Перечитываем свежий заказ для уведомления админу
                    with Session(engine) as sess:
                        fresh_order = sess.get(Order, oid)
                        if fresh_order:
                            await notify_admins_order_shipped(fresh_order)
                else:
                    # Откат статуса при ошибке СДЭК
                    with Session(engine) as sess:
                        order_rollback = sess.get(Order, oid)
                        if order_rollback:
                            order_rollback.status = OrderStatus.READY.value
                            sess.commit()
                    await notify_admin(f"⚠️ СДЭК не принял заказ #{oid}, требуется внимание")

            await cb.answer()

        except Exception as e:
            logger.exception("Ошибка оплаты")
            await notify_admin(f"❌ Ошибка оплаты #{oid}\n{e}")
            await cb.answer("Ошибка при оплате", show_alert=True)


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
    try:
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

            sess.commit()
    except Exception as e:
        logger.error(f"DB error in cb_orders_list: {e}")
        await cb.answer("Временная ошибка сервера. Попробуйте позже.", show_alert=True)
        return

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
            "Введите новый адрес ПВЗ (например: «Профсоюзная, 93»):",
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
        logger.info("Admin access denied")
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    orders = get_all_orders_by_status(OrderStatus.PREPAID.value)
    if not orders:
        await edit_or_send(cb.message, "Нет заказов для сборки.", kb_admin_panel())
    else:
        await edit_or_send(cb.message, "Заказы для сборки:", kb_admin_orders(orders))
    await cb.answer()

@r.callback_query(F.data == CallbackData.ADMIN_ORDERS_READY.value)
async def cb_admin_orders_ready(cb: CallbackQuery):
    logger.info(f"Orders ready callback: user_id={cb.from_user.id}, data={cb.data}")
    if not await is_admin(cb):
        logger.info("Admin access denied")
        await cb.answer("Доступ запрещён", show_alert=True)
        return
    orders = get_all_orders_by_status(OrderStatus.READY.value)
    if not orders:
        await edit_or_send(cb.message, "Нет заказов с дооплатой или готовых к отправке.", kb_admin_panel())
    else:
        await edit_or_send(cb.message, "Заказы с дооплатой или готовые к отправке:", kb_admin_orders(orders))
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
    try:
        oid = int(cb.data.split(":")[2])
        order = get_order_admin(oid)
        if not order:
            await cb.answer("Заказ не найден", show_alert=True)
            return
        if not await is_admin(cb):
            logger.info("Admin access denied")
            await cb.answer("Доступ запрещён", show_alert=True)
            return
        await edit_or_send(cb.message, format_order_admin(order), kb_admin_order_actions(order))
        await cb.answer()
    except Exception as e:
        logger.error(f"Admin order details error: {e}")
        await notify_admin(f"❌ Ошибка просмотра заказа #{oid}")
        await cb.answer("Ошибка", show_alert=True)

@r.callback_query(F.data.startswith(CallbackData.ADMIN_SET_READY.value))
async def cb_admin_set_ready(cb: CallbackQuery):
    logger.info(f"Set ready callback: user_id={cb.from_user.id}, data={cb.data}")
    try:
        oid = int(cb.data.split(":")[2])  # admin:set_ready:1

        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            order = sess.get(Order, oid)
            if not order or order.status != OrderStatus.PREPAID.value:
                await cb.answer("Нельзя перевести в готовность", show_alert=True)
                return

            if not await is_admin(cb):
                logger.info("Admin access denied")
                await cb.answer("Доступ запрещён", show_alert=True)
                return

            order.status = OrderStatus.READY.value
            sess.commit()

        await notify_admins_order_ready(order)
        await notify_client_order_ready(order, cb.message)
        await edit_or_send(cb.message, f"Заказ #{oid} готов к отправке.", kb_admin_panel())
        await cb.answer()

    except Exception as e:
        logger.error(f"Admin set ready error: {e}")
        await notify_admin(
            f"❌ Ошибка перевода заказа #{oid if 'oid' in locals() else 'неизвестный'} в готовность"
        )
        await cb.answer("Ошибка", show_alert=True)



@r.callback_query(F.data.startswith(CallbackData.ADMIN_SET_ARCHIVED.value))
async def cb_admin_set_archived(cb: CallbackQuery):
    logger.info(f"Set archived callback: user_id={cb.from_user.id}, data={cb.data}")
    try:
        oid = int(cb.data.split(":")[2])  # Извлекаем oid из третьей части (admin:set_archived:1)
        order = get_order_admin(oid)
        if not order or order.status not in [OrderStatus.PAID.value, OrderStatus.SHIPPED.value]:
            await cb.answer("Нельзя архивировать заказ", show_alert=True)
            return
        if not await is_admin(cb):
            logger.info("Admin access denied")
            await cb.answer("Доступ запрещён", show_alert=True)
            return
        order.status = OrderStatus.ARCHIVED.value
        await notify_admins_order_archived(order)
        await edit_or_send(cb.message, f"Заказ #{oid} заархивирован.", kb_admin_panel())
        await cb.answer()
    except Exception as e:
        logger.error(f"Admin set archived error: {e}")
        await notify_admin(f"❌ Ошибка архивирования заказа #{oid if 'oid' in locals() else 'неизвестный'}")
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
        user.awaiting_manual_pvz = False

    await cb.message.edit_text(
        "Введите адрес ПВЗ ещё раз (например: Барклая, 5А):",
        reply_markup=create_inline_keyboard([
            [{"text": "Отмена", "callback_data": CallbackData.MENU.value}]
        ])
    )
    sess.commit()
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
    # ===== 1. БЕЗОПАСНО парсим callback_data =====
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
        # ===== 2. Загружаем пользователя =====
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        # ===== 3. Проверка списка ПВЗ =====
        if not user.temp_pvz_list or not (0 <= idx < len(user.temp_pvz_list)):
            await cb.answer("Список ПВЗ устарел — введите адрес заново", show_alert=True)
            return

        pvz = user.temp_pvz_list[idx]

        # ===== 4. Защита от устаревших кнопок =====
        current_code = pvz.get("code")
        if str(current_code) != str(old_code):
            await cb.answer("Эта кнопка устарела — выберите ПВЗ заново", show_alert=True)
            return

        # ===== 5. Защита от повторного выбора =====
        if user.pvz_for_order_id is not None:
            await cb.answer("ПВЗ уже выбран. Продолжайте оформление.", show_alert=True)
            return

        # ===== 6. Парсим код ПВЗ =====
        raw_code = pvz.get("code")
        if isinstance(raw_code, str) and raw_code.startswith("MSK"):
            real_code = int(raw_code.replace("MSK", ""))
        elif isinstance(raw_code, int):
            real_code = raw_code
        elif isinstance(raw_code, str):
            real_code = int("".join(filter(str.isdigit, raw_code)))
        else:
            await cb.answer("Ошибка кода ПВЗ", show_alert=True)
            return

        # ===== 7. city_code с fallback =====
        city_code = pvz.get("location", {}).get("code") or Config.CDEK_FROM_CITY_CODE
        city_code = str(city_code)

        full_address = pvz["location"]["address_full"]
        work_time = pvz.get("work_time") or "Пн–Пт 10:00–20:00, Сб–Вс 10:00–18:00"

        # ===== 8. Сохраняем выбранный ПВЗ =====
        user.temp_selected_pvz = {
            "code": real_code,
            "city_code": city_code,
            "address": full_address,
            "work_time": work_time
        }

        # ===== 9. Считаем доставку =====
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

        # ===== 10. Создаём заказ =====
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

        # ===== 11. Фиксируем, что заказ начат =====
        user.pvz_for_order_id = order_id
        user.awaiting_gift_message = False
        user.temp_gift_order_id = None

        sess.commit()

    # ===== 12. UI (вне сессии) =====
    await edit_or_send(
        cb.message,
        f"<b>ПВЗ сохранён!</b>\n\n"
        f"{full_address}\n"
        f"Режим работы: {work_time}\n\n"
        f"Доставка: <b>{delivery_cost} ₽</b>\n"
        f"Срок: <b>≈ {period_text} дн.</b>\n\n"
        f"<b>Итого: {total} ₽</b>"
    )

    await cb.answer("Готово!")

    user.temp_gift_order_id = order_id

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

        # ищем последний активный заказ
        orders = get_user_orders_db(sess, cb.from_user.id)
        order = next(
            (o for o in reversed(orders or []) if o.status == OrderStatus.NEW.value),
            None
        )

        if not order:
            user.awaiting_gift_message = False
            user.temp_gift_order_id = None
            sess.commit()

            await cb.answer("Нет активного заказа", show_alert=True)
            return

        # защита от повторного нажатия
        if user.awaiting_gift_message and user.temp_gift_order_id == order.id:
            await cb.answer("Вы уже вводите послание", show_alert=True)
            return

        # фиксируем состояние
        user.awaiting_gift_message = True
        user.temp_gift_order_id = order.id
        sess.commit()

    await cb.message.edit_text(
        "✍️ Напишите текст послания (до 300 символов):",
        reply_markup=create_inline_keyboard([
            [{"text": "Отмена", "callback_data": "gift:cancel"}]
        ])
    )
    await cb.answer()



@r.callback_query(F.data == "gift:no")
async def cb_gift_no(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)

    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        order_id = user.temp_gift_order_id

        if not order_id:
            await cb.answer("Хорошо, переходим к оплате")
            return

        order = sess.get(Order, order_id)
        if not order or order.user_id != cb.from_user.id:
            user.awaiting_gift_message = False
            user.temp_gift_order_id = None
            sess.commit()

            await cb.answer("Заказ недоступен", show_alert=True)
            return

        # закрываем состояние
        user.awaiting_gift_message = False
        user.temp_gift_order_id = None
        sess.commit()

    await cb.message.edit_text("Ок, без послания.", reply_markup=None)
    await send_payment_keyboard(cb.message, order)
    await cb.answer("Хорошо, переходим к оплате")


async def send_payment_keyboard(msg: Message, order):
    total = order.total_price_kop // 100
    prepay = (total * Config.PREPAY_PERCENT + 99) // 100

    await msg.answer(
        f"<b>Оплата заказа #{order.id}</b>\n\n"
        f"Итого: <b>{total} ₽</b>\n"
        f"• Предоплата 30% = {prepay} ₽\n"
        f"• Остаток = {total - prepay} ₽",
        reply_markup=create_inline_keyboard([
            [{"text": f"Оплатить 100% ({total} ₽)", "callback_data": f"pay:full:{order.id}"}],
            [{"text": f"Предоплата 30% ({prepay} ₽)", "callback_data": f"pay:pre:{order.id}"}],
            [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
        ])
    )


@r.callback_query(F.data == "gift:cancel")
async def cb_gift_cancel(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)

    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        order_id = user.temp_gift_order_id
        if not order_id:
            await cb.answer("Хорошо", show_alert=False)
            return

        order = sess.get(Order, order_id)

        user.awaiting_gift_message = False
        user.temp_gift_order_id = None
        sess.commit()

    await cb.message.edit_text("Ок, без послания.", reply_markup=None)
    if order:
        await send_payment_keyboard(cb.message, order)
    await cb.answer()



@r.callback_query(F.data == "pvz_manual")
async def cb_pvz_manual(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

        user.awaiting_pvz_address = False
        user.awaiting_manual_pvz = True
        sess.add(user)
        sess.commit()

    await cb.message.edit_text(
        "Напиши код ПВЗ (например, MSK123) или полный адрес пункта выдачи так, как он указан у СДЭК.\n\n"
        "Мы оформим заказ на этот пункт.",
        reply_markup=create_inline_keyboard([
            [{"text": "Назад к списку ПВЗ", "callback_data": "pvz_backlist"}],
            [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
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
            reply_markup=create_inline_keyboard([
                [{"text": f"Оплатить 100% ({total} ₽)", "callback_data": f"pay:full:{order.id}"},
                 {"text": f"Предоплата {Config.PREPAY_PERCENT}% ({prepay} ₽)", "callback_data": f"pay:pre:{order.id}"}],
                [{"text": "Назад", "callback_data": CallbackData.GALLERY.value}],
            ])
        )
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

        # ===== 1. ПОДАРОЧНОЕ ПОСЛАНИЕ =====
        if user.awaiting_gift_message:
            order_id = user.temp_gift_order_id

            if not order_id:
                user.awaiting_gift_message = False
                sess.commit()
                await message.answer("Послание больше нельзя добавить.", reply_markup=kb_main())
                return

            order = sess.get(Order, order_id)

            if not order or order.user_id != user.telegram_id or order.status != OrderStatus.NEW.value:
                user.awaiting_gift_message = False
                user.temp_gift_order_id = None
                sess.commit()

                await message.answer(
                    "Послание больше нельзя добавить — заказ недоступен.",
                    reply_markup=kb_main()
                )
                return

            if not text:
                await message.answer("Послание не может быть пустым.")
                return

            if len(text) > 300:
                await message.answer("Максимум 300 символов.")
                return

            order.extra_data["gift_message"] = text
            user.awaiting_gift_message = False
            user.temp_gift_order_id = None
            sess.commit()

            await message.answer("💌 Послание сохранено!")
            await send_payment_keyboard(message, order)
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
            if not order or order.status not in [OrderStatus.READY.value, OrderStatus.PAID.value]:
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

            await notify_admins_order_shipped(order)
            await notify_client_order_shipped(order, message)
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
            user.awaiting_pvz_address = False
            sess.commit()

            await message.answer("Ищу ближайшие ПВЗ СДЭК...")

            pvz_list = await find_best_pvz(text, city="Москва")
            if not pvz_list:
                await message.answer("Не нашёл ПВЗ. Попробуйте другой адрес.")
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
    elif text in {"мои практики", "практики"}:
        await cb_practices(type("obj", (), {"from_user": message.from_user, "message": message, "answer": lambda *a, **k: None, "data": ""})())
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
            "Использование: /admin <действие> [order_id] [track]\n"
            "Действия: list, ready, shipped, archived"
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
                    OrderStatus.PREPAID.value: "prepaid",
                    OrderStatus.READY.value: "ready",
                    OrderStatus.PAID.value: "paid",
                    OrderStatus.SHIPPED.value: "shipped",
                    OrderStatus.ARCHIVED.value: "archived",
                }.get(o.status, o.status)

            rows = [f"#{o.id}: {tag(o)} | {o.address or '—'} | user_{o.user_id}" for o in all_orders]
            await message.answer("Заказы:\n" + "\n".join(rows[:50]))  # лимит, чтобы не спамить

        elif action in ["ready", "shipped", "archived"]:
            if not args or not args[0].isdigit():
                await message.answer(f"Укажите order_id. Пример: /admin {action} 1")
                return

            order_id = int(args[0])
            order = sess.get(Order, order_id)
            if not order:
                await message.answer(f"Заказ #{order_id} не найден.")
                return

            if action == "ready":
                if order.status != OrderStatus.PREPAID.value:
                    await message.answer("Заказ не в статусе предоплаты.")
                    return
                order.status = OrderStatus.READY.value
                await notify_admins_order_ready(order)
                await notify_client_order_ready(order, message)
                await message.answer(f"Заказ #{order_id} переведён в READY")

            elif action == "shipped":
                track = args[1] if len(args) > 1 else None
                if not track:
                    await message.answer("Укажите трек-номер: /admin shipped 1 ТРЕК123")
                    return
                if order.status not in [OrderStatus.READY.value, OrderStatus.PAID.value]:
                    await message.answer("Заказ не готов к отправке.")
                    return
                order.status = OrderStatus.SHIPPED.value
                # предположим, что в модели Order есть поле track (строка)
                order.track = track
                await notify_admins_order_shipped(order)
                await notify_client_order_shipped(order, message)
                await message.answer(f"📦 Заказ #{order_id} отправлен! Трек: {track}")

            elif action == "archived":
                if order.status not in [OrderStatus.PAID.value, OrderStatus.SHIPPED.value]:
                    await message.answer("Заказ не может быть заархивирован.")
                    return
                order.status = OrderStatus.ARCHIVED.value
                await notify_admins_order_archived(order)
                await message.answer(f"🗄 Заказ #{order_id} заархивирован")

            sess.commit()

        else:
            await message.answer("Неизвестное действие. Доступно: list, ready, shipped, archived")

# ========== НОВЫЕ ФУНКЦИИ СДЭК ==========

async def get_cdek_pvz_list(address_query: str, city: str = None, limit: int = 10) -> List[dict]:
    token = await get_cdek_token()
    if not token:
        logger.error("Нет токена для поиска ПВЗ")
        return []

    url = "https://api.edu.cdek.ru/v2/deliverypoints"
    params = {
        "address": address_query.strip(),
        "type": "PVZ",
        "limit": limit
    }
    if city:
        params["city"] = city

    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = await asyncio.to_thread(requests.get, url, params=params, headers=headers, timeout=15)
        if resp.status_code == 200:
            points = resp.json()
            logger.info(f"Найдено {len(points)} ПВЗ по запросу '{address_query}'")
            return points
        else:
            logger.warning(f"Ошибка поиска ПВЗ: {resp.status_code} {resp.text}")
            return []
    except Exception as e:
        logger.error(f"Исключение при поиске ПВЗ: {e}")
        return []


def _shorten_address(address: str) -> str:
    if not address:
        return ""

    # Пример: "г Москва, ул Барклая, д 7 к 1" → "ул Барклая 7 к 1"
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) < 2:
        return address[:50]

    # Ищем часть с улицей
    street_part = ""
    house_part = parts[-1]

    for p in parts:
        if any(kw in p.lower() for kw in STREET_KEYWORDS + ["барклая", "ленинский", "профсоюзная"]):
            street_part = p
            break

    # Очищаем дом от лишнего
    house_clean = house_part.split("стр.")[0].split("лит")[0].strip(" ,.")

    result = f"{street_part} {house_clean}".strip()
    return result if result else address.split(",", 1)[-1].strip()


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


def _make_exact_matcher(address_query: str):
    """
    Возвращает функцию, которая проверяет, совпадает ли адрес ПВЗ
    с домом из запроса (по улице + номеру дома).
    """
    m = _ADDR_RE.match(address_query.strip())
    if not m:
        return lambda addr: False

    street_q = m.group("street").strip().lower()
    house_q = m.group("house").strip().lower()

    # возьмём первые «основные» слова
    street_main = street_q.split()[0]
    house_main = re.split(r"[, ]", house_q)[0]

    def matcher(addr: str) -> bool:
        al = (addr or "").lower()
        return street_main in al and house_main in al

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

async def find_best_pvz(address_query: str, city: str = None, limit: int = 10) -> List[dict]:
    variants = _normalize_address_variants(address_query)
    logger.info(f"Варианты адреса для поиска ПВЗ: {variants}")

    all_points: dict[str, dict] = {}

    for idx, q in enumerate(variants):
        pts = await get_cdek_pvz_list(q, city=city, limit=50)
        logger.info(f"Вариант #{idx+1}: '{q}' → {len(pts)} ПВЗ")
        for p in pts:
            code = str(p.get("code") or "") + "|" + (p.get("uuid") or "")
            if code not in all_points:
                all_points[code] = p

    if not all_points:
        return []

    points = list(all_points.values())

    # --- помечаем ПВЗ с точным совпадением дома ---
    q_street, q_house = _extract_street_house(address_query)
    q_key = _addr_key(q_street, q_house)

    if q_key:
        for p in points:
            loc = p.get("location") or {}
            addr = loc.get("address_full") or loc.get("address") or ""
            p_street, p_house = _extract_street_house(addr)
            if _addr_key(p_street, p_house) == q_key:
                p["_amv_exact"] = True

    def _dist(p: dict) -> int:
        d = p.get("distance")
        return int(d) if isinstance(d, (int, float)) else 10**9

    # точное совпадение — всегда раньше, потом по distance
    points.sort(key=lambda p: (0 if p.get("_amv_exact") else 1, _dist(p)))

    for p in points[:20]:
        d = p.get("distance")
        addr = (p.get("location") or {}).get("address_full") or (p.get("location") or {}).get("address")
        logger.info(f"PVZ {p.get('code')} | {d} м | {addr}")

    return points[:limit]



def format_pvz_button(pvz: dict, index: int) -> dict:
    code = pvz["code"]
    loc = pvz.get("location", {}) or {}
    address = loc.get("address_full") or loc.get("address") or ""
    short_addr = _shorten_address(address) or f"ПВЗ {code}"

    dist = pvz.get("distance")
    dist_text = f" · {int(dist)}м" if isinstance(dist, (int, float)) and 0 < dist < 10000 else ""

    wt = (pvz.get("work_time") or "").strip()
    if wt:
        if "круглосуточно" in wt.lower():
            time_text = " · 24/7"
        else:
            # Берём первую строку до ;
            first_line = wt.split(";", 1)[0].strip()
            # Обрезаем до 12 символов, чтобы не вылезти
            time_text = f" · {first_line[:12]}"
    else:
        time_text = ""

    text = f"{index + 1}. {short_addr}{dist_text}{time_text}"
    if len(text) > 64:
        text = text[:61] + "..."

    return {
        "text": text,
        "callback_data": f"pvz_sel:{code}:{index}"
    }


def kb_pvz_list(pvz_list: List[dict]) -> InlineKeyboardMarkup:
    buttons = []

    for i, pvz in enumerate(pvz_list[:10]):
        buttons.append([format_pvz_button(pvz, i)])

    buttons.append([{"text": "Не вижу свой ПВЗ", "callback_data": "pvz_manual"}])
    buttons.append([{"text": "Ввести адрес заново", "callback_data": "pvz_reenter"}])
    buttons.append([{"text": "Назад в меню", "callback_data": CallbackData.MENU.value}])

    return create_inline_keyboard(buttons)


# Храним последний известный статус, чтобы не спамить
last_status_cache: Dict[int, str] = {}  # order_id → status_text

async def check_all_shipped_orders():
    from sqlalchemy import inspect  # импортируем здесь
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

            for order in orders_to_check:
                with Session(engine) as sess:

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
                        old_track = order.track
                        order.track = new_track
                    sess.commit()

                    # Красивое финальное сообщение клиенту — ТОЛЬКО ОДИН РАЗ!
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

                    # Админу тоже радостная новость
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

        except Exception as e:
            logger.exception(f"КРИТИЧНАЯ ошибка в check_all_shipped_orders: {e}")
            await notify_admin(f"ОШИБКА в фоновой задаче СДЭК:\n{e}")

        # Проверяем каждые 2-3 минуты в первые 2 часа после создания, потом реже - но пока просто 5 минут
        await asyncio.sleep(300)  # 5 минут - оптимально


# ========== ENTRYPOINT ==========
async def main():
    logger.info("Бот запущен - режим polling с автоматическим переподключением")
    logger.info("BOT VERSION MARK: 2025-12-23 FINAL")

    engine = make_engine(Config.DB_PATH)
    init_db(engine)

    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    logger.info(f"Таблицы после init_db: {tables}")
    if 'orders' not in tables:
        logger.error("Таблица orders НЕ создана! Проверь import models в init_db.py")

    # Засеиваем данные
    with Session(engine) as sess:
        seed_data(sess, anxiety_codes=None)
        sess.commit()

    await asyncio.sleep(15)
    asyncio.create_task(check_all_shipped_orders())

    while True:
        try:
            logger.info("Запуск polling с Telegram...")
            await dp.start_polling(bot)
        except Exception as e:
            logger.error(f"Polling упал: {type(e).__name__}: {e}")
            logger.info("Жду 15 секунд перед повторным подключением...")
            await asyncio.sleep(15)


if __name__ == "__main__":
    asyncio.run(main())