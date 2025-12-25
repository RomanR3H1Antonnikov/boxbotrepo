import os
import re
import asyncio
import logging
import requests
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

# ============DATABASE===========
def get_order_by_id(order_id: int, user_id: int) -> Optional[Order]:
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        return sess.get(Order, order_id) if sess.get(Order, order_id) and sess.get(Order, order_id).user_id == user_id else None


def get_all_orders_by_status(status: str) -> List[Order]:
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        stmt = select(Order).where(Order.status == status)
        return sess.scalars(stmt).all()
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


async def create_cdek_order(order: Order) -> bool:
    token = await get_cdek_token()
    if not token:
        logger.error("Нет токена СДЭК")
        return False

    pvz_code = order.extra_data.get("pvz_code")
    if not pvz_code:
        logger.error(f"Нет pvz_code для заказа #{order.id}")
        return False

    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        u = get_user_by_id(sess, order.user_id)
        if not u or not u.full_name or not u.phone:
            logger.error(f"Нет данных пользователя для заказа #{order.id}")
            return False

    payload = {
        "type": 2,
        "number": f"BOX{order.id}",
        "tariff_code": 136,
        "comment": f"Заказ из бота «ТВОЯ КОРОБОЧКА» #{order.id}",
        "shipment_point": Config.CDEK_SHIPMENT_POINT_CODE,

        "delivery_recipient_cost": {"value": 0},

        "to_location": {
            "code": str(pvz_code),
            "address": order.address or "г. Москва, ПВЗ СДЭК",
            "postal_code": "121096"
        },

        "sender": {
            "company": "ИП Романов Р. А.",
            "name": "Роман",
            "phones": [{"number": "+79999999999"}]
        },

        "recipient": {
            "name": u.full_name,
            "phones": [{"number": u.phone.replace("+","").replace(" ","").replace("-","")}]
        },

        "packages": [{
            "number": f"BOX{order.id}",
            "weight": Config.PACKAGE_WEIGHT_G,
            "length": Config.PACKAGE_LENGTH_CM,
            "width": Config.PACKAGE_WIDTH_CM,
            "height": Config.PACKAGE_HEIGHT_CM,
            "comment": "Подарочная коробочка с антистресс-набором",
            "items": [{
                "name": "Коробочка «Отпусти тревогу»",
                "ware_key": f"BOX{order.id}",
                "payment": {"value": 0},
                "cost": Config.PRICE_RUB,
                "weight": Config.PACKAGE_WEIGHT_G,
                "amount": 1
            }]
        }],

        "services": [
            {"code": "INSURANCE", "parameter": Config.PRICE_RUB + 590}
        ]
    }


    import json
    pretty_payload = json.dumps(payload, ensure_ascii=False, indent=2)
    logger.info(f"\n\n=== ОТПРАВЛЯЕМ В СДЭК ЗАКАЗ #{order.id} ===\n{pretty_payload}\n{'='*50}")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    url = "https://api.edu.cdek.ru/v2/orders"

    try:
        r = await asyncio.to_thread(requests.post, url, json=payload, headers=headers, timeout=30)
        logger.info(f"СДЭК ответил: {r.status_code}\n{r.text[:2000]}")

        # 200 / 201 — обычный успех (прод), 202 — асинхронный успех на edu.cdek.ru
        if r.status_code in (200, 201, 202):
            data = r.json()

            # На edu в ответе 202 uuid лежит прямо в entity.uuid
            uuid = data.get("entity", {}).get("uuid")

            if uuid:
                order.extra_data["cdek_uuid"] = uuid
                # На edu трек-номер приходит позже — пока ставим заглушку
                if uuid:
                    order.extra_data["cdek_uuid"] = uuid
                    # Сразу ставим UUID как временный трек — клиент увидит нормальный вид
                    order.track = uuid
                    logger.info(f"СДЭК: ЗАКАЗ ПРИНЯТ! UUID: {uuid} → используем как временный трек")

                logger.info(f"СДЭК: ЗАКАЗ ПРИНЯТ! UUID: {uuid} | Заказ #{order.id}")
                await notify_admin(f"Заказ #{order.id} успешно принят СДЭК (UUID: {uuid})\n"
                                   f"Трек-номер придёт через 10–90 сек автоматически.")

                # Сразу переводим в SHIPPED — клиент увидит, что всё ок
                order.status = OrderStatus.SHIPPED.value
                return True
            else:
                logger.error(f"СДЭК вернул {r.status_code}, но без uuid: {data}")
        else:
            logger.error(f"СДЭК ОШИБКА #{order.id}: {r.status_code} {r.text}")
            await notify_admin(f"Ошибка СДЭК #{order.id}\n{r.status_code}\n{r.text[:1000]}")

        return False

    except Exception as e:
        logger.exception(f"Исключение при создании заказа СДЭК #{order.id}")
        await notify_admin(f"Исключение при создании заказа СДЭК #{order.id}: {e}")
        return False


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
    if not address or not address.strip():
        return False, "Адрес не может быть пустым."
    if not re.match(r"^[А-Я][а-я]+,\s*\d+$", address):
        return False, "Адрес: Улица, номер (например, Профсоюзная, 93)."
    return True, "Адрес валиден."

# ======== ADMIN HELPERS ========
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
    await message.answer(
        f"Ваш заказ #{order.id} собран! Требуется дооплата {order.remainder_amount} ₽.",
        reply_markup=kb_ready_message(order)
    )

async def notify_client_order_shipped(order: Order, message: Message):
    await message.answer(
        f"Ваш заказ #{order.id} отправлен! Трек-номер: {order.track}",
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
    *, force_new: bool = False, edit_only: bool = False
):
    if force_new:
        return await msg.answer(text, reply_markup=reply_markup)
    if edit_only:
        try:
            await msg.edit_text(text, reply_markup=reply_markup)
            return
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
                logger.warning(f"Edit failed (edit_only): {e}")
            return
    try:
        await msg.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest:
        await msg.answer(text, reply_markup=reply_markup)

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

def kb_gallery(team_shown: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "Хочу заказать", "callback_data": CallbackData.CHECKOUT_START.value}],
        [{"text": "FAQ", "callback_data": CallbackData.FAQ.value}],
        [{"text": "Назад", "callback_data": CallbackData.MENU.value}],
    ]
    if not team_shown:
        buttons.insert(1, [{"text": "Команда коробочки", "callback_data": CallbackData.TEAM.value}])
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

def kb_faq() -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "1. Что такое коробочка?", "callback_data": "faq:q1"}],
        [{"text": "2. Чем коробочка отличается от консультации психолога?", "callback_data": "faq:q2"}],
        [{"text": "3. Для кого подходит коробочка?", "callback_data": "faq:q3"}],
        [{"text": "4. Что внутри коробочки?", "callback_data": "faq:q4"}],
        [{"text": "5. Кто создаёт практики для коробочки?", "callback_data": "faq:q5"}],
        [{"text": "6. Как пользоваться коробочкой?", "callback_data": "faq:q6"}],
        [{"text": "7. Можно ли использовать коробочку несколько раз?", "callback_data": "faq:q7"}],
        [{"text": "8. Сколько времени занимает работа с коробочкой?", "callback_data": "faq:q8"}],
        [{"text": "9. Сколько стоит коробочка?", "callback_data": "faq:q9"}],
        [{"text": "10. Как заказать коробочку?", "callback_data": "faq:q10"}],
        [{"text": "11. Сколько ждать доставку?", "callback_data": "faq:q11"}],
        [{"text": "12. Можно ли заказать коробочку в подарок?", "callback_data": "faq:q12"}],
        [{"text": "13. А если я потерял доступ к онлайн-практикам?", "callback_data": "faq:q13"}],
        [{"text": "14. Будут ли новые коробочки?", "callback_data": "faq:q14"}],
        [{"text": "15. Можно ли купить несколько коробочек сразу?", "callback_data": "faq:q15"}],
        [{"text": "16. Чем коробочка отличается от обычного подарочного набора?", "callback_data": "faq:q16"}],
        [{"text": "17. Есть ли доставка за пределы России?", "callback_data": "faq:q17"}],
        [{"text": "18. Где я смогу увидеть результаты других и поделиться своими?", "callback_data": "faq:q18"}],
        [{"text": "19. Что делать, если у меня остались вопросы?", "callback_data": "faq:q19"}],
        [{"text": "Назад к товару", "callback_data": CallbackData.GALLERY.value}],
    ]
    return create_inline_keyboard(buttons)

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
    gift = order.extra_data.get("gift_message")
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
    # reset_waiting_flags(ustate(cb.from_user.id))
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
    sess.commit()
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
    # user.awaiting_contact = True
    await cb.message.answer(
        "Введите данные в 3 строки:\nИмя Фамилия\n+7XXXXXXXXXX\nemail@example.com",
        reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.CABINET.value}]])
    )
    sess.commit()
    await cb.answer()

# ========== GALLERY + FAQ + TEAM ==========
@r.callback_query(F.data == CallbackData.GALLERY.value)
async def cb_gallery(cb: CallbackQuery):
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

        if user.gallery_viewed:
            await cb.message.answer(Config.GALLERY_TEXT, reply_markup=kb_gallery(team_shown=user.team_viewed))
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

        await cb.message.answer(Config.GALLERY_TEXT, reply_markup=kb_gallery(team_shown=user.team_viewed))

        user.gallery_viewed = True
        sess.commit()
    await cb.answer()

@r.callback_query(F.data == CallbackData.FAQ.value)
async def cb_faq(cb: CallbackQuery):
    # reset_waiting_flags(ustate(cb.from_user.id))
    await edit_or_send(cb.message, "Частые вопросы:", kb_faq())
    await cb.answer()

@r.callback_query(F.data.startswith("faq:q"))
async def cb_faq_answer(cb: CallbackQuery):
    ans = Config.FAQ_ANSWERS.get(cb.data, "Вопрос в обработке.")
    await edit_or_send(cb.message, ans, create_inline_keyboard([
        [{"text": "Назад к FAQ", "callback_data": CallbackData.FAQ.value}],
        [{"text": "Назад к товару", "callback_data": CallbackData.GALLERY.value}],
    ]))
    await cb.answer()

@r.callback_query(F.data == CallbackData.TEAM.value)
async def cb_team(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка", show_alert=True)
            return

        if user.team_viewed:
            await cb.message.answer(
                "Ты уже знаком с командой коробочки - смотри кружочки выше!",
                reply_markup=kb_gallery(team_shown=True)
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
            reply_markup=kb_gallery(team_shown=True)
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
    if not user.is_authorized:
        await edit_or_send(cb.message, "Пожалуйста, авторизуйтесь.", kb_cabinet_unauth())
        await cb.answer(); return
    if not user.practices:
        await edit_or_send(cb.message, "У вас нет практик.\nАктивируйте код или закажите коробочку.", kb_empty_practices())
        await cb.answer(); return
    await edit_or_send(cb.message, "Твои практики:", kb_practices_list(user.practices))
    sess.commit()
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
    if user.is_authorized:
        await cb.message.answer(
            f"Проверьте данные:\n• ФИО: {user.full_name}\n• Телефон: {user.phone}\n• Email: {user.email}\n\nХотите изменить?",
            reply_markup=kb_change_contact()
        )
    else:
        await cb.message.answer(
            "Введите данные в 3 строки:\nИмя Фамилия\n+7XXXXXXXXXX\nemail@example.com",
            reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.MENU.value}]])
        )
    sess.commit()
    await cb.answer()

@r.callback_query(F.data.startswith("change_contact:"))
async def cb_change_contact(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return
    user.awaiting_pvz_address = True
    if cb.data == CallbackData.CHANGE_CONTACT_YES.value:
        await cb.message.answer(
            "Введите новые данные:\nИмя Фамилия\n+7XXXXXXXXXX\nemail@example.com",
            reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.GALLERY.value}]])
        )
    else:
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
        await cb.answer(); return
    user.pvz_for_order_id = None
    user.awaiting_pvz_address = True
    await cb.message.answer(
        "Введите адрес ПВЗ (например: «Профсоюзная, 93»):",
        reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.GALLERY.value}]])
    )
    sess.commit()
    await cb.answer()

async def show_review(msg: Message, order: Order):
    await edit_or_send(msg, format_order_review(order), kb_review(order))

# ========== PAYMENT ==========
@r.callback_query(F.data.startswith("pay:"))
async def cb_pay(cb: CallbackQuery):
    try:
        parts = cb.data.split(":")
        kind = parts[1]           # "full" | "pre" | "rem"
        oid_str = parts[2]
        oid = int(oid_str) if oid_str != "0" else None
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

        # === 1. Находим или создаём заказ ===
        if oid:
            order = get_order_by_id(oid, cb.from_user.id)
            if not order or order.user_id != cb.from_user.id:
                await cb.answer("Заказ не найден", show_alert=True)
                return
        else:
            # Новый заказ — только при первой оплате (100% или 30%)
            if not user.temp_selected_pvz:
                await cb.answer("Ошибка: ПВЗ не выбран", show_alert=True)
                return
            order = user.new_order(cb.from_user.id)
            order.shipping_method = "cdek_pvz"
            order.address = user.temp_selected_pvz["address"]
            order.extra_data.update({
                "pvz_code": user.temp_selected_pvz["code"],
                "delivery_cost": user.extra_data.get("delivery_cost", 590),
                "delivery_period": user.extra_data.get("delivery_period", "3–7"),
            })
            user.temp_selected_pvz = None

        # Устанавливаем итоговую цену (один раз)
        if order.total_price == 0:
            delivery_cost = order.extra_data.get("delivery_cost", 590)
            order.total_price = Config.PRICE_RUB + delivery_cost

        # Уведомляем админа о начале оплаты
        await notify_admins_payment_started(order)

        # ==================================================================
        # === СЦЕНАРИЙ 1: Полная оплата сразу (100%) =========================
        # ==================================================================
        if kind == "full":
            order.status = OrderStatus.PAID.value
            order.payment_kind = "full"

            order.status = OrderStatus.READY.value
            await notify_admins_payment_success(order)

            await cb.message.answer(
                "Полная оплата получена! Спасибо огромное! ❤️\n\n"
                f"Заказ <b>#{order.id}</b> уже собирается и скоро будет передан в СДЭК.\n"
                "Трек-номер пришлю автоматически через 1–2 минуты(либо нажмите кнопку Обновить статус)",
                reply_markup=kb_order_status(order)
            )

            # Сразу отправляем в СДЭК
            success = await create_cdek_order(order)
            if success:
                order.status = OrderStatus.SHIPPED.value
                await notify_admins_order_shipped(order)
            else:
                order.status = OrderStatus.READY.value
                await cb.message.answer(
                    "Оплата прошла, но временная задержка с СДЭК\n"
                    "Админ уже в курсе - отправим в ближайшие минуты!",
                    reply_markup=kb_order_status(order)
                )

        # ==================================================================
        # === СЦЕНАРИЙ 2: Предоплата 30% =====================================
        # ==================================================================
        elif kind == "pre":
            order.status = OrderStatus.PREPAID.value
            order.payment_kind = "pre"

            await notify_admins_payment_success(order)

            await cb.message.answer(
                "Предоплата получена! Спасибо огромное! ❤️\n\n"
                f"Заказ <b>#{order.id}</b> принят на сборку.\n"
                "Как только коробочка будет готова - пришлю ссылку на дооплату остатка и сразу отправлю посылку",
                reply_markup=kb_order_status(order)
            )

            # Пока НЕ отправляем в СДЭК — ждём полной оплаты

        # ==================================================================
        # === СЦЕНАРИЙ 3: Дооплата остатка (после предоплаты) =================
        # ==================================================================
        elif kind == "rem":
            # Защита от случайного нажатия
            if order.status not in [OrderStatus.PREPAID.value, OrderStatus.READY.value]:
                await cb.answer("Этот заказ уже полностью оплачен", show_alert=True)
                return

            order.status = OrderStatus.PAID.value
            order.payment_kind = "remainder"  # или "full" — как хочешь

            await notify_admins_payment_remainder(order)

            await cb.message.answer(
                "Полная оплата получена! Спасибо! ❤️\n\n"
                f"Заказ <b>#{order.id}</b> отправляется в СДЭК прямо сейчас!\n"
                "Трек-номер пришлю автоматически через 10–90 секунд",
                reply_markup=kb_order_status(order)
            )

            # Отправляем в СДЭК немедленно
            success = await create_cdek_order(order)
            if success:
                order.status = OrderStatus.SHIPPED.value
                await notify_admins_order_shipped(order)
            else:
                order.status = OrderStatus.READY.value
                await cb.message.answer(
                    "Оплата прошла, но сейчас небольшая задержка с оформлением в СДЭК\n"
                    "Админ уже в курсе — отправим в течение часа!",
                    reply_markup=kb_order_status(order)
                )
                await notify_admin(f"ВНИМАНИЕ: Заказ #{order.id} — дооплата прошла, но create_cdek_order упал")

        await cb.answer()

    except Exception as e:
        logger.error(f"Pay error: {e}", exc_info=True)
        await notify_admin(f"Ошибка в cb_pay: {e}\nДанные: {cb.data}")
        await cb.answer("Произошла ошибка при оплате", show_alert=True)


# ========== ORDER STATUS ==========
@r.callback_query(F.data.startswith("order:"))
async def cb_order_status(cb: CallbackQuery):
    def get_order_by_id(order_id: int, user_id: int) -> Order | None:
        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            stmt = select(Order).where(Order.id == order_id, Order.user_id == user_id)
            return sess.scalar(stmt)
    try:
        oid = int(cb.data.split(":")[1])
        order = get_order_by_id(oid, cb.from_user.id)
        if not order or order.user_id != cb.from_user.id:
            await cb.answer("Заказ не найден", show_alert=True)
            return

        status_text = {
            OrderStatus.SHIPPED.value: f"Отправлен!\nТрек: <code>{order.track}</code>",
            OrderStatus.READY.value: f"Готов к отправке\nОстаток: {order.remainder_amount} ₽",
            OrderStatus.PAID.value: "Оплачен полностью\nОжидает отправки",
            OrderStatus.PREPAID.value: f"Предоплачено\nОстаток: {order.remainder_amount} ₽",
        }.get(order.status, f"Статус: {order.status}")

        # ←←← Добавляем срок доставки ←←←
        period = order.extra_data.get("delivery_period")
        if period and order.status in [
            OrderStatus.PREPAID.value,
            OrderStatus.READY.value,
            OrderStatus.PAID.value,
            OrderStatus.SHIPPED.value
        ]:
            status_text += f"\nСрок доставки: ≈ <b>{period} дн.</b>"

        await edit_or_send(
            cb.message,
            f"Заказ #{order.id}\n\n{status_text}",
            kb_order_status(order)
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

            order = get_order_by_id(oid, cb.from_user.id)
            if not order:
                await cb.answer("Заказ не найден", show_alert=True)
                return

        user.pvz_for_order_id = oid

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
        order = get_order_by_id(oid, 0)
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
        oid = int(cb.data.split(":")[2])  # Извлекаем oid из третьей части (admin:set_ready:1)
        order = get_order_by_id(oid, 0)
        if not order or order.status != OrderStatus.PREPAID.value:
            await cb.answer("Нельзя перевести в готовность", show_alert=True)
            return
        if not await is_admin(cb):
            logger.info("Admin access denied")
            await cb.answer("Доступ запрещён", show_alert=True)
            return
        order.status = OrderStatus.READY.value
        await notify_admins_order_ready(order)
        await notify_client_order_ready(order, cb.message)
        await edit_or_send(cb.message, f"Заказ #{oid} готов к отправке.", kb_admin_panel())
        await cb.answer()
    except Exception as e:
        logger.error(f"Admin set ready error: {e}")
        await notify_admin(f"❌ Ошибка перевода заказа #{oid if 'oid' in locals() else 'неизвестный'} в готовность")
        await cb.answer("Ошибка", show_alert=True)


@r.callback_query(F.data.startswith(CallbackData.ADMIN_SET_ARCHIVED.value))
async def cb_admin_set_archived(cb: CallbackQuery):
    logger.info(f"Set archived callback: user_id={cb.from_user.id}, data={cb.data}")
    try:
        oid = int(cb.data.split(":")[2])  # Извлекаем oid из третьей части (admin:set_archived:1)
        order = get_order_by_id(oid, 0)
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
            oid = int(cb.data.split(":")[1])
            order = get_order_by_id(oid, 0)
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
    # НЕ сбрасываем pvz_for_order_id, чтобы помнить, редактируем ли заказ
    # user.awaiting_pvz_address = True
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
    sess.commit()
    await cb.answer()



# === ОБНОВЛЁННЫЙ обработчик выбора ПВЗ ===
@r.callback_query(lambda c: (c.data or "").startswith("pvz_sel:"))
async def cb_pvz_select(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return
    try:
        _, old_code, idx_str = cb.data.split(":")
        idx = int(idx_str)

        if not (0 <= idx < len(user.temp_pvz_list)):
            await cb.answer("Список ПВЗ устарел — введите адрес заново", show_alert=True)
            return

        pvz = user.temp_pvz_list[idx]

        # Правильно парсим код
        raw_code = pvz.get("code")
        if isinstance(raw_code, str) and raw_code.startswith("MSK"):
            real_code = int(raw_code.replace("MSK", ""))
        elif isinstance(raw_code, int):
            real_code = raw_code
        elif isinstance(raw_code, str):
            real_code = int(''.join(filter(str.isdigit, raw_code)))
        else:
            real_code = 0

        if real_code == 0:
            await cb.answer("Ошибка кода ПВЗ", show_alert=True)
            return

        logger.info(f"PVZ выбран: {pvz['location']['address_full']} → код: {real_code}")

        full_address = pvz["location"]["address_full"]
        work_time = pvz.get("work_time") or "Пн–Пт 10:00–20:00, Сб–Вс 10:00–18:00"

        # Сохраняем выбранный ПВЗ
        user.temp_selected_pvz = {
            "code": real_code,
            "address": full_address,
            "work_time": work_time
        }

        # Считаем доставку
        await cb.message.answer("Считаю стоимость доставки…")
        delivery_info = await calculate_cdek_delivery_cost(str(real_code))

        delivery_cost = delivery_info["cost"] if delivery_info else 590
        period_text = "3–7"
        if delivery_info:
            mn = delivery_info["period_min"]
            mx = delivery_info["period_max"] or mn + 2
            period_text = f"{mn}" if mn == mx else f"{mn}–{mx}"

        total = Config.PRICE_RUB + delivery_cost
        prepay = (total * Config.PREPAY_PERCENT + 99) // 100

        # ←←← СОЗДАЁМ ЗАКАЗ СРАЗУ ЗДЕСЬ ←←←
        engine = make_engine(Config.DB_PATH)
        with Session(engine) as sess:
            order = create_order_db(
                sess,
                user_id=cb.from_user.id,
                product_id=1,  # ID коробочки "anxiety" из БД
                status=OrderStatus.NEW.value,
                shipping_method="cdek_pvz",
                address=full_address,
                total_price_kop=(total * 100),  # в копейках!
                delivery_cost_kop=(delivery_cost * 100),
                extra_data={
                    "pvz_code": real_code,
                    "delivery_cost": delivery_cost,
                    "delivery_period": period_text,
                }
            )

        # ←←← ВАЖНО: используем правильные callback_data! ←←←
        await edit_or_send(
            cb.message,
            f"<b>ПВЗ сохранён!</b>\n\n"
            f"{full_address}\n"
            f"Режим работы: {work_time}\n\n"
            f"Доставка: <b>{delivery_cost} ₽</b>\n"
            f"Срок: <b>≈ {period_text} дн.</b>\n\n"
            f"<b>Итого: {total} ₽</b>\n"
            f"• Предоплата 30% = {prepay} ₽\n"
            f"• Остаток = {total - prepay} ₽",
            create_inline_keyboard([
                [{"text": f"Оплатить 100% ({total} ₽)", "callback_data": f"pay:full:{order.id}"}],
                [{"text": f"Предоплата 30% ({prepay} ₽)", "callback_data": f"pay:pre:{order.id}"}],
                [{"text": "Выбрать другой ПВЗ", "callback_data": "pvz_backlist"}],
                [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
            ])
        )
        await cb.answer("Готово!")

        user.awaiting_gift_message = True
        await cb.message.answer(
            "Хотите добавить личное послание в подарок получателю?\n(Текст будет вложен в коробочку)",
            reply_markup=create_inline_keyboard([
                [{"text": "Да, добавить", "callback_data": "gift:yes"}],
                [{"text": "Нет, без послания", "callback_data": "gift:no"}],
                [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
            ])
        )

    except Exception as e:
        logger.error(f"cb_pvz_select error: {e}", exc_info=True)
        await cb.answer("Ошибка", show_alert=True)
    sess.commit()


@r.callback_query(F.data.startswith("gift:"))
async def cb_gift_message(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return

    if cb.data == "gift:yes":
        # user.awaiting_gift_message = True
        # user.gift_message = None
        await cb.message.answer(
            "Напишите текст послания (до 300 символов):",
            reply_markup=create_inline_keyboard([[{"text": "Отмена", "callback_data": "gift:cancel"}]])
        )
    else:
        # user.awaiting_gift_message = False
        orders = get_user_orders_db(sess, cb.from_user.id)
        if orders:
            order = orders[-1]  # самый новый
            await show_review(cb.message, order)
        else:
            await cb.message.answer("У вас нет заказов.")
    sess.commit()
    await cb.answer()


@r.callback_query(F.data == "pvz_manual")
async def cb_pvz_manual(cb: CallbackQuery):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, cb.from_user.id)
        if not user:
            await cb.answer("Ошибка доступа", show_alert=True)
            return
    # Сбрасываем только ожидания текста, но НЕ pvz_for_order_id
    user.awaiting_pvz_address = False
    user.awaiting_manual_pvz = True

    await cb.message.edit_text(
        "Напиши код ПВЗ (например, MSK123) или полный адрес пункта выдачи так, как он указан у СДЭК.\n\n"
        "Мы оформим заказ на этот пункт.",
        reply_markup=create_inline_keyboard([
            [{"text": "Назад к списку ПВЗ", "callback_data": "pvz_backlist"}],
            [{"text": "В меню", "callback_data": CallbackData.MENU.value}],
        ])
    )
    sess.commit()
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
        # вдруг бот перезапустился и память очистилась
        # user.awaiting_pvz_address = True
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
    sess.commit()
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
    code = pvz["code"]
    full_address = pvz["address"]  # добавляем
    real_code = code  # добавляем

    await cb.message.answer("Считаю стоимость и срок доставки…")
    delivery_info = await calculate_cdek_delivery_cost(str(code))

    if delivery_info is None:
        delivery_cost = 590
        period_text = "3–7"
    else:
        delivery_cost = delivery_info["cost"]
        pmin = delivery_info["period_min"]
        pmax = delivery_info["period_max"] or pmin + 2
        period_text = f"{pmin}" if pmin == pmax else f"{pmin}–{pmax}"

    total = Config.PRICE_RUB + delivery_cost  # добавляем

    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
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
                "delivery_cost": delivery_cost,
                "delivery_period": period_text,
            }
        )
        sess.commit()

    total = Config.PRICE_RUB + delivery_cost
    order.total_price = total
    prepay = (total * Config.PREPAY_PERCENT + 99) // 100

    await edit_or_send(
        cb.message,
        f"Отлично! ПВЗ сохранён:\n\n"
        f"{pvz['address']}\n"
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
    sess.commit()
    await cb.answer("Готово!")


@r.message()  # Ловит текст, когда ждём адрес ПВЗ
async def handle_pvz_address(message: Message):
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, message.from_user.id)
        if not user:
            return

        if getattr(user, "awaiting_pvz_address", False):
            address = message.text.strip()
            ok, msg = validate_address(address)
            if not ok:
                await message.answer(f"Ошибка формата адреса: {msg}\nПопробуйте ещё раз (например: Профсоюзная, 93).")
                return

            # Сохраняем адрес и ищем ПВЗ
            user.extra_data["pvz_query"] = address
            user.awaiting_pvz_address = False
            sess.commit()

            await message.answer("Ищу ближайшие ПВЗ СДЭК...")

            pvz_list = await find_best_pvz(address, city="Москва")  # или без city
            if not pvz_list:
                await message.answer("Не нашёл ПВЗ по этому адресу 😔\nПопробуйте другой или введите код ПВЗ вручную.")
                return

            user.temp_pvz_list = pvz_list
            sess.commit()

            await message.answer(
                f"Нашёл {len(pvz_list)} ПВЗ рядом с «{address}».\nВыбери нужный:",
                reply_markup=kb_pvz_list(pvz_list)
            )
            return

    # Если не ввод адреса — передаём дальше
    await handle_auth_input(message)


@r.message()  # Это ловит ВСЕ текстовые сообщения
async def handle_auth_input(message: Message):
    # Проверяем, ожидает ли пользователь ввода данных (по наличию записи в БД)
    engine = make_engine(Config.DB_PATH)
    with Session(engine) as sess:
        user = get_user_by_id(sess, message.from_user.id)
        if not user:
            return

        # Если пользователь ещё не авторизован — пытаемся обработать ввод
        if not user.is_authorized:
            text = message.text.strip()
            lines = [line.strip() for line in text.split("\n") if line.strip()]

            if len(lines) == 3:
                full_name, phone, email = lines
                ok, msg = validate_data(full_name, phone, email)
                if ok:
                    user.full_name = full_name
                    user.phone = phone
                    user.email = email
                    user.is_authorized = True
                    sess.commit()

                    await message.answer(
                        f"Спасибо, {full_name.split()[0]}! Данные сохранены.\n"
                        "Теперь вы авторизованы.",
                        reply_markup=kb_main()
                    )
                    return
                else:
                    await message.answer(f"Ошибка: {msg}\nПопробуйте ещё раз.", reply_markup=kb_main())
                    return
            else:
                # Если не 3 строки — просто напомним формат
                await message.answer(
                    "Введите данные в 3 строки:\nИмя Фамилия\n+7XXXXXXXXXX\nemail@example.com"
                )
                return

    # Если не авторизация — передаём дальше в общий обработчик
    await on_text(message)


@r.message()
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
        await cb_checkout_start(type("obj", (), {"from_user": message.from_user, "message": message, "answer": lambda *a, **k: None})())
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
                uuid = order.extra_data["cdek_uuid"]
                info = await get_cdek_order_info(uuid)  # лучше полная инфа, а не только статус

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
                        # f"Клиент: {ustate(order.user_id).full_name or 'Неизвестно'}"
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