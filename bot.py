import os
import re
import asyncio
import logging
import requests
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from enum import Enum
from datetime import datetime, timedelta

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
load_dotenv()

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
        response = requests.post(url, data=data, timeout=15)
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
        r = requests.post(url, json=payload, headers=headers, timeout=15)
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
        r = requests.get(url, headers=headers, timeout=15)
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
        r = requests.get(url, headers=headers, timeout=15)
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
    WELCOME_TEXT = "Добро пожаловать! Здесь ты сможешь избавиться от тревоги"
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
        "faq:q1": "Обычно 2–5 дней по РФ. Точный срок зависит от вашего города и работы СДЭК.",
        "faq:q2": "В коробочке есть карточка с секретным кодом. Введите его в боте — доступ откроется после авторизации.",
        "faq:q3": "Напишите нам в поддержку, поможем восстановить доступ.",
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

# ========== DATA CLASSES ==========
@dataclass
class Order:
    id: int
    user_id: int
    contact_raw: str = ""
    shipping_method: str = "cdek_pvz"
    address: str = ""
    status: str = OrderStatus.NEW.value
    track: Optional[str] = None
    payment_kind: Optional[str] = None
    extra_data: dict = field(default_factory=dict)
    total_price: int = 0

    @property
    def prepay_amount(self) -> int:
        return (Config.PRICE_RUB * Config.PREPAY_PERCENT + 99) // 100

    @property
    def remainder_amount(self) -> int:
        return max(Config.PRICE_RUB - self.prepay_amount, 0)

@dataclass
class UserState:
    awaiting_code: bool = False
    awaiting_contact: bool = False
    awaiting_pvz_address: bool = False
    pvz_for_order_id: Optional[int] = None
    awaiting_manual_pvz: bool = False

    full_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    practices: List[str] = field(default_factory=list)
    temp_address: Optional[str] = None
    temp_pvz_list: List[dict] = field(default_factory=list)
    selected_pvz_code: Optional[str] = None
    temp_selected_pvz: Optional[dict] = None
    extra_data: dict = field(default_factory=dict)

    @property
    def is_authorized(self) -> bool:
        return bool(self.full_name and self.phone and self.email)

class BotState:
    def __init__(self):
        self.total_price: int = 0
        self.users: Dict[int, UserState] = {}
        self.orders: Dict[int, Order] = {}
        self.next_order_id: int = 1
        self.used_codes: set[str] = set()
        self.pending_tasks: Dict[int, asyncio.Task] = {}

    def get_user(self, uid: int) -> UserState:
        if uid not in self.users:
            self.users[uid] = UserState()
        return self.users[uid]

    def new_order(self, uid: int) -> Order:
        order = Order(id=self.next_order_id, user_id=uid)
        self.orders[order.id] = order
        self.next_order_id += 1
        logger.info(f"NEW ORDER: #{order.id} | user {uid}")
        return order


state = BotState()
def ustate(uid: int) -> UserState:
    return state.get_user(uid)


# ========== ADMIN ==========
ADMIN_USERNAMES = {"@RE_HY"}
ADMIN_ID = 1049170524

# ========== BOOTSTRAP ==========
bot = Bot(
    Config.TOKEN,
    default=DefaultBotProperties(
        parse_mode=ParseMode.HTML
    ),
    proxy="socks5://t.me/socks?server=149.154.160.1&port=443&user=telegram&pass=telegram"
)
dp = Dispatcher()
r = Router()
dp.include_router(r)

state = BotState()
def ustate(uid: int) -> UserState:
    return state.get_user(uid)

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

    u = ustate(order.user_id)

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
            "name": u.full_name or "Клиент",
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
        r = requests.post(url, json=payload, headers=headers, timeout=30)
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
    if not re.match(r"^[А-Я][а-я]+(\s+[А-Я][а-я]+)$", full_name.strip()):
        return False, "ФИО: Имя и Фамилия с заглавной буквы, без отчества."
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
    u = ustate(order.user_id)
    await notify_admin(
        f"🔔 Новый заказ #{order.id}\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Тип оплаты: {order.payment_kind}\n"
        f"Адрес: {order.address or '—'}\n"
        f"Статус: {order.status}"
    )

async def notify_admins_payment_success(order: Order):
    u = ustate(order.user_id)
    await notify_admin(
        f"✅ Предоплата #{order.id} получена\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Статус: {order.status}"
    )

async def notify_admins_order_ready(order: Order):
    u = ustate(order.user_id)
    await notify_admin(
        f"📦 Заказ #{order.id} собран\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Статус: {order.status}"
    )

async def notify_admins_payment_remainder(order: Order):
    u = ustate(order.user_id)
    await notify_admin(
        f"💸 Заказ #{order.id} полностью оплачен\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Статус: {order.status}"
    )

async def notify_admins_order_shipped(order: Order):
    u = ustate(order.user_id)
    await notify_admin(
        f"🚚 Заказ #{order.id} отправлен\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Трек: {order.track}\n"
        f"Статус: {order.status}"
    )

async def notify_admins_order_archived(order: Order):
    u = ustate(order.user_id)
    await notify_admin(
        f"🗄 Заказ #{order.id} заархивирован\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Статус: {order.status}"
    )


async def notify_admins_order_address_changed(order: Order):
    u = ustate(order.user_id)
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

async def schedule_payment_timeout(order_id: int, message: Message):
    if order_id in state.pending_tasks:
        try:
            state.pending_tasks[order_id].cancel()
        except Exception:
            pass
    async def _job():
        try:
            await asyncio.sleep(Config.PAYMENT_TIMEOUT_SEC)
            order = state.orders.get(order_id)
            if order and order.status == OrderStatus.PENDING.value:
                order.status = OrderStatus.ABANDONED.value
                await notify_admin(f"🕓 Заказ #{order_id} отменён (таймаут оплаты)")
                await notify_client_order_abandoned(order, message)
        except asyncio.CancelledError:
            return
    task = asyncio.create_task(_job())
    state.pending_tasks[order_id] = task

def cancel_payment_timeout(order_id: int):
    t = state.pending_tasks.pop(order_id, None)
    if t:
        t.cancel()

# ======== RESET UTILS ========
def reset_waiting_flags(st: UserState):
    st.awaiting_code = False
    st.awaiting_contact = False
    st.awaiting_pvz_address = False
    st.awaiting_track_for_order = None
    st.awaiting_manual_pvz = False


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

async def handle_code(message: Message, ust: UserState, text: str):
    if CODE_RE.fullmatch(text) and text in Config.CODES_POOL and text not in state.used_codes:
        state.used_codes.add(text)
        ust.awaiting_code = False
        ust.practices = Config.DEFAULT_PRACTICES.copy()
        await message.answer("Готово! Доступ к практикам открыт")
        await message.answer("Твои практики:", reply_markup=kb_practices_list(ust.practices))
    else:
        await message.answer("Код неверный или уже использован. Проверьте код.")

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
    return create_inline_keyboard([
        [{"text": "Команда коробочки", "callback_data": CallbackData.TEAM.value}],
        [{"text": "Хочу заказать", "callback_data": CallbackData.CHECKOUT_START.value}],
        [{"text": "FAQ", "callback_data": CallbackData.FAQ.value}],
        [{"text": "Назад", "callback_data": CallbackData.MENU.value}],
    ])

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
    return create_inline_keyboard([
        [{"text": "Сколько идёт доставка?", "callback_data": "faq:q1"}],
        [{"text": "Как активировать доступ?", "callback_data": "faq:q2"}],
        [{"text": "Что делать, если код потерялся?", "callback_data": "faq:q3"}],
        [{"text": "Назад к товару", "callback_data": CallbackData.GALLERY.value}],
    ])

def kb_change_contact() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Да", "callback_data": CallbackData.CHANGE_CONTACT_YES.value}],
        [{"text": "Нет", "callback_data": CallbackData.CHANGE_CONTACT_NO.value}],
        [{"text": "Назад", "callback_data": CallbackData.GALLERY.value}],
    ])

def kb_admin_panel() -> InlineKeyboardMarkup:
    return create_inline_keyboard([
        [{"text": "Заказы для сборки", "callback_data": CallbackData.ADMIN_ORDERS_PREPAID.value}],
        [{"text": "Заказы с дооплатой", "callback_data": CallbackData.ADMIN_ORDERS_READY.value}],
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
    u = ustate(order.user_id)
    pvz_code = order.extra_data.get("pvz_code", "—")
    return (
        f"Заказ #{order.id}\n"
        f"Пользователь: {u.full_name or 'Не авторизован'} ({order.user_id})\n"
        f"Статус: {order.status}\n"
        f"ПВЗ код: {pvz_code}\n"
        f"Адрес: {order.address or '—'}\n"
        f"Трек: {order.track or '—'}\n"
        f"Тип оплаты: {order.payment_kind or '—'}"
    )

# ========== START / MENU ==========
@r.message(CommandStart())
async def on_start(message: Message):
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
    st = ustate(message.from_user.id)
    reset_waiting_flags(st)
    st.pvz_for_order_id = None
    await message.answer("Выбери действие:", reply_markup=kb_main())

@r.message(Command("admin_panel"))
async def cmd_admin_panel(message: Message):
    if not await is_admin(message):
        return
    await message.answer("Панель администратора:", reply_markup=kb_admin_panel())

@r.callback_query(F.data == CallbackData.MENU.value)
async def cb_menu(cb: CallbackQuery):
    logger.info(f"Menu callback: user_id={cb.from_user.id}, data={cb.data}")
    reset_waiting_flags(ustate(cb.from_user.id))
    await edit_or_send(cb.message, "Выбери действие:", kb_main())
    await cb.answer()

# ========== CABINET ==========
@r.callback_query(F.data == CallbackData.CABINET.value)
async def cb_cabinet(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    reset_waiting_flags(st)
    name = cb.from_user.first_name or "друг"
    if not st.is_authorized:
        await edit_or_send(cb.message, f"Добро пожаловать, {name}!\nВы не авторизованы.", kb_cabinet_unauth())
    else:
        await edit_or_send(cb.message, f"Добро пожаловать, {name}!\nВы авторизованы как {st.full_name}.", kb_cabinet())
    await cb.answer()

@r.callback_query(F.data == CallbackData.HELP.value)
async def cb_help(cb: CallbackQuery):
    reset_waiting_flags(ustate(cb.from_user.id))
    await edit_or_send(cb.message, f"При ошибке обращайтесь: {Config.ADMIN_HELP_NICK}",
                       create_inline_keyboard([[{"text": "В меню", "callback_data": CallbackData.MENU.value}]]))
    await cb.answer()

# ========== AUTH ==========
@r.callback_query(F.data == CallbackData.AUTH_START.value)
async def cb_auth_start(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    reset_waiting_flags(st)
    st.awaiting_contact = True
    await cb.message.answer(
        "Введите данные в 3 строки:\n1. Имя Фамилия\n2. +7XXXXXXXXXX\n3. email@example.com",
        reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.CABINET.value}]])
    )
    await cb.answer()

# ========== GALLERY + FAQ + TEAM ==========
@r.callback_query(F.data == CallbackData.GALLERY.value)
async def cb_gallery(cb: CallbackQuery):
    reset_waiting_flags(ustate(cb.from_user.id))
    try:
        await cb.message.answer_video(
            video=Config.VIDEO1_ID,
            caption=""
        )
        await cb.message.answer_video(
            video=Config.VIDEO2_ID,
            caption=""
        )
        await cb.message.answer_video(
            video=Config.VIDEO3_ID,
            caption=""
        )
    except Exception as e:
        logger.error(f"Failed to send gallery videos: {e}")
        await cb.message.answer("Ошибка при загрузке видео. Свяжитесь с администратором.")

    await edit_or_send(cb.message, Config.GALLERY_TEXT, kb_gallery())
    await cb.answer()

@r.callback_query(F.data == CallbackData.FAQ.value)
async def cb_faq(cb: CallbackQuery):
    reset_waiting_flags(ustate(cb.from_user.id))
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
        else:
            await cb.message.answer("Видео отсутствует")
        await cb.message.answer(f"<b>{name}</b>", parse_mode=ParseMode.HTML)
        await asyncio.sleep(0.6)
    await cb.message.answer("Знакомься, команда коробочки!", reply_markup=kb_gallery())
    await cb.answer()

# ========== PRACTICES ==========
@r.callback_query(F.data == CallbackData.PRACTICES.value)
async def cb_practices(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    reset_waiting_flags(st)
    if not st.is_authorized:
        await edit_or_send(cb.message, "Пожалуйста, авторизуйтесь.", kb_cabinet_unauth())
        await cb.answer(); return
    if not st.practices:
        await edit_or_send(cb.message, "У вас нет практик.\nАктивируйте код или закажите коробочку.", kb_empty_practices())
        await cb.answer(); return
    await edit_or_send(cb.message, "Твои практики:", kb_practices_list(st.practices))
    await cb.answer()

@r.callback_query(F.data.startswith("practice:"))
async def cb_open_practice(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    parts = cb.data.split(":")
    if len(parts) >= 3 and parts[1] == "play":
        await cb.answer(); return
    try:
        idx = int(parts[1])
    except:
        await cb.message.answer("Ошибка.", reply_markup=kb_practices_list(st.practices))
        await cb.answer(); return
    if not (st.is_authorized and 0 <= idx < len(st.practices)):
        await cb.message.answer("Доступ ограничен.", reply_markup=kb_practices_list(st.practices))
        await cb.answer(); return
    title = st.practices[idx]
    note_id = Config.PRACTICE_NOTES.get(idx)
    if note_id:
        try:
            await cb.message.answer_video_note(note_id)
        except Exception as e:
            logger.error(f"Practice video error: {e}")
    await send_practice_intro(cb.message, idx, title)
    await cb.message.answer(f"<b>Практика:</b> {title}\n\nНачинаем?", reply_markup=kb_practice_card(idx))
    await cb.answer()

# ========== REDEEM ==========
@r.callback_query(F.data == CallbackData.REDEEM_START.value)
async def cb_redeem_start(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    reset_waiting_flags(st)
    if not st.is_authorized:
        await cb.message.answer("Сначала авторизуйтесь.", reply_markup=kb_cabinet_unauth())
        await cb.answer(); return
    st.awaiting_code = True
    await cb.message.answer("Введите <b>код с карточки</b>:",
                            reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.CABINET.value}]]))
    await cb.answer()

# ========== CHECKOUT ==========
@r.callback_query(F.data == CallbackData.CHECKOUT_START.value)
async def cb_checkout_start(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    st.pvz_for_order_id = None
    if st.is_authorized:
        await cb.message.answer(
            f"Проверьте данные:\n• ФИО: {st.full_name}\n• Телефон: {st.phone}\n• Email: {st.email}\n\nХотите изменить?",
            reply_markup=kb_change_contact()
        )
    else:
        reset_waiting_flags(st)
        st.awaiting_contact = True
        await cb.message.answer(
            "Введите данные в 3 строки:\n1. Имя Фамилия\n2. +7XXXXXXXXXX\n3. email@example.com",
            reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.MENU.value}]])
        )
    await cb.answer()

@r.callback_query(F.data.startswith("change_contact:"))
async def cb_change_contact(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    if cb.data == CallbackData.CHANGE_CONTACT_YES.value:
        reset_waiting_flags(st)
        st.awaiting_contact = True
        await cb.message.answer(
            "Введите новые данные:\n1. Имя Фамилия\n2. +7XXXXXXXXXX\n3. email@example.com",
            reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.GALLERY.value}]])
        )
    else:
        reset_waiting_flags(st)
        st.awaiting_pvz_address = True
        await cb.message.answer(
            "Введите адрес ПВЗ (например: «Профсоюзная, 93»):",
            reply_markup=create_inline_keyboard([[{"text": "Назад", "callback_data": CallbackData.GALLERY.value}]])
        )
    await cb.answer()

@r.callback_query(F.data == CallbackData.SHIP_CDEK.value)
async def cb_shipping_cdek(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    if not st.is_authorized:
        await cb.message.answer("Сначала авторизуйтесь.", reply_markup=kb_cabinet_unauth())
        await cb.answer(); return
    reset_waiting_flags(st)
    st.pvz_for_order_id = None
    st.awaiting_pvz_address = True
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
    try:
        parts = cb.data.split(":")
        kind = parts[1]           # "full" | "pre" | "rem"
        oid_str = parts[2]
        oid = int(oid_str) if oid_str != "0" else None
        st = ustate(cb.from_user.id)

        # === 1. Находим или создаём заказ ===
        if oid:
            order = state.orders.get(oid)
            if not order or order.user_id != cb.from_user.id:
                await cb.answer("Заказ не найден", show_alert=True)
                return
        else:
            # Новый заказ — только при первой оплате (100% или 30%)
            if not st.temp_selected_pvz:
                await cb.answer("Ошибка: ПВЗ не выбран", show_alert=True)
                return
            order = state.new_order(cb.from_user.id)
            order.shipping_method = "cdek_pvz"
            order.address = st.temp_selected_pvz["address"]
            order.extra_data.update({
                "pvz_code": st.temp_selected_pvz["code"],
                "delivery_cost": st.extra_data.get("delivery_cost", 590),
                "delivery_period": st.extra_data.get("delivery_period", "3–7"),
            })
            st.temp_selected_pvz = None

        # Устанавливаем итоговую цену (один раз)
        if order.total_price == 0:
            delivery_cost = order.extra_data.get("delivery_cost", 590)
            order.total_price = Config.PRICE_RUB + delivery_cost

        # Уведомляем админа о начале оплаты
        await notify_admins_payment_started(order)

        # Отменяем таймаут оплаты
        cancel_payment_timeout(order.id)

        # ==================================================================
        # === СЦЕНАРИЙ 1: Полная оплата сразу (100%) =========================
        # ==================================================================
        if kind == "full":
            order.status = OrderStatus.PAID.value
            order.payment_kind = "full"

            await notify_admins_payment_success(order)

            await cb.message.answer(
                "Полная оплата получена! Спасибо огромное! ❤️\n\n"
                f"Заказ <b>#{order.id}</b> уже собирается и скоро будет передан в СДЭК.\n"
                "Трек-номер пришлю автоматически через 1–2 минуты",
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
                    "Админ уже в курсе — отправим в ближайшие минуты!",
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
                "Как только коробочка будет готова — пришлю ссылку на дооплату остатка и сразу отправлю посылку",
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
    try:
        oid = int(cb.data.split(":")[1])
        order = state.orders.get(oid)
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
    st = ustate(cb.from_user.id)
    reset_waiting_flags(st)
    if not st.is_authorized:
        await edit_or_send(cb.message, "Пожалуйста, авторизуйтесь.", kb_cabinet_unauth())
        await cb.answer(); return
    ids = [oid for oid, o in state.orders.items() if o.user_id == cb.from_user.id]
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
        order = state.orders.get(oid)
        if not order or order.user_id != cb.from_user.id:
            await cb.answer("Заказ не найден", show_alert=True)
            return

        st = ustate(cb.from_user.id)
        reset_waiting_flags(st)
        st.pvz_for_order_id = oid          # 👈 запоминаем, для какого заказа меняем адрес
        st.awaiting_pvz_address = True

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
    orders = [o for o in state.orders.values() if o.status == OrderStatus.PREPAID.value]
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
    orders = [o for o in state.orders.values() if o.status in [OrderStatus.READY.value, OrderStatus.PAID.value]]
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
    orders = [o for o in state.orders.values() if o.status == OrderStatus.SHIPPED.value]
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
    orders = [o for o in state.orders.values() if o.status == OrderStatus.ARCHIVED.value]
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
        order = state.orders.get(oid)
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
        order = state.orders.get(oid)
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
        order = state.orders.get(oid)
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

# ========== TEXT HANDLERS ==========
async def handle_contact(message: Message, ust: UserState, text: str):
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    if len(lines) != 3:
        await message.answer("Введите 3 строки: Имя Фамилия, +7..., email."); return
    full_name, phone, email = lines
    ok, msg = validate_data(full_name, phone, email)
    if not ok:
        await message.answer(msg + "\nПопробуйте снова."); return
    ust.awaiting_contact = False
    ust.full_name, ust.phone, ust.email = full_name, phone, email
    await message.answer(f"Успешно, {message.from_user.first_name}! Вы в системе.", reply_markup=kb_cabinet())

async def handle_pvz_address(message: Message, ust: UserState, text: str):
    query = text.strip()
    if len(query) < 3:
        await message.answer("Слишком короткий запрос. Укажи хотя бы часть адреса или название города.")
        return

    # Определяем город по тексту
    city_name = "Москва"
    city_code = "44"

    lower = query.lower()
    for name, code in Config.POPULAR_CITIES.items():
        if name.lower() in lower or name.lower().split()[0] in lower:
            city_name = name
            city_code = code
            break

    await message.answer(f"Ищу ПВЗ в городе <b>{city_name}</b>…", parse_mode="HTML")

    pvz_list = await find_best_pvz(query, city=city_name, limit=20)

    if not pvz_list:
        await message.answer(
            f"Не нашёл ПВЗ в <b>{city_name}</b>.\nПопробуй ввести адрес точнее или другой город.",
            parse_mode="HTML",
            reply_markup=create_inline_keyboard([
                [{"text": "Попробовать ещё раз", "callback_data": "pvz_reenter"}],
                [{"text": "В меню", "callback_data": CallbackData.MENU.value}]
            ])
        )
        return

    # Сохраняем найденные ПВЗ и город
    ust.temp_pvz_list = pvz_list
    ust.extra_data["city"] = city_name
    ust.extra_data["city_code"] = city_code

    await message.answer(
        f"Нашёл {len(pvz_list)} ПВЗ в <b>{city_name}</b>.\nВыбери нужный ниже:",
        parse_mode="HTML",
        reply_markup=kb_pvz_list(pvz_list)
    )


async def handle_manual_pvz(message: Message, ust: UserState, text: str):
    desc = text.strip()
    if not desc:
        await message.answer("Опиши ПВЗ: код или полный адрес.")
        return

    ust.awaiting_manual_pvz = False

    # 1) Если мы в режиме "меняем адрес у конкретного заказа"
    if ust.pvz_for_order_id and ust.pvz_for_order_id in state.orders:
        order = state.orders[ust.pvz_for_order_id]
        ust.pvz_for_order_id = None

        order.shipping_method = "cdek_pvz_manual"
        order.address = desc
        order.extra_data["pvz_manual"] = desc

        await notify_admins_order_address_changed(order)

        kb = kb_ready_message(order) if order.status == OrderStatus.READY.value else kb_order_status(order)
        await message.answer(
            f"Адрес ПВЗ для заказа #{order.id} обновлён.\n\n{desc}",
            reply_markup=kb
        )
        return

    # 2) Обычный сценарий оформления нового заказа
    order = state.new_order(message.from_user.id)
    order.shipping_method = "cdek_pvz_manual"
    order.address = desc
    order.extra_data = {"pvz_manual": desc}

    await notify_admins_payment_started(order)

    await message.answer(
        "Зафиксировали ПВЗ вручную.\n"
        "Адрес/код для сбора заказа:\n"
        f"{desc}\n\n" + format_order_review(order),
        reply_markup=kb_review(order)
    )


@r.callback_query(F.data == "pvz_reenter")
async def cb_pvz_reenter(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    # НЕ сбрасываем pvz_for_order_id, чтобы помнить, редактируем ли заказ
    st.awaiting_pvz_address = True
    st.awaiting_manual_pvz = False

    await cb.message.edit_text(
        "Введите адрес ПВЗ ещё раз (например: Барклая, 5А):",
        reply_markup=create_inline_keyboard([
            [{"text": "Отмена", "callback_data": CallbackData.MENU.value}]
        ])
    )
    await cb.answer()


@r.callback_query(F.data == "pvz_backlist")
async def cb_pvz_backlist(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    if not st.temp_pvz_list:
        await cb.answer("Список устарел, введите адрес заново", show_alert=True)
        return

    await edit_or_send(
        cb.message,
        "Выбери нужный ПВЗ:",
        kb_pvz_list(st.temp_pvz_list)
    )
    await cb.answer()



# === ОБНОВЛЁННЫЙ обработчик выбора ПВЗ ===
@r.callback_query(lambda c: (c.data or "").startswith("pvz_sel:"))
async def cb_pvz_select(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    try:
        _, old_code, idx_str = cb.data.split(":")
        idx = int(idx_str)

        if not (0 <= idx < len(st.temp_pvz_list)):
            await cb.answer("Список ПВЗ устарел — введите адрес заново", show_alert=True)
            return

        pvz = st.temp_pvz_list[idx]

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
        st.temp_selected_pvz = {
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
        order = state.new_order(cb.from_user.id)
        order.shipping_method = "cdek_pvz"
        order.address = full_address
        order.total_price = total
        order.extra_data.update({
            "pvz_code": real_code,
            "delivery_cost": delivery_cost,
            "delivery_period": period_text,
        })

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

    except Exception as e:
        logger.error(f"cb_pvz_select error: {e}", exc_info=True)
        await cb.answer("Ошибка", show_alert=True)


@r.callback_query(F.data == "pvz_manual")
async def cb_pvz_manual(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    # Сбрасываем только ожидания текста, но НЕ pvz_for_order_id
    st.awaiting_pvz_address = False
    st.awaiting_manual_pvz = True

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
    st = ustate(cb.from_user.id)
    pvz_list = st.temp_pvz_list

    if not pvz_list:
        # вдруг бот перезапустился и память очистилась
        st.awaiting_pvz_address = True
        await cb.message.edit_text(
            "Список ПВЗ устарел.\nВведите адрес ПВЗ ещё раз (например: Барклая, 5А):",
            reply_markup=create_inline_keyboard([
                [{"text": "Отмена", "callback_data": CallbackData.MENU.value}]
            ])
        )
        await cb.answer()
        return

    query = st.extra_data.get("pvz_query", "выбранным адресом")

    await edit_or_send(
        cb.message,
        f"Нашёл {len(pvz_list)} ПВЗ рядом с «{query}» (Москва).\nВыбери нужный:",
        kb_pvz_list(pvz_list)
    )
    await cb.answer()


@r.callback_query(F.data == "pvz_confirm")
async def cb_pvz_confirm(cb: CallbackQuery):
    st = ustate(cb.from_user.id)
    if not st.temp_selected_pvz:
        await cb.answer("Ошибка выбора", show_alert=True)
        return

    pvz = st.temp_selected_pvz
    code = pvz["code"]

    await cb.message.answer("Считаю стоимость и срок доставки…")
    delivery_info = await calculate_cdek_delivery_cost(st.temp_selected_pvz["code"])

    if delivery_info is None:
        delivery_cost = 590
        period_text = "3–7"
        period_min = 3
        period_max = 7
    else:
        delivery_cost = delivery_info["cost"]
        pmin = delivery_info["period_min"]
        pmax = delivery_info["period_max"] or pmin + 2
        period_text = f"{pmin}" if pmin == pmax else f"{pmin}–{pmax}"
        period_min = pmin
        period_max = pmax

    # ←←← СОЗДАЁМ ЗАКАЗ ЗДЕСЬ ←←←
    order = state.new_order(cb.from_user.id)
    order.shipping_method = "cdek_pvz"
    order.address = pvz["address"]
    order.extra_data.update({
        "pvz_code": code,
        "delivery_cost": delivery_cost,
        "delivery_period": period_text,          # ← это будет показываться в статусе
        "delivery_period_min": period_min,
        "delivery_period_max": period_max,
    })

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
    await cb.answer("Готово!")


@r.message()
async def on_text(message: Message):
    uid = message.from_user.id
    st = ustate(uid)
    text = (message.text or "").strip()
    low = text.lower()

    if text.startswith("/"):
        if text.startswith("/admin "):
            await handle_admin_command(message, text)
        return
    if low in {"меню", "/menu"}: await cmd_menu(message); return
    if low in {"мои практики", "практики"}:
        await cb_practices(type("obj", (), {"from_user": message.from_user, "message": message, "answer": lambda *a, **k: None, "data": ""})())
        return
    if low in {"личный кабинет", "кабинет"}:
        await cb_cabinet(type("obj", (), {"from_user": message.from_user, "message": message, "answer": lambda *a, **k: None})())
        return
    if low in {"заказать"}:
        await cb_checkout_start(type("obj", (), {"from_user": message.from_user, "message": message, "answer": lambda *a, **k: None})())
        return

    if st.awaiting_code: await handle_code(message, st, text); return
    if st.awaiting_contact: await handle_contact(message, st, text); return
    if st.awaiting_manual_pvz: await handle_manual_pvz(message, st, text); return
    if st.awaiting_pvz_address: await handle_pvz_address(message, st, text); return

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
        await message.answer("Использование: /admin <действие> [order_id] [track]\nДействия: list, ready, paid, shipped\nПримеры: /admin list, /admin ready 1, /admin shipped 1 PVZ123")
        return

    action, *args = parts[1], parts[2:]

    if action == "list":
        if not state.orders:
            await message.answer("Нет заказов.")
            return
        def tag(o: Order) -> str:
            return {
                OrderStatus.NEW.value: "new",
                OrderStatus.PENDING.value: "pending",
                OrderStatus.PREPAID.value: "prepaid",
                OrderStatus.READY.value: "ready",
                OrderStatus.PAID.value: "paid",
                OrderStatus.SHIPPED.value: "shipped",
                OrderStatus.ARCHIVED.value: "archived",
                OrderStatus.ABANDONED.value: "abandoned",
            }.get(o.status, o.status)
        rows = [f"#{oid}: {tag(o)} | {o.address or '—'} | {ustate(o.user_id).full_name or o.user_id}" for oid, o in state.orders.items()]
        await message.answer("Заказы:\n" + "\n".join(rows))

    elif action == "ready":
        if len(args) == 0 or not args[0].isdigit():
            await message.answer(f"Укажите order_id. Пример: /admin ready 1")
            return
        order_id = int(args[0])
        order = state.orders.get(order_id)
        if order and order.status == OrderStatus.PREPAID.value:
            cancel_payment_timeout(order_id)
            order.status = OrderStatus.READY.value
            await notify_admins_order_ready(order)
            await notify_client_order_ready(order, message)
            await message.answer(f"✅ Заказ #{order_id}: READY")
        else:
            await message.answer(f"Заказ #{order_id} не найден или не в предоплате.")

    elif action == "shipped":
        if len(args) < 1 or not args[0].isdigit():
            await message.answer("Укажите order_id. Пример: /admin shipped 1 PVZ123")
            return
        order_id = int(args[0])
        track = args[1] if len(args) > 1 else None
        if not track:
            await message.answer("Укажите трек. Пример: /admin shipped 1 PVZ123")
            return
        order = state.orders.get(order_id)
        if order and order.status in [OrderStatus.READY.value, OrderStatus.PAID.value]:
            cancel_payment_timeout(order_id)
            order.status = OrderStatus.SHIPPED.value
            order.track = track
            await notify_admins_order_shipped(order)
            await notify_client_order_shipped(order, message)
            await message.answer(f"📦 Заказ #{order_id} отправлен! Трек: {track}")
        else:
            await message.answer(f"Заказ #{order_id} не найден или не готов к отправке.")

    elif action == "archived":
        if len(args) == 0 or not args[0].isdigit():
            await message.answer(f"Укажите order_id. Пример: /admin archived 1")
            return
        order_id = int(args[0])
        order = state.orders.get(order_id)
        if order and order.status in [OrderStatus.PAID.value, OrderStatus.SHIPPED.value]:
            order.status = OrderStatus.ARCHIVED.value
            await notify_admins_order_archived(order)
            await message.answer(f"🗄 Заказ #{order_id} заархивирован")
        else:
            await message.answer(f"Заказ #{order_id} не найден или не может быть заархивирован.")

    else:
        await message.answer("Неверное действие. Доступные: list, ready, shipped, archived")

# ========== НОВЫЕ ФУНКЦИИ СДЭК ==========

async def get_cdek_pvz_list(address_query: str, city: str = "Москва", limit: int = 10) -> List[dict]:
    """Ищет ПВЗ СДЭК по адресу + обязательно город (в тестовой среде без города — 0 результатов)"""
    token = await get_cdek_token()
    if not token:
        logger.error("Нет токена для поиска ПВЗ")
        return []

    url = "https://api.edu.cdek.ru/v2/deliverypoints"
    params = {
        "city": city,
        "address": address_query.strip(),
        "type": "PVZ",
        "limit": limit
    }
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        if resp.status_code == 200:
            points = resp.json()
            logger.info(f"Найдено {len(points)} ПВЗ по запросу '{address_query}' в городе '{city}'")
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

async def find_best_pvz(address_query: str, city: str = "Москва", limit: int = 10) -> List[dict]:
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
    """Фоновая задача: проверяет статусы заказов в СДЭК и присылает трек-номер + обновления"""
    while True:
        try:
            logger.info("Запуск проверки статусов СДЭК...")
            orders_to_check = [
                order for order in state.orders.values()
                if order.status == OrderStatus.SHIPPED.value
                and order.extra_data.get("cdek_uuid")
            ]

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
                    await order.save()

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
                        f"Клиент: {ustate(order.user_id).full_name or 'Неизвестно'}"
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
    logger.info("Бот запущен")
    asyncio.create_task(check_all_shipped_orders())
    while True:
        try:
            logger.info("Попытка подключения к Telegram...")
            await dp.start_polling(bot)
        except Exception as e:
            logger.error(f"Ошибка подключения: {e}. Повтор через 15 секунд...")
            await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())