from __future__ import annotations

import random
from datetime import date
from pathlib import Path

from masking.db import get_client, run_sql_file, scalar

SEED = 20251101
N_CLIENTS = 1000
WINDOW_START = date(2024, 11, 1)
N_MONTHS = 24
DDL = Path(__file__).parent / "ddl_dwh.sql"

CITIES = {
    "Москва": 20, "Ярославль": 4, "Тула": 3, "Владимир": 3, "Суздаль": 3, "Кострома": 3,
    "Санкт-Петербург": 12, "Калининград": 3, "Великий Новгород": 2, "Мурманск": 1,
    "Сочи": 9, "Краснодар": 4, "Анапа": 3, "Геленджик": 3, "Ростов-на-Дону": 3,
    "Казань": 5, "Нижний Новгород": 3, "Самара": 2,
    "Екатеринбург": 3, "Тюмень": 2,
    "Новосибирск": 2, "Иркутск": 2,
    "Владивосток": 2, "Хабаровск": 1,
    "Кисловодск": 1, "Пятигорск": 1,
}

TARIFFS = [(1, "Старт", 4900), (2, "Бизнес", 9900), (3, "Премиум", 19900), (4, "Сеть", 39900)]

MALE = [("Иван", "Иванович"), ("Алексей", "Сергеевич"), ("Дмитрий", "Андреевич"),
        ("Сергей", "Викторович"), ("Андрей", "Павлович"), ("Михаил", "Олегович"),
        ("Николай", "Петрович"), ("Павел", "Дмитриевич"), ("Артём", "Игоревич"),
        ("Олег", "Николаевич"), ("Владимир", "Александрович"), ("Егор", "Михайлович")]
FEMALE = [("Анна", "Сергеевна"), ("Елена", "Викторовна"), ("Ольга", "Андреевна"),
          ("Мария", "Ивановна"), ("Татьяна", "Павловна"), ("Наталья", "Олеговна"),
          ("Ирина", "Дмитриевна"), ("Светлана", "Николаевна"), ("Юлия", "Александровна"),
          ("Екатерина", "Михайловна"), ("Дарья", "Игоревна"), ("Ксения", "Петровна")]
SURNAMES = ["Иванов", "Смирнов", "Кузнецов", "Попов", "Васильев", "Петров", "Соколов",
            "Михайлов", "Новиков", "Фёдоров", "Морозов", "Волков", "Алексеев", "Лебедев",
            "Семёнов", "Егоров", "Павлов", "Козлов", "Степанов", "Николаев", "Орлов",
            "Андреев", "Макаров", "Никитин", "Захаров", "Зайцев", "Соловьёв", "Борисов"]
HOTEL_TYPES = ["Отель", "Гостиница", "Мини-отель", "Гостевой дом", "Апарт-отель", "Хостел", "Бутик-отель"]
HOTEL_NAMES = ["Волга", "Парус", "Уют", "Северная звезда", "Берёзка", "Причал", "Лазурь",
               "Старый город", "Купеческий", "Империал", "Байкал", "Морской бриз", "Сосновый бор",
               "Акварель", "Метрополь", "Тайга", "Ласточка", "Резиденция", "Панорама", "Кремлёвский"]
DOMAINS = ["mail.ru", "yandex.ru", "gmail.com", "bk.ru", "list.ru", "inbox.ru"]

TRANSLIT = dict(zip("абвгдеёжзийклмнопрстуфхцчшщъыьэюя",
                    ["a", "b", "v", "g", "d", "e", "e", "zh", "z", "i", "y", "k", "l", "m", "n", "o",
                     "p", "r", "s", "t", "u", "f", "kh", "ts", "ch", "sh", "sch", "", "y", "", "e",
                     "yu", "ya"]))


def translit(s: str) -> str:
    return "".join(TRANSLIT.get(ch, ch) for ch in s.lower())


def inn10(r: random.Random) -> str:
    d = [r.randint(0, 9) for _ in range(9)]
    d[0] = r.randint(1, 9)
    w = [2, 4, 10, 3, 5, 9, 4, 6, 8]
    return "".join(map(str, d)) + str(sum(a * b for a, b in zip(d, w)) % 11 % 10)


def inn12(r: random.Random) -> str:
    d = [r.randint(0, 9) for _ in range(10)]
    d[0] = r.randint(1, 9)
    w1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
    d.append(sum(a * b for a, b in zip(d, w1)) % 11 % 10)
    w2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
    d.append(sum(a * b for a, b in zip(d, w2)) % 11 % 10)
    return "".join(map(str, d))


def add_months(d: date, n: int) -> date:
    m = d.month - 1 + n
    return date(d.year + m // 12, m % 12 + 1, 1)


def rooms(r: random.Random) -> int:
    bucket = r.choices([(4, 20), (21, 50), (51, 100), (101, 450)], [42, 33, 17, 8])[0]
    return r.randint(*bucket)


def connected(r: random.Random) -> date:
    q = r.choices(range(6), [20, 20, 18, 16, 14, 12])[0]
    year, qn = 2024 + q // 4, q % 4
    return date(year, qn * 3 + r.randint(1, 3), r.randint(1, 28))


def make_clients(r: random.Random) -> list[tuple]:
    clients, phones, inns, emails = [], set(), set(), set()
    cities, cw = list(CITIES), list(CITIES.values())
    for key in range(1, N_CLIENTS + 1):
        female = r.random() < 0.5
        first, patr = r.choice(FEMALE if female else MALE)
        surname = r.choice(SURNAMES) + ("а" if female else "")
        name = f"{surname} {first} {patr}"
        while True:
            email = f"{translit(first)}.{translit(surname)}{r.randint(1, 999)}@{r.choice(DOMAINS)}"
            if email not in emails:
                emails.add(email)
                break
        while True:
            p = f"9{r.randint(0, 99):02d}{r.randint(0, 9999999):07d}"
            if p not in phones:
                phones.add(p)
                break
        phone = f"+7 ({p[:3]}) {p[3:6]}-{p[6:8]}-{p[8:]}"
        while True:
            inn = inn12(r) if r.random() < 0.35 else inn10(r)
            if inn not in inns:
                inns.add(inn)
                break
        hotel = f"{r.choice(HOTEL_TYPES)} «{r.choice(HOTEL_NAMES)}»"
        clients.append((key, name, email, phone, inn, hotel,
                        r.choices(cities, cw)[0], rooms(r), connected(r)))
    return clients


def tariff_for(rooms_count: int, r: random.Random) -> int:
    if rooms_count > 100:
        return r.choice([3, 4, 4])
    if rooms_count > 50:
        return r.choice([2, 3, 3])
    if rooms_count > 20:
        return r.choice([1, 2, 2, 3])
    return r.choice([1, 1, 2])


def make_facts(r: random.Random, clients: list[tuple]) -> list[tuple]:
    price = {k: p for k, _, p in TARIFFS}
    facts = []
    for key, *_, rooms_count, conn in clients:
        tariff = tariff_for(rooms_count, r)
        health = r.betavariate(5, 2)
        start = max(add_months(conn.replace(day=1), 1), WINDOW_START)
        month, decline = start, 0
        while month < add_months(WINDOW_START, N_MONTHS):
            hazard = 0.004 + 0.05 * (1 - health) ** 2 + 0.03 * decline
            churn = r.random() < hazard
            if r.random() < 0.08 * (1 - health):
                decline += 1
            k = max(0.15, health - 0.15 * decline)
            staff = max(1, int(rooms_count / 8) + 1)
            active = max(1, round(staff * k * r.uniform(0.7, 1.1)))
            logins = round(active * 22 * k * r.uniform(0.7, 1.2))
            bookings = round(rooms_count * 2.2 * k * r.uniform(0.6, 1.3))
            features = max(1, min(30, round(5 + tariff * 5 * k * r.uniform(0.7, 1.2))))
            tickets = max(0, round(r.gauss(1 + 3 * (1 - k), 1)))
            late = 1 if r.random() < 0.03 + 0.25 * (1 - k) else 0
            if r.random() < 0.02 and tariff < 4:
                tariff += 1
            facts.append((key, tariff, month, logins, active, bookings, features, tickets,
                          late, price[tariff], 1 if churn else 0))
            if churn:
                break
            month = add_months(month, 1)
    return facts


def generate() -> dict:
    r = random.Random(SEED)
    clients = make_clients(r)
    facts = make_facts(r, clients)
    dates = []
    for i in range(N_MONTHS):
        d = add_months(WINDOW_START, i)
        dates.append((d, d.year, (d.month - 1) // 3 + 1, d.month))

    ch = get_client()
    run_sql_file(ch, DDL)
    ch.insert("dwh.dim_client", clients, column_names=[
        "client_key", "contact_name", "email", "phone", "inn", "hotel_name", "city",
        "rooms_count", "connected_date"])
    ch.insert("dwh.dim_tariff", TARIFFS, column_names=["tariff_key", "tariff_name", "price"])
    ch.insert("dwh.dim_date", dates, column_names=["date_key", "year", "quarter", "month"])
    ch.insert("dwh.fact_client_activity", facts, column_names=[
        "client_key", "tariff_key", "date_key", "logins_count", "active_users",
        "bookings_processed", "features_used", "tickets_count", "late_payments", "mrr",
        "churn_flag"])

    orphans = scalar(ch, """
        SELECT countIf(c.client_key = 0) + countIf(t.tariff_key = 0) + countIf(d.year = 0)
        FROM dwh.fact_client_activity f
        LEFT JOIN dwh.dim_client c ON f.client_key = c.client_key
        LEFT JOIN dwh.dim_tariff t ON f.tariff_key = t.tariff_key
        LEFT JOIN dwh.dim_date   d ON f.date_key   = d.date_key""")
    if orphans:
        raise RuntimeError(f"dwh: {orphans} строк факта без пары в измерениях")
    dupes = scalar(ch, """SELECT count() - uniqExact(client_key, date_key)
                          FROM dwh.fact_client_activity""")
    if dupes:
        raise RuntimeError(f"dwh: нарушено зерно клиент×месяц ({dupes} дублей)")
    return {"dim_client": len(clients), "dim_tariff": len(TARIFFS),
            "dim_date": len(dates), "fact_client_activity": len(facts)}
