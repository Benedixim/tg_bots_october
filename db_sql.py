# db_sql.py
import os
import sqlite3
import datetime
from typing import Any, Dict, List, Tuple, Optional

DB_PATH = "banks_backup_20251122_010000.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


# ---------- BANKS ----------
def get_banks() -> List[Tuple[int, str, str]]:
    """[(id, name, loyalty_url), ...]"""
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name, loyalty_url FROM banks ORDER BY name;")
        return cur.fetchall()
    finally:
        conn.close()


def get_all_bank_ids() -> List[int]:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM banks;")
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


# ---------- SCRAPER CONFIG ----------
def fetch_categories_scrape_config(bank_id: int) -> Dict[str, Any]:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT loyalty_url, cookie, container, element
            FROM banks WHERE id=?
        """, (bank_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"bank_id={bank_id} not found")
        return {
            "url": row[0] or "",
            "cookie_text": row[1] or "",
            "container_selector": row[2] or "",
            "element_selector": row[3] or "",
        }
    finally:
        conn.close()


def fetch_partners_scrape_config(bank_id: int) -> Dict[str, Any]:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT button_more, partners_list, partner_name, partner_bonus, bonus_unit
            FROM banks WHERE id=?
        """, (bank_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"bank_id={bank_id} not found")
        return {
            "button_more": row[0] or "",
            "partners_list": row[1] or "",
            "partner_name": row[2] or "",
            "partner_bonus": row[3] or "",
            "bonus_unit": row[4] or "",
        }
    finally:
        conn.close()


# ---------- TABLE ENSURE ----------
def ensure_categories_table(conn: Optional[sqlite3.Connection] = None) -> None:
    close = False
    if conn is None:
        conn = _conn()
        close = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_id INTEGER NOT NULL,
            partners_count INTEGER,
            checked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            FOREIGN KEY(bank_id) REFERENCES banks(id)
        );
    """)
    conn.commit()
    if close:
        conn.close()


def ensure_partners_table(conn: Optional[sqlite3.Connection] = None) -> None:
    close = False
    if conn is None:
        conn = _conn()
        close = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS partners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            partner_name TEXT NOT NULL,
            partner_bonus TEXT,
            partner_link TEXT,
            checked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(bank_id) REFERENCES banks(id),
            FOREIGN KEY(category_id) REFERENCES categories(id)
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_partners_bank_cat_name ON partners(bank_id, category_id, partner_name);")
    conn.commit()
    if close:
        conn.close()


# ---------- CATEGORIES ----------
def save_single_category(category: Dict[str, Any], bank_id: int) -> int:
    """
    Создаёт новую запись категории, если изменились url/partners_count, иначе возвращает id последней.
    """
    conn = _conn()
    try:
        ensure_categories_table(conn)
        cur = conn.cursor()
        checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        name = category["category_name"]
        new_count = category.get("partners_count")
        new_url = category["category_url"]

        cur.execute("""
            SELECT id, partners_count, url
            FROM categories
            WHERE bank_id=? AND name=?
            ORDER BY checked_at DESC
            LIMIT 1
        """, (bank_id, name))
        last = cur.fetchone()

        if last is None or last[1] != new_count or last[2] != new_url:
            cur.execute("""
                INSERT INTO categories (bank_id, partners_count, checked_at, name, url)
                VALUES (?, ?, ?, ?, ?)
            """, (bank_id, new_count, checked_at, name, new_url))
            category_id = cur.lastrowid
        else:
            category_id = last[0]

        conn.commit()
        return category_id
    finally:
        conn.close()


def get_latest_categories_by_bank(bank_id: int) -> List[Tuple[int, str, str]]:
    """
    [(category_id, name, url), ...] — только последние версии категорий по имени.
    """
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT c.id, c.name, c.url
            FROM categories c
            INNER JOIN (
                SELECT name, MAX(checked_at) as max_checked
                FROM categories
                WHERE bank_id = ?
                GROUP BY name
            ) sub ON c.name = sub.name AND c.checked_at = sub.max_checked
            WHERE c.bank_id = ?
            ORDER BY c.name;
        """, (bank_id, bank_id))
        return cur.fetchall()
    finally:
        conn.close()


# ---------- PARTNERS ----------
# def save_partners(partners: List[Dict[str, Any]], bank_id: int, category_id: int) -> None:
#     conn = _conn()
#     try:
#         ensure_partners_table(conn)
#         cur = conn.cursor()
#         checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         for p in partners:
#             cur.execute("""
#                 SELECT partner_bonus, partner_link
#                 FROM partners
#                 WHERE bank_id=? AND category_id=? AND partner_name=? AND partner_bonus=?
#                 ORDER BY checked_at DESC
#                 LIMIT 1
#             """, (bank_id, category_id, p["partner_name"], p.get("partner_bonus")))
#             last = cur.fetchone()
#             bonus = p.get("partner_bonus")
#             link = p.get("partner_link") or ""
#             if last is None or last[0] != bonus or last[1] != link:
#                 cur.execute("""
#                     INSERT INTO partners (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at)
#                     VALUES (?, ?, ?, ?, ?, ?)
#                 """, (bank_id, category_id, p["partner_name"], bonus, link, checked_at))
#         conn.commit()
#     finally:
#         conn.close()
def save_partners(partners: List[Dict[str, Any]], bank_id: int, category_id: int) -> None:
    conn = _conn()
    try:
        ensure_partners_table(conn)
        cur = conn.cursor()
        checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for p in partners:
            bonus = p.get("partner_bonus")
            link = p.get("partner_link")

            # 🚫 Пропускаем, если бонус пустой
            if not bonus or str(bonus).strip() == "":
                continue

            # 🚫 Пропускаем, если ссылки нет или она пустая
            if not link or str(link).strip() == "":
                continue

            # link иногда = None → подстрахуемся
            link = link.strip()

            # Проверяем последнюю запись
            cur.execute("""
                SELECT partner_bonus, partner_link
                FROM partners
                WHERE bank_id=? AND category_id=? AND partner_name=? AND partner_bonus=?
                ORDER BY checked_at DESC
                LIMIT 1
            """, (bank_id, category_id, p["partner_name"], bonus))

            last = cur.fetchone()

            # Изменилось? → сохраняем
            if last is None or last[0] != bonus or last[1] != link:
                cur.execute("""
                    INSERT INTO partners (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (bank_id, category_id, p["partner_name"], bonus, link, checked_at))

        conn.commit()
    finally:
        conn.close()


def get_partners_latest_by_bank_category(bank_id: int, category_id: int) -> List[Tuple[str, Optional[str], Optional[str]]]:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT partner_name, partner_bonus, partner_link
            FROM partners
            WHERE bank_id = ? AND category_id = ?
            AND checked_at = (SELECT MAX(checked_at) FROM partners p2 WHERE p2.bank_id=? AND p2.category_id=?)
            ORDER BY partner_name;
        """, (bank_id, category_id, bank_id, category_id))
        return cur.fetchall()
    finally:
        conn.close()


def search_partners_latest(query: str) -> List[Tuple[str, str, str, Optional[str], Optional[str], Optional[str]]]:
    """
    Возвращает:
    (bank_name, category_name, partner_name, partner_bonus, bonus_unit, partner_link)
    только с последней версией по каждой паре (bank_id, category_id).
    """
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b.name as bank_name,
                   c.name as category_name,
                   p.partner_name,
                   p.partner_bonus,
                   b.bonus_unit,
                   p.partner_link
            FROM partners p
            JOIN banks b ON p.bank_id = b.id
            JOIN categories c ON p.category_id = c.id
            WHERE p.partner_name LIKE ?
            AND p.checked_at = (
                SELECT MAX(p2.checked_at)
                FROM partners p2
                WHERE p2.bank_id = p.bank_id AND p2.category_id = p.category_id
            )
            ORDER BY b.name, c.name, p.partner_name;
        """, (f"%{query}%",))
        return cur.fetchall()
    finally:
        conn.close()


def get_partner_counts_by_bank(bank_id: int) -> List[Tuple[str, int]]:
    """
    [(category_name, partners_count), ...] — подсчёт партнёров по категориям для графика (DESC).
    """
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT c.name, COUNT(p.partner_name) AS partner_cnt
            FROM categories c
            LEFT JOIN partners p ON c.id = p.category_id AND p.bank_id = ?
            WHERE c.bank_id = ?
            GROUP BY c.name
            ORDER BY partner_cnt DESC, c.name ASC;
        """, (bank_id, bank_id))
        return cur.fetchall()
    finally:
        conn.close()

def get_bank_name(bank_id: int) -> str:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM banks WHERE id=?;", (bank_id,))
        row = cur.fetchone()
        return row[0] if row else f"bank_id={bank_id}"
    finally:
        conn.close()

def backup_database(dest_dir: str = ".", filename: str | None = None) -> str:
    """
    Делает безопасную копию banks.db и возвращает путь к файлу.
    Используется SQLite backup API (безопасно при WAL).
    """
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = filename or f"banks_backup_{ts}.db"
    out_path = os.path.join(dest_dir, out_name)

    src = _conn()
    try:
        # сбрасываем WAL перед копированием, если включён
        try:
            src.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            pass

        dst = sqlite3.connect(out_path)
        try:
            src.backup(dst)  # атомарная копия
        finally:
            dst.close()
    finally:
        src.close()

    return out_path


#------------Update-------------------
def get_today_partner_changes() -> list[dict]:
    """
    Возвращает список словарей:
    {
        bank_name,
        category_name,
        partner_name,
        partner_bonus,
        change_type: "new" | "updated",
        checked_at: "YYYY-MM-DD HH:MM:SS"
    }
    Только те партнёры, у кого последняя запись за сегодняшний день.
    """
    today = datetime.date.today()
    since = datetime.datetime.combine(today, datetime.time(0, 0, 0))
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")

    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            WITH latest AS (
                SELECT
                    p.bank_id,
                    p.category_id,
                    p.partner_name,
                    p.partner_bonus,
                    p.partner_link,
                    p.checked_at,
                    (
                        SELECT COUNT(*)
                        FROM partners p2
                        WHERE p2.bank_id = p.bank_id
                          AND p2.category_id = p.category_id
                          AND p2.partner_name = p.partner_name
                    ) AS hist_count
                FROM partners p
                WHERE p.checked_at = (
                    SELECT MAX(p2.checked_at)
                    FROM partners p2
                    WHERE p2.bank_id = p.bank_id
                      AND p2.category_id = p.category_id
                      AND p2.partner_name = p.partner_name
                )
            )
            SELECT
                b.name as bank_name,
                c.name as category_name,
                l.partner_name,
                l.partner_bonus,
                l.checked_at,
                l.hist_count
            FROM latest l
            JOIN banks b ON b.id = l.bank_id
            JOIN categories c ON c.id = l.category_id
            WHERE l.checked_at >= ?
            ORDER BY b.name, c.name, l.partner_name;
        """, (since_str,))
        rows = cur.fetchall()
    finally:
        conn.close()

    result = []
    for bank_name, category_name, partner_name, partner_bonus, checked_at, hist_count in rows:
        change_type = "new" if hist_count == 1 else "updated"
        result.append({
            "bank_name": bank_name,
            "category_name": category_name,
            "partner_name": partner_name,
            "partner_bonus": partner_bonus,
            "change_type": change_type,
            "checked_at": checked_at,
        })
    return result

# ---------- TELEGRAM USERS ----------

def ensure_tg_users_table() -> None:
    """
    Гарантируем, что таблица tg_users существует.
    Хранит chat_id всех, кому потом можно отправлять утренний дайджест.
    """
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tg_users (
                chat_id INTEGER PRIMARY KEY,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    finally:
        conn.close()


def remember_user(chat_id: int) -> None:
    """
    Сохраняем chat_id пользователя, если ещё не сохранён.
    Вызываем, например, в /start и/или в других хендлерах бота.
    """
    ensure_tg_users_table()
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO tg_users(chat_id) VALUES (?);",
            (chat_id,)
        )
        conn.commit()
    finally:
        conn.close()


def get_all_chat_ids() -> List[int]:
    """
    Возвращает список chat_id всех пользователей,
    которым можно отправлять утренний дайджест.
    """
    ensure_tg_users_table()
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM tg_users;")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
