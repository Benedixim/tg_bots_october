# db_sql.py
import os
import sqlite3
import datetime
from typing import Any, Dict, List, Tuple, Optional

DB_PATH = "banks_backup_20260215_005959.db"


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
    print(cur.fetchall())

def get_banks_name(bank_id: int) -> str:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM banks WHERE id=?;", (bank_id,))
        result = cur.fetchone() 

        if result:
            return result[0] 
        return None
    finally:
        conn.close()

def get_categories(category_id: int) -> Tuple[str, str]:
    """Возвращает (название, ссылку) категории"""
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name, url FROM categories WHERE id=?;", (category_id,))
        result = cur.fetchone()
        if result:
            name, url = result  # распаковываем кортеж
            return name, url
        return None, None
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
            SELECT loyalty_url, cookie, container, element, parser_type
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
            "parser_type": row[4] or "default",
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


def get_today_partner_changes() -> list[dict]:
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
                    p.status
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
                l.partner_link,
                l.checked_at,
                l.status,
                b.bonus_unit
            FROM latest l
            JOIN banks b ON b.id = l.bank_id
            JOIN categories c ON c.id = l.category_id
            WHERE l.checked_at >= ?
            ORDER BY b.name, c.name, l.partner_name;
        """, (since_str,))
        rows = cur.fetchall()
    finally:
        conn.close()

    result: list[dict] = []
    for bank_name, category_name, partner_name, partner_bonus, partner_link, checked_at, status, bonus_unit in rows:
        if status == "new":
            change_type = "new"
        elif status == "live":
            change_type = "updated"
        elif status in ("new_delete", "delete"):
            change_type = "deleted"
        else:
            continue

        result.append({
            "bank_name": bank_name,
            "category_name": category_name,
            "partner_name": partner_name,
            "partner_bonus": partner_bonus,
            "partner_link": partner_link,
            "status": status,             
            "change_type": change_type,    
            "checked_at": checked_at,
            "bonus_unit": bonus_unit or "",
        })
    return result


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
            status TEXT,
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
def save_partners(partners: List[Dict[str, Any]], bank_id: int, category_id: int) -> None:
    conn = _conn()
    try:
        ensure_partners_table(conn)
        cur = conn.cursor()
        checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cur.execute("""
            UPDATE partners
            SET status = 'ready'
            WHERE bank_id = ? AND category_id = ?
              AND status IN ('new', 'live')
        """, (bank_id, category_id))

        # проверка - партнер точно ли удален партнер status -> delete
        cur.execute("""
            UPDATE partners
            SET status = 'delete'
            WHERE bank_id = ? AND category_id = ?
              AND status = 'new_delete'
        """, (bank_id, category_id))

        current_names: set[str] = set()

        for p in partners:
            
            bank = p.get("bank_id")
            category = p.get("category_id")
            

            bonus = p.get("partner_bonus")
            link = p.get("partner_link")
            name = p["partner_name"]
            current_names.add(name)
            # 🚫 Пропускаем, если бонус пустой
            #if not bonus or str(bonus).strip() == "":
            #    continue

            # 🚫 Пропускаем, если ссылки нет или она пустая
            #if not link or str(link).strip() == "":
            #    continue

            # link иногда = None → подстрахуемся
            link = link.strip() if isinstance(link, str) else ""

            # Проверяем последнюю запись
            cur.execute("""
                SELECT partner_bonus, partner_link, bank_id, category_id, partner_name, id
                FROM partners
                WHERE bank_id=? AND category_id=? AND partner_name=? 
                        AND COALESCE(NULLIF(TRIM(partner_bonus),''),'') = COALESCE(NULLIF(TRIM(?),''),'')
                        AND COALESCE(NULLIF(TRIM(partner_link),''),'') = COALESCE(NULLIF(TRIM(?),''),'')
                        AND status = 'ready'
                ORDER BY checked_at DESC
                LIMIT 1
            """, (bank_id, category_id, name, bonus, link))

            last = cur.fetchone()

            # Изменилось? → сохраняем
            

            
            if last is None:# or last[0] != bonus or last[1] != link:
                status = "new" #новая
                cur.execute("""
                    INSERT INTO partners (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (bank_id, category_id, name, bonus, link, checked_at, status))

            else:
                # есть ли партнер по такой же ссылке?
                cur.execute("""
                SELECT partner_bonus, partner_link, bank_id, category_id, partner_name, id
                FROM partners
                WHERE bank_id=? AND category_id=? AND partner_name=? 
                        AND COALESCE(NULLIF(TRIM(partner_link),''),'') = COALESCE(NULLIF(TRIM(?),''),'')
                        AND status = 'ready'
                ORDER BY checked_at DESC
                LIMIT 1
                """, (bank_id, category_id, name, link))

                previous  = cur.fetchone()

                # если бонус другой
                if previous[0] != bonus:# другой бонус # неправильная проверка
                    cur.execute(
                        """
                        UPDATE partners
                        SET partner_bonus = ?,
                            checked_at = ?, status = 'live'
                        WHERE id = ?
                        """,
                        (bonus, checked_at, previous[5])
                    )
                else: # та же запись
                    cur.execute(#ready
                        """
                        UPDATE partners
                        SET status = 'live'
                        WHERE id = ?
                        """,
                        (last[5],)
                    )





        placeholders = ",".join("?" for _ in current_names)
        base_params = [checked_at, bank_id, category_id, *current_names]

        # проверка на удаление партнера status -> new_delete

        # убрать массив

        #cur.execute(f"""
        #    UPDATE partners
        #    SET status = 'new_delete', checked_at = ?
        #    WHERE bank_id = ? AND category_id = ?
        #    AND status = 'ready'
        #""", (checked_at, bank_id, category_id))

        cur.execute(f"""
            UPDATE partners
            SET status = 'new_delete'
            WHERE bank_id = ? AND category_id = ?
            AND status = 'ready'
        """, (bank_id, category_id))


        conn.commit()
    finally:
        conn.close()


def get_partners_latest_by_bank_category(bank_id: int, category_id: int) -> List[Tuple[str, Optional[str], Optional[str]]]:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT partner_name, partner_bonus, partner_link
            FROM partners p
            WHERE bank_id = ? AND category_id = ?
            AND checked_at = (
                SELECT MAX(p2.checked_at)
                FROM partners p2
                WHERE p2.bank_id = p.bank_id
                    AND p2.category_id = p.category_id
                    AND p2.partner_name = p.partner_name
            )
            AND status IN ('new','live')
            ORDER BY partner_name;
        """, (bank_id, category_id))
        return cur.fetchall()
    finally:
        conn.close()

def debug_show_akv():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, partner_name, status, checked_at
        FROM partners
        WHERE LOWER(partner_name) LIKE '%акв%'
        ORDER BY partner_name
        LIMIT 10;
    """)
    print("DEBUG LIKE akv:", cur.fetchall())
    conn.close()


def normalize(text: str) -> str:
    return (
        text.lower()
        .replace("ё", "е")
        .replace("«", "")
        .replace("»", "")
        .replace("-", "")
        .replace(" ", "")
        .replace(".", "")
        .replace(",", "")
        .strip()
    )


def search_partners(query: str):
    conn = _conn()
    try:
        cur = conn.cursor()

        q = normalize(query)
        if not q:
            return []

        cur.execute("""
            SELECT 
                b.name,
                COALESCE(c.name, 'Без категории'),
                p.partner_name,
                p.partner_bonus,
                b.bonus_unit,
                p.partner_link
            FROM partners p
            JOIN banks b ON b.id = p.bank_id
            LEFT JOIN categories c ON c.id = p.category_id
            WHERE p.status NOT IN ('delete', 'new_delete')
        """)

        results = []

        for row in cur.fetchall():
            partner_name = normalize(row[2])

            if q in partner_name:
                results.append(row)

        return results

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
                WHERE p2.bank_id = p.bank_id
                    AND p2.category_id = p.category_id
                    AND p2.partner_name = p.partner_name
            )
            AND p.status IN ('new','live')
            ORDER BY b.name, c.name, p.partner_name;
        """, (f"%{query}%",))
        return cur.fetchall()
    finally:
        conn.close()


def get_partner_counts_by_bank(bank_id: int) -> list[tuple]:
    conn = _conn()
    cur = conn.cursor()

    if bank_id == 13:
        # Кактус: уникальные партнёры по имени
        cur.execute("""
            SELECT c.name AS category_name,
                   COUNT(DISTINCT p.partner_name) AS partners_unique
            FROM partners p
            JOIN categories c ON c.id = p.category_id
            WHERE p.bank_id = 13
              AND p.status IN ('new','live')
            GROUP BY c.name
            ORDER BY partners_unique DESC;
        """)
        rows = cur.fetchall()
        return [(row[0], row[1]) for row in rows]

    elif bank_id in (1, 2):  # Белкарт и БНБ – без категорий
        cur.execute("""
            SELECT 'Все партнёры' AS category_name,
                   COUNT(DISTINCT p.partner_name) AS partners_count
            FROM partners p
            WHERE p.bank_id = ?
              AND p.status IN ('new','live');
        """, (bank_id,))
        rows = cur.fetchall()
        return [(row[0], row[1]) for row in rows if row[1] > 0]

    else:
        # остальные банки с реальными категориями
        cur.execute("""
            SELECT c.name AS category_name,
                   COUNT(DISTINCT p.partner_name) AS partners_count
            FROM partners p
            JOIN categories c ON c.id = p.category_id
            WHERE p.bank_id = ?
              AND p.status IN ('new','live')
            GROUP BY c.name
            ORDER BY partners_count DESC;
        """, (bank_id,))
        rows = cur.fetchall()
        return [(row[0], row[1]) for row in rows]



def get_bank_name(bank_id: int) -> str:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM banks WHERE id=?;", (bank_id,))
        row = cur.fetchone()
        return row[0] if row else f"bank_id={bank_id}"
    finally:
        conn.close()


def get_partner_counts()-> List[Tuple[str, int]]:
    """
    [(bank_name, partners_count), ...] — подсчёт партнёров по банкам для графика (DESC).
    """
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b.name, COUNT(p.partner_name) AS partner_cnt
            FROM banks b
            LEFT JOIN partners p ON b.id = p.bank_id
            GROUP BY b.name
            ORDER BY partner_cnt DESC, b.name ASC;
        """)
        return cur.fetchall()
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




def get_test_digest_data():
    """Возвращает тестовые данные для статичного дайджеста"""
    conn = _conn()
    try:
        cur = conn.cursor()
        
        # Берем последние 50 партнеров из БД как статичные данные
        cur.execute("""
            SELECT 
                b.name as bank_name,
                c.name as category_name,
                p.partner_name,
                p.partner_bonus,
                b.bonus_unit,
                p.partner_link,
                p.checked_at
            FROM partners p
            JOIN banks b ON p.bank_id = b.id
            JOIN categories c ON p.category_id = c.id
            WHERE p.partner_bonus IS NOT NULL 
            AND p.partner_bonus != ''
            ORDER BY p.checked_at DESC
            LIMIT 30
        """)
        
        rows = cur.fetchall()
        
        # Конвертируем в нужный формат
        changes = []
        for row in rows:
            changes.append({
                "bank_name": row[0],
                "category_name": row[1],
                "partner_name": row[2],
                "partner_bonus": row[3],
                "bonus_unit": row[4] or "",
                "partner_link": row[5] or "#",
                "checked_at": row[6],
                "change_type": "updated"
            })
        
        return changes
        
    finally:
        conn.close()

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