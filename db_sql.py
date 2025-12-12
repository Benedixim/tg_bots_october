# db_sql.py
import os
import sqlite3
import datetime
from typing import Any, Dict, List, Tuple, Optional

DB_PATH = "banks_backup_20251212_071846.db"


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


def get_today_partner_changes() -> list[dict]:
    """
    Возвращает список словарей:
    {
        bank_name,
        category_name,
        partner_name,
        partner_bonus,
        bonus_unit,  # <- ДОБАВИЛИ ЭТО
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
                l.hist_count,
                b.bonus_unit  -- ДОБАВИЛИ ЭТО
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
    for bank_name, category_name, partner_name, partner_bonus, checked_at, hist_count, bonus_unit in rows:
        change_type = "new" if hist_count == 1 else "updated"
        result.append({
            "bank_name": bank_name,
            "category_name": category_name,
            "partner_name": partner_name,
            "partner_bonus": partner_bonus,
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
            #if not bonus or str(bonus).strip() == "":
            #    continue

            # 🚫 Пропускаем, если ссылки нет или она пустая
            #if not link or str(link).strip() == "":
            #    continue

            # link иногда = None → подстрахуемся
            link = link.strip()

            # Проверяем последнюю запись
            cur.execute("""
                SELECT partner_bonus, partner_link
                FROM partners
                WHERE bank_id=? AND category_id=? AND partner_name=? 
                        AND COALESCE(NULLIF(TRIM(partner_bonus),''),'') = COALESCE(NULLIF(TRIM(?),''),'')
                        AND COALESCE(NULLIF(TRIM(partner_link),''),'') = COALESCE(NULLIF(TRIM(?),''),'')
                ORDER BY checked_at DESC
                LIMIT 1
            """, (bank_id, category_id, p["partner_name"], bonus, link))

            last = cur.fetchone()

            # Изменилось? → сохраняем
            if last is None:# or last[0] != bonus or last[1] != link:
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


# #------------Update-------------------
# def get_today_partner_changes() -> list[dict]:
#     """
#     Возвращает список словарей:
#     {
#         bank_name,
#         category_name,
#         partner_name,
#         partner_bonus,
#         change_type: "new" | "updated",
#         checked_at: "YYYY-MM-DD HH:MM:SS"
#     }
#     Только те партнёры, у кого последняя запись за сегодняшний день.
#     """
#     today = datetime.date.today()
#     since = datetime.datetime.combine(today, datetime.time(0, 0, 0))
#     since_str = since.strftime("%Y-%m-%d %H:%M:%S")

#     conn = _conn()
#     try:
#         cur = conn.cursor()
#         cur.execute("""
#             WITH latest AS (
#                 SELECT
#                     p.bank_id,
#                     p.category_id,
#                     p.partner_name,
#                     p.partner_bonus,
#                     p.partner_link,
#                     p.checked_at,
#                     (
#                         SELECT COUNT(*)
#                         FROM partners p2
#                         WHERE p2.bank_id = p.bank_id
#                           AND p2.category_id = p.category_id
#                           AND p2.partner_name = p.partner_name
#                     ) AS hist_count
#                 FROM partners p
#                 WHERE p.checked_at = (
#                     SELECT MAX(p2.checked_at)
#                     FROM partners p2
#                     WHERE p2.bank_id = p.bank_id
#                       AND p2.category_id = p.category_id
#                       AND p2.partner_name = p.partner_name
#                 )
#             )
#             SELECT
#                 b.name as bank_name,
#                 c.name as category_name,
#                 l.partner_name,
#                 l.partner_bonus,
#                 l.checked_at,
#                 l.hist_count
#             FROM latest l
#             JOIN banks b ON b.id = l.bank_id
#             JOIN categories c ON c.id = l.category_id
#             WHERE l.checked_at >= ?
#             ORDER BY b.name, c.name, l.partner_name;
#         """, (since_str,))
#         rows = cur.fetchall()
#     finally:
#         conn.close()

#     result = []
#     for bank_name, category_name, partner_name, partner_bonus, checked_at, hist_count in rows:
#         change_type = "new" if hist_count == 1 else "updated"
#         result.append({
#             "bank_name": bank_name,
#             "category_name": category_name,
#             "partner_name": partner_name,
#             "partner_bonus": partner_bonus,
#             "change_type": change_type,
#             "checked_at": checked_at,
#         })
#     return result



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


# ---------- STATUS SYSTEM ----------
def ensure_status_columns():
    """Создаёт необходимые колонки для системы статусов"""
    conn = _conn()
    try:
        cur = conn.cursor()
        
        # Добавляем колонку status в partners если её нет
        cur.execute("PRAGMA table_info(partners);")
        columns = [col[1] for col in cur.fetchall()]
        
        if 'status' not in columns:
            cur.execute("ALTER TABLE partners ADD COLUMN status TEXT DEFAULT 'live';")
            print("✓ Добавлена колонка status в таблицу partners")
            
        # Создаём таблицу для логирования изменений
        cur.execute("""
            CREATE TABLE IF NOT EXISTS status_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                partner_name TEXT NOT NULL,
                bank_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT,
                changed_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
    finally:
        conn.close()

def get_partners_current_status(bank_id: int, category_id: int) -> Dict[str, str]:
    """
    Возвращает словарь {partner_name: status} для текущих записей
    Использует только последнюю версию каждого партнёра
    """
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT p.partner_name, p.status
            FROM partners p
            WHERE p.bank_id = ? AND p.category_id = ?
            AND p.checked_at = (
                SELECT MAX(p2.checked_at)
                FROM partners p2
                WHERE p2.bank_id = p.bank_id
                AND p2.category_id = p.category_id
                AND p2.partner_name = p.partner_name
            )
        """, (bank_id, category_id))
        
        return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()

def save_partners_with_status_logic(partners: List[Dict[str, Any]], bank_id: int, category_id: int) -> None:
    """
    Умное сохранение партнёров с логикой статусов
    """
    conn = _conn()
    try:
        ensure_partners_table(conn)
        cur = conn.cursor()
        checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Получаем текущие статусы партнёров
        current_statuses = get_partners_current_status(bank_id, category_id)
        
        # Словарь новых партнёров
        new_partners = {p["partner_name"]: p for p in partners}
        
        # 1. Обновляем существующих партнёров
        for partner_name in set(current_statuses.keys()) & set(new_partners.keys()):
            partner = new_partners[partner_name]
            old_status = current_statuses[partner_name]
            
            # Если партнёр был в ready - переводим в live
            new_status = 'live' if old_status == 'ready' else old_status
            
            cur.execute("""
                INSERT INTO partners 
                (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                bank_id, category_id,
                partner_name,
                partner.get("partner_bonus"),
                partner.get("partner_link") or "",
                checked_at,
                new_status
            ))
            
            # Логируем изменение если статус поменялся
            if old_status != new_status:
                cur.execute("""
                    INSERT INTO status_log (partner_name, bank_id, category_id, old_status, new_status)
                    VALUES (?, ?, ?, ?, ?)
                """, (partner_name, bank_id, category_id, old_status, new_status))
        
        # 2. Добавляем новых партнёров
        for partner_name in set(new_partners.keys()) - set(current_statuses.keys()):
            partner = new_partners[partner_name]
            
            cur.execute("""
                INSERT INTO partners 
                (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at, status)
                VALUES (?, ?, ?, ?, ?, ?, 'new')
            """, (
                bank_id, category_id,
                partner_name,
                partner.get("partner_bonus"),
                partner.get("partner_link") or "",
                checked_at
            ))
            
            # Логируем создание нового
            cur.execute("""
                INSERT INTO status_log (partner_name, bank_id, category_id, old_status, new_status)
                VALUES (?, ?, ?, 'none', 'new')
            """, (partner_name, bank_id, category_id))
        
        # 3. Помечаем отсутствующих партнёров
        for partner_name in set(current_statuses.keys()) - set(new_partners.keys()):
            old_status = current_statuses[partner_name]
            
            if old_status in ['live', 'ready']:
                # Был активным, но теперь отсутствует - помечаем на удаление
                new_status = 'new_delete'
                
                cur.execute("""
                    INSERT INTO partners 
                    (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    bank_id, category_id,
                    partner_name,
                    None,  # bonus
                    None,  # link
                    checked_at,
                    new_status
                ))
                
                # Логируем
                cur.execute("""
                    INSERT INTO status_log (partner_name, bank_id, category_id, old_status, new_status)
                    VALUES (?, ?, ?, ?, ?)
                """, (partner_name, bank_id, category_id, old_status, new_status))
            elif old_status == 'new':
                # Был новым, но теперь отсутствует - удаляем
                cur.execute("""
                    DELETE FROM partners 
                    WHERE bank_id = ? AND category_id = ? AND partner_name = ?
                """, (bank_id, category_id, partner_name))
        
        conn.commit()
        
    finally:
        conn.close()

def prepare_statuses_for_update():
    """
    Подготовка статусов перед обновлением:
    - live, new → ready
    - new_delete → delete
    """
    conn = _conn()
    try:
        cur = conn.cursor()
        
        # Находим последние записи каждого партнёра
        cur.execute("""
            UPDATE partners 
            SET status = CASE 
                WHEN status IN ('live', 'new') THEN 'ready'
                WHEN status = 'new_delete' THEN 'delete'
                ELSE status
            END
            WHERE id IN (
                SELECT p.id
                FROM partners p
                INNER JOIN (
                    SELECT bank_id, category_id, partner_name, MAX(checked_at) as max_checked
                    FROM partners
                    GROUP BY bank_id, category_id, partner_name
                ) latest ON p.bank_id = latest.bank_id 
                    AND p.category_id = latest.category_id 
                    AND p.partner_name = latest.partner_name 
                    AND p.checked_at = latest.max_checked
            )
        """)
        
        updated = cur.rowcount
        conn.commit()
        return updated
        
    finally:
        conn.close()

def finalize_statuses_after_update():
    """
    Финальная обработка после обновления:
    - ready → live
    """
    conn = _conn()
    try:
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE partners 
            SET status = 'live'
            WHERE status = 'ready'
            AND id IN (
                SELECT p.id
                FROM partners p
                INNER JOIN (
                    SELECT bank_id, category_id, partner_name, MAX(checked_at) as max_checked
                    FROM partners
                    GROUP BY bank_id, category_id, partner_name
                ) latest ON p.bank_id = latest.bank_id 
                    AND p.category_id = latest.category_id 
                    AND p.partner_name = latest.partner_name 
                    AND p.checked_at = latest.max_checked
            )
        """)
        
        updated = cur.rowcount
        conn.commit()
        return updated
        
    finally:
        conn.close()

def cleanup_deleted_partners():
    """Удаляет партнёров со статусом 'delete'"""
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM partners WHERE status = 'delete'")
        deleted = cur.rowcount
        conn.commit()
        return deleted
    finally:
        conn.close()

def get_status_report() -> Dict[str, Any]:
    """Статистика по статусам"""
    conn = _conn()
    try:
        cur = conn.cursor()
        
        # Статистика по статусам
        cur.execute("""
            SELECT status, COUNT(*) as count
            FROM (
                SELECT p.status
                FROM partners p
                INNER JOIN (
                    SELECT bank_id, category_id, partner_name, MAX(checked_at) as max_checked
                    FROM partners
                    GROUP BY bank_id, category_id, partner_name
                ) latest ON p.bank_id = latest.bank_id 
                    AND p.category_id = latest.category_id 
                    AND p.partner_name = latest.partner_name 
                    AND p.checked_at = latest.max_checked
            )
            GROUP BY status
        """)
        
        status_stats = {row[0]: row[1] for row in cur.fetchall()}
        
        # Последние изменения
        cur.execute("""
            SELECT 
                partner_name,
                bank_id,
                category_id,
                old_status,
                new_status,
                changed_at
            FROM status_log
            ORDER BY changed_at DESC
            LIMIT 10
        """)
        
        recent_changes = cur.fetchall()
        
        return {
            "stats": status_stats,
            "recent_changes": recent_changes
        }
        
    finally:
        conn.close()

def get_today_changes_with_status() -> list[dict]:
    """
    Возвращает изменения за сегодня с учетом статусов
    Включает партнёров со статусом 'new_delete'
    """
    today = datetime.date.today()
    since = datetime.datetime.combine(today, datetime.time(0, 0, 0))
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")

    conn = _conn()
    try:
        cur = conn.cursor()
        
        # Проверяем, есть ли колонка status
        cur.execute("PRAGMA table_info(partners);")
        columns = [col[1] for col in cur.fetchall()]
        
        if 'status' not in columns:
            # Если колонки нет, используем упрощенный запрос
            return get_today_partner_changes()  # вернуть данные без статусов
        
        # Если колонка есть, выполняем полный запрос
        cur.execute("""
            WITH latest AS (
                SELECT
                    p.bank_id,
                    p.category_id,
                    p.partner_name,
                    p.partner_bonus,
                    p.partner_link,
                    p.status,
                    p.checked_at,
                    b.bonus_unit,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.bank_id, p.category_id, p.partner_name 
                        ORDER BY p.checked_at DESC
                    ) as rn
                FROM partners p
                JOIN banks b ON p.bank_id = b.id
                WHERE p.checked_at >= ?
            )
            SELECT
                b.name as bank_name,
                c.name as category_name,
                l.partner_name,
                l.partner_bonus,
                l.partner_link,
                l.status,
                l.checked_at,
                l.bonus_unit
            FROM latest l
            JOIN banks b ON b.id = l.bank_id
            JOIN categories c ON c.id = l.category_id
            WHERE l.rn = 1  -- Берем только последнюю запись
            AND l.status IN ('new', 'new_delete', 'ready', 'live')
            ORDER BY b.name, c.name, l.partner_name
        """, (since_str,))
        
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            bank_name, category_name, partner_name, partner_bonus, partner_link, status, checked_at, bonus_unit = row
            
            result.append({
                "bank_name": bank_name,
                "category_name": category_name,
                "partner_name": partner_name,
                "partner_bonus": partner_bonus,
                "partner_link": partner_link or "#",
                "status": status,
                "checked_at": checked_at,
                "bonus_unit": bonus_unit or ""
            })
        
        return result
        
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
