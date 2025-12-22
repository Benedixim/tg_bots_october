# db_sql.py
import os
import sqlite3
import datetime
from typing import Any, Dict, List, Tuple, Optional

DB_PATH = "new_db.db"



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
                SELECT
                    loyalty_url,
                    cookie,
                    container,
                    element,
                    parser_type
                FROM banks
                WHERE id=?
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
    """
    Возвращает список словарей:
    {
        bank_name,
        category_name,
        partner_name,
        partner_bonus,
        bonus_unit,  # <- ДОБАВИЛИ ЭТО
        partner_link,
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
                l.partner_link,  
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

    changes = []
    for bank_name, category_name, partner_name, partner_bonus, checked_at, partner_link, hist_count, bonus_unit in rows:
        change_type = "new" if hist_count == 1 else "updated"
        changes.append({
            "bank_name": bank_name,
            "category_name": category_name,
            "partner_name": partner_name,
            "partner_bonus": partner_bonus,
            "bonus_unit": bonus_unit or "", 
            "partner_link": partner_link or "#",  
            "change_type": change_type,
            "checked_at": checked_at,
        })
    return changes


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

def get_special_banks():
    """
    Возвращает только банки с кастомными парсерами, 
    которые мы хотим обновлять через /up_bank.
    """
    # Задаём конкретные parser_type для нужных банков
    SPECIAL_PARSERS = ('belkart', 'simple_js_categories')  # Белкарт и БНБ

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        try:
            cur.execute(f"""
                SELECT DISTINCT bank_id
                FROM categories_scrape_config
                WHERE parser_type IN ({','.join('?' for _ in SPECIAL_PARSERS)})
            """, SPECIAL_PARSERS)
        except sqlite3.OperationalError as e:
            raise RuntimeError(
                f"❌ Таблица categories_scrape_config не найдена.\n"
                f"Проверьте DB_PATH: {DB_PATH}"
            ) from e

        return [row[0] for row in cur.fetchall()]


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

# to do func fith status
def save_partners(partners: List[Dict[str, Any]], bank_id: int, category_id: int) -> None:
    """
    Сохраняет партнёров с логикой статусов:
    1. Перед обновлением: все партнёры получают статус 'ready'
    2. При проверке каждого партнёра:
       - Существует (есть последняя запись) → status = 'live'
       - Новый (первая запись) → status = 'new'
       - Исчез (был, но нет в новых данных) → status = 'new_delete'
       - Был 'new_delete', исчез снова → status = 'delete'
    """
    conn = _conn()
    try:
        ensure_partners_table(conn)
        ensure_status_columns()  
        
        cur = conn.cursor()
        checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
   
        cur.execute("""
            UPDATE partners
            SET status = 'ready'
            WHERE bank_id = ? AND category_id = ?
            AND status != 'delete'
        """, (bank_id, category_id))
        
        print(f"📊 Подготовка: отмечено {cur.rowcount} партнёров как 'ready'")
        
   
        cur.execute("""
            SELECT DISTINCT partner_name, status
            FROM partners
            WHERE bank_id = ? AND category_id = ?
        """, (bank_id, category_id))
        
        current_partners = {row[0]: row[1] for row in cur.fetchall()}
        
       
        new_partner_names = set()
        
        for p in partners:
            partner_name = p.get("partner_name")
            bonus = p.get("partner_bonus")
            link = p.get("partner_link") or ""
            
            if not partner_name:
                continue
            
            new_partner_names.add(partner_name)
            
    
            link = link.strip() if link else ""
            
     
            cur.execute("""
                SELECT id, status
                FROM partners
                WHERE bank_id = ? AND category_id = ? AND partner_name = ?
                AND COALESCE(NULLIF(TRIM(partner_bonus),''),'') = COALESCE(NULLIF(TRIM(?),''),'')
                AND COALESCE(NULLIF(TRIM(partner_link),''),'') = COALESCE(NULLIF(TRIM(?),''),'')
                ORDER BY checked_at DESC
                LIMIT 1
            """, (bank_id, category_id, partner_name, bonus or "", link))
            
            last = cur.fetchone()
            

            if last is None:
           
                old_status = current_partners.get(partner_name)
                
                if old_status is None:
                
                    status = 'new'
                elif old_status in ['new_delete', 'delete']:
                    # Партнёр вернулся после удаления
                    status = 'live'
                else:
                    # Данные изменились у существующего партнёра
                    status = 'live'
                
                # Сохраняем новую запись
                cur.execute("""
                    INSERT INTO partners 
                    (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (bank_id, category_id, partner_name, bonus, link, checked_at, status))
            else:
                # Партнёр существует с такими же данными
                last_id, last_status = last
                
                # Если статус был 'ready', то меняем на 'live'
                if last_status == 'ready':
                    cur.execute("""
                        UPDATE partners
                        SET status = 'live'
                        WHERE id = ?
                    """, (last_id,))
        
        # ШАГ 3: Помечаем отсутствующих партнёров как 'new_delete' или 'delete'
        missing_partners = set(current_partners.keys()) - new_partner_names
        
        for partner_name in missing_partners:
            old_status = current_partners[partner_name]
            
            # Определяем новый статус
            if old_status == 'new_delete':
                # Был уже удалён, теперь окончательно удаляем
                new_status = 'delete'
            else:
                # Первый раз удаляем
                new_status = 'new_delete'
            
            # Сохраняем запись об удалении
            cur.execute("""
                INSERT INTO partners 
                (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (bank_id, category_id, partner_name, None, None, checked_at, new_status))
        
        if missing_partners:
            print(f"🗑️ Помечено как удалённые: {len(missing_partners)} партнёров")
        
        conn.commit()
        print(f"✅ save_partners завершена для bank_id={bank_id}, category_id={category_id}")
        
    except Exception as e:
        print(f"❌ Ошибка в save_partners: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
# def save_partners(partners: List[Dict[str, Any]], bank_id: int, category_id: int) -> None:
#     conn = _conn()
#     try:
#         ensure_partners_table(conn)
#         cur = conn.cursor()
#         checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#         for p in partners:
#             bonus = p.get("partner_bonus")
#             link = p.get("partner_link")
#             # 🚫 Пропускаем, если бонус пустой
#             #if not bonus or str(bonus).strip() == "":
#             #    continue

#             # 🚫 Пропускаем, если ссылки нет или она пустая
#             #if not link or str(link).strip() == "":
#             #    continue

#             # link иногда = None → подстрахуемся
#             link = link.strip()

#             # Проверяем последнюю запись
#             cur.execute("""
#                 SELECT partner_bonus, partner_link
#                 FROM partners
#                 WHERE bank_id=? AND category_id=? AND partner_name=? 
#                         AND COALESCE(NULLIF(TRIM(partner_bonus),''),'') = COALESCE(NULLIF(TRIM(?),''),'')
#                         AND COALESCE(NULLIF(TRIM(partner_link),''),'') = COALESCE(NULLIF(TRIM(?),''),'')
#                 ORDER BY checked_at DESC
#                 LIMIT 1
#             """, (bank_id, category_id, p["partner_name"], bonus, link))

#             last = cur.fetchone()

#             # Изменилось? → сохраняем
#             if last is None:# or last[0] != bonus or last[1] != link:
#                 cur.execute("""
#                     INSERT INTO partners (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at)
#                     VALUES (?, ?, ?, ?, ?, ?)
#                 """, (bank_id, category_id, p["partner_name"], bonus, link, checked_at))

#         conn.commit()
#     finally:
#         conn.close()


# def get_partners_latest_by_bank_category(bank_id: int, category_id: int) -> List[Tuple[str, Optional[str], Optional[str]]]:
#     conn = _conn()
#     try:
#         cur = conn.cursor()
#         cur.execute("""
#             SELECT partner_name, partner_bonus, partner_link
#             FROM partners
#             WHERE bank_id = ? AND category_id = ?
#             AND checked_at = (SELECT MAX(checked_at) FROM partners p2 WHERE p2.bank_id=? AND p2.category_id=?)
#             ORDER BY partner_name;
#         """, (bank_id, category_id, bank_id, category_id))
#         return cur.fetchall()
#     finally:
#         conn.close()

def get_partners_latest_by_bank_category(bank_id: int, category_id: int) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """
    Возвращает список (partner_name, partner_bonus, partner_link)
    только последние версии, БЕЗ ДУБЛЕЙ по partner_name
    """
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT partner_name, partner_bonus, partner_link
            FROM partners
            WHERE bank_id = ? AND category_id = ?
            AND checked_at = (
                SELECT MAX(p2.checked_at)
                FROM partners p2
                WHERE p2.bank_id = ? 
                  AND p2.category_id = ?
                  AND p2.partner_name = partners.partner_name
            )
            ORDER BY partner_name
        """, (bank_id, category_id, bank_id, category_id))
        
        # Дополнительная дедубликация на уровне Python
        seen = set()
        result = []
        for row in cur.fetchall():
            partner_name = row[0]
            if partner_name not in seen:
                seen.add(partner_name)
                result.append(row)
        
        return result
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
        
     
        cur.execute("PRAGMA table_info(partners);")
        columns = [col[1] for col in cur.fetchall()]
        
        if 'status' not in columns:
            cur.execute("ALTER TABLE partners ADD COLUMN status TEXT DEFAULT 'live';")
            print("✓ Добавлена колонка status в таблицу partners")
            
      
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

# def get_partners_current_status(conn, bank_id, category_id):
#     cur = conn.cursor()

#     cur.execute("""
#         SELECT p.partner_name, p.status
#         FROM partners p
#         WHERE p.bank_id = ? AND p.category_id = ?
#         AND p.checked_at = (
#             SELECT MAX(p2.checked_at)
#             FROM partners p2
#             WHERE p2.bank_id = p.bank_id
#             AND p2.category_id = p.category_id
#             AND p2.partner_name = p.partner_name            )
#     """, (bank_id, category_id))
        
#     return {row[0]: row[1] for row in cur.fetchall()}

def get_partners_current_status(conn, bank_id, category_id):
    """Получает текущие статусы партнёров для банка и категории"""
    cur = conn.cursor()
    
    
    if bank_id == 2:
        category_id = 0
    
    cur.execute("""
        SELECT DISTINCT p.partner_name, p.status
        FROM partners p
        INNER JOIN (
            SELECT bank_id, category_id, partner_name, MAX(checked_at) as max_checked
            FROM partners
            WHERE bank_id = ? AND category_id = ?
            GROUP BY bank_id, category_id, partner_name
        ) latest ON p.bank_id = latest.bank_id 
            AND p.category_id = latest.category_id 
            AND p.partner_name = latest.partner_name 
            AND p.checked_at = latest.max_checked
    """, (bank_id, category_id))
    
    return {row[0]: row[1] for row in cur.fetchall()}

def save_partners_with_status_logic(partners: List[Dict[str, Any]], bank_id: int, category_id: int) -> None:
    """
    Умное сохранение партнёров с логикой статусов
    """
   
    if bank_id == 2:
        category_id = 0
        
    conn = _conn()
    try:
        ensure_partners_table(conn)
        cur = conn.cursor()
        checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
       
        normalized_partners = []
        for p in partners:
            partner_name = p.get("partner_name") or p.get("name") or p.get("company") or p.get("title")
            if not partner_name:
                continue 
                
            normalized_partners.append({
                "partner_name": partner_name,
                "partner_bonus": p.get("partner_bonus") or p.get("bonus") or p.get("cashback"),
                "partner_link": p.get("partner_link") or p.get("link") or "",
            })
        

        if not normalized_partners:
            print(f"ℹ️ Нет партнёров для сохранения: bank_id={bank_id}, category_id={category_id}")
            return
        
        
        current_statuses = get_partners_current_status(conn, bank_id, category_id)
        

        new_partners = {p["partner_name"]: p for p in normalized_partners}
        
        print(f"🔍 Статистика: текущих партнёров={len(current_statuses)}, новых партнёров={len(new_partners)}")
        
       
        updated_count = 0
        new_count = 0
        
        for partner_name, partner_data in new_partners.items():
            old_status = current_statuses.get(partner_name, 'none')
            
            
            cur.execute("""
                SELECT partner_bonus, partner_link, status
                FROM partners
                WHERE bank_id=? AND category_id=? AND partner_name=?
                ORDER BY checked_at DESC
                LIMIT 1
            """, (bank_id, category_id, partner_name))

            last = cur.fetchone()
            

            if old_status == 'none':
                status = 'new'
                new_count += 1
            elif old_status in ['new_delete', 'delete']:
                status = 'live'
            else:
                status = old_status if old_status in ['live', 'ready', 'new'] else 'live'
            
            should_save = False
            if not last:
                should_save = True
            else:
                last_bonus, last_link, last_status = last
                current_bonus = partner_data.get("partner_bonus") or ""
                current_link = partner_data.get("partner_link") or ""
                
                if (last_bonus or "") != (current_bonus or "") or \
                   (last_link or "") != (current_link or "") or \
                   (last_status or "") != (status or ""):
                    should_save = True
            
            if should_save:
                cur.execute("""
                    INSERT INTO partners 
                    (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    bank_id, category_id,
                    partner_name,
                    partner_data.get("partner_bonus"),
                    partner_data.get("partner_link") or "",
                    checked_at,
                    status
                ))
                
                if old_status != status:
                    cur.execute("""
                        INSERT INTO status_log (partner_name, bank_id, category_id, old_status, new_status)
                        VALUES (?, ?, ?, ?, ?)
                    """, (partner_name, bank_id, category_id, old_status, status))
                
                updated_count += 1
        

        deleted_count = 0
        for partner_name in set(current_statuses.keys()) - set(new_partners.keys()):
            old_status = current_statuses[partner_name]
            

            if old_status in ['live', 'ready', 'new']:
                status = 'new_delete'
                
                cur.execute("""
                    INSERT INTO partners 
                    (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    bank_id, category_id,
                    partner_name,
                    None,
                    None,
                    checked_at,
                    status
                ))
                
                cur.execute("""
                    INSERT INTO status_log (partner_name, bank_id, category_id, old_status, new_status)
                    VALUES (?, ?, ?, ?, ?)
                """, (partner_name, bank_id, category_id, old_status, status))
                
                deleted_count += 1
        
        conn.commit()
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении партнёров: {e}")
        import traceback
        traceback.print_exc()
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
                WHEN status = 'live' THEN 'ready'
                WHEN status = 'new_delete' THEN 'delete'
                ELSE status
            END
            WHERE id IN (
                SELECT p.id
                FROM partners p
                INNER JOIN (
                    SELECT bank_id, category_id, partner_name, MAX(checked_at) AS max_checked
                    FROM partners
                    GROUP BY bank_id, category_id, partner_name
                ) latest
                ON p.bank_id = latest.bank_id
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
        cur.execute("""
            DELETE FROM partners
            WHERE id IN (
                SELECT p.id
                FROM partners p
                INNER JOIN (
                    SELECT bank_id, category_id, partner_name, MAX(checked_at) AS max_checked
                    FROM partners
                    GROUP BY bank_id, category_id, partner_name
                ) latest
                ON p.bank_id = latest.bank_id
            AND p.category_id = latest.category_id
            AND p.partner_name = latest.partner_name
            AND p.checked_at = latest.max_checked
            AND p.status = 'delete'
                )
        """)
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
    Возвращает изменения за сегодня с учетом статусов.
    Включает:
    - 'new' — новые партнёры
    - 'new_delete' — партнёры, которых больше нет (первый раз удалены)
    - 'delete' — окончательно удалённые
    Исключает 'live', 'ready' (без изменений)
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
            print("⚠️ Колонка status не найдена")
            return []
        
        # Получаем партнёров со статусами 'new' и 'new_delete' за сегодня (только новых и удалённых)
        cur.execute("""
            SELECT
                b.name as bank_name,
                COALESCE(c.name, '—') as category_name,
                p.partner_name,
                p.partner_bonus,
                p.partner_link,
                p.status,
                p.checked_at,
                b.bonus_unit
            FROM partners p
            JOIN banks b ON p.bank_id = b.id
            LEFT JOIN categories c ON p.category_id = c.id AND p.category_id != 0
            WHERE p.checked_at >= ?
            AND p.status IN ('new', 'new_delete')
            ORDER BY p.checked_at DESC, b.name, p.partner_name
        """, (since_str,))
        
        rows = cur.fetchall()
        
        result = []
        seen = set()  # Для дедубликации
        
        for row in rows:
            bank_name, category_name, partner_name, partner_bonus, partner_link, status, checked_at, bonus_unit = row
            
            # Дедубликация: берём только ПОСЛЕДНИЙ статус партнёра за день
            key = (bank_name, category_name, partner_name)
            if key in seen:
                continue
            seen.add(key)
            
            # Преобразуем status для фронта
            if status == 'new':
                change_type = 'new'
            elif status == 'new_delete':
                change_type = 'deleted'
            elif status == 'delete':
                change_type = 'deleted'
            else:
                change_type = 'updated'
            
            result.append({
                "bank_name": bank_name,
                "category_name": category_name or "—",
                "partner_name": partner_name,
                "partner_bonus": partner_bonus,
                "partner_link": partner_link or "#",
                "status": status,
                "change_type": change_type,
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
