# belkart.py
import os
import time
import json
import re
from collections import defaultdict
from typing import List, Dict, Any, Optional, Callable, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from dotenv import load_dotenv
from back_db import save_partners

from gigachat import GigaChat

ProgressFn = Optional[Callable[[int, int, str], None]]

BASE_URL = "https://belkart.by/BELKART/reklamnye-aktsii/"

load_dotenv()
GIGACHAT_TOKEN = os.getenv("GIGACHAT_TOKEN")

gc = GigaChat(
    credentials=GIGACHAT_TOKEN,
    scope="GIGACHAT_API_B2B",
    verify_ssl_certs=False,
    model="GigaChat-2-Max",
)
print("GIGACHAT_TOKEN =", GIGACHAT_TOKEN)

# кэш для результатов GigaChat: "raw_title raw_status" -> {"company": ..., "bonus": ...}
_GIGA_CACHE: Dict[str, Dict[str, Any]] = {}


# ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ----------

def nlp_company_bonus(raw_text: str) -> Dict[str, Any]:
    """
    Обёртка над extract_company_and_bonus с простым in‑memory кэшем.
    Один и тот же текст не отправляем в GigaChat повторно.
    """
    raw_text = raw_text.strip()
    if not raw_text:
        return {"company": None, "bonus": None}

    if raw_text in _GIGA_CACHE:
        return _GIGA_CACHE[raw_text]

    data = extract_company_and_bonus(raw_text)
    _GIGA_CACHE[raw_text] = data
    return data


def normalize_bonus(bonus) -> Optional[str]:
    """
    Приводит бонус к аккуратной строке:
    - если список → склеиваем;
    - убираем лишние пробелы и переводы строк;
    - пустое → None.
    """
    if bonus is None:
        return None

    if isinstance(bonus, list):
        bonus = " ".join(str(b).strip() for b in bonus if b)

    bonus = str(bonus).strip()
    bonus = " ".join(bonus.split())
    return bonus or None


def extract_bonus_number(bonus_str: Optional[str]) -> float:
    """
    Извлекает числовое значение из строки бонуса.
    Примеры: "скидка 15%" → 15, "20 дней" → 20, "2 книги в подарок" → 2.
    """
    if not bonus_str:
        return -1.0

    numbers = re.findall(r"\d+(?:[\.,]\d+)?", str(bonus_str))
    if not numbers:
        return -1.0

    try:
        return float(numbers[0].replace(",", "."))
    except Exception:
        return -1.0


# ---------- GIGACHAT ----------

def extract_company_and_bonus(text: str) -> dict:
    """Извлекает компанию и бонус через GigaChat."""
    text = text.strip()
    if not text:
        return {"company": None, "bonus": None}

    prompt = f"""
Извлеки из текста название компании и размер бонуса.
Если бонус указан в разных форматах (скидка 15%, 15%, -15%, кешбэк 20%, 2 книги в подарок), верни его как есть.

Текст:
\"\"\"{text}\"\"\"

Верни СТРОГО JSON без пояснений:
{{
  "company": "...",
  "bonus": "..."
}}

Если данных нет – используй null.
"""

    try:
        resp = gc.chat(prompt)
        raw = resp.choices[0].message.content.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        data = json.loads(raw)

        company = data.get("company")
        bonus = normalize_bonus(data.get("bonus"))

        return {"company": company, "bonus": bonus}
    except Exception as e:
        print(f"⚠️ GigaChat error: {e}")
        return {"company": None, "bonus": None}


# ---------- ПАРСИНГ СТРАНИЦ ----------

def _parse_page(url: str, retry_count: int = 3) -> Tuple[List[Dict[str, Any]], BeautifulSoup]:
    """
    Парсит одну страницу с повторными попытками при сетевых ошибках.
    Возвращает список партнёров и объект BeautifulSoup.
    """
    last_error: Optional[str] = None

    for attempt in range(1, retry_count + 1):
        try:
            print(f"  📡 Попытка загрузки {attempt}/{retry_count}: {url}")
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "lxml")
            cards = soup.select("ul.card-list li.card-list__item")
            print(f"    🔍 Найдено карточек: {len(cards)}")

            results: List[Dict[str, Any]] = []

            for i, card in enumerate(cards, start=1):
                try:
                    link_tag = card.select_one("a.card-list__link")
                    title_tag = card.select_one(".card-list__title")
                    status_tag = card.select_one(".card-list__label")

                    raw_title = (title_tag.text or "").strip() if title_tag else ""
                    raw_status = (status_tag.text or "").strip() if status_tag else ""
                    raw_link = urljoin(BASE_URL, link_tag["href"]) if link_tag and link_tag.get("href") else ""

                    if not raw_title:
                        continue

                    raw_text = f"{raw_title} {raw_status}".strip()
                    nlp = nlp_company_bonus(raw_text)

                    company = (nlp.get("company") or raw_title).strip()
                    bonus = nlp.get("bonus")

                    results.append(
                        {
                            "title": raw_title,
                            "link": raw_link,
                            "status": raw_status,
                            "company": company,
                            "bonus": bonus,
                        }
                    )
                except Exception as e:
                    print(f"    ⚠️ Ошибка парсинга карточки #{i}: {e}")

            return results, soup

        except requests.exceptions.RequestException as e:
            last_error = f"Ошибка сети/таймаут на попытке {attempt}: {e}"
            print(f"  ⚠️ {last_error}")
            time.sleep(2)
        except Exception as e:
            last_error = f"Ошибка парсинга: {e}"
            print(f"  ❌ {last_error}")
            break

    print(f"  ❌ Не удалось загрузить страницу после {retry_count} попыток: {last_error}")
    return [], BeautifulSoup("", "lxml")


def _get_next_page_url(soup: BeautifulSoup) -> Optional[str]:
    """
    Находит URL следующей страницы из пагинации Белкарта.
    Опирается на активный элемент и номер следующей страницы.
    """
    active_page = soup.select_one("a.pagination-link.active")
    if not active_page:
        print("    ℹ️ Активная страница пагинации не найдена")
        return None

    try:
        current_num = int(active_page.text.strip())
    except (ValueError, AttributeError):
        print("    ⚠️ Не удалось определить номер текущей страницы")
        return None

    next_page_num = current_num + 1

    # 1) Прямая ссылка на PAGEN_1=next_page_num
    next_link = soup.select_one(f"a.pagination-link[href*='PAGEN_1={next_page_num}']")
    if next_link and next_link.get("href"):
        full_url = urljoin(BASE_URL, next_link["href"])
        print(f"    ✅ Найдена следующая страница ({next_page_num}): {full_url}")
        return full_url

    # 2) Кнопка "След."
    next_btn = soup.select_one("a.pagination-button:not(.disabled)")
    if next_btn and next_btn.get("href") and next_btn["href"] != "javascript:void(0);":
        full_url = urljoin(BASE_URL, next_btn["href"])
        print(f"    ✅ Найдена кнопка 'След.': {full_url}")
        return full_url

    print(f"    ℹ️ Следующая страница ({next_page_num}) не найдена - это последняя страница")
    return None


# ---------- ДЕДУПЛИКАЦИЯ И СОХРАНЕНИЕ ----------

def save_belkart_items(bank_id: int, items: List[Dict[str, Any]]) -> None:
    """
    Сохраняет партнёров Белкарта, гарантируя:
    1. Все в категории 0.
    2. Нет дубликатов по компании.
    3. Выбирается запись с максимальным числовым бонусом.
    4. Ссылки стараемся не терять.
    """
    if not items:
        print("⚠️ Нет предметов для сохранения")
        return

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    # Группировка по компании
    for item in items:
        company = (item.get("company") or item.get("title") or "").strip()
        if not company:
            print(f"⚠️ Пропускаем партнёра без названия: {item}")
            continue

        bonus = normalize_bonus(item.get("bonus"))
        link = item.get("link") or ""

        grouped[company].append({"bonus": bonus, "link": link})

    partners_data: List[Dict[str, Any]] = []

    for company, records in grouped.items():
        if not records:
            print(f"⚠️ Нет записей для {company}")
            continue

        best_record: Optional[Dict[str, Any]] = None
        best_bonus_value = -1.0

        for rec in records:
            bonus = rec["bonus"]
            link = rec["link"]

            bonus_num = extract_bonus_number(bonus) if bonus else -1.0

            if (
                best_record is None
                or bonus_num > best_bonus_value
                or (bonus_num == best_bonus_value and link and not best_record.get("link"))
            ):
                best_bonus_value = bonus_num
                best_record = rec

        if best_record is None:
            continue

        final_link = best_record.get("link") or ""
        if not final_link:
            for rec in records:
                if rec.get("link"):
                    final_link = rec["link"]
                    break

        final_bonus = best_record.get("bonus")

        partners_data.append(
            {
                "partner_name": company,
                "partner_bonus": final_bonus,
                "partner_link": final_link,
            }
        )

        print(f"  ✅ {company} → бонус: {final_bonus or 'нет'}, ссылка: {'да' if final_link else 'нет'}")

    print(f"\n📝 Сохраняю {len(partners_data)} уникальных партнёров...")
    save_partners(
        partners=partners_data,
        bank_id=bank_id,
        category_id=0,
    )
    print(f"✅ Сохранено {len(partners_data)} уникальных партнёров Белкарта")


# ---------- ГЛАВНАЯ ФУНКЦИЯ ----------

def fetch_promotions(
    bank_id: int,
    progress: ProgressFn = None,
    banks_done: int = 0,
    banks_total: int = 0,
) -> List[Dict[str, Any]]:
    """
    Загружает ВСЕ страницы Белкарта и сохраняет партнёров.
    Делает:
    1. Правильную пагинацию.
    2. Повторные попытки при ошибках.
    3. Извлечение названий и бонусов через GigaChat.
    4. Дедупликацию по компании.
    5. Защиту от циклов по URL.
    """
    all_items: List[Dict[str, Any]] = []
    current_url = BASE_URL
    page_num = 1
    max_pages = 100
    visited_urls: set[str] = set()

    while page_num <= max_pages:
        note = f"[bank {bank_id}] 📄 Белкарт – страница {page_num}"
        print(note)
        if progress:
            progress(banks_done, banks_total, note)

        if current_url in visited_urls:
            print(f"[bank {bank_id}] ⚠️ Цикл! Страница уже была посещена: {current_url}")
            break
        visited_urls.add(current_url)

        items, soup = _parse_page(current_url)
        if not items:
            print(f"[bank {bank_id}] ℹ️ Страница {page_num} пуста - конец каталога")
            break

        all_items.extend(items)
        print(f"[bank {bank_id}] ✅ Стр. {page_num}: +{len(items)} партнёров (всего: {len(all_items)})")

        next_url = _get_next_page_url(soup)
        if not next_url:
            print(f"[bank {bank_id}] ✅ Достигнута последняя страница ({page_num})")
            break

        current_url = next_url
        page_num += 1
        time.sleep(1)

    if all_items:
        print(f"\n[bank {bank_id}] 📊 Всего загружено: {len(all_items)} партнёров со страниц 1-{page_num}")
        save_belkart_items(bank_id, all_items)
    else:
        print(f"[bank {bank_id}] ⚠️ Партнёры не загружены")

    done_msg = f"[bank {bank_id}] ✅ Белкарт завершён: {len(all_items)} партнёров загружено"
    print(done_msg)
    if progress:
        progress(banks_done, banks_total, done_msg)

    return all_items
