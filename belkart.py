# belkart_parser.py
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv
from collections import defaultdict
from typing import List, Dict, Any, Optional, Callable, Tuple

from db_sql import save_single_category, save_partners_with_status_logic

ProgressFn = Optional[Callable[[int, int, str], None]]

BASE_URL = "https://belkart.by/BELKART/reklamnye-aktsii/"

from gigachat import GigaChat
import json
import os

load_dotenv()

GIGACHAT_TOKEN = os.getenv("GIGACHAT_TOKEN")

gc = GigaChat(
    credentials=GIGACHAT_TOKEN,
    scope="GIGACHAT_API_B2B",
    verify_ssl_certs=False,
    model="GigaChat-2-Max",
)
print("GIGACHAT_TOKEN =", GIGACHAT_TOKEN)

_GIGA_CACHE: Dict[str, Dict[str, Any]] = {}


def _parse_page(url: str) -> Tuple[List[Dict[str, Any]], BeautifulSoup]:
    """Парсит страницу и возвращает список партнёров с ссылками"""
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    cards = soup.select("ul.card-list li.card-list__item")
    results = []
    
    print(f"🔍 Парсим {len(cards)} карточек...")

    for i, card in enumerate(cards):
        link_tag = card.select_one("a.card-list__link")
        title_tag = card.select_one(".card-list__title")
        status_tag = card.select_one(".card-list__label")

        raw_title = title_tag.text.strip() if title_tag else ""
        raw_status = status_tag.text.strip() if status_tag else ""
        raw_link = urljoin(BASE_URL, link_tag["href"]) if link_tag and link_tag.get("href") else ""
        
        # Объединяем текст для GigaChat
        raw_text = f"{raw_title} {raw_status}".strip()
        
        print(f"  {i+1}. '{raw_title}' | статус: '{raw_status}'")

        if raw_text in _GIGA_CACHE:
            nlp = _GIGA_CACHE[raw_text]
            print(f"    ♻️ Cache: {nlp}")
        else:
            nlp = extract_company_and_bonus(raw_text)
            _GIGA_CACHE[raw_text] = nlp
            print(f"    🧠 GigaChat: {nlp}")

        company = nlp.get("company") or raw_title
        bonus = nlp.get("bonus")
        
        company = company.strip()
        
        results.append({
            "title": raw_title,
            "link": raw_link,  
            "status": raw_status,
            "company": company,
            "bonus": bonus,
        })

    return results, soup


def extract_company_and_bonus(text: str) -> dict:
    if not text.strip():
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

        Если данных нет — используй null.
        """

    try:
        resp = gc.chat(prompt)
        raw = resp.choices[0].message.content.strip()


        raw = raw.replace("```json", "").replace("```", "").strip()
        data = json.loads(raw)

        company = data.get("company")
        bonus = data.get("bonus")
        

        if bonus:
            bonus = str(bonus).strip()
            bonus = " ".join(bonus.split())

        return {
            "company": company,
            "bonus": bonus if bonus else None,
        }

    except Exception as e:
        print(f"⚠️ GigaChat error: {e}")
        return {
            "company": None,
            "bonus": None,
        }


def save_belkart_items(bank_id: int, items: list):
    """
    Сохраняет партнёров Белкарт, гарантируя:
    1. Все в категории 0
    2. Нет дублей по компании
    3. Максимальный/лучший бонус для каждой компании
    4. Ссылки всегда заполнены
    """
    if not items:
        print("⚠️ Нет предметов для сохранения")
        return

    # Группируем по компании: компания → список кортежей (бонус, ссылка)
    grouped = defaultdict(list)
    
    for item in items:
        company = (item.get("company") or item.get("title") or "").strip()
        
        if not company:
            print(f"⚠️ Пропускаем партнёра без названия: {item}")
            continue
        
        bonus = item.get("bonus")
        link = item.get("link") or ""
        
        # Нормализуем бонус
        if bonus:
            bonus = str(bonus).strip()
            bonus = " ".join(bonus.split())  # удаляем лишние пробелы
        
        grouped[company].append({
            "bonus": bonus,
            "link": link,
        })

    # Формируем финальный список без дублей
    partners_data = []
    
    for company, records in grouped.items():
        best_record = records[0] if records else None  
        best_bonus_value = -1
        
        if not best_record:
            print(f"⚠️ Нет записей для {company}")
            continue
        
        for rec in records:
            bonus = rec.get("bonus")
            link = rec.get("link")
            
            # Нормализуем бонус (может быть список от GigaChat)
            if isinstance(bonus, list):
                bonus = " ".join(str(b).strip() for b in bonus if b)
                rec["bonus"] = bonus if bonus else None
            
            # Пытаемся извлечь числовое значение бонуса для сравнения
            bonus_num = extract_bonus_number(bonus) if bonus else -1
            
            # Выбираем запись с максимальным числовым бонусом
            if bonus_num > best_bonus_value or (bonus_num == best_bonus_value and link and not best_record.get("link")):
                best_bonus_value = bonus_num
                best_record = rec
        
        # Если нет ссылки, пытаемся найти среди дублей
        final_link = best_record.get("link") or ""
        if not final_link:
            for rec in records:
                if rec.get("link"):
                    final_link = rec.get("link")
                    break

        final_bonus = best_record.get("bonus")
        # Нормализуем финальный бонус если это список
        if isinstance(final_bonus, list):
            final_bonus = " ".join(str(b).strip() for b in final_bonus if b) or None

        partners_data.append({
            "partner_name": company,
            "partner_bonus": final_bonus,
            "partner_link": final_link
        })
        
        print(f"  ✓ {company} → бонус: {final_bonus or 'нет'}, ссылка: {'да' if final_link else 'нет'}")

    # Сохраняем в категорию 0
    save_partners_with_status_logic(
        partners=partners_data,
        bank_id=bank_id,
        category_id=0
    )
    
    print(f"✅ Сохранено {len(partners_data)} уникальных партнёров Белкарт")


def extract_bonus_number(bonus_str: Optional[str]) -> float:
    """
    Пытается извлечь числовое значение из строки бонуса.
    Например: "скидка 15%" → 15, "20 дней" → 20, "2 книги в подарок" → 2
    """
    if not bonus_str:
        return -1
    
    import re
    
    # Ищем числа в строке
    numbers = re.findall(r'\d+(?:[\.,]\d+)?', str(bonus_str))
    
    if numbers:
        try:
            # Берём первое число
            return float(numbers[0].replace(',', '.'))
        except:
            return -1
    
    return -1


def fetch_promotions(
    bank_id: int,
    progress: ProgressFn = None,
    banks_done: int = 0,
    banks_total: int = 0,
) -> List[Dict[str, Any]]:
    """
    Загружает все страницы Белкарта и собирает всех партнёров.
    Затем сохраняет их одним запросом.
    """
    all_items: List[Dict[str, Any]] = []
    page = 1

    while True:
        url = BASE_URL if page == 1 else f"{BASE_URL}?PAGEN_1={page}"
        note = f"[bank {bank_id}] 📄 Белкарт — страница {page}"
        print(note)
        if progress:
            progress(banks_done, banks_total, note)

        try:
            items, soup = _parse_page(url)
        except Exception as e:
            err = f"[bank {bank_id}] ❌ Ошибка загрузки страницы {page}: {e}"
            print(err)
            if progress:
                progress(banks_done, banks_total, err)
            break

        if not items:
            break

        all_items.extend(items)
        
        # Проверяем следующую страницу
        next_page = soup.select_one(
            f'ul.pagination-list a.pagination-link[href*="PAGEN_1={page + 1}"]'
        )
        if not next_page:
            break

        page += 1

    # Сохраняем ВСЕ партнёры одним запросом после загрузки всех страниц
    if all_items:
        save_belkart_items(bank_id, all_items)
    
    done = f"[bank {bank_id}] ✅ Белкарт — всего акций: {len(all_items)}"
    print(done)
    if progress:
        progress(banks_done, banks_total, done)

    return all_items