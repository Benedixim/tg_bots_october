# belkart.py
import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
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


def _parse_page(url: str, retry_count: int = 3) -> Tuple[List[Dict[str, Any]], BeautifulSoup]:
    """
    Парсит страницу с повторными попытками при ошибках.
    Возвращает список партнёров и объект BeautifulSoup.
    """
    last_error = None
    
    for attempt in range(retry_count):
        try:
            print(f"  📡 Попытка загрузки {attempt+1}/{retry_count}: {url}")
            
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            cards = soup.select("ul.card-list li.card-list__item")
            results = []
            
            print(f"    🔍 Найдено карточек: {len(cards)}")

            for i, card in enumerate(cards):
                try:
                    link_tag = card.select_one("a.card-list__link")
                    title_tag = card.select_one(".card-list__title")
                    status_tag = card.select_one(".card-list__label")

                    raw_title = title_tag.text.strip() if title_tag else ""
                    raw_status = status_tag.text.strip() if status_tag else ""
                    raw_link = urljoin(BASE_URL, link_tag["href"]) if link_tag and link_tag.get("href") else ""
                    
                    if not raw_title:
                        continue
                    
                    # Объединяем текст для GigaChat
                    raw_text = f"{raw_title} {raw_status}".strip()
                    
                    # Кэш или GigaChat
                    if raw_text in _GIGA_CACHE:
                        nlp = _GIGA_CACHE[raw_text]
                    else:
                        nlp = extract_company_and_bonus(raw_text)
                        _GIGA_CACHE[raw_text] = nlp

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

                except Exception as e:
                    print(f"    ⚠️ Ошибка парсинга карточки #{i+1}: {e}")
                    continue

            return results, soup
            
        except requests.exceptions.Timeout:
            last_error = f"Таймаут на попытке {attempt+1}"
            print(f"  ⏱️ {last_error}, повторяем...")
            time.sleep(2)
            
        except requests.exceptions.RequestException as e:
            last_error = f"Ошибка сети: {e}"
            print(f"  ⚠️ {last_error}, повторяем...")
            time.sleep(2)
            
        except Exception as e:
            last_error = f"Ошибка парсинга: {e}"
            print(f"  ❌ {last_error}")
            return [], BeautifulSoup("", "lxml")
    
    print(f"  ❌ Не удалось загрузить страницу после {retry_count} попыток: {last_error}")
    return [], BeautifulSoup("", "lxml")


def _get_next_page_url(soup: BeautifulSoup, current_page: int) -> Optional[str]:
    """
    Находит URL следующей страницы из пагинации Белкарта.
    
    Структура:
    <div class="pagination">
        <ul class="pagination-list">
            <li><a class="pagination-link active" href="javascript:void(0);">1</a></li>
            <li><a class="pagination-link" href="/BELKART/reklamnye-aktsii/?PAGEN_1=2">2</a></li>
            <li><a class="pagination-button" href="/BELKART/reklamnye-aktsii/?PAGEN_1=2">
    """
    # Ищем активную страницу
    active_page = soup.select_one("a.pagination-link.active")
    if not active_page:
        print(f"    ℹ️ Активная страница пагинации не найдена")
        return None
    
    # Получаем текст активной страницы (номер)
    try:
        current_num = int(active_page.text.strip())
    except (ValueError, AttributeError):
        print(f"    ⚠️ Не удалось определить номер текущей страницы")
        return None
    
    # Ищем ссылку на следующую страницу
    next_page_num = current_num + 1
    
    # Вариант 1: Прямая ссылка на следующий номер в пагинации
    next_link = soup.select_one(f"a.pagination-link[href*='PAGEN_1={next_page_num}']")
    if next_link and next_link.get("href"):
        full_url = urljoin(BASE_URL, next_link.get("href"))
        print(f"    ✅ Найдена следующая страница ({next_page_num}): {full_url}")
        return full_url
    
    # Вариант 2: Кнопка "След." (Next button)
    next_btn = soup.select_one("a.pagination-button:not(.disabled)")
    if next_btn and next_btn.get("href") and next_btn.get("href") != "javascript:void(0);":
        full_url = urljoin(BASE_URL, next_btn.get("href"))
        print(f"    ✅ Найдена кнопка 'След.': {full_url}")
        return full_url
    
    # Вариант 3: Проверяем все ссылки пагинации
    all_links = soup.select("a.pagination-link")
    link_numbers = []
    for link in all_links:
        try:
            num = int(link.text.strip())
            link_numbers.append((num, link))
        except (ValueError, AttributeError):
            continue
    
    if link_numbers:
        link_numbers.sort(key=lambda x: x[0])
        for num, link in link_numbers:
            if num == next_page_num and link.get("href"):
                full_url = urljoin(BASE_URL, link.get("href"))
                print(f"    ✅ Найдена ссылка на страницу {next_page_num}: {full_url}")
                return full_url
    
    print(f"    ℹ️ Следующая страница ({next_page_num}) не найдена - это последняя страница")
    return None


def extract_company_and_bonus(text: str) -> dict:
    """Извлекает компанию и бонус через GigaChat"""
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

        Если данных нет – используй null.
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
    Сохраняет партнёров Белкарта, гарантируя:
    1. Все в категории 0
    2. Нет дубликатов по компании
    3. Максимальный/лучший бонус для каждой компании
    4. Ссылки всегда заполнены
    """
    if not items:
        print("⚠️ Нет предметов для сохранения")
        return

    # Группируем по компании: компания → список записей (бонус, ссылка)
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
            bonus = " ".join(bonus.split())
        
        grouped[company].append({
            "bonus": bonus,
            "link": link,
        })

    # Форматируем финальный список без дубликатов
    partners_data = []
    
    for company, records in grouped.items():
        best_record = records[0] if records else None  
        best_bonus_value = -1
        
        if not best_record:
            print(f"⚠️ Нет записей для {company}")
            continue
        
        # Выбираем запись с максимальным числовым бонусом
        for rec in records:
            bonus = rec.get("bonus")
            link = rec.get("link")
            
            # Нормализуем бонус если это список
            if isinstance(bonus, list):
                bonus = " ".join(str(b).strip() for b in bonus if b)
                rec["bonus"] = bonus if bonus else None
            
            # Извлекаем числовое значение бонуса
            bonus_num = extract_bonus_number(bonus) if bonus else -1
            
            # Выбираем лучший бонус
            if bonus_num > best_bonus_value or (bonus_num == best_bonus_value and link and not best_record.get("link")):
                best_bonus_value = bonus_num
                best_record = rec
        
        # Если нет ссылки, ищем среди дубликатов
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
        
        print(f"  ✅ {company} → бонус: {final_bonus or 'нет'}, ссылка: {'да' if final_link else 'нет'}")

    # Сохраняем в БД
    print(f"\n📝 Сохраняю {len(partners_data)} уникальных партнёров...")
    save_partners_with_status_logic(
        partners=partners_data,
        bank_id=bank_id,
        category_id=0
    )
    
    print(f"✅ Сохранено {len(partners_data)} уникальных партнёров Белкарта")


def extract_bonus_number(bonus_str: Optional[str]) -> float:
    """
    Извлекает числовое значение из строки бонуса.
    Примеры: "скидка 15%" → 15, "20 дней" → 20, "2 книги в подарок" → 2
    """
    if not bonus_str:
        return -1
    
    import re
    
    # Ищем числа в строке
    numbers = re.findall(r'\d+(?:[\.,]\d+)?', str(bonus_str))
    
    if numbers:
        try:
            # Берём первое число (обычно самое значимое)
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
    Загружает ВСЕ страницы Белкарта и сохраняет партнёров.
    Обрабатывает:
    1. Все страницы с правильной пагинацией
    2. Повторные попытки при ошибках
    3. Извлечение названий и бонусов через GigaChat
    4. Дедупликацию по компании
    5. Защита от циклов - отслеживание посещённых URL
    """
    all_items: List[Dict[str, Any]] = []
    current_url = BASE_URL
    page_num = 1
    max_pages = 100  # Защита от бесконечного цикла
    visited_urls = set()  # Отслеживаем посещённые страницы

    while page_num <= max_pages:
        note = f"[bank {bank_id}] 📄 Белкарт – страница {page_num}"
        print(note)
        if progress:
            progress(banks_done, banks_total, note)

        # ✅ Проверяем, не посещали ли уже эту страницу (защита от циклов)
        if current_url in visited_urls:
            print(f"[bank {bank_id}] ⚠️ Обнаружен цикл! Страница уже была посещена: {current_url}")
            print(f"[bank {bank_id}] ✅ Завершён парсинг - всего загружено страниц: {len(visited_urls)}")
            break
        
        visited_urls.add(current_url)

        try:
            items, soup = _parse_page(current_url)
        except Exception as e:
            err = f"[bank {bank_id}] ❌ Ошибка загрузки страницы {page_num}: {e}"
            print(err)
            if progress:
                progress(banks_done, banks_total, err)
            break

        if not items:
            print(f"[bank {bank_id}] ℹ️ Страница {page_num} пуста - конец каталога")
            break

        all_items.extend(items)
        print(f"[bank {bank_id}] ✅ Страница {page_num}: +{len(items)} партнёров (всего: {len(all_items)})")
        
        # Ищем следующую страницу
        next_url = _get_next_page_url(soup, page_num)
        
        if not next_url:
            print(f"[bank {bank_id}] ✅ Достигнута последняя страница ({page_num})")
            break

        current_url = next_url
        page_num += 1
        time.sleep(1)  # Задержка между запросами

    # Сохраняем все партнёры одним запросом
    if all_items:
        print(f"\n[bank {bank_id}] 📊 Всего загружено: {len(all_items)} партнёров со страниц 1-{page_num-1}")
        save_belkart_items(bank_id, all_items)
    else:
        print(f"[bank {bank_id}] ⚠️ Партнёры не загружены")
    
    done = f"[bank {bank_id}] ✅ Белкарт завершён: {len(all_items)} партнёров загружено"
    print(done)
    if progress:
        progress(banks_done, banks_total, done)

    return all_items