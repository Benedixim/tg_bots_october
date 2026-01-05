# update_bnb_fixed.py
import time
import re
from typing import List, Dict, Any
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException


def normalize_partner_name_improved(text: str) -> str:
    """
    Улучшенная нормализация названия партнера
    """
    if not text:
        return ""
    
    # Приводим к нижнему регистру
    text = text.lower().strip()
    
    # Удаляем разные типы дефисов и тире
    text = re.sub(r'[–—‑\-]\s*.*$', '', text)  # Удаляем всё после дефиса
    
    # Удаляем общие слова-мусор
    stop_words = [
        'интернет-магазин', 'интернет магазин', 'онлайн-магазин', 'онлайн магазин',
        'официальный магазин', 'официальный дилер', 'официальный дистрибьютор',
        'магазин', 'торговый центр', 'сеть магазинов', 'салон',
        'кафе', 'ресторан', 'бар', 'кофейня'
    ]
    
    for word in stop_words:
        text = text.replace(word, '')
    
    # Удаляем лишние пробелы и спецсимволы
    text = re.sub(r'[^\w\s]', '', text)  # Удаляем всё кроме букв, цифр и пробелов
    text = re.sub(r'\s+', ' ', text)     # Заменяем множественные пробелы на один
    
    # Убираем пробелы по краям
    text = text.strip()
    
    # Заменяем букву ё на е для унификации
    text = text.replace('ё', 'е')
    
    return text


def get_unique_partners(partners: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Удаляет дубликаты из списка партнеров
    """
    seen = set()
    unique_partners = []
    
    for partner in partners:
        name = partner.get("partner_name", "").strip()
        normalized = normalize_partner_name_improved(name)
        
        if not name or not normalized:
            continue
            
        # Создаем ключ для сравнения (имя + бонус)
        bonus = partner.get("partner_bonus") or ""
        key = f"{normalized}_{bonus}"
        
        if key not in seen:
            seen.add(key)
            unique_partners.append(partner)
        else:
            print(f"♻️ Пропущен дубликат: {name}")
    
    return unique_partners


def fetch_categories_simple_bank(
    bank_id: int,
    progress=None,
    banks_done: int = 0,
    banks_total: int = 0,
):
    """Исправленная версия парсера БНБ без дублей"""
    from db_sql import save_single_category, save_partners_with_status_logic
    
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=opts)
    wait = WebDriverWait(driver, 15)

    note = f"[bank {bank_id}] 📍 Открываем БНБ банк"
    print(note)
    if progress:
        progress(banks_done, banks_total, note)

    driver.get("https://bnb.by/bonus/")
    print(f"📄 URL: {driver.current_url}")
    
    # Ждем загрузки
    time.sleep(3)

    category_selector = 'a.js-action_section[data-id="all"]'

    # Считаем категории
    try:
        categories = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, category_selector))
        )
        category_count = len(categories)
    except TimeoutException:
        print("⚠️ Категории не найдены")
        driver.quit()
        return

    if category_count == 0:
        print("⚠️ Категории не найдены")
        driver.quit()
        return

    print(f"📂 Найдено категорий: {category_count}")

    all_categories_data = []
    
    for index in range(category_count):
        # Каждый раз обновляем список элементов
        categories = driver.find_elements(By.CSS_SELECTOR, category_selector)
        if index >= len(categories):
            break
            
        category = categories[index]
        category_name = category.text.strip()
        
        

        print(f"\n➡️ Категория: {category_name}")
        
        # Создаем категорию
        category_data = {
            "category_name": category_name,
            "category_url": f"https://bnb.by/bonus/#cat={category_name}",
            "partners_count": None,
        }
        
        try:
            category_id = save_single_category(category_data, bank_id)
        except Exception as e:
            print(f"❌ Ошибка сохранения категории: {e}")
            continue

        # Кликаем по категории
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", category)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", category)
            time.sleep(2)  # Ждем загрузки
        except Exception as e:
            print(f"❌ Ошибка клика по категории: {e}")
            continue

        # Парсим партнеров
        try:
            partners = parse_partners_fixed(driver)
            
            # Убираем дубликаты
            unique_partners = get_unique_partners(partners)
            
            if unique_partners:
                # Сохраняем уникальных партнеров
                save_partners_with_status_logic(unique_partners, bank_id, category_id)
                print(f"✅ Сохранено уникальных партнёров: {len(unique_partners)} (было {len(partners)})")
                
                category_data["partners_count"] = len(unique_partners)
                all_categories_data.append(category_data)
            else:
                print("⚠️ Нет уникальных партнеров в категории")
                
        except Exception as e:
            print(f"❌ Ошибка парсинга партнеров: {e}")
            continue

    driver.quit()
    return all_categories_data


def parse_partners_fixed(driver) -> List[Dict[str, Any]]:
    """Парсинг партнеров с улучшенной обработкой"""
    partners = []
    
    try:
        # Ждем загрузки партнеров
        WebDriverWait(driver, 15).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, "a.partner")) > 0
        )

        cards = driver.find_elements(By.CSS_SELECTOR, "a.partner")
        print(f"🧪 BNB: карточек в DOM = {len(cards)}")

        
        # Получаем все карточки
        partner_cards = driver.find_elements(By.CSS_SELECTOR, "a.partner")
        print(f"🔍 Найдено карточек: {len(partner_cards)}")
        
        for idx, card in enumerate(partner_cards):
            try:
                # Пропускаем рекламные карточки
                card_classes = card.get_attribute("class") or ""
                if "ad" in card_classes.lower() or "реклама" in card.text.lower():
                    continue
                
                # Название партнера
                try:
                    title_el = card.find_element(By.CSS_SELECTOR, ".partner__title")
                    raw_title = title_el.get_attribute("textContent") or title_el.text
                    raw_title = raw_title.strip()
                    
                    if not raw_title:
                        continue
                        
                except Exception:
                    continue
                
                # Кастомизированная нормализация для БНБ
                name = raw_title
                
                # Убираем типы организации
                name = re.sub(r'[–—‑\-]\s*(интернет[-\s]*магазин|онлайн[-\s]*магазин|магазин|официальный.*)', '', name, flags=re.IGNORECASE)
                name = name.strip()
                
                # Бонус
                cashback = None
                try:
                    cashback_el = card.find_element(By.CSS_SELECTOR, ".label_manyback")
                    cashback = cashback_el.get_attribute("textContent") or cashback_el.text
                    cashback = cashback.strip()
                except Exception:
                    pass
                
                # Ссылка
                link = card.get_attribute("href") or ""
                
                partners.append({
                    "partner_name": name,
                    "partner_bonus": cashback,
                    "partner_link": link,
                    "raw_name": raw_title  # Для отладки
                })
                
            except StaleElementReferenceException:
                # Пропускаем устаревший элемент
                continue
            except Exception as e:
                print(f"⚠️ Ошибка парсинга карточки #{idx+1}: {e}")
                continue
                
    except TimeoutException:
        print("⚠️ Таймаут при загрузке партнеров")
    except Exception as e:
        print(f"❌ Ошибка парсинга: {e}")
    
    print(f"📊 Успешно распарсено: {len(partners)} партнёров")
    return partners
