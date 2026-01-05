# kaktus_fixed.py
import time
from typing import List, Dict, Any
from urllib.parse import urljoin
import re

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from db_sql import save_single_category, save_partners_with_status_logic

BASE_URL = "https://www.mtbank.by/cards/cactus/part/"

def fetch_cactus_partners(
    bank_id: int,
    progress=None,
    banks_done: int = 0,
    banks_total: int = 0,
) -> List[Dict[str, Any]]:
    from update_nw import _driver, _click_cookie
    
    driver = _driver()
    try:
        note = f"[bank {bank_id}] 🌵 Кактус - запуск парсера"
        print(note)
        if progress:
            progress(banks_done, banks_total, note)
        
        # 1. Открываем главную страницу
        driver.get(BASE_URL)
        time.sleep(3)
        _click_cookie(driver, "Согласен")
        
        # 2. Парсим категории из чекбоксов
        categories = _parse_categories(driver)
        print(f"[bank {bank_id}] 📂 Найдено категорий: {len(categories)}")
        
        if not categories:
            note = f"[bank {bank_id}] ⚠️ Категории не найдены"
            print(note)
            if progress:
                progress(banks_done, banks_total, note)
            return []
        
        all_categories_data = []
        
        # 3. Обрабатываем каждую категорию
        for idx, (category_name, category_value) in enumerate(categories, 1):
            cat_note = f"[bank {bank_id}] 📋 Категория {idx}/{len(categories)}: {category_name}"
            print(cat_note)
            if progress:
                progress(banks_done, banks_total, cat_note)
            
            # Обрабатываем категорию
            category_data = _process_category(
                driver, bank_id, category_name, category_value, 
                progress, banks_done, banks_total
            )
            
            if category_data:
                all_categories_data.append(category_data)
            
            # Возвращаемся к началу и снимаем фильтр
            _reset_category_filter(driver, category_value)
            time.sleep(1)
        
        print(f"[bank {bank_id}] ✅ Кактус: обработано {len(all_categories_data)} категорий")
        return all_categories_data
        
    except Exception as e:
        print(f"[bank {bank_id}] ❌ Ошибка парсера Кактуса: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        driver.quit()


def _parse_categories(driver) -> List[tuple]:
    """
    Парсит список категорий из чекбоксов
    Возвращает список кортежей: [(name, value), ...]
    """
    categories = []
    
    try:
        # Ждём загрузки чекбоксов
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".checkboxs.js-bind-checkboxes"))
        )
        
        # Находим все чекбоксы в контейнере
        checkbox_wraps = driver.find_elements(By.CSS_SELECTOR, ".checkboxs.js-bind-checkboxes .checkbox-wrap")
        
        print(f"🔍 Найдено чекбоксов: {len(checkbox_wraps)}")
        
        for wrap in checkbox_wraps:
            try:
                # Получаем текст категории
                text_elem = wrap.find_element(By.CSS_SELECTOR, ".checkbox-el__text.js-checkbox-text")
                category_name = text_elem.text.strip()
                
                # Получаем value чекбокса
                checkbox = wrap.find_element(By.CSS_SELECTOR, "input[type='checkbox']")
                category_value = checkbox.get_attribute("value")
                
                if category_name and category_value:
                    categories.append((category_name, category_value))
                    print(f"  ✅ {category_name} (value={category_value})")
                
            except Exception as e:
                print(f"  ⚠️ Ошибка парсинга категории: {e}")
                continue
        
        print(f"✅ Успешно загружено {len(categories)} категорий")
        
    except TimeoutException:
        print("⚠️ Таймаут при загрузке категорий")
    except Exception as e:
        print(f"❌ Ошибка парсинга категорий: {e}")
        import traceback
        traceback.print_exc()
    
    return categories


def _process_category(driver, bank_id, category_name, category_value, 
                     progress, banks_done, banks_total) -> Dict[str, Any]:
    """Обрабатывает одну категорию - активирует фильтр и парсит партнёров"""
    
    # 1. Сохраняем категорию в БД с category_id=0 (для Кактуса)
    category = {
        "category_name": category_name,
        "partners_count": 0,
        "category_url": f"{BASE_URL}?filter[59][value][]={category_value}",
    }
    
    try:
        category_id = save_single_category(category, bank_id)
        print(f"✅ Категория сохранена в БД: id={category_id}")
    except Exception as e:
        print(f"❌ Ошибка сохранения категории: {e}")
        return None
    
    # 2. Активируем фильтр категории
    if not _apply_category_filter(driver, category_value):
        print(f"❌ Не удалось активировать фильтр для {category_name}")
        return None
    
    # 3. Парсим партнёров
    try:
        partners = _parse_page_partners(driver)
        
        if partners:
            # Сохраняем партнёров
            save_partners_with_status_logic(partners, bank_id, category_id)
            print(f"  ✅ Сохранено партнёров: {len(partners)}")
        else:
            print(f"⚠️ Партнёры не найдены для {category_name}")
        
        return {
            "category_name": category_name,
            "partners_count": len(partners),
            "category_url": category["category_url"],
        }
        
    except Exception as e:
        print(f"❌ Ошибка парсинга партнёров: {e}")
        return None


def _apply_category_filter(driver, category_value):
    """Активирует чекбокс фильтра категории"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            checkbox_xpath = f"//input[@type='checkbox' and @value='{category_value}']"
            checkbox = driver.find_element(By.XPATH, checkbox_xpath)
            
            if not checkbox.is_selected():
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", checkbox)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", checkbox)
                print(f"✅ Фильтр активирован (попытка {attempt+1}): {category_value}")
            
            # Ждём загрузки партнёров (КЛЮЧЕВОЙ СЕЛЕКТОР!)
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".about-banners__item"))
            )
            time.sleep(2)  # Дополнительная задержка
            return True
            
        except TimeoutException:
            if attempt < max_retries - 1:
                print(f"⚠️ Таймаут (попытка {attempt+1}), повторяем...")
                time.sleep(1)
            else:
                print(f"❌ Не удалось загрузить партнёров после {max_retries} попыток")
                return False
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            return False
    
    return False


def _reset_category_filter(driver, category_value):
    """Снимает фильтр категории"""
    if not category_value:
        return
    
    try:
        checkbox_xpath = f"//input[@type='checkbox' and @value='{category_value}']"
        checkbox = driver.find_element(By.XPATH, checkbox_xpath)
        
        if checkbox.is_selected():
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", checkbox)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", checkbox)
            time.sleep(1)
            print(f"✅ Фильтр снят: {category_value}")
    except Exception as e:
        print(f"⚠️ Ошибка снятия фильтра: {e}")


def _parse_page_partners(driver) -> List[Dict[str, Any]]:
    """
    Парсит партнёров на странице.
    
    Селекторы для mtbank.by Кактус:
    - Контейнер партнёров: div.about-banners__item (или div.grid-s-wrap)
    - Название: h3.subpage-banner__title
    - Бонус: p.subpage-banner__text (содержит текст типа "Возврат за покупки 3% бонусными баллами")
    - Ссылка: a.subpage-banner__link[href]
    """
    partners = []
    
    try:
        # Ждём загрузки карточек партнёров (новый селектор!)
        WebDriverWait(driver, 15).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".about-banners__item")) > 0
        )
        
        time.sleep(2)  # Даём время на полную загрузку
        
        # Находим все карточки партнёров
        cards = driver.find_elements(By.CSS_SELECTOR, ".about-banners__item")
        print(f"🔍 Найдено карточек: {len(cards)}")
        
        if len(cards) == 0:
            print("⚠️ Карточки не найдены - возможно неверный селектор")
            return partners
        
        for idx, card in enumerate(cards, 1):
            try:
                # Название партнёра
                try:
                    title_elem = card.find_element(By.CSS_SELECTOR, ".subpage-banner__title")
                    name = title_elem.text.strip()
                except NoSuchElementException:
                    print(f"  ⚠️ Карточка #{idx}: название не найдено")
                    name = None
                
                if not name:
                    continue
                
                # Бонус (ищем в тексте)
                bonus = None
                try:
                    text_elem = card.find_element(By.CSS_SELECTOR, ".subpage-banner__text")
                    bonus_text = text_elem.text.strip()
                    # Извлекаем процент из текста типа "Возврат за покупки 3% бонусными баллами"
                    match = re.search(r'(\d+(?:[.,]\d+)?)\s*%', bonus_text)
                    if match:
                        bonus = match.group(1).replace(',', '.')
                except NoSuchElementException:
                    pass
                
                # Ссылка
                link = ""
                try:
                    link_elem = card.find_element(By.CSS_SELECTOR, ".subpage-banner__link")
                    link = link_elem.get_attribute("href") or ""
                except NoSuchElementException:
                    pass
                
                partners.append({
                    "partner_name": name,
                    "partner_bonus": bonus,
                    "partner_link": link,
                })
                
                print(f"  ✅ #{idx}: {name} | Бонус: {bonus or 'нет'}")
                
            except Exception as e:
                print(f"  ⚠️ Ошибка парсинга карточки #{idx}: {e}")
                continue
        
        print(f"✅ Успешно распарсено: {len(partners)} партнёров")
        return partners
        
    except TimeoutException:
        print("⚠️ Таймаут при загрузке партнёров")
        return []
    except Exception as e:
        print(f"❌ Ошибка парсинга: {e}")
        import traceback
        traceback.print_exc()
        return []