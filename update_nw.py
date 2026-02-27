# update_nw.py
import traceback
import time
import gc
from typing import Dict, Any, List, Callable, Optional
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
)

from back_db import (
    get_all_bank_ids,
    fetch_categories_scrape_config,
    fetch_partners_scrape_config,
    save_single_category,
    save_partners
)

ProgressFn = Optional[Callable[[int, int, str], None]]

from сaсtus import fetch_cactus_partners
from bnb import fetch_promotions_bnb
from belkart import fetch_promotions

PARSER_REGISTRY = {
    "default": None,
    "simple_js_categories": fetch_promotions_bnb,   # Банк 1 (БНБ)
    "belkart": fetch_promotions,                    # Банк 2 (Белкарт)
    "cactus": fetch_cactus_partners,                # Банк 13 (Кактус)
}

# Глобальный драйвер для переиспользования
#_global_driver = None

def _get_driver() -> webdriver.Chrome:
    """Переиспользуем драйвер для снижения утечек памяти"""
    #global _global_driver
    
    #if _global_driver is None:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
        #opts.add_argument("--disable-gpu")
        #opts.add_argument("--disable-extensions")
        #opts.add_argument("--disable-plugins")
        # Ограничиваем использование памяти
        #opts.add_argument("--memory-pressure-off")
        
    _global_driver = webdriver.Chrome(options=opts)
    
    return _global_driver

def _cleanup_driver():
    """Очищаем глобальный драйвер"""
    a = 1
    #global _global_driver
    #try:
    #    if _global_driver:
    #        _global_driver.quit()
    #        _global_driver = None
    #except:
    #    pass
    #gc.collect()

def _click_cookie(driver: webdriver.Chrome, cookie_text: str) -> None:
    if not cookie_text:
        return
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{cookie_text}')]"))
        )
        driver.execute_script("arguments[0].click();", btn)
        print("✅ Cookie окно закрыто")
    except TimeoutException:
        print("⚠️ Окно cookie не появилось – продолжаем")

def fetch_categories_for_bank(
    bank_id: int,
    progress: ProgressFn = None,
    banks_done: int = 0,
    banks_total: int = 0,
) -> List[Dict[str, Any]]:
    """Router с поддержкой разных парсеров"""


    cfg = fetch_categories_scrape_config(bank_id)
    print("DEBUG cfg:", bank_id, cfg.get("parser_type"))
    parser_type = cfg.get("parser_type", "default")



    if parser_type != "default":
        _cleanup_driver() 
        parser = PARSER_REGISTRY.get(parser_type)
        if not parser:
            raise ValueError(f"Неизвестный parser_type: {parser_type}")

        return parser(
            bank_id=bank_id,
            progress=progress,
            banks_done=banks_done,
            banks_total=banks_total,
        )
    
    container_selector = cfg.get("container_selector")
    if not container_selector:
        raise ValueError(
            f"[bank {bank_id}] container_selector пустой для default-парсера"
        )

    url = cfg["url"]
    if not url:
        msg = f"bank_id={bank_id} has empty loyalty_url"
        if progress:
            progress(banks_done, banks_total, f"[bank {bank_id}] ❌ {msg}")
        raise ValueError(msg)

    driver = _get_driver()
    
    try:
        note_start = f"[bank {bank_id}] Открываем {url}"
        print(note_start)
        if progress:
            progress(banks_done, banks_total, note_start)

        driver.get(url)
        
        # Очищаем кеш браузера периодически
        #if banks_done % 5 == 0:
        #    driver.execute_script("window.localStorage.clear();")
        #    driver.execute_script("window.sessionStorage.clear();")

        _click_cookie(driver, cfg.get("cookie_text", ""))

        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, cfg["container_selector"]))
        )
        time.sleep(2)

        cat_elements = container.find_elements(By.CSS_SELECTOR, cfg["element_selector"])
        category_names = [
            el.text.strip().split("\n")[0].strip()
            for el in cat_elements
            if el.text.strip() and el.text.strip() not in ("Все", "Категории")
        ]

        print("📋 Категории:", category_names)
        if progress:
            progress(
                banks_done,
                banks_total,
                f"[bank {bank_id}] Найдено категорий: {len(category_names)}",
            )

        categories: List[Dict[str, Any]] = []
        el_tag, _ = (cfg["element_selector"].split(".", 1) + [None])[:2]
        print("Элемент категорий:", el_tag)

        for idx, category_name in enumerate(category_names, start=1):
            cat_prefix = f"[bank {bank_id} cat {idx}/{len(category_names)} '{category_name}']"

            if progress:
                progress(
                    banks_done,
                    banks_total,
                    f"{cat_prefix} ▶️ Старт обработки категории",
                )

            print(f"\n➡️ Обработка категории: {category_name}")
            label_xpath = f"//{el_tag}[normalize-space(text())='{category_name}']"

            try:
                label = WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, label_xpath))
                )
            except TimeoutException:
                msg = f"{cat_prefix} ⚠️ Категория не найдена (Timeout)"
                print(msg)
                if progress:
                    progress(banks_done, banks_total, msg)
                continue

            try:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", label
                )
                time.sleep(0.3)
                try:
                    driver.execute_script("arguments[0].click();", label)
                except (ElementClickInterceptedException, StaleElementReferenceException):
                    driver.execute_script("arguments[0].click();", label)
            except Exception as e:
                msg = f"[bank {bank_id}] ❌ Ошибка на уровне банка: {e}"
                print(msg)
                if progress:
                    progress(banks_done, banks_total, msg)
                continue

            try:
                WebDriverWait(driver, 10).until(lambda d: d.current_url != url)
            except TimeoutException:
                warn = f"{cat_prefix} ⚠️ URL не изменился"
                print(warn)
                continue

            time.sleep(3)
            category_url = driver.current_url
            print("🌐 URL категории:", category_url)

            category = {
                "category_name": category_name,
                "partners_count": None,
                "category_url": category_url,
            }
            categories.append(category)

            try:
                category_id = save_single_category(category, bank_id)
            except Exception as e:
                msg = f"{cat_prefix} ❌ Ошибка сохранения категории в БД: {e}"
                print(msg)
                if progress:
                    progress(banks_done, banks_total, msg)
                continue

            try:
                partners = _parse_partners(
                    driver,
                    category_url,
                    bank_id,
                    category_id,
                    progress=progress,
                    banks_done=banks_done,
                    banks_total=banks_total,
                    cat_prefix=cat_prefix,
                )
                ok = f"{cat_prefix} ✅ Готово, партнёров: {len(partners)}"
                print(ok)
                if progress:
                    progress(banks_done, banks_total, ok)
            except Exception as e:
                msg = f"{cat_prefix} ❌ Ошибка при парсинге партнёров: {e}"
                print(msg)
                if progress:
                    progress(banks_done, banks_total, msg)

            try:
                label = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, label_xpath))
                )
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", label
                )
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", label)
                print(f"{cat_prefix} ♻️ Фильтр сброшен")
                if progress:
                    progress(banks_done, banks_total, f"{cat_prefix} ♻️ Фильтр сброшен")
            except TimeoutException:
                warn = f"{cat_prefix} ⚠️ Не удалось сбросить фильтр"
                print(warn)
                driver.back()
            except Exception as e:
                warn = f"{cat_prefix} ⚠️ Ошибка при сбросе фильтра: {e}"
                print(warn)

            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, cfg["container_selector"])
                    )
                )
                time.sleep(2)
            except TimeoutException:
                warn = f"{cat_prefix} ⚠️ После сброса не появился контейнер категорий"
                print(warn)
                continue

        return categories

    finally:
        gc.collect()

def _parse_partners(
    driver: webdriver.Chrome,
    base_url: str,
    bank_id: int,
    category_id: int,
    progress: ProgressFn = None,
    banks_done: int = 0,
    banks_total: int = 0,
    cat_prefix: str = "",
) -> List[Dict[str, Any]]:
    """Парсинг партнёров по категории"""
    
    pcfg = fetch_partners_scrape_config(bank_id)

    if cat_prefix == "":
        cat_prefix = f"[bank {bank_id} cat ?]"

    if progress:
        progress(
            banks_done,
            banks_total,
            f"{cat_prefix} ▶️ Раскрываем список партнёров",
        )

    max_clicks = 20
    clicks = 0

    while clicks < max_clicks:
        try:

            time.sleep(2)
            btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//button[contains(., '{pcfg['button_more']}')]")
                )
            )
            print("Нашёл кнопку:", btn.text)
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)

            try:
                btn.click()
                clicks += 1
            except (ElementClickInterceptedException, StaleElementReferenceException):
                driver.execute_script("arguments[0].click();", btn)
            time.sleep(2)
        except TimeoutException:
            msg = f"{cat_prefix} ℹ️ Кнопка 'Показать ещё' больше не найдена"
            print(msg)
            break
        except Exception as e:
            msg = f"{cat_prefix} ❌ Ошибка при клике: {e}"
            print(msg)
            break
    
        
    if clicks == max_clicks:
        print(f"{cat_prefix} ⚠️ Превышен лимит кликов 'Показать ещё'")

    cards = driver.find_elements(By.CSS_SELECTOR, pcfg["partners_list"])
    msg_found = f"{cat_prefix} 🔍 Найдено партнёров: {len(cards)}"
    print(msg_found)
    if progress:
        progress(banks_done, banks_total, msg_found)

    result: List[Dict[str, Any]] = []

    for card in cards:
        try:
            name_el = card.find_element(By.CSS_SELECTOR, pcfg["partner_name"])
            name_t = name_el.text.strip()

            if not name_t:
                tc = (name_el.get_attribute("textContent") or "").strip()
                if tc:
                    name_t = tc

            if "," in name_t:
                name = name_t.split(",", 1)[0].strip()
                rest = name_t.split(",", 1)[1].strip()
            else:
                name = name_t
                rest = None
                
            if not name:
                name = "—"
        except Exception:
            print(f"⚠️ Не удалось найти имя партнёра")
            name = "—"
            rest = None

        bonus = None
        try:
            bonus_el = card.find_element(By.CSS_SELECTOR, pcfg["partner_bonus"])
            bonus_raw = bonus_el.text.strip()
            bonus = bonus_raw.replace(pcfg["bonus_unit"], "").strip() or None
        except Exception:
            if rest:
                bonus = rest.replace(pcfg["bonus_unit"], "").strip() or None

        try:
            href_raw = card.get_attribute("href") or ""
            link = urljoin(base_url, href_raw) if href_raw else ""
        except Exception:
            link = ""

        result.append({
            "partner_name": name,
            "partner_bonus": bonus,
            "partner_link": link,
        })

    try:
        print("💾 Сохраняем партнёров...")
        save_partners(result, bank_id, category_id)
        msg_saved = f"{cat_prefix} ✅ Сохранено партнёров: {len(result)}"
        print(msg_saved)
        if progress:
            progress(banks_done, banks_total, msg_saved)
    except Exception as e:
        msg = f"{cat_prefix} ❌ Ошибка при сохранении в БД: {e}"
        print(msg)
        if progress:
            progress(banks_done, banks_total, msg)

    return result

def update_all_banks_categories(progress: ProgressFn = None) -> None:
    """Обходит все банки и запускает парсинг"""
    
    bank_ids = get_all_bank_ids()
    total = len(bank_ids)
    
    if total == 0:
        if progress:
            progress(1, 1, "В таблице banks нет записей")
        return

    done = 0
    
    try:
        for bank_id in bank_ids:
            if bank_id == 13:
                continue
            if progress:
                progress(done, total, f"[bank {bank_id}] ▶️ Старт парсинга банка")

            try:
                fetch_categories_for_bank(
                    bank_id,
                    progress=progress,
                    banks_done=done,
                    banks_total=total,
                )
            except Exception as e:
                print(f"[bank {bank_id}] ❌ Ошибка банка: {e}")
            finally:
                done += 1
                if progress:
                    progress(done, total, f"[bank {bank_id}] ⏭ Переход к следующему банку")

                    
    finally:
        _cleanup_driver()
        gc.collect()