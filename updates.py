# category_scraper.py (update.py)
import traceback
import time
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

from db_sql import (
    get_all_bank_ids,
    fetch_categories_scrape_config,
    fetch_partners_scrape_config,
    save_single_category,
    save_partners,
)

import sqlite3
import datetime

ProgressFn = Optional[Callable[[int, int, str], None]]  # progress(done, total, note)


def _driver() -> webdriver.Chrome:
    opts = Options()
    # opts.page_load_strategy = 'none'  # можно включить, если нужно ускорение
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    drv = webdriver.Chrome(options=opts)
    return drv


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
        print("⚠️ Окно cookie не появилось — продолжаем")


def fetch_categories_for_bank(
    bank_id: int,
    progress: ProgressFn = None,
    banks_done: int = 0,
    banks_total: int = 0,
) -> List[Dict[str, Any]]:
    """
    Парсит категории и партнёров для одного банка.

    ДОБАВЛЕНО:
    - progress-логирование по категориям:
      * старт категории
      * успешное завершение
      * ошибки (категория не найдена, ошибки при парсинге партнёров и т.п.)
    """
    cfg = fetch_categories_scrape_config(bank_id)
    url = cfg["url"]
    if not url:
        msg = f"bank_id={bank_id} has empty loyalty_url"
        if progress:
            progress(banks_done, banks_total, f"[bank {bank_id}] ❌ {msg}")
        raise ValueError(msg)

    driver = _driver()
    try:
        try:
            driver.maximize_window()
        except Exception:
            driver.set_window_size(1920, 1080)

        note_start = f"[bank {bank_id}] Открываем {url}"
        print(note_start)
        if progress:
            progress(banks_done, banks_total, note_start)

        driver.get(url)

        # 1. Cookie
        _click_cookie(driver, cfg.get("cookie_text", ""))

        # 2. Контейнер категорий
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, cfg["container_selector"]))
        )
        time.sleep(2)

        # 3. Список категорий
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

        # 4. Цикл по именам категорий
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
                label = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, label_xpath))
                )
            except TimeoutException:
                msg = f"{cat_prefix} ⚠️ Категория не найдена (Timeout)"
                print(msg)
                if progress:
                    progress(banks_done, banks_total, msg)
                continue

            # Клик по категории
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
                done += 1
                tb = traceback.format_exc()
                msg = f"[bank {bank_id}] ❌ Ошибка на уровне банка: {e}\n{tb}"
                print(msg)
                if progress:
                    progress(done, total, msg)
                continue

            # ждём, пока изменится URL (если меняется)
            try:
                WebDriverWait(driver, 10).until(lambda d: d.current_url != url)
            except TimeoutException:
                warn = f"{cat_prefix} ⚠️ URL не изменился, возможно контент обновляется динамически"
                print(warn)
                if progress:
                    progress(banks_done, banks_total, warn)

            time.sleep(3)
            category_url = driver.current_url
            print("🌐 URL категории:", category_url)

            category = {
                "category_name": category_name,
                "partners_count": None,
                "category_url": category_url,
            }
            categories.append(category)

            # сохраняем категорию и получаем её id
            try:
                category_id = save_single_category(category, bank_id)
            except Exception as e:
                msg = f"{cat_prefix} ❌ Ошибка сохранения категории в БД: {e}"
                print(msg)
                if progress:
                    progress(banks_done, banks_total, msg)
                # даже если категория не сохранилась — идём дальше
                continue

            # парсим партнёров этой категории
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

            # сброс фильтра
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
                warn = f"{cat_prefix} ⚠️ Не удалось сбросить фильтр — пробуем back()"
                print(warn)
                if progress:
                    progress(banks_done, banks_total, warn)
                driver.back()
            except Exception as e:
                warn = f"{cat_prefix} ⚠️ Ошибка при сбросе фильтра: {e}"
                print(warn)
                if progress:
                    progress(banks_done, banks_total, warn)

            # ждём возврата контейнера категорий
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
                if progress:
                    progress(banks_done, banks_total, warn)

        return categories

    finally:
        driver.quit()


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
    """
    Парсинг партнёров по категории.

    ДОБАВЛЕНО:
    - progress-логирование:
      * старт раскрытия "Показать ещё"
      * завершение раскрытия
      * ошибки кликов по кнопке
      * итоговое количество партнёров
      * ошибки при сохранении в БД
    """
    pcfg = fetch_partners_scrape_config(bank_id)

    if cat_prefix == "":
        cat_prefix = f"[bank {bank_id} cat ?]"

    # 1. Нажимаем "Показать ещё" до конца
    if progress:
        progress(
            banks_done,
            banks_total,
            f"{cat_prefix} ▶️ Раскрываем список партнёров ('{pcfg['button_more']}')",
        )

    while True:
        try:
            btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//button[contains(., '{pcfg['button_more']}')]")
                )
            )
            print("Нашёл кнопку:", btn.text)
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            try:
                btn.click()
            except (ElementClickInterceptedException, StaleElementReferenceException):
                driver.execute_script("arguments[0].click();", btn)
            time.sleep(2)
        except TimeoutException:
            msg = f"{cat_prefix} ℹ️ Кнопка 'Показать ещё' больше не найдена — выходим из цикла"
            print(msg)
            if progress:
                progress(banks_done, banks_total, msg)
            break
        except Exception as e:
            msg = f"{cat_prefix} ❌ Ошибка при клике по 'Показать ещё': {e}"
            print(msg)
            if progress:
                progress(banks_done, banks_total, msg)
            break

    # 2. Парсим карточки партнёров
    cards = driver.find_elements(By.CSS_SELECTOR, pcfg["partners_list"])
    msg_found = f"{cat_prefix} 🔍 Найдено партнёров: {len(cards)}"
    print(msg_found)
    if progress:
        progress(banks_done, banks_total, msg_found)

    result: List[Dict[str, Any]] = []

    for card in cards:
        # name
        try:
            name_el = card.find_element(By.CSS_SELECTOR, pcfg["partner_name"])
            name_t = name_el.text.strip()
            if "," in name_t:
                original_name = name_t
                name = name_t.split(",", 1)[0].strip()
                rest = name_t.split(",", 1)[1].strip()
                print(f"✂️ Название '{original_name}' обрезано до '{name}'")
            else:
                name = name_t
                rest = None
            if not name:
                print(f"⚠️ Пустое имя по селектору: {pcfg['partner_name']}")
                name = "—"
        except Exception:
            print(f"❌ Не удалось найти элемент имени по селектору: {pcfg['partner_name']}")
            name = "—"
            rest = None

        # bonus
        bonus = None
        try:
            bonus_el = card.find_element(By.CSS_SELECTOR, pcfg["partner_bonus"])
            bonus_raw = bonus_el.text.strip()
            bonus = bonus_raw.replace(pcfg["bonus_unit"], "").strip() or None
        except Exception:
            if rest:
                bonus = rest.replace(pcfg["bonus_unit"], "").strip() or None

        # link
        try:
            href_raw = card.get_attribute("href") or ""
            link = urljoin(base_url, href_raw) if href_raw else ""
        except Exception:
            link = ""

        result.append(
            {
                "partner_name": name,
                "partner_bonus": bonus,
                "partner_link": link,
            }
        )

    # 3. Сохраняем партнёров
    try:
        print("💾 Сохраняем партнёров через save_partners...")
        save_partners(result, bank_id, category_id)
        msg_saved = f"{cat_prefix} ✅ Сохранено партнёров: {len(result)}"
        print(msg_saved)
        if progress:
            progress(banks_done, banks_total, msg_saved)
    except Exception as e:
        msg = f"{cat_prefix} ❌ Ошибка при сохранении партнёров в БД: {e}"
        print(msg)
        if progress:
            progress(banks_done, banks_total, msg)
        # не пробрасываем дальше — вернём то, что напарсили
    return result


def update_all_banks_categories(progress: ProgressFn = None) -> None:
    # """
    # Обходит все банки и запускает парсинг.
    # Если передан progress(done, total, note), будет вызывать его:
    # - по банкам (как раньше);
    # - ДОБАВЛЕНО: по категориям и по партнёрам внутри fetch_categories_for_bank/_parse_partners.
    # """
    # bank_ids = get_all_bank_ids()
    # total = len(bank_ids)
    # if total == 0:
    #     if progress:
    #         progress(1, 1, "В таблице banks нет записей")
    #     return

    # done = 0
    # for bank_id in bank_ids:
    #     if progress:
    #         progress(done, total, f"[bank {bank_id}] ▶️ Старт парсинга банка")
    #     try:
    #         fetch_categories_for_bank(
    #             bank_id,
    #             progress=progress,
    #             banks_done=done,
    #             banks_total=total,
    #         )
    #         done += 1
    #         if progress:
    #             progress(done, total, f"[bank {bank_id}] ✅ Готово по банку")
    #     except Exception as e:
    #         done += 1
    #         msg = f"[bank {bank_id}] ❌ Ошибка на уровне банка: {e}"
    #         print(msg)
    #         if progress:
    #             progress(done, total, msg)
    try:
        print("🔍 Парсим категории с ")
        categories = fetch_categories(bank_id=2)
        print(f"✅ Найдено {len(categories)} категорий")

        for c in categories:
            print(f"- {c['category_name']} → {c['category_url']}")

        #save_categories_to_db(categories, bank_id=3)
        print("✅ Категории успешно сохранены в базу данных!")
     
    except TimeoutException:
        print("❌ Не удалось найти блок с категориями.")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def fetch_categories(bank_id):
    bank = fetch_categories_from_db(bank_id)
    url = bank["url"]
    print(url)


    driver = _driver()
    try:
        # в headless режиме maximize_window может падать, поэтому можно вообще не вызывать
        # try:
        #     driver.maximize_window()
        # except Exception:
        #     driver.set_window_size(1920, 1080)

        driver.get(url)
        ...
    finally:
        driver.quit()

    # === 1. Закрываем окно cookie по тексту кнопки ===
    if bank["cookie_text"]:
        try:
            cookie_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{bank['cookie_text']}')]"))
            )
            driver.execute_script("arguments[0].click();", cookie_btn)
            print("✅ Cookie окно закрыто")
        except TimeoutException:
            print("⚠️ Окно cookie не появилось — продолжаем")

    # === 2. Ждём контейнер категорий ===
    container = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, bank["container_selector"]))
    )
    time.sleep(2)
    #print(container.get_attribute('innerHTML'))

    # === 3. Получаем текст всех категорий один раз ===
    cat_elements = container.find_elements(By.CSS_SELECTOR, bank["element_selector"])
    
    #category_names = [el.text.strip() for el in cat_elements if el.text.strip() and el.text.strip() != "Все" and el.text.strip() != "Категории"]
    category_names = [
        el.text.strip().split("\n")[0].strip()
        for el in cat_elements
        if el.text.strip()
        and el.text.strip() not in ("Все", "Категории")
    ]

    
    print("📋 Категории:", category_names)

    categories = []
    #el_tag, el_class = bank["element_selector"]
    el_tag, el_class = (bank["element_selector"].split('.', 1) + [None])[:2]

    print(el_tag, " ", el_class)
    cont_tag, cont_class = bank["container_selector"].split('.', 1)
    print(cont_tag, " ", cont_class)

    # === 4. Цикл по именам, не по элементам ===
    for category_name in category_names:
        print(f"\n➡️ Обработка категории: {category_name}")

        # Находим категорию по тексту
        try:
            #label_xpath = f"//span[normalize-space(text())='{category_name}']/ancestor::div[contains(@class, '_item_t9nap_5')]"
            #label_xpath = f"//span[normalize-space(text())='{category_name}']"

            #label_xpath = (
            #f"//{element.split('.')[0]}[contains(@class, '{element.split('.')[1]}') "
            #f"and normalize-space(text())='{category_name}']"
            #f"/ancestor::{container.split('.')[0]}[contains(@class, '{container.split('.')[1]}')]"
            #)

            
            
            label_xpath = (
                f"//{el_tag}[normalize-space(text())='{category_name}']"
                #f"/ancestor::{cont_tag}[contains(@class, '{cont_class}')]"
            )

            #print(label_xpath)

            
            label = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, label_xpath))
            )
        except TimeoutException:
            print(f"⚠️ Категория '{category_name}' не найдена, пропускаем")
            continue
        

        # Кликаем для активации фильтра
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", label)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", label)

        # === 5. Ожидаем смену URL или обновление контента ===
        try:
            WebDriverWait(driver, 10).until(lambda d: d.current_url != url)
        except TimeoutException:
            print("⚠️ URL не изменился, возможно контент обновляется динамически")

        time.sleep(3)
        category_url = driver.current_url
        print("🌐 URL категории:", category_url)

        #categories.append({
        #    "category_name": category_name,
        #    "partners_count": None,
        #    "category_url": category_url
        #})

        
        # сохранить категорию одну, а не вмесет в конце
        category = {
            "category_name": category_name,
            "partners_count": None,
            "category_url": category_url
        }
        categories.append(category)
        
        # Сохраняем категорию сразу и получаем её ID
        category_id = save_single_category_to_db(category, bank_id)

        
        #To do here something beautiful like parsing partners
        partners = parse_partners(driver, category_url, bank_id, category_id)
        print(f"Для категории '{category_name}' найдено {len(partners)} партнёров.")

        
        # === 6. Сбрасываем фильтр (повторный клик по названию) ===
        try:
            label = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, label_xpath))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", label)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", label)
            print(f"♻️ Фильтр '{category_name}' сброшен")
        except TimeoutException:
            print(f"⚠️ Не удалось сбросить фильтр '{category_name}'")
            driver.back()

        # Подстраховка — ждём появления контейнера
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, bank["container_selector"]))
        )
        time.sleep(2)

    driver.quit()
    return categories


# -----------------------Парсинг партнеров---------------------
def parse_partners(driver, base_url, bank_id, category_id):
    """
    Прокручивает страницу, нажимает 'Показать еще', парсит всех партнёров
    и сохраняет их через функцию save_partners_to_db().
    """

    # === 0. Берём настройки из таблицы banks ===
    conn = sqlite3.connect("banks_live.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT button_more, partners_list, partner_name, partner_bonus, bonus_unit
        FROM banks WHERE id=?
    """, (bank_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise ValueError(f"❌ Не найден банк с id={bank_id}")

    button_more_selector, partners_list_selector, name_selector, bonus_selector, bonus_unit = row

    
    # === 1. Нажимаем "Показать еще" до конца ===
    while True:
        try:
            # динамически формируем XPath по тексту из БД
            btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{button_more_selector}')]"))
            )
            print("Нашёл кнопку:", btn.text)
    
            # скроллим к кнопке
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            try:
                btn.click()
            except (ElementClickInterceptedException, StaleElementReferenceException):
                driver.execute_script("arguments[0].click();", btn)
    
            time.sleep(2)
    
        except TimeoutException:
            print("Кнопка больше не найдена — выходим из цикла")
            break
        except Exception as e:
            print("Ошибка при клике:", e)
            break



    # === 2. Парсим контент ===
    #html = driver.page_source
    #soup = BeautifulSoup(html, "html.parser")
    #partners = soup.find_all("a", class_="_item_1czb7_1")
    
    partners = driver.find_elements(By.CSS_SELECTOR, partners_list_selector)
    print(f"🔍 Найдено партнёров: {len(partners)}")

    result = []
    for p in partners:
        #html = p.get_attribute("outerHTML")
        #print(html)

        
        #try:
        #    name_el = p.find_element(By.CSS_SELECTOR, name_selector)
        #    name = name_el.text.strip()
        #except Exception:
        #    name = "—"

        try:
            name_el = p.find_element(By.CSS_SELECTOR, name_selector)
            
            name = name_el.text.strip()
    
            # 🟡 Если в названии есть запятая — берём только часть до неё
            if ',' in name:
                original_name = name
                name = name.split(',', 1)[0].strip()
                print(f"✂️ Название '{original_name}' обрезано до '{name}'")
    
            if not name:
                print(f"⚠️ У партнёра найден пустой тег имени — возможно, неверный селектор: {name_selector}")
                name = "—"
        except Exception:
            print(f"❌ Не удалось найти элемент имени по селектору: {name_selector}")
            name = "—"
        
        try:
            bonus_el = p.find_element(By.CSS_SELECTOR, bonus_selector)
            bonus_value = bonus_el.text.strip().replace(bonus_unit, "").strip()
        except Exception:
            #bonus_value = None  # или None, если хочешь хранить как отсутствие данных
            # Если бонус не найден или пустой — пробуем взять часть после запятой из name
            try:
                name_el = p.find_element(By.CSS_SELECTOR, name_selector)
                name_text = name_el.text.strip()
                if "," in name_text:
                    # часть после запятой
                    bonus_value = name_text.split(",", 1)[1].replace(bonus_unit, "").strip()
                else:
                    bonus_value = None
            except Exception:
                bonus_value = None

            
    
        try:
            link = p.get_attribute("href")
            full_link = urljoin(base_url, link)
        except Exception:
            full_link = ""
    
        result.append({
            "partner_name": name,
            "partner_bonus": bonus_value,
            "partner_link": full_link
        })


    # === 3. Сохраняем через твою функцию ===
    print("💾 Сохраняем партнёров в базу...")
    save_partners_to_db(result, bank_id, category_id)
    print(f"✅ Сохранено {len(result)} партнёров для категории {category_id}")

    return result

def fetch_categories_from_db(bank_id):
    conn = sqlite3.connect("banks_live.db")
    cursor = conn.cursor()
    cursor.execute("SELECT loyalty_url, cookie, container, element FROM banks WHERE id=?", (bank_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise ValueError(f"❌ Банк с id={bank_id} не найден в базе данных")
    print("с запроса " + row[0] + row[1] + row[2] + row[3])

    return {
        "url": row[0],
        "cookie_text": row[1],
        "container_selector": row[2],
        "element_selector": row[3]
    }

def save_categories_to_db(categories, bank_id):
    conn = sqlite3.connect("banks_live.db")
    cursor = conn.cursor()
    cursor.execute("""
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
    checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for cat in categories:
        cursor.execute("""
            SELECT partners_count, url
            FROM categories
            WHERE bank_id=? AND name=?
            ORDER BY checked_at DESC
            LIMIT 1
        """, (bank_id, cat['category_name']))
        last = cursor.fetchone()
        new_count = cat.get('partners_count')
        new_url = cat['category_url']

        if last is None or last[0] != new_count or last[1] != new_url:
            cursor.execute(
                "INSERT INTO categories (bank_id, partners_count, checked_at, name, url) VALUES (?, ?, ?, ?, ?)",
                (bank_id, new_count, checked_at, cat['category_name'], new_url)
            )
            new_category_id = cursor.lastrowid
            update_partners_category_id(bank_id, cat['category_name'], new_category_id, conn)
    
    conn.commit()
    conn.close()

def save_single_category_to_db(category, bank_id):
    """
    Сохраняет одну категорию в базу данных и возвращает её id.
    Если категория уже существует — возвращает существующий id.
    Если есть изменения (url или количество партнёров) — создаёт новую запись.
    """
    conn = sqlite3.connect("banks_live.db")
    cursor = conn.cursor()

    # Гарантируем, что таблица существует
    cursor.execute("""
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

    checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Проверяем последнюю запись этой категории
    cursor.execute("""
        SELECT id, partners_count, url
        FROM categories
        WHERE bank_id=? AND name=?
        ORDER BY checked_at DESC
        LIMIT 1
    """, (bank_id, category['category_name']))
    last = cursor.fetchone()

    new_count = category.get('partners_count')
    new_url = category['category_url']

    if last is None or last[1] != new_count or last[2] != new_url:
        # Создаём новую запись, если нет предыдущей или были изменения
        cursor.execute(
            "INSERT INTO categories (bank_id, partners_count, checked_at, name, url) VALUES (?, ?, ?, ?, ?)",
            (bank_id, new_count, checked_at, category['category_name'], new_url)
        )
        category_id = cursor.lastrowid
    else:
        # Возвращаем существующий id
        category_id = last[0]

    conn.commit()
    conn.close()

    return category_id


#обновляет id у партнеров
def update_partners_category_id(bank_id, category_name, new_category_id, conn=None):
    close_conn = False
    if conn is None:
        conn = sqlite3.connect("banks_live.db")
        close_conn = True
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id FROM categories
        WHERE bank_id = ? AND name = ? AND id != ?
        ORDER BY checked_at DESC
    """, (bank_id, category_name, new_category_id))
    old_ids = [row[0] for row in cursor.fetchall()]
    if not old_ids:
        if close_conn: conn.close()
        return
    for old_id in old_ids:
        cursor.execute("""
            UPDATE partners
            SET category_id = ?
            WHERE bank_id = ? AND category_id = ?
        """, (new_category_id, bank_id, old_id))
    conn.commit()
    if close_conn: conn.close()

def save_partners_to_db(partners, bank_id, category_id):
    conn = sqlite3.connect("banks_live.db")
    cursor = conn.cursor()
    
    checked_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for p in partners:
        # Проверяем последнюю версию партнёра по имени, банку и категории
        cursor.execute("PRAGMA journal_mode=WAL;")

        cursor.execute("""
            SELECT partner_bonus, partner_link
            FROM partners
            WHERE bank_id=? AND category_id=? AND partner_name=?
            ORDER BY checked_at DESC
            LIMIT 1
        """, (bank_id, category_id, p['partner_name']))
        last = cursor.fetchone()
        partner_bonus = p.get('partner_bonus')
        partner_link = p['partner_link']

        # Записываем только если были изменения, либо записи не было
        if last is None or last[0] != partner_bonus or last[1] != partner_link:
            #print("0")
            #if(last is not None):
                #print("+1")
                #print(last[0] , " vs ", partner_bonus)
            cursor.execute(
                "INSERT INTO partners (bank_id, category_id, partner_name, partner_bonus, partner_link, checked_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    bank_id,
                    category_id,
                    p['partner_name'],
                    partner_bonus,
                    partner_link,
                    checked_at
                )
            )
    conn.commit()
    conn.close()

