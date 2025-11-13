# category_scraper.py (update.py)
import time
from typing import Dict, Any, List
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


def _driver() -> webdriver.Chrome:
    """
    ИЗМЕНЕНО:
    - убран set_page_load_timeout(5)
    - стратегия загрузки оставлена по умолчанию (как в ноутбуке),
      но headless и остальное оставляем для сервера.
    """
    opts = Options()
    # opts.page_load_strategy = 'none'  # можно раскомментировать, если нужно ускорение
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


def fetch_categories_for_bank(bank_id: int) -> List[Dict[str, Any]]:
    """
    ИЗМЕНЕНО:
    Логика максимально приближена к fetch_categories из notebook:

    - Получаем cfg из db_sql (аналог fetch_categories_from_db).
    - driver.get(url) → _click_cookie → ждём container.
    - Вытаскиваем category_names как в ноутбуке.
    - Для каждой категории:
        * ищем по XPATH, scrollIntoView + click через execute_script
        * ждём смену URL (по сравнению с исходным loyalty_url)
        * сохраняем категорию через save_single_category
        * парсим партнёров через _parse_partners
        * сбрасываем фильтр повторным кликом или через back().
    """
    cfg = fetch_categories_scrape_config(bank_id)
    url = cfg["url"]
    if not url:
        raise ValueError(f"bank_id={bank_id} has empty loyalty_url")

    driver = _driver()
    try:
        # как в ноутбуке: пробуем maximize, если не работает — задать размер
        try:
            driver.maximize_window()
        except Exception:
            driver.set_window_size(1920, 1080)

        print("Запрашиваем URL:", url)
        driver.get(url)

        # 1. Cookie
        _click_cookie(driver, cfg.get("cookie_text", ""))

        # 2. Контейнер категорий
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, cfg["container_selector"]))
        )
        time.sleep(2)

        # 3. Список категорий (как в notebook-коде)
        cat_elements = container.find_elements(By.CSS_SELECTOR, cfg["element_selector"])
        category_names = [
            el.text.strip().split("\n")[0].strip()
            for el in cat_elements
            if el.text.strip() and el.text.strip() not in ("Все", "Категории")
        ]

        print("📋 Категории:", category_names)

        categories: List[Dict[str, Any]] = []

        el_tag, _ = (cfg["element_selector"].split(".", 1) + [None])[:2]
        print("Элемент категорий:", el_tag)

        # 4. Цикл по именам категорий
        for category_name in category_names:
            print(f"\n➡️ Обработка категории: {category_name}")

            label_xpath = f"//{el_tag}[normalize-space(text())='{category_name}']"

            try:
                label = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, label_xpath))
                )
            except TimeoutException:
                print(f"⚠️ Категория '{category_name}' не найдена, пропускаем")
                continue

            # Клик по категории (точь-в-точь как в notebook)
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", label
            )
            time.sleep(0.3)
            try:
                driver.execute_script("arguments[0].click();", label)
            except (ElementClickInterceptedException, StaleElementReferenceException):
                driver.execute_script("arguments[0].click();", label)

            # ждём, пока изменится URL (если меняется)
            try:
                WebDriverWait(driver, 10).until(lambda d: d.current_url != url)
            except TimeoutException:
                print("⚠️ URL не изменился, возможно контент обновляется динамически")

            time.sleep(3)
            category_url = driver.current_url
            print("🌐 URL категории:", category_url)

            category = {
                "category_name": category_name,
                "partners_count": None,
                "category_url": category_url,
            }
            categories.append(category)

            # сохраняем категорию и получаем её id (аналог save_single_category_to_db)
            category_id = save_single_category(category, bank_id)

            # парсим партнёров этой категории
            partners = _parse_partners(driver, category_url, bank_id, category_id)
            print(f"Для категории '{category_name}' найдено {len(partners)} партнёров.")

            # сброс фильтра (повторный клик, как в notebook-коде)
            try:
                label = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, label_xpath))
                )
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", label
                )
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", label)
                print(f"♻️ Фильтр '{category_name}' сброшен")
            except TimeoutException:
                print(f"⚠️ Не удалось сбросить фильтр '{category_name}' — пробуем back()")
                driver.back()

            # ждём, пока снова появится контейнер
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, cfg["container_selector"])
                )
            )
            time.sleep(2)

        return categories

    finally:
        driver.quit()


def _parse_partners(
    driver: webdriver.Chrome,
    base_url: str,
    bank_id: int,
    category_id: int,
) -> List[Dict[str, Any]]:
    """
    ИЗМЕНЕНО:
    Логика максимально приближена к parse_partners из notebook:

    - вытягиваем button_more, partners_list, partner_name, partner_bonus, bonus_unit;
    - жмём "Показать ещё" до конца;
    - для каждого партнёра:
        * name: берём текст, режем по запятой;
        * bonus: сначала отдельный селектор, если не найден — часть после запятой;
        * link: href с urljoin;
    - сохраняем через save_partners (db_sql).
    """
    pcfg = fetch_partners_scrape_config(bank_id)

    # 1. Нажимаем "Показать ещё" до конца
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
            print("Кнопка 'Показать ещё' больше не найдена — выходим из цикла")
            break
        except Exception as e:
            print("Ошибка при клике по кнопке 'Показать ещё':", e)
            break

    # 2. Парсим карточки партнёров
    cards = driver.find_elements(By.CSS_SELECTOR, pcfg["partners_list"])
    print(f"🔍 Найдено партнёров: {len(cards)}")

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
            # как в notebook-коде: пробуем вытащить из части после запятой
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

    print("💾 Сохраняем партнёров через save_partners...")
    save_partners(result, bank_id, category_id)
    print(f"✅ Сохранено {len(result)} партнёров для категории {category_id}")

    return result


def update_all_banks_categories(progress=None) -> None:
    """
    Обходит все банки и запускает парсинг.
    Если передан progress(done, total, note), будет вызывать его между шагами.
    """
    bank_ids = get_all_bank_ids()
    total = len(bank_ids)
    if total == 0:
        if progress:
            progress(1, 1, "В таблице banks нет записей")
        return

    done = 0
    for bank_id in bank_ids:
        if progress:
            progress(done, total, f"Старт bank_id={bank_id}")
        try:
            fetch_categories_for_bank(bank_id)
            done += 1
            if progress:
                progress(done, total, f"Готово bank_id={bank_id}")
        except Exception as e:
            done += 1
            if progress:
                progress(done, total, f"Ошибка bank_id={bank_id}: {e}")
