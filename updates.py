# category_scraper.py (update.py)
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
                msg = f"{cat_prefix} ❌ Ошибка при клике по категории: {e}"
                print(msg)
                if progress:
                    progress(banks_done, banks_total, msg)
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
    """
    Обходит все банки и запускает парсинг.
    Если передан progress(done, total, note), будет вызывать его:
    - по банкам (как раньше);
    - ДОБАВЛЕНО: по категориям и по партнёрам внутри fetch_categories_for_bank/_parse_partners.
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
            progress(done, total, f"[bank {bank_id}] ▶️ Старт парсинга банка")
        try:
            fetch_categories_for_bank(
                bank_id,
                progress=progress,
                banks_done=done,
                banks_total=total,
            )
            done += 1
            if progress:
                progress(done, total, f"[bank {bank_id}] ✅ Готово по банку")
        except Exception as e:
            done += 1
            msg = f"[bank {bank_id}] ❌ Ошибка на уровне банка: {e}"
            print(msg)
            if progress:
                progress(done, total, msg)
