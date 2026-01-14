#cactus
import time
import re
from typing import List, Dict, Any, Optional, Tuple

from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
import urllib3
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from back_db import save_single_category, save_partners

BASE_URL = "https://www.mtbank.by/cards/cactus/part/"


def fetch_cactus_partners(
    bank_id: int,
    progress=None,
    banks_done: int = 0,
    banks_total: int = 0,
) -> List[Dict[str, Any]]:
    from update_nw import _get_driver, _click_cookie

    driver = _get_driver()
    print("✅ Драйвер успешно инициализирован")
    categories_data: List[Dict[str, Any]] = []

    try:
        note = f"[bank {bank_id}] 🌵 Кактус - запуск парсера"
        print(note)
        if progress:
            progress(banks_done, banks_total, note)

        try:
            driver.set_page_load_timeout(30)  # или 40–60, как комфортно
            driver.get(BASE_URL)
        except TimeoutException as e:
            msg = f"[bank {bank_id}] ⏱️ Таймаут при загрузке {BASE_URL}: {e}"
            print(msg)
            if progress:
                progress(banks_done, banks_total, msg)
            return []  # Не роняем весь цикл, а просто скипаем Кактус
        except WebDriverException as e:
            msg = f"[bank {bank_id}] ❌ WebDriver ошибка при загрузке {BASE_URL}: {e}"
            print(msg)
            if progress:
                progress(banks_done, banks_total, msg)
            return []
        except (urllib3.exceptions.ReadTimeoutError, TimeoutError) as e:
            msg = f"[bank {bank_id}] ⏱️ Сетевой таймаут при загрузке {BASE_URL}: {e}"
            print(msg)
            if progress:
                progress(banks_done, banks_total, msg)
            return []

        time.sleep(3)
        _click_cookie(driver, "Согласен")

        # 2. Категории
        categories = _parse_categories(driver)
        print(f"[bank {bank_id}] 📂 Найдено категорий: {len(categories)}")

        if not categories:
            note = f"[bank {bank_id}] ⚠️ Категории не найдены"
            print(note)
            if progress:
                progress(banks_done, banks_total, note)
            return []

        # 3. Обработка категорий
        for idx, (category_name, category_value) in enumerate(categories, 1):
            cat_note = f"[bank {bank_id}] 📋 Категория {idx}/{len(categories)}: {category_name}"
            print(cat_note)
            if progress:
                progress(banks_done, banks_total, cat_note)

            category_data = _process_category(
                driver=driver,
                bank_id=bank_id,
                category_name=category_name,
                category_value=category_value,
                progress=progress,
                banks_done=banks_done,
                banks_total=banks_total,
            )

            if category_data:
                categories_data.append(category_data)

            _reset_category_filter(driver, category_value)
            time.sleep(1)

        print(f"[bank {bank_id}] ✅ Кактус: обработано {len(categories_data)} категорий")
        return categories_data

    except Exception as e:
        print(f"[bank {bank_id}] ❌ Ошибка парсера Кактуса: {e}")
        import traceback
        traceback.print_exc()
        return []


def _parse_categories(driver) -> List[Tuple[str, str]]:
    categories: List[Tuple[str, str]] = []

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".checkboxs.js-bind-checkboxes"))
        )

        checkbox_wraps = driver.find_elements(
            By.CSS_SELECTOR, ".checkboxs.js-bind-checkboxes .checkbox-wrap"
        )
        print(f"🔍 Найдено чекбоксов: {len(checkbox_wraps)}")

        for wrap in checkbox_wraps:
            try:
                text_elem = wrap.find_element(
                    By.CSS_SELECTOR, ".checkbox-el__text.js-checkbox-text"
                )
                category_name = text_elem.text.strip()

                checkbox = wrap.find_element(By.CSS_SELECTOR, "input[type='checkbox']")
                category_value = checkbox.get_attribute("value")

                if category_name and category_value:
                    categories.append((category_name, category_value))
                    print(f"  ✅ {category_name} (value={category_value})")
            except Exception as e:
                print(f"  ⚠️ Ошибка парсинга категории: {e}")

        print(f"✅ Успешно загружено {len(categories)} категорий")

    except TimeoutException:
        print("⚠️ Таймаут при загрузке категорий")
    except Exception as e:
        print(f"❌ Ошибка парсинга категорий: {e}")
        import traceback
        traceback.print_exc()

    return categories


def _process_category(
    driver,
    bank_id: int,
    category_name: str,
    category_value: str,
    progress,
    banks_done: int,
    banks_total: int,
) -> Optional[Dict[str, Any]]:
    """Активирует фильтр категории, обходит все страницы и сохраняет партнёров."""

    category_url = f"{BASE_URL}?filter[59][value][]={category_value}"
    category = {
        "category_name": category_name,
        "partners_count": 0,
        "category_url": category_url,
    }

    try:
        category_id = save_single_category(category, bank_id)
        print(f"✅ Категория сохранена в БД: id={category_id}")
    except Exception as e:
        print(f"❌ Ошибка сохранения категории: {e}")
        return None

    if not _apply_category_filter(driver, category_value):
        print(f"❌ Не удалось активировать фильтр для {category_name}")
        return None

    try:
        all_partners: List[Dict[str, Any]] = []

        # 1. Текущая страница (та, куда загрузился фильтр)
        print("  📄 Страница (текущая после фильтра)")
        all_partners.extend(_parse_page_partners(driver))

        # 2. Собираем все ссылки пагинации и обходим их
        page_links = driver.find_elements(
            By.CSS_SELECTOR, ".pagination__list a.pagination__page"
        )
        page_urls = []
        for a in page_links:
            href = a.get_attribute("href")
            if href:
                page_urls.append(href)

        # уникализация с сохранением порядка
        page_urls = list(dict.fromkeys(page_urls))

        for url in page_urls:
            print(f"  📄 Доп. страница: {url}")
            driver.get(url)
            time.sleep(2)
            all_partners.extend(_parse_page_partners(driver))

        if all_partners:
            save_partners(all_partners, bank_id, category_id)
            print(f"  ✅ Сохранено всего партнёров: {len(all_partners)}")
        else:
            print(f"⚠️ Партнёры не найдены для {category_name}")

        return {
            "category_name": category_name,
            "partners_count": len(all_partners),
            "category_url": category_url,
        }

    except Exception as e:
        print(f"❌ Ошибка парсинга партнёров: {e}")
        import traceback
        traceback.print_exc()
        return None


def _apply_category_filter(driver, category_value: str) -> bool:
    max_retries = 3
    checkbox_xpath = f"//input[@type='checkbox' and @value='{category_value}']"

    for attempt in range(1, max_retries + 1):
        try:
            checkbox = driver.find_element(By.XPATH, checkbox_xpath)

            if not checkbox.is_selected():
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", checkbox)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", checkbox)
                print(f"✅ Фильтр активирован (попытка {attempt}): {category_value}")

            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".about-banners__item"))
            )
            time.sleep(2)
            return True

        except TimeoutException:
            if attempt < max_retries:
                print(f"⚠️ Таймаут (попытка {attempt}), повторяем...")
                time.sleep(1)
            else:
                print(f"❌ Не удалось загрузить партнёров после {max_retries} попыток")
                return False
        except Exception as e:
            print(f"❌ Ошибка при активации фильтра: {e}")
            return False

    return False


def _reset_category_filter(driver, category_value: str) -> None:
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
    partners: List[Dict[str, Any]] = []

    try:
        WebDriverWait(driver, 15).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".about-banners__item")) > 0
        )
        time.sleep(2)

        cards = driver.find_elements(By.CSS_SELECTOR, ".about-banners__item")
        print(f"  🔍 Найдено карточек: {len(cards)}")

        if not cards:
            print("  ⚠️ Карточки не найдены - возможно неверный селектор")
            return partners

        for idx, card in enumerate(cards, 1):
            try:
                try:
                    title_elem = card.find_element(By.CSS_SELECTOR, ".subpage-banner__title")
                    name = title_elem.text.strip()
                except NoSuchElementException:
                    print(f"    ⚠️ Карточка #{idx}: название не найдено")
                    continue

                if not name:
                    continue

                bonus = None
                try:
                    text_elem = card.find_element(By.CSS_SELECTOR, ".subpage-banner__text")
                    bonus_text = text_elem.text.strip()
                    match = re.search(r"(\d+(?:[.,]\d+)?\s*%)", bonus_text)

                    if match:
                        bonus = match.group(1).replace(",", ".")
                except NoSuchElementException:
                    pass

                link = ""
                try:
                    link_elem = card.find_element(By.CSS_SELECTOR, ".subpage-banner__link")
                    link = link_elem.get_attribute("href") or ""
                except NoSuchElementException:
                    pass

                partners.append(
                    {
                        "partner_name": name,
                        "partner_bonus": bonus,
                        "partner_link": link,
                    }
                )
                print(f"    ✅ #{idx}: {name} | Бонус: {bonus or 'нет'}")

            except Exception as e:
                print(f"    ⚠️ Ошибка парсинга карточки #{idx}: {e}")

        print(f"  ✅ Распарсено на этой странице: {len(partners)} партнёров")
        return partners

    except TimeoutException:
        print("  ⚠️ Таймаут при загрузке партнёров")
        return []
    except Exception as e:
        print(f"  ❌ Ошибка парсинга: {e}")
        import traceback
        traceback.print_exc()
        return []
