# category_scraper.py
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
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=opts)


def _click_cookie(driver: webdriver.Chrome, cookie_text: str) -> None:
    if not cookie_text:
        return
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{cookie_text}')]"))
        )
        driver.execute_script("arguments[0].click();", btn)
    except TimeoutException:
        pass


def fetch_categories_for_bank(bank_id: int) -> List[Dict[str, Any]]:
    cfg = fetch_categories_scrape_config(bank_id)
    url = cfg["url"]
    if not url:
        raise ValueError(f"bank_id={bank_id} has empty loyalty_url")

    driver = _driver()
    driver.get(url)
    _click_cookie(driver, cfg["cookie_text"])

    container = WebDriverWait(driver, 12).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, cfg["container_selector"]))
    )
    time.sleep(0.5)

    cat_elements = container.find_elements(By.CSS_SELECTOR, cfg["element_selector"])
    category_names = [
        el.text.strip().split("\n")[0].strip()
        for el in cat_elements
        if el.text.strip() and el.text.strip() not in ("Все", "Категории")
    ]

    el_tag = cfg["element_selector"].split(".", 1)[0]

    categories = []
    for name in category_names:
        label_xpath = f"//{el_tag}[normalize-space(text())='{name}']"
        try:
            label = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, label_xpath))
            )
        except TimeoutException:
            continue

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", label)
        try:
            label.click()
        except (ElementClickInterceptedException, StaleElementReferenceException):
            driver.execute_script("arguments[0].click();", label)

        try:
            WebDriverWait(driver, 8).until(lambda d: d.current_url != url)
        except TimeoutException:
            pass

        time.sleep(0.8)
        category_url = driver.current_url

        category = {"category_name": name, "partners_count": None, "category_url": category_url}
        categories.append(category)

        category_id = save_single_category(category, bank_id)
        partners = _parse_partners(driver, category_url, bank_id, category_id)
        # сброс фильтра
        try:
            label = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, label_xpath))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", label)
            driver.execute_script("arguments[0].click();", label)
        except TimeoutException:
            driver.back()
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, cfg["container_selector"]))
            )
            time.sleep(0.4)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, cfg["container_selector"]))
        )
        time.sleep(0.2)

    driver.quit()
    return categories


def _parse_partners(driver: webdriver.Chrome, base_url: str, bank_id: int, category_id: int) -> List[Dict[str, Any]]:
    pcfg = fetch_partners_scrape_config(bank_id)

    # раскрыть все карточки
    while True:
        try:
            btn = WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{pcfg['button_more']}')]"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            try:
                btn.click()
            except (ElementClickInterceptedException, StaleElementReferenceException):
                driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.8)
        except TimeoutException:
            break

    cards = driver.find_elements(By.CSS_SELECTOR, pcfg["partners_list"])
    result: List[Dict[str, Any]] = []
    for card in cards:
        # name
        try:
            name_t = card.find_element(By.CSS_SELECTOR, pcfg["partner_name"]).text.strip()
        except Exception:
            name_t = "—"
        if "," in name_t:
            name = name_t.split(",", 1)[0].strip()
            rest = name_t.split(",", 1)[1].strip()
        else:
            name, rest = name_t, None

        # bonus
        bonus = None
        try:
            bonus_raw = card.find_element(By.CSS_SELECTOR, pcfg["partner_bonus"]).text.strip()
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

        result.append({"partner_name": name, "partner_bonus": bonus, "partner_link": link})

    save_partners(result, bank_id, category_id)
    return result


def update_all_banks_categories() -> None:
    for bank_id in get_all_bank_ids():
        try:
            fetch_categories_for_bank(bank_id)
        except Exception as e:
            print(f"[SCRAPER] bank_id={bank_id} error: {e}")
