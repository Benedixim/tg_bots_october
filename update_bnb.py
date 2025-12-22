import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException



from db_sql import (
    save_single_category,
    save_partners_with_status_logic
)


# ---------------- DRIVER ----------------

def _driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=opts)


# ---------------- UTILS ----------------

def normalize_partner_name(text: str) -> str:
    """
    'Домотехника – интернет-магазин' -> 'Домотехника'
    """
    if not text:
        return "—"

    text = text.strip()
    text = text.split("–", 1)[0]
    text = text.split("-", 1)[0]
    return text.strip()


# ---------------- MAIN LOGIC ----------------

def fetch_categories_simple_bank(
    bank_id: int,
    progress=None,
    banks_done: int = 0,
    banks_total: int = 0,
):
    driver = _driver()
    wait = WebDriverWait(driver, 15)

    note = f"[bank {bank_id}] Открываем https://bnb.by/bonus/"
    print(note)
    if progress:
        progress(banks_done, banks_total, note)

    driver.get("https://bnb.by/bonus/")
    print(f"📄 URL: {driver.current_url}")
    print(f"📏 Длина страницы: {len(driver.page_source)} символов")

    # debug HTML при необходимости
    with open("debug_page.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)

    category_selector = 'a.js-action_section[data-id ="all"]'

    # считаем количество категорий (НЕ храним элементы!)
    category_count = len(
        wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, category_selector)
            )
        )
    )

    if category_count == 0:
        print("⚠️ Категории не найдены")
        driver.quit()
        return

    print(f"📂 Найдено категорий: {category_count}")

    for index in range(category_count):
        # каждый раз берём элементы заново (ВАЖНО)
        categories = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, category_selector)
            )
        )

        category = categories[index]


        category_name = category.text.strip()
        # if category_name.lower() == "все":
        #     print(f"⚠️ Пропускаем категорию: {category_name}")
        #     continue

        if not category_name:
            continue

        print(f"➡️ Категория: {category_name}")

        category_id = save_single_category(
            category={
                "category_name": category_name,
                "category_url": driver.current_url,
                "partners_count": None,
            },
            bank_id=bank_id
        )

        # скроллим и кликаем безопасно
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});", category
        )
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", category)

        # ждём загрузки партнёров
        try:
            wait.until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "a.partner")
                )
            )
        except TimeoutException:
            print("⚠️ Партнёры не загрузились")
            continue

        partners = _parse_partners(driver)

        save_partners_with_status_logic(
            partners=partners,
            bank_id=bank_id,
            category_id=category_id
        )

        print(f"✅ Партнёров: {len(partners)}")

    driver.quit()




def _parse_partners(driver):
    partners = []
    wait = WebDriverWait(driver, 15)

    # Ждём появления блока партнёров
    wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "div.partners-list__block")
        )
    )

    partner_cards = driver.find_elements(By.CSS_SELECTOR, "a.partner")
    print(f"🔍 Найдено карточек партнёров: {len(partner_cards)}")

    for idx in range(len(partner_cards)):
        try:
            # ⚠️ ПЕРЕПОЛУЧАЕМ элемент каждый раз
            card = driver.find_elements(By.CSS_SELECTOR, "a.partner")[idx]

            title_el = card.find_element(By.CSS_SELECTOR, ".partner__title")

            # 🔥 ВАЖНО
            raw_title = title_el.get_attribute("textContent").strip()

            if not raw_title:
                print(f"⚠️ Пустое название у партнёра #{idx+1}")
                continue

            partner_name = normalize_partner_name(raw_title)

            try:
                cashback = card.find_element(
                    By.CSS_SELECTOR, ".label_manyback"
                ).get_attribute("textContent").strip()
            except Exception:
                cashback = None

            link = card.get_attribute("href") or ""

            partners.append({
                "partner_name": partner_name,
                "partner_bonus": cashback,
                "partner_link": link,
            })

            print(f"✅ {partner_name} | {cashback}")

        except StaleElementReferenceException:
            print(f"♻️ stale element у партнёра #{idx+1}, повтор")
            continue

        except Exception as e:
            print(f"❌ Ошибка партнёра #{idx+1}: {e}")
            continue

    print(f"✅ Успешно распарсено партнёров: {len(partners)}")
    return partners
