import time
from typing import List, Dict, Any, Optional, Callable
from urllib.parse import urljoin
from collections import defaultdict

import requests
from bs4 import BeautifulSoup

from back_db import save_partners

ProgressFn = Optional[Callable[[int, int, str], None]]

BASE_URL = "https://bnb.by/bonus/"


def _parse_page(url: str, retry_count: int = 3) -> List[Dict[str, Any]]:
    """
    Парсит главную страницу БНБ с повторными попытками при ошибках.
    Извлекает все карточки партнёров: название, ссылку и бонус.
    """
    last_error = None

    for attempt in range(1, retry_count + 1):
        try:
            print(f"  📡 Попытка загрузки {attempt}/{retry_count}: {url}")

            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            cards = soup.select("a.partner.popup-modal.js-var_seall.js-var_se")
            results: List[Dict[str, Any]] = []

            print(f"    🔍 Найдено карточек: {len(cards)}")

            for i, card in enumerate(cards, start=1):
                try:
                    link = card.get("href") or ""
                    if link:
                        link = urljoin(BASE_URL, link)

                    bonus_tag = card.select_one(".label_manyback")
                    bonus = (bonus_tag.text or "").strip() if bonus_tag else ""

                    title_tag = card.select_one(".partner__title")
                    title = (title_tag.text or "").strip() if title_tag else ""
                    title = " ".join(title.split())

                    if not title:
                        print(f"    ⚠️ Карточка #{i}: пропускаем (нет названия)")
                        continue

                    results.append(
                        {
                            "title": title,
                            "link": link,
                            "bonus": bonus,
                        }
                    )
                    print(f"    ✓ {title[:40]} → {bonus}")
                except Exception as e:
                    print(f"    ⚠️ Ошибка парсинга карточки #{i}: {e}")

            return results

        except requests.exceptions.Timeout:
            last_error = f"Таймаут на попытке {attempt}"
            print(f"  ⏱️ {last_error}, повторяем...")
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            last_error = f"Ошибка сети: {e}"
            print(f"  ⚠️ {last_error}, повторяем...")
            time.sleep(2)
        except Exception as e:
            last_error = f"Ошибка парсинга: {e}"
            print(f"  ❌ {last_error}")
            return []

    print(f"  ❌ Не удалось загрузить страницу после {retry_count} попыток: {last_error}")
    return []


def save_bnb_items(bank_id: int, items: List[Dict[str, Any]]) -> None:
    """
    Сохраняет партнёров БНБ, гарантируя:
    1. Все в категории 0.
    2. Нет дубликатов по названию компании.
    3. Выбирается запись с непустым бонусом (если есть).
    4. Ссылки всегда заполнены, если где‑то были.
    """
    if not items:
        print("⚠️ Нет партнёров для сохранения")
        return

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for item in items:
        title = (item.get("title") or "").strip()
        if not title:
            print("⚠️ Пропускаем партнёра без названия")
            continue

        bonus = str(item.get("bonus") or "").strip()
        bonus = " ".join(bonus.split())
        link = item.get("link") or ""

        grouped[title].append({"bonus": bonus, "link": link})

    partners_data: List[Dict[str, Any]] = []

    for company, records in grouped.items():
        best_record = None
        for rec in records:
            if rec.get("bonus"):
                best_record = rec
                break
        if best_record is None:
            best_record = records[0]

        final_link = best_record.get("link") or ""
        if not final_link:
            for rec in records:
                if rec.get("link"):
                    final_link = rec["link"]
                    break

        final_bonus = best_record.get("bonus") or None

        partners_data.append(
            {
                "partner_name": company,
                "partner_bonus": final_bonus,
                "partner_link": final_link,
            }
        )

        print(
            f"  ✅ {company[:50]} → бонус: {final_bonus or 'нет'}, "
            f"ссылка: {'да' if final_link else 'нет'}"
        )

    print(f"\n📝 Сохраняю {len(partners_data)} уникальных партнёров...")
    save_partners(partners=partners_data, bank_id=bank_id, category_id=0)
    print(f"✅ Сохранено {len(partners_data)} уникальных партнёров БНБ")


def fetch_promotions_bnb(
    bank_id: int,
    progress: ProgressFn = None,
    banks_done: int = 0,
    banks_total: int = 0,
) -> List[Dict[str, Any]]:
    """
    Загружает всех партнёров с главной страницы БНБ и сохраняет их.
    """
    note = f"[bank {bank_id}] 📄 БНБ – загрузка главной страницы"
    print(note)
    if progress:
        progress(banks_done, banks_total, note)

    try:
        all_items = _parse_page(BASE_URL)

        if not all_items:
            print(f"[bank {bank_id}] ⚠️ Не удалось загрузить партнёров")
            done = f"[bank {bank_id}] ❌ БНБ завершён: 0 партнёров загружено"
            print(done)
            if progress:
                progress(banks_done, banks_total, done)
            return []

        print(f"\n[bank {bank_id}] ✅ Загружено: {len(all_items)} партнёров")

        save_bnb_items(bank_id, all_items)

        done = f"[bank {bank_id}] ✅ БНБ завершён: {len(all_items)} партнёров загружено"
        print(done)
        if progress:
            progress(banks_done, banks_total, done)

        return all_items

    except Exception as e:
        err = f"[bank {bank_id}] ❌ Ошибка загрузки: {e}"
        print(err)
        if progress:
            progress(banks_done, banks_total, err)
        return []
