# processing.py
import re
import locale
from datetime import datetime
from deep_translator import GoogleTranslator
from transliterate import translit

# Установим локаль, если нужно обрабатывать русские даты (зависит от ОС)
try:
    locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
except locale.Error:
    pass  # Windows может не поддерживать ru_RU, обрабатываем через альтернативу

# --- 1. Company Name Transliteration ---
def transliterate_company_name(company_name: str) -> str:
    suffixes = ["ООО", "АО", "ИП", "ЗАО", "ПАО", "ОАО"]
    for suffix in suffixes:
        company_name = company_name.replace(suffix, "").strip()
    try:
        transliterated_name = translit(company_name, 'ru', reversed=True)
    except Exception:
        transliterated_name = company_name
    return re.sub(r'\s+', ' ', transliterated_name).strip()

# --- 2. Date Parsing ---
def parse_posted_date(raw_date: str) -> str:
    try:
        raw_date = raw_date.strip()
        if ',' in raw_date:
            date_obj = datetime.strptime(raw_date, "%B %d, %Y")
        else:
            # Попробуем английский -> если не получится — русские месяцы
            try:
                date_obj = datetime.strptime(raw_date, "%d %B %Y")
            except:
                # Пример: "12 июля 2024"
                month_map = {
                    'января': 'January', 'февраля': 'February', 'марта': 'March', 'апреля': 'April',
                    'мая': 'May', 'июня': 'June', 'июля': 'July', 'августа': 'August',
                    'сентября': 'September', 'октября': 'October', 'ноября': 'November', 'декабря': 'December'
                }
                for ru, en in month_map.items():
                    if ru in raw_date.lower():
                        raw_date = raw_date.lower().replace(ru, en)
                        break
                date_obj = datetime.strptime(raw_date, "%d %B %Y")
        return date_obj.date().isoformat()
    except Exception:
        return None

# --- 3. Skills Extraction ---
def extract_skills(text: str, skill_list: list) -> list:
    found_skills = set()
    if not text:
        return []
    text_lower = text.lower()
    for skill in skill_list:
        if skill.lower() in text_lower:
            found_skills.add(skill)
    return sorted(found_skills)

# --- 4. Location Extraction ---
def extract_location_from_text(text: str) -> str:
    if not text:
        return "N/A"
    location_pattern = r"\b(?:в|in|da)\s+([a-zA-Zа-яА-ЯёЁўқғҳʼ\- ]+(?:,?\s?[a-zA-Zа-яА-ЯёЁўқғҳʼ\- ]+)?)\b"
    match = re.search(location_pattern, text)
    if match:
        return match.group(1).strip()
    return "N/A"

# --- 5. Salary Extraction ---
def extract_salary(salary_text: str, usd_to_uzs=13000, rub_to_uzs=150) -> str:
    if not salary_text:
        return "N/A"

    salary_text_clean = salary_text.replace(",", "").replace(" ", "").lower()

    try:
        # Диапазон: от ... до ...
        range_match = re.search(r'(?:from|от)(\d+)(?:to|до)(\d+)', salary_text_clean)
        if range_match:
            min_salary = int(range_match.group(1))
            max_salary = int(range_match.group(2))
            median_salary = (min_salary + max_salary) // 2
        else:
            # Одиночная сумма
            single_match = re.search(r'(\d+)', salary_text_clean)
            if not single_match:
                return "N/A"
            median_salary = int(single_match.group(1))

        # Определение валюты
        if "$" in salary_text or "usd" in salary_text.lower():
            return str(int(median_salary * usd_to_uzs))
        if "₽" in salary_text or "rub" in salary_text.lower():
            return str(int(median_salary * rub_to_uzs))
        if "so'm" in salary_text.lower() or "сум" in salary_text.lower() or "uzs" in salary_text.lower():
            return str(median_salary)

        return "N/A"
    except:
        return "N/A"

# --- 6. Text Translation ---
def translate_to_english(text: str) -> str:
    try:
        cleaned_text = text.strip()
        if not cleaned_text:
            return ""
        # Ограничим длину для устойчивости
        if len(cleaned_text) > 1000:
            cleaned_text = cleaned_text[:1000]
        translated = GoogleTranslator(source='auto', target='en').translate(cleaned_text)
        return translated
    except Exception:
        return text  # Возвращаем оригинал, если не удалось перевести
