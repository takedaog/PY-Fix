import re

def extract_salary(salary_text, usd_to_uzs=13000, rub_to_uzs=150):
    """
    Extracts and processes salary information from a given text.
    
    Args:
        salary_text (str): The raw salary text.
        usd_to_uzs (float): Conversion rate from USD to Uzbek sum.
        rub_to_uzs (float): Conversion rate from RUB to Uzbek sum.
    
    Returns:
        int or str: The salary in Uzbek sum, or "N/A" if no valid salary is found.
    """
    # Expanded regex patterns to include UZS
    range_pattern = r"(от|from)\s*([\d\s]*)\s*(до|to)\s*([\d\s]+)\s*(so'm|сум|₽|[a-zA-Z$]+|UZS)"
    single_amount_pattern = r"([\d\s]+)\s*(so'm|сум|₽|[a-zA-Z$]+|UZS)"

    # Clean up the input text
    salary_text = salary_text.replace(",", "").strip()

    print(f"Processing text: {salary_text}")  # Debug output

    # Handle "None" in the salary text and treat it as missing value
    if "None" in salary_text:
        salary_text = salary_text.replace("None", "").strip()

    # Check for range pattern
    match = re.search(range_pattern, salary_text)
    if match:
        min_salary = match.group(2).replace(" ", "")
        max_salary = match.group(4).replace(" ", "")
        currency = match.group(5).strip().lower()
        
        # Handle missing values
        if not min_salary and max_salary:
            min_salary = max_salary
        if not max_salary and min_salary:
            max_salary = min_salary
        
        if not min_salary or not max_salary:
            return "N/A"

        try:
            min_salary = int(min_salary)
            max_salary = int(max_salary)
        except ValueError:
            return "N/A"

        median_salary = (min_salary + max_salary) // 2
        print(f"Range detected: min={min_salary}, max={max_salary}, median={median_salary}, currency={currency}")  # Debug output
    else:
        # Check for single amount pattern
        match = re.search(single_amount_pattern, salary_text)
        if match:
            salary_value = match.group(1).replace(" ", "")
            currency = match.group(2).strip().lower()
            
            if not salary_value:
                return "N/A"
            try:
                median_salary = int(salary_value)
            except ValueError:
                return "N/A"
            print(f"Single amount detected: salary={median_salary}, currency={currency}")  # Debug output
        else:
            return "N/A"

    # Convert to Uzbek sum if necessary
    if "so'm" in currency or "сум" in currency or "uzs" in currency:
        print(f"Salary is already in UZS: {median_salary}")
        return median_salary
    elif "$" in currency or "usd" in currency:
        uzs_salary = int(median_salary * usd_to_uzs)
        print(f"Converted USD to UZS: {uzs_salary}")
        return uzs_salary
    elif "rub" in currency or "₽" in currency:
        uzs_salary = int(median_salary * rub_to_uzs)
        print(f"Converted RUB to UZS: {uzs_salary}")
        return uzs_salary
    else:
        print(f"Unsupported currency: {currency}")
        return "N/A"

# Test cases
print(extract_salary("None UZS to 20800000 UZS"))  # ✅ Должно вернуть 20800000
print(extract_salary("from 800 to 2 000 $ after taxes"))  # ✅ Конвертация в UZS
print(extract_salary("from 10 000 000 to 25 000 000 so'm after taxes"))  # ✅ В суммах
print(extract_salary("2 000 $ before tax"))  # ✅ Конвертация из USD
print(extract_salary("15 000 ₽ after taxes"))  # ✅ Конвертация из RUB
print(extract_salary("до 1 000 $ до вычета налогов"))  # ✅ Конвертация из USD (только max)
print(extract_salary("от 10 000 000 до 20 000 000 so'm до вычета налогов"))  # ✅ В суммах
print(extract_salary("от 3 000 000 до 5 000 000 so'm на руки"))  # ✅ В суммах
