# ai_processing.py
import google.generativeai as genai
import time
import config

def identify_job_titles(titles: list, skills: list, batch_size=10) -> list:
    """
    Identifies job titles using Google Gemini API in batches.
    Returns a list of identified titles corresponding to the input.
    """
    if len(titles) != len(skills):
        raise ValueError("titles и skills должны быть одной длины")

    genai.configure(api_key=config.API_KEY)
    model = genai.GenerativeModel("gemini-1.5-flash")
    all_identified_titles = []

    prompt_template = """
You are an expert job title classifier. Your task is to match the given job `Title` and `Skills` to a single, most appropriate title from the predefined list.

### Predefined Job Titles (Your only valid outputs):
{valid_titles}

### Rules:
1.  Analyze both the `Title` and `Skills` to make the best match.
2.  If a clear match is not possible from the provided information, you MUST return "unknown".
3.  The number of titles in your output MUST exactly match the number of jobs in the input.
4.  Your output MUST be a single line of comma-separated values. Do not add explanations or formatting.

### Example Input:
Title: ["Ведущий разработчик Java", "Data analyst", "Инженер по данным"]
Skills: ["Spring, SQL", "Tableau, Excel", "ETL, Airflow, Python"]

### Example Output:
Backend Developer, Data Analyst, Data Engineer

---
### New Input to Process:
Title: {titles}
Skills: {skills}

### Output:
"""

    for i in range(0, len(titles), batch_size):
        titles_batch = titles[i:i + batch_size]
        skills_batch = skills[i:i + batch_size]

        prompt = prompt_template.format(
            valid_titles=", ".join(config.VALID_JOB_TITLES),
            titles=titles_batch,
            skills=skills_batch
        )

        print(f"\n--- Sending batch {i//batch_size + 1} to AI for title identification ---")

        try:
            response = model.generate_content(prompt)

            if not response.text:
                raise ValueError("Empty AI response")

            identified = [t.strip() for t in response.text.strip().split(',')]

            if len(identified) == len(titles_batch):
                all_identified_titles.extend(identified)
                print(f"  ✅ AI response received for batch: {identified}")
            else:
                print(f"  ❌ AI response mismatch. Expected {len(titles_batch)} titles, got {len(identified)}. Filling with 'unknown'.")
                all_identified_titles.extend(['unknown'] * len(titles_batch))

        except Exception as e:
            print(f"  ❌ AI API Error for batch: {e}. Filling with 'unknown'.")
            all_identified_titles.extend(['unknown'] * len(titles_batch))

        time.sleep(5)

    return all_identified_titles
