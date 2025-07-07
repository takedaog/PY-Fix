# --- START OF FILE Matched_data.py ---

import pandas as pd
import os

def cleaned_data_to_csv():
    raw_csv_path = os.path.join("Data", "job_data_raw.csv")
    title_csv_path = "Title.csv"
    final_csv_path = os.path.join("Data", "cleaned_job_titles_final.csv")  # ⬅ сохранение в папку Data

    if not os.path.exists(raw_csv_path):
        print(f"❌ Error: Raw data file '{raw_csv_path}' not found. Cannot perform cleaning.")
        return

    df = pd.read_csv(raw_csv_path, keep_default_na=False, na_values=['', 'N/A'], encoding='utf-8')
    print(f"\n📥 Read {len(df)} rows from {raw_csv_path} for cleaning.")

    # --- Merge AI-identified titles ---
    if os.path.exists(title_csv_path):
        try:
            df_titles = pd.read_csv(title_csv_path)
            if len(df_titles) == len(df):
                df["Job_Title_from_List"] = df_titles["Title"]
                print("✅ Successfully merged AI-identified titles from Title.csv.")
            else:
                print(f"⚠️ Warning: Row count mismatch (raw: {len(df)}, titles: {len(df_titles)}). Skipping merge.")
        except Exception as e:
            print(f"⚠️ Error reading Title.csv: {e}")
    else:
        print("⚠️ Title.csv not found. Column 'Job_Title_from_List' will remain unchanged or missing.")

    df_cleaned = df.copy()

    # List of valid AI titles
    valid_job_titles = [
        "Backend Developer", "Frontend Developer", "Full Stack Developer", "Data Analyst", "Data Engineer", "Data Scientist",
        "AI Engineer", "Android Developer", "IOS Developer", "Game Developer", "DevOps Engineer", "IT Project Manager", "Network Engineer",
        "Cybersecurity Analyst", "Cloud Architect", "QA Engineer", "UI/UX Designer", "System Administrator", "IT Support Specialist",
        "Graphic Designer"
    ]

    # 1. Filter by valid AI titles
    initial_rows = len(df_cleaned)
    df_cleaned = df_cleaned[df_cleaned['Job_Title_from_List'].isin(valid_job_titles)]
    print(f"🧹 Filtered by valid titles: {initial_rows} → {len(df_cleaned)} rows.")

    # 2. Drop rows missing critical fields
    initial_rows = len(df_cleaned)
    df_cleaned.dropna(subset=['ID', 'Job_Title', 'Company', 'Posted_date'], inplace=True)
    print(f"🧹 Dropped rows missing required info: {initial_rows} → {len(df_cleaned)} rows.")

    # 3. Remove duplicates
    initial_rows = len(df_cleaned)
    df_cleaned.drop_duplicates(subset=['Company', 'Job_Title', 'Location'], keep='first', inplace=True)
    print(f"🧹 Removed duplicates: {initial_rows} → {len(df_cleaned)} rows.")

    # 4. Add static values if missing
    df_cleaned['Country'] = "Uzbekistan"
    df_cleaned['Source'] = "hh.uz"

    # 5. Reindex columns for DB insert
    required_columns = [
        'ID', 'Posted_date', 'Job_Title_from_List', 'Job_Title', 'Company', 
        'Company_Logo_URL', 'Country', 'Location', 'Skills', 'Salary_Info', 'Source'
    ]
    final_df = df_cleaned.reindex(columns=required_columns, fill_value='N/A')

    # 6. Save final cleaned file
    os.makedirs("Data", exist_ok=True)
    final_df.to_csv(final_csv_path, index=False, encoding='utf-8')
    print(f"✅ Final cleaned data ({len(final_df)} rows) saved to '{final_csv_path}'")

# --- END OF FILE Matched_data.py ---
