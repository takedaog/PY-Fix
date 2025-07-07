# main.py
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

# Import from our refactored modules
import config
from scraper import GhhScraper
import ai_processing
import database

def clean_and_prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the DataFrame after AI processing."""
    print("\n--- Starting final data cleaning ---")
    
    initial_rows = len(df)
    df_cleaned = df[df['Job_Title_from_List'].isin(config.VALID_JOB_TITLES) & (df['Job_Title_from_List'] != 'unknown')]
    print(f"Filtered by valid AI titles: {initial_rows} -> {len(df_cleaned)} rows.")
    
    initial_rows = len(df_cleaned)
    df_cleaned = df_cleaned.dropna(subset=['ID', 'Job_Title', 'Company', 'Posted_date'])
    print(f"Dropped rows with missing critical info: {initial_rows} -> {len(df_cleaned)} rows.")
    
    initial_rows = len(df_cleaned)
    df_cleaned = df_cleaned.drop_duplicates(subset=['Company', 'Job_Title', 'Location'], keep='first')
    print(f"Dropped duplicate listings: {initial_rows} -> {len(df_cleaned)} rows.")

    db_columns = [
        'ID', 'Posted_date', 'Job_Title_from_List', 'Job_Title', 'Company', 
        'Company_Logo_URL', 'Country', 'Location', 'Skills', 'Salary_Info', 'Source'
    ]
    final_df = df_cleaned.reindex(columns=db_columns, fill_value='N/A')

    return final_df

def main():
    """Main function to orchestrate the scraping and data processing pipeline."""
    print("--- Starting Job Scraper ---")
    if config.SCRAPE_LIMIT:
        print(f"⚠️ Running in test mode. Scrape limit is set to {config.SCRAPE_LIMIT} jobs.")

    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 10)

    try:
        # --- 2. SCRAPE DATA ---
        scraper = GhhScraper(driver, wait, limit=config.SCRAPE_LIMIT)
        scraped_data = scraper.scrape()

        if not scraped_data or 'ID' not in scraped_data or not scraped_data['ID']:
            print("Scraping returned no data. Exiting.")
            return

        df_raw = pd.DataFrame(scraped_data)

        # Save raw data
        data_folder = 'Data'
        os.makedirs(data_folder, exist_ok=True)
        raw_file_path = os.path.join(data_folder, 'job_data_raw.csv')
        df_raw.to_csv(raw_file_path, index=False, encoding='utf-8')
        print(f"\nRaw data with {len(df_raw)} rows saved to '{raw_file_path}'")

        # --- 3. AI PROCESSING ---
        titles_to_identify = df_raw['Job_Title'].tolist()
        skills_to_identify = df_raw['Skills'].tolist()

        identified_titles = ai_processing.identify_job_titles(titles_to_identify, skills_to_identify)
        if len(identified_titles) != len(df_raw):
            print("❌ AI returned mismatched title count. Exiting.")
            return

        df_raw['Job_Title_from_List'] = identified_titles
        df_raw['Country'] = "Uzbekistan"
        df_raw['Source'] = "hh.uz"

        # --- 4. CLEAN AND SAVE FINAL DATA ---
        df_cleaned = clean_and_prepare_data(df_raw)

        cleaned_file_path = os.path.join(data_folder, 'job_data_cleaned.csv')
        df_cleaned.to_csv(cleaned_file_path, index=False, encoding='utf-8')
        print(f"✅ Cleaned data with {len(df_cleaned)} rows saved to '{cleaned_file_path}'")

        # --- 5. PUSH TO DATABASE ---
        database.insert_to_sql(df_cleaned, config.DB_CONFIG)

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()
        print("\n--- Process complete. Browser closed. ---")

if __name__ == "__main__":
    main()
