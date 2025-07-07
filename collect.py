import pandas as pd
import os
import pyodbc
import traceback

def collect_into_dataframe(job_ids, company_names, job_titles, location_jobs,
                           post_dates, technical_skills, salary, company_logo_urls,
                           job="unknown"):  # Default for Job_Title_from_List
    """Collects scraped data into a DataFrame and saves to CSV."""
    num_rows = len(company_names)

    # Ensure all fields match in length, else pad with 'N/A'
    def pad_or_default(lst, name):
        if len(lst) == num_rows:
            return lst
        print(f"⚠️ Warning: Length mismatch for '{name}'. Padding with 'N/A'.")
        return (list(lst) + ['N/A'] * num_rows)[:num_rows]

    data = {
        "ID": pad_or_default(job_ids, "ID"),
        "Posted_date": pad_or_default(post_dates, "Posted_date"),
        "Job_Title_from_List": [job] * num_rows,
        "Job_Title": pad_or_default(job_titles, "Job_Title"),
        "Company": pad_or_default(company_names, "Company"),
        "Company_Logo_URL": pad_or_default(company_logo_urls, "Company_Logo_URL"),
        "Country": ["Uzbekistan"] * num_rows,
        "Location": pad_or_default(location_jobs, "Location"),
        "Skills": pad_or_default(technical_skills, "Skills"),
        "Salary_Info": pad_or_default(salary, "Salary_Info"),
        "Source": ["hh.uz"] * num_rows
    }

    df = pd.DataFrame(data)

    folder_path = 'Data'
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, 'job_data_raw.csv')
    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"✅ Raw data saved to '{file_path}'.")

    # INSERT to SQL Server — replace placeholders below
    insert_to_sql_server(
        df,
        server="localhost",           # 🔁 change to your server
        database="YourDatabaseName",  # 🔁 change to your DB
        table_name="YourTableName",   # 🔁 change to your table
        username="your_username",     # 🔁 change to your login
        password="your_password"      # 🔁 change to your password
    )

def insert_to_sql_server(df, server, database, table_name, username, password):
    """Inserts DataFrame into SQL Server table using pyodbc."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
    )

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected to SQL Server.")

        # Clean Posted_date column to valid datetime.date
        df['Posted_date'] = pd.to_datetime(df['Posted_date'], errors='coerce').dt.date

        insert_query = f"""
            INSERT INTO {table_name} (
                ID, Posted_date, Job_Title_from_List, Job_Title,
                Company, Company_Logo_URL, Country, Location,
                Skills, Salary_Info, Source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.fast_executemany = True
        rows = [tuple(row) for row in df.itertuples(index=False)]

        cursor.executemany(insert_query, rows)
        conn.commit()
        print(f"✅ Inserted {len(rows)} rows into table '{table_name}'.")

    except Exception as e:
        print("❌ SQL Server Error:")
        traceback.print_exc()
    finally:
        try:
            cursor.close()
            conn.close()
            print("🔒 SQL connection closed.")
        except:
            pass
