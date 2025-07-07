# import pandas as pd
# from Title_Identify_with_ai import identify_title
# import time
# import re

# # Read the CSV file
# def give_to_ai():
#     df = pd.read_csv(r"Data\job_data.csv")

#     # Extract 'Job Title' and 'Skills' columns as lists
#     titles = df["Job Title"].tolist()
#     skills = df["Skills"].tolist()

#     # Iterate over the list of titles
#     for i in range(len(titles)):
#         # Clean each title
#         title = titles[i].strip()  # Remove leading/trailing whitespace
#         title = re.sub(r'[^\w\s]', '', title)  # Remove all special characters (except spaces)
#         title = re.sub(r'\s+', ' ', title)  # Replace multiple spaces with a single space
#         title = title.lower()  # Optional: Convert to lowercase if case-insensitive matching is preferred
        
#         # Update the title back into the list
#         titles[i] = title

#     print(len(titles))

#     # Batch processing with specified batch size
#     batch_size = 10
#     for i in range(0, len(titles), batch_size):
#         # Slice titles and skills for the current batch
#         titles_batch = titles[i:i + batch_size]
#         skills_batch = skills[i:i + batch_size]

#         # Call the function with the current batch
#         identify_title(titles=titles_batch, skills=skills_batch)

#         # Print the batch for debugging
#         print(titles_batch)

#         # Add a delay to avoid overwhelming the system
#         time.sleep(5)

# # give_to_ai()
import pandas as pd
from Title_Identify_with_ai import identify_title
import time
import re
import os # Added for path joining

# Read the CSV file
def give_to_ai():
    # --- Use the correct raw data filename ---
    raw_csv_path = os.path.join("Data", "job_data_raw.csv") # Use os.path.join

    if not os.path.exists(raw_csv_path):
        print(f"Error in give_to_ai: Input file not found at '{raw_csv_path}'")
        return # Exit if file doesn't exist

    try:
        # --- Read using underscore column names ---
        # Check header=0 if pandas fails to detect headers correctly
        df = pd.read_csv(raw_csv_path, keep_default_na=False, encoding='utf-8')
        print(f"Read {len(df)} rows from {raw_csv_path} for AI processing.")

        # --- Use correct underscore column names ---
        if "Job_Title" not in df.columns or "Skills" not in df.columns:
             print("Error in give_to_ai: Required columns 'Job_Title' or 'Skills' not found in CSV.")
             # Log actual columns for debugging: print(f"Actual columns: {df.columns.tolist()}")
             return

        titles = df["Job_Title"].tolist() # Use underscore
        skills = df["Skills"].tolist()    # Use underscore

        # Iterate over the list of titles
        cleaned_titles = []
        for title in titles:
            # Clean each title
            title_str = str(title).strip() # Ensure string, Remove leading/trailing whitespace
            title_str = re.sub(r'[^\w\s]', '', title_str)  # Remove all special characters (except spaces)
            title_str = re.sub(r'\s+', ' ', title_str)  # Replace multiple spaces with a single space
            title_str = title_str.lower()  # Optional: Convert to lowercase
            cleaned_titles.append(title_str)

        print(f"Processing {len(cleaned_titles)} titles for AI.")

        # Batch processing with specified batch size
        batch_size = 10 # Consider making this smaller if AI times out
        for i in range(0, len(cleaned_titles), batch_size):
            # Slice titles and skills for the current batch
            titles_batch = cleaned_titles[i:i + batch_size]
            skills_batch = skills[i:i + batch_size] # Assumes skills list matches length

            # Call the function with the current batch
            print(f"\nProcessing AI batch {i//batch_size + 1}...")
            try:
                # Ensure identify_title saves results to Title.csv or handles output
                identify_title(titles=titles_batch, skills=skills_batch)
                print(f"Sent batch to AI: {titles_batch}")
            except Exception as ai_call_error:
                 print(f"ERROR calling identify_title for batch starting at index {i}: {ai_call_error}")
                 # Decide whether to continue or stop on AI error

            # Add a delay
            time.sleep(5)
        print("\nFinished AI processing.")

    except FileNotFoundError:
         print(f"Error in give_to_ai: File not found at '{raw_csv_path}'")
    except Exception as e:
         print(f"An error occurred in give_to_ai: {e}")
         import traceback
         traceback.print_exc()


# Ensure identify_title function exists and correctly writes to "Title.csv"
# Example call (if running this script directly):
# if __name__ == "__main__":
#    give_to_ai()