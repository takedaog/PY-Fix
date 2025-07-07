import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import locators as sel
import processing as proc
import config
import pandas as pd
import os

class GhhScraper:
    def __init__(self, driver, wait, limit=None):
        self.driver = driver
        self.wait = wait
        self.limit = limit
        self.results = {
            "ID": [], "Posted_date": [], "Job_Title": [], "Company": [],
            "Company_Logo_URL": [], "Location": [], "Skills": [], "Salary_Info": []
        }
        self.technical_skills_list = [
            ".NET", "SQL", "Python", "Java", "C++", "JavaScript", "React",
            "Angular", "Vue.js", "Node.js", "Docker", "Kubernetes",
            "AWS", "Azure", "GCP", "Terraform", "Git"
        ]

    def scrape(self):
        page_num = 0
        jobs_processed_count = 0

        while True:
            if self.limit is not None and jobs_processed_count >= self.limit:
                print(f"\n--- Reached scrape limit of {self.limit} jobs. ---")
                break

            paginated_url = config.BASE_URL.format(page_num=page_num)
            print(f"\n--- Navigating to page {page_num} ---")
            self.driver.get(paginated_url)

            try:
                self.wait.until(EC.presence_of_element_located((By.XPATH, sel.job_list_urls_xpath)))
                time.sleep(random.uniform(2, 4))
                job_elements = self.driver.find_elements(By.XPATH, sel.job_list_urls_xpath)

                if not job_elements:
                    print("No more job listings found. Ending scrape.")
                    break

                job_links = []
                for el in job_elements:
                    href = el.get_attribute('href')
                    if href and '/vacancy/' in href:
                        job_id = href.split('/vacancy/')[1].split('?')[0]
                        job_links.append({'url': href, 'id': job_id})

                main_window = self.driver.current_window_handle
                for job_info in job_links:
                    if self.limit is not None and jobs_processed_count >= self.limit:
                        break

                    jobs_processed_count += 1
                    print(f"\nProcessing Job #{jobs_processed_count} | ID: {job_info['id']}")

                    self.driver.execute_script("window.open(arguments[0], '_blank');", job_info['url'])
                    self.driver.switch_to.window(self.driver.window_handles[-1])

                    try:
                        self.wait.until(EC.presence_of_element_located((By.XPATH, sel.job_title_xpath)))
                        time.sleep(random.uniform(1, 2))
                        self._extract_job_details(job_info['id'])
                    except (TimeoutException, WebDriverException) as e:
                        print(f"  ❌ Error loading job detail page. Skipping. Error: {e}")
                    finally:
                        self.driver.close()
                        self.driver.switch_to.window(main_window)

                page_num += 1

            except NoSuchElementException:
                print("\n--- No 'Next' button found. Reached the end of search results. ---")
                break
            except TimeoutException:
                print(f"Timed out waiting for job listings on page {page_num}. Ending scrape.")
                break

        print(f"\n--- Scraping finished. Total jobs processed: {jobs_processed_count} ---")

        self.save_to_csv()
        return self.results

    def _extract_job_details(self, job_id):
        self.results["ID"].append(job_id)

        company_raw = self._get_text(sel.company_name_xpath)
        self.results["Company"].append(proc.transliterate_company_name(company_raw))

        job_title_raw = self._get_text(sel.job_title_xpath)
        self.results["Job_Title"].append(proc.translate_to_english(job_title_raw))

        location_date_text = self._get_text(sel.location_and_date_xpath)
        self.results["Posted_date"].append(proc.parse_posted_date(location_date_text))

        raw_location = proc.extract_location_from_text(location_date_text)
        self.results["Location"].append(proc.identify_region(raw_location))

        skills_text = self._get_text(sel.skills_xpath)
        self.results["Skills"].append(proc.extract_skills(skills_text, self.technical_skills_list))

        salary_text = self._get_text(sel.salary_info_xpath)
        self.results["Salary_Info"].append(proc.extract_salary(salary_text))

        logo_url = self._get_attribute(sel.company_logo_url_xpath, 'src')
        self.results["Company_Logo_URL"].append(logo_url)

    def _get_text(self, xpath):
        try:
            element = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            if element:
                return element.text.strip()
        except:
            pass
        return "N/A"

    def _get_attribute(self, xpath, attribute):
        try:
            element = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            if element:
                return element.get_attribute(attribute)
        except:
            pass
        return "N/A"

    def save_to_csv(self):
        print("\n--- Saving scraped data to CSV ---")
        df = pd.DataFrame(self.results)
        folder = 'Data'
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, 'job_data_raw.csv')
        df.to_csv(path, index=False, encoding='utf-8')
        print(f"✅ Saved {len(df)} rows to '{path}'")

    def get_all_data(self):
        return self.results
