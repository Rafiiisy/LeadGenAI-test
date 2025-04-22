import os
import shutil
import logging
import pandas as pd
import random
import time
from dotenv import load_dotenv
from tqdm import tqdm
from .utils.chromeUtils import get_chrome_driver, save_chrome_info
from .scraping.login import login_to_linkedin
from .scraping.scraper import scrape_linkedin
from .utils.fileUtils import read_csv
from .utils.proxyUtils import generate_smartproxy_url

# === Environment and Configs ===
load_dotenv()
CSV_INPUT = "input/sample_data1.csv"
CSV_OUTPUT_BASE = "output/linkedin_scraped_results"
USERNAME = os.getenv("LINKEDIN_USERNAME") or "rithik2112004@gmail.com"
PASSWORD = os.getenv("LINKEDIN_PASSWORD") or "123Testing90."
BATCH_SIZE = 10
WAIT_BETWEEN_BATCHES = (10, 20)
CLIENT_ID = "client01"  # Replace with dynamic ID in multi-user setup

# === Setup Logging ===
def get_next_log_filename(log_dir="log", base_name="log"):
    os.makedirs(log_dir, exist_ok=True)
    i = 1
    while os.path.exists(f"{log_dir}/{base_name}_{i}.log"):
        i += 1
    return f"{log_dir}/{base_name}_{i}.log"

log_file = get_next_log_filename()
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_next_output_filename(base):
    i = 3
    while os.path.exists(f"{base}{i}.csv"):
        i += 1
    return f"{base}{i}.csv"

CSV_OUTPUT = get_next_output_filename(CSV_OUTPUT_BASE)


def run_batch(
    batch_df: pd.DataFrame,
    batch_index: int,
    total_batches: int,
    global_progress: list[dict],
    global_bar,
    output_path: str | None,
    li_at: str
) -> list[dict]:
    results: list[dict] = []
    driver = None

    try:
        driver = get_chrome_driver(li_at=li_at, headless=True)
        driver.get("https://www.linkedin.com/feed")
        if "login" in driver.current_url.lower():
            logging.error("❌ Cookie login failed; aborting batch.")
            return results
        logging.info("✅ Authenticated; scraping %d companies", len(batch_df))

        with tqdm(total=len(batch_df), desc=f"Batch {batch_index+1}/{total_batches}", position=1, leave=False) as batch_bar:
            for _, row in batch_df.iterrows():
                company = row.get("Company", "UNKNOWN")
                logging.info(f"🔍 Scraping company: {company}")
                slug = re.sub(r"[^a-z0-9-]", "", company.lower().replace(" ", "-"))
                about_url = f"https://www.linkedin.com/company/{slug}/about/"

                driver.get(about_url)
                time.sleep(random.uniform(1.5, 2.5))

                path = urlparse(driver.current_url).path.lower()
                if path.startswith(("/login", "/signup")):
                    logging.error("🚫 Redirected to %s during %s, cookie expired; aborting batch", path, company)
                    results.append({"Business Name": company, "Error": f"Redirected to {path}"})
                    break

                try:
                    result = scrape_linkedin(
                        driver,
                        company,
                        expected_city=row.get("City"),
                        expected_state=row.get("State"),
                        expected_website=row.get("Website")
                    )
                    result["Business Name"] = company
                    results.append(result)
                    logging.info("✅ Scraped: %s", company)
                except Exception as exc:
                    logging.error("❌ Error scraping %s: %s", company, exc, exc_info=True)
                    error_result = {"Business Name": company, "Error": str(exc)}
                    results.append(error_result)

                main_h = driver.current_window_handle
                for h in driver.window_handles:
                    if h != main_h:
                        driver.switch_to.window(h)
                        driver.close()
                driver.switch_to.window(main_h)

                delay = random.uniform(5, 10)
                logging.debug("🕒 Sleeping %.2f seconds before next company...", delay)
                time.sleep(delay)

                if "Error" in results[-1] and "429" in results[-1]["Error"]:
                    sleep_min = random.uniform(3, 4)
                    logging.warning("⚠️ 429 detected; sleeping %.1f min", sleep_min)
                    time.sleep(sleep_min * 60)

                global_bar.update(1)
                batch_bar.update(1)

                # Save CSV every 5 rows scraped
                if output_path and len(results) % 5 == 0:
                    pd.DataFrame(results).to_csv(output_path, index=False)
                    logging.info("💾 Auto-saved %d rows → %s", len(results), output_path)

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                logging.debug("🛑 Driver quit raised during cleanup.")

    return results


def main():
    if not os.path.exists(CSV_INPUT):
        logging.error(f"CSV file not found: {CSV_INPUT}")
        return

    df = read_csv(CSV_INPUT)
    if df.empty or "Company" not in df.columns:
        logging.error("CSV is empty or missing 'Company' column.")
        return

    all_results = run_batches(df, client_id=CLIENT_ID)

    if all_results:
        df_results = pd.DataFrame(all_results)

        if "Business Name" in df_results.columns:
            cols = ["Business Name"] + [col for col in df_results.columns if col != "Business Name"]
            df_results = df_results[cols]

        df_results.to_csv(CSV_OUTPUT, index=False)
        logging.info(f"💾 Final save: {len(all_results)} rows → {CSV_OUTPUT}")

    logging.info("✅ Finished scraping.")


if __name__ == "__main__":
    main()