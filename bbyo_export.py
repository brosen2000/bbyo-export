#!/usr/bin/env python3
import os
import time
import glob
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def send_ntfy(message):
    """
    Send a notification via ntfy.sh.
    """
    headers = {"Authorization": f"Bearer {NTFY_TOKEN}"}
    resp = requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=message.encode("utf-8"), headers=headers)
    if resp.status_code >= 400:
        print(f"❌ ntfy.sh error {resp.status_code}: {resp.text}")
    else:
        print("✅ Notification sent via ntfy.sh")

### ─── CONFIG ─────────────────────────────────────────────────────────────
BBYO_USER     = os.getenv("BBYO_USER")
BBYO_PASS     = os.getenv("BBYO_PASS")
NTFY_TOKEN    = os.getenv("NTFY_TOKEN")
if not all([BBYO_USER, BBYO_PASS, NTFY_TOKEN]):
    print("❌ Missing environment variables (BBYO_USER, BBYO_PASS, or NTFY_TOKEN)")
    exit(1)
NTFY_TOPIC    = "BBYO"
DOWNLOAD_DIR  = "./downloads"
SPREADSHEET_ID = "1F-ZyR0GEB7Et1c70fkPB1XCGw5nmA9m7iTGfe_UbQbw"
SHEET_NAME     = "Sheet1"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "./service_account.json"
KEY_COL       = "Full Name"
### ────────────────────────────────────────────────────────────────────────

def get_driver():
    if not os.path.exists(DOWNLOAD_DIR):
        try:
            os.makedirs(DOWNLOAD_DIR, mode=0o755)
            print(f"Created directory: {DOWNLOAD_DIR}")
        except Exception as e:
            print(f"❌ Failed to create directory {DOWNLOAD_DIR}: {e}")
            raise
    elif not os.access(DOWNLOAD_DIR, os.W_OK):
        print(f"❌ Directory {DOWNLOAD_DIR} is not writable")
        raise PermissionError(f"Directory {DOWNLOAD_DIR} is not writable")
    else:
        print(f"Directory {DOWNLOAD_DIR} already exists and is writable")

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--remote-debugging-port=9222")
    opts.add_argument("--disable-setuid-sandbox")
    prefs = {
        "download.default_directory": os.path.abspath(DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    opts.add_experimental_option("prefs", prefs)
    try:
        driver = webdriver.Chrome(options=opts)
        print("✅ ChromeDriver initialized successfully")
        return driver
    except Exception as e:
        print(f"❌ Failed to initialize ChromeDriver: {e}")
        raise

def wait_for_download(dirpath, timeout=60):
    end_time = time.time() + timeout
    while time.time() < end_time:
        files = glob.glob(os.path.join(dirpath, "*.xlsx"))
        if files:
            if not any(f.endswith(".crdownload") for f in files):
                return max(files, key=os.path.getctime)
        time.sleep(0.5)
    raise TimeoutError("Timed out waiting for download")

def fetch_export():
    driver = get_driver()
    try:
        print("Navigating to login page...")
        driver.get("https://bbyo.my.site.com/s/login/")
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='text' or @type='email']")))
        username_field = driver.find_element(By.XPATH, "//input[@type='text' or @type='email']")
        username_field.clear()
        username_field.send_keys(BBYO_USER)
        wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='password']")))
        password_field = driver.find_element(By.XPATH, "//input[@type='password']")
        password_field.clear()
        password_field.send_keys(BBYO_PASS)
        password_field.send_keys(Keys.RETURN)
        time.sleep(5)
        WebDriverWait(driver, 20).until(EC.url_contains("/s"))
        print("Navigated to community page")
        driver.get("https://bbyo.my.site.com/s/my-chapter")
        try:
            export_btn = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "//button[@data-element-id='button' and .//span[text()='Export']]"
                ))
            )
        except Exception as e:
            print("❌ Failed to locate the initial Export button. Current URL:", driver.current_url)
            snippet = driver.page_source[:500]
            print("Page source snippet:", snippet)
            raise
        for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*")):
            os.remove(f)
        export_btn.click()
        time.sleep(2)
        try:
            confirm_btn = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "//button[contains(@class,'slds-button_brand') and normalize-space(text())='Export']"
                ))
            )
            driver.execute_script("arguments[0].click();", confirm_btn)
        except Exception as e:
            print("❌ Failed to locate the confirm Export button. URL:", driver.current_url)
            snippet = driver.page_source[:500]
            print("Page source snippet:", snippet)
            raise
        time.sleep(2)
        path = wait_for_download(DOWNLOAD_DIR, timeout=60)
        print(f"Downloaded file: {path}")
        return path
    finally:
        driver.quit()

def read_sheet():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    rv = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}"
    ).execute()
    data = rv.get("values", [])
    if not data or len(data) < 1:
        print("No data or header in Google Sheet.")
        return pd.DataFrame(), service
    header, rows = data[0], data[1:] if len(data) > 1 else []
    header_len = len(header)
    cleaned_rows = []
    for i, row in enumerate(rows):
        if len(row) != header_len:
            print(f"Warning: Row {i+2} has {len(row)} columns, expected {header_len}. Adjusting...")
            if len(row) < header_len:
                row.extend([""] * (header_len - len(row)))
            elif len(row) > header_len:
                row = row[:header_len]
        cleaned_rows.append(row)
    try:
        return pd.DataFrame(cleaned_rows, columns=header), service
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        print("Header:", header)
        print("Sample row:", cleaned_rows[0] if cleaned_rows else "No rows")
        raise

def append_rows(service, rows):
    body = {"values": rows}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_NAME,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

def main():
    xlsx_path = fetch_export()
    try:
        file_size = os.path.getsize(xlsx_path)
    except Exception as e:
        file_size = None
    print(f"Downloaded file path: {xlsx_path}, size: {file_size} bytes")
    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
        print("Workbook sheet names:", xls.sheet_names)
    except Exception as e:
        print(f"Error loading workbook: {e}")
    prev_size = -1
    for _ in range(10):
        try:
            size = os.path.getsize(xlsx_path)
        except Exception:
            size = -1
        if size == prev_size:
            break
        prev_size = size
        time.sleep(1)
    try:
        if 'xls' in locals():
            first_sheet = xls.sheet_names[0]
            df_new = pd.read_excel(xlsx_path, sheet_name=first_sheet, engine="openpyxl")
        else:
            df_new = pd.read_excel(xlsx_path, engine="openpyxl")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        df_new = pd.DataFrame()

    df_sheet, service = read_sheet()
    print("Sheet columns:", df_sheet.columns.tolist())
    print(f"Sheet has {len(df_sheet)} rows")
    print("Export columns:", df_new.columns.tolist())

    if df_sheet.empty:
        print("Sheet is empty, will append all rows.")
        new = df_new.copy()
    else:
        if KEY_COL not in df_sheet.columns:
            print(f"❌ '{KEY_COL}' column missing in sheet.")
            return
        if KEY_COL not in df_new.columns:
            print(f"❌ '{KEY_COL}' column missing in export.")
            return
        new = df_new[~df_new[KEY_COL].isin(df_sheet[KEY_COL])]

    if new.empty:
        print("✅ No new rows to append.")
        return

    rows = new.astype(str).values.tolist()
    append_rows(service, rows)
    print(f"✅ Appended {len(rows)} new rows.")
    details = []
    for _, row in new.iterrows():
        details.append(f"{row['Full Name']} | {row['Grad Year']} | {row['AZA or BBG']} | {row['Chapter Name']}")
    message = f"Appended {len(rows)} new BBYO member rows:\n" + "\n".join(details)
    send_ntfy(message)

if __name__ == "__main__":
    main()
