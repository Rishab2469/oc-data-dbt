from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time

# Setup
options = Options()
driver = webdriver.Chrome(options=options)
driver.get("https://www.nebraska.gov/sos/corp/corpsearch.cgi")

print("⚠️ Manually solve the CAPTCHA and submit a search (e.g. 'AAA')")
input("✅ Press Enter once the first result page is fully loaded...")

def extract_table():
    rows = driver.find_elements(By.XPATH, "//table//tr")[1:]  # Skip header
    data = []
    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 3:
            data.append({
                "Name": cols[0].text.strip(),
                "Account Number": cols[1].text.strip(),
                "Status": cols[2].text.strip()
            })
    return data

all_data = []
page = 1

while True:
    print(f"📄 Scraping page {page}...")
    all_data.extend(extract_table())

    try:
        # Try to click using normalized visible text
        next_btn = driver.find_element(By.XPATH, "//a[normalize-space(text())='Next Page ►']")
        next_btn.click()
        time.sleep(2)
        page += 1
    except Exception as e:
        print(f"✅ No more pages or error: {e}")
        break

# Save results
df = pd.DataFrame(all_data)
df.to_csv("nebraska_AAA_results.csv", index=False)
print("✅ Saved to nebraska_AAA_results.csv")
driver.quit()
