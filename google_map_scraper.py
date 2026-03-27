from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd

# Setup driver
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 30)

driver.get("https://www.google.com/maps")

# Handle Google consent popup if it appears
try:
    consent_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[.//span[text()="Accept all"]]')))
    consent_button.click()
    time.sleep(2)
except:
    pass  # No consent popup

# Search query
search_query = "salons in Bangalore"

# Use name="q" attribute which is stable, unlike the dynamic IDs
search_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="q"]')))
search_box.send_keys(search_query)
driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Search"]').click()

time.sleep(5)

# Wait for results to load, then scroll
scrollable_div = wait.until(EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]')))

for _ in range(10):
    driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
    time.sleep(3)

# Get all listings - use role="feed" children with links
listings = driver.find_elements(By.XPATH, '//div[@role="feed"]//a[contains(@href, "maps/place")]')

print(f"Found {len(listings)} listings")

data = []

for i in range(len(listings)):
    try:
        listings = driver.find_elements(By.XPATH, '//div[@role="feed"]//a[contains(@href, "maps/place")]')
        listings[i].click()
        time.sleep(4)  # Wait for detail panel to load

        # Debug: save screenshot on first iteration
        if i == 0:
            driver.save_screenshot("/tmp/gmap_detail.png")

        # Name - use the h1 heading in the detail panel
        try:
            name = wait.until(EC.presence_of_element_located((By.XPATH, '//h1'))).text
        except:
            name = ""

        # Rating
        try:
            rating = driver.find_element(By.XPATH, '//div[@role="img" and contains(@aria-label, "star")]').get_attribute("aria-label").split(" ")[0]
        except:
            rating = ""

        # Reviews
        try:
            reviews = driver.find_element(By.XPATH, '//button[contains(@aria-label, "review")]').get_attribute("aria-label")
        except:
            reviews = ""

        # Address
        try:
            address = driver.find_element(By.XPATH, '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]').text
        except:
            try:
                address = driver.find_element(By.XPATH, '//button[@data-item-id="address"]').text
            except:
                address = ""

        # Phone
        try:
            phone = driver.find_element(By.XPATH, '//button[contains(@data-item-id, "phone")]//div[contains(@class, "fontBodyMedium")]').text
        except:
            try:
                phone = driver.find_element(By.XPATH, '//button[contains(@data-item-id, "phone")]').text
            except:
                phone = ""

        # Website
        try:
            website = driver.find_element(By.XPATH, '//a[contains(@data-item-id, "authority")]').get_attribute("href")
        except:
            website = ""

        data.append({
            "Name": name,
            "Rating": rating,
            "Reviews": reviews,
            "Address": address,
            "Phone": phone,
            "Website": website
        })

        print(f"Scraped: {name}")

    except Exception as e:
        print("Error:", e)
        continue

# Save data
df = pd.DataFrame(data)
df.to_csv("google_maps_data.csv", index=False)

driver.quit()

print("✅ Data saved to google_maps_data.csv")