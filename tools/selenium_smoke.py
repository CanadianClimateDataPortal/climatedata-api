"""Open the local API in Chrome and verify its landing page with Selenium."""

import argparse
import sys

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:5000/", help="API URL to open")
    parser.add_argument("--headless", action="store_true", help="Run Chrome without a visible window")
    args = parser.parse_args()

    options = Options()
    # The local HTTPS task uses a self-signed certificate.
    options.accept_insecure_certs = True
    if args.headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1280,900")

    # Selenium Manager finds/downloads a compatible driver; no system chromedriver
    # installation is needed.
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(args.url)
        WebDriverWait(driver, 10).until(
            lambda browser: browser.find_element(By.TAG_NAME, "h1").text == "ClimateData API"
        )
        print(f"Selenium opened {driver.current_url}: {driver.title}")
        if not args.headless and sys.stdin.isatty():
            input("Browser is open. Press Enter to close it... ")
        elif not args.headless:
            print("Browser check completed; closing because stdin is not interactive.")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
