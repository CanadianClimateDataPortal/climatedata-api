import base64
import functools

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time

from flask import send_file, request
from flask import current_app as app
from werkzeug.exceptions import BadRequest

import numpy as np

import uuid
import validators

from urllib.parse import urlparse


def get_selenium_driver():
    chrome_options = Options()
    chrome_options.headless = True
    chrome_options.add_argument("--window-size=2560,1440")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')  # /dev/shm can be too small within docker

    return webdriver.Chrome('/bin/chromedriver', options=chrome_options)


def get_explore_variable_raster(url, output_img_path):
    """
        Raster the "explore location" chart.
        Note: URL is encoded using tools/encoder.html

        ex: To raster the following URL: https://climatedata.crim.ca/explore/variable/?coords=62.5325943454858,-98.48144531250001,4&delta=&dataset=cmip6&geo-select=&var=ice_days&var-group=other&mora=ann&rcp=ssp585&decade=1970s&sector=
            curl 'http://localhost:5000/raster?url=aHR0cHM6Ly9jbGltYXRlZGF0YS5jcmltLmNhL2V4cGxvcmUvdmFyaWFibGUvP2Nvb3Jkcz02Mi41MzI1OTQzNDU0ODU4LC05OC40ODE0NDUzMTI1MDAwMSw0JmRlbHRhPSZkYXRhc2V0PWNtaXA2Jmdlby1zZWxlY3Q9JnZhcj1pY2VfZGF5cyZ2YXItZ3JvdXA9b3RoZXImbW9yYT1hbm4mcmNwPXNzcDU4NSZkZWNhZGU9MTk3MHMmc2VjdG9yPXw3NDIyMTkyNjU%3D' > output.png
        :param url: encoded URL to raster
        :param output_img_path: output path of the raster
    """
    driver = get_selenium_driver()
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.TAG_NAME, "body")))

        driver.execute_script("return document.getElementById('main-header').remove();")
        driver.execute_script("return document.getElementById('var-sliders').remove();")
        driver.execute_script("return document.getElementById('map-controls').remove();")
        driver.execute_script("return document.getElementsByClassName('page-tour')[0].remove();")
        time.sleep(1)
        driver.find_element(By.TAG_NAME, "body").screenshot(output_img_path)
    finally:
        driver.quit()


def get_explore_location_raster(url, output_img_path):
    """
        Raster the "explore location" chart.
        Note: URL is encoded using tools/encoder.html

        ex: To raster the following URL: https://climatedata.ca/explore/location/?loc=EHHUN&location-select-temperature=tx_max
            curl 'http://localhost:5000/raster?url=aHR0cHM6Ly9jbGltYXRlZGF0YS5jYS9leHBsb3JlL2xvY2F0aW9uLz9sb2M9RUhIVU4mbG9jYXRpb24tc2VsZWN0LXRlbXBlcmF0dXJlPXR4X21heHwyMDYxMTkzNjY5'  > output.png
        :param url: URL to raster
        :param output_img_path: output path of the raster
    """
    driver = get_selenium_driver()
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='temperature-chart']/div[2]/div[2]")))

        driver.execute_script("return document.getElementsByClassName('chart-tour-trigger')[0].remove();")
        driver.execute_script("return document.getElementsByClassName('highcharts-exporting-group')[0].remove();")
        time.sleep(1)
        driver.find_elements(By.CLASS_NAME, "var-chart")[0].screenshot(output_img_path)
    finally:
        driver.quit()


def calculate_hash(s):
    """
        Calculate a simple hash from a string.
        Same formula as in Java lang String.java: s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1], in 32 bits signed arithmetic
        :param: s: String to hash
        :return: integer value of hash
    """
    def _calculate_hash(a, b):
        a = ((a << np.int32(5)) - a) + np.int32(ord(b))
        return a
    with np.errstate(over='ignore'):
        return functools.reduce(_calculate_hash, s, np.int32(0))


def decode_and_validate_url(encoded):
    """
        Decode and validate encoded URL (format: "URL|hashcode")
        :param: encoded: Encoded URL
        :return: decoded URL
    """
    try:
        url, request_hash = base64.b64decode(encoded).decode('utf-8').split("|")
        request_hash = int(request_hash)
        computed_hash = calculate_hash(url + app.config['SALT'])
    except Exception as e:
        raise BadRequest("Invalid encoded url")

    if request_hash != computed_hash:
        raise BadRequest("Invalid encoded hash")

    return url


def get_raster_route():
    """
        Dispatch raster action to handler functions.
        See handler functions for usage.
        :return: response containing the output image
    """
    encoded = request.args.get('url')
    url = decode_and_validate_url(encoded)
    parsed_url = urlparse(url)

    # make sure the URL param is trustworthy
    if not validators.url(url):
        return "Please provide a valid `url` query parameter.", 400
    if parsed_url.netloc not in app.config['ALLOWED_DOMAINS']:
        return "Please provide a trusted `url` query parameter.", 400

    output_img_path = "/tmp/" + str(uuid.uuid4()) + ".png"
    
    if "/explore/variable" in url:
        get_explore_variable_raster(url, output_img_path)
        page_name = "variable"
    elif "/explore/location" in url:
        get_explore_location_raster(url, output_img_path)
        page_name = "location"
    else:
        return "Undefined raster handler for URL.", 400
        
    return send_file(output_img_path, mimetype='image/png', as_attachment=True, download_name=f'climatedata.ca - {page_name} - {parsed_url.query}.png')
