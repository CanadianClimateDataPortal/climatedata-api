from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from flask import send_file, request

import uuid


def get_selenium_driver():
    chrome_options = Options()
    chrome_options.headless = True
    chrome_options.add_argument("--window-size=2560,1440")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage') # /dev/shm can be too small within docker

    return webdriver.Chrome('/bin/chromedriver', options=chrome_options)


def get_explore_variable_raster(url, output_img_path):
    """
        Raster the "explore location" chart.
        ex: curl 'http://localhost:5000/raster?url=https://climatedata.ca/explore/variable/?coords=62.51231793838694,-98.525390625,4&delta=&geo-select=&var=tx_max&var-group=temperature&mora=ann&rcp=rcp85&decade=1970s&sector= > output.png'
        :param url: URL to raster
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

        driver.find_element(By.TAG_NAME, "body").screenshot(output_img_path)
    finally:
        driver.quit()


def get_explore_location_raster(url, output_img_path):
    """
        Raster the "explore location" chart.
        ex: curl 'http://localhost:5000/raster?url=https://climatedata.ca/explore/location/?loc=EHHUN&location-select-temperature=tx_max > output.png'
        :param url: URL to raster
        :param output_img_path: output path of the raster
    """
    driver = get_selenium_driver()
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='temperature-chart']/div[2]/div[2]")))

        driver.execute_script("return document.getElementsByClassName('chart-tour-trigger')[0].remove();")
        driver.execute_script("return document.getElementsByClassName('highcharts-exporting-group')[0].remove();")

        # TODO : make sure we select the right chart index
        driver.find_elements(By.CLASS_NAME, "var-chart")[0].screenshot(output_img_path)
    finally:
        driver.quit()


def get_raster_route():
    """
        Dispatch raster action to handler functions.
        See handler functions for usage.
        :return: response containing the output image
    """
    url = request.args.get('url')
    output_img_path = "/tmp/" + str(uuid.uuid4()) + ".png"

    if url is None:
        return "Please provide a valid `url` query parameter."
    
    if "/explore/variable" in url:
        get_explore_variable_raster(url, output_img_path)
    elif "/explore/location" in url:
        get_explore_location_raster(url, output_img_path)
    else:
        return "Undefined raster handler for URL."
        
    return send_file(output_img_path, mimetype='image/png')
