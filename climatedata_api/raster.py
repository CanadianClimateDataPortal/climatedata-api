from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

from flask import Flask, send_file, request

import uuid

SERVER_DOMAIN = "https://climatedata.ca/"


def get_selenium_driver():
    chrome_options = Options()
    chrome_options.headless = True
    chrome_options.add_argument("--window-size=2560,1440")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage') # /dev/shm can be too small within docker

    return webdriver.Chrome('/bin/chromedriver', options=chrome_options)


def get_explore_variable_raster(climate_data_map_url, output_img_path):
    driver = get_selenium_driver()
    driver.get(climate_data_map_url)

    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.TAG_NAME, "body")))

        driver.execute_script("return document.getElementById('main-header').remove();")
        driver.execute_script("return document.getElementById('var-sliders').remove();")
        driver.execute_script("return document.getElementById('map-controls').remove();")
        driver.execute_script("return document.getElementsByClassName('page-tour')[0].remove();")

        driver.find_element(By.TAG_NAME, "body").screenshot(output_img_path)
    finally:
        driver.quit()


def get_explore_location_raster(climate_data_map_url, output_img_path):
    driver = get_selenium_driver()
    driver.get(climate_data_map_url)

    try:
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='temperature-chart']/div[2]/div[2]")))

        driver.execute_script("return document.getElementsByClassName('chart-tour-trigger')[0].remove();")
        driver.execute_script("return document.getElementsByClassName('highcharts-exporting-group')[0].remove();")

        # TODO : make sure we select the right chart index
        driver.find_elements(By.CLASS_NAME, "var-chart")[0].screenshot(output_img_path)
    finally:
        driver.quit()


def get_raster_route(path):
    request_full_path = request.full_path
    request_args_str = ""

    if len(request_full_path.split("?")) > 1:
        request_args_str = request_full_path.split("?")[1]

    server_url = SERVER_DOMAIN + path + "?" + request_args_str
    output_img_path = "/tmp/" + str(uuid.uuid4()) + ".png"

    if len(path) < 10:
        return "Please provide URL."
    
    if "/explore/variable" in server_url:
        get_explore_variable_raster(server_url, output_img_path)
    elif "/explore/location" in server_url:
        get_explore_location_raster(server_url, output_img_path)
    else:
        return "Undefined raster handler for URL."
        
    return send_file(output_img_path, mimetype='image/png')
