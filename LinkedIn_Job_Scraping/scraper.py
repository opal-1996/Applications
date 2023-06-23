import time
import pandas as pd
import os
import json

from selenium import webdriver
from selenium.webdriver.common.keys import Keys 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import ElementClickInterceptedException

chromedriver = "LOCAL PATH TO CHROME DRIVER"

options = webdriver.ChromeOptions()
options.add_argument("start-maximized") #maximize chrome window
options.add_argument('--disable-blink-features=AutomationControlled') #prevent detection of automation by websites
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)   

#initiate driver
driver = webdriver.Chrome(options=options, executable_path=chromedriver)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                                                     'AppleWebKit/537.36 (KHTML, like Gecko) '
                                                                     'Chrome/85.0.4183.102 Safari/537.36'})

# log into Linkedin
login_url = "https://www.linkedin.com/home"
driver.get(login_url)

USERNAME = "YOUR USERNAME FOR LINKEDIN (EMAIL ADDRESS)"
PASSWORD = "YOUR PASSWORD FOR LINKEDIN"

time.sleep(3)
email = driver.find_element(By.XPATH, '//*[@id="session_key"]')
email.send_keys(USERNAME)

time.sleep(5)
password = driver.find_element(By.XPATH, '/html/body/main/section[1]/div/div/form[1]/div[1]/div[2]/div/div/input')
password.send_keys(PASSWORD)

#submit form
time.sleep(5)
driver.find_element(By.XPATH, '/html/body/main/section[1]/div/div/form[1]/div[2]/button').click()

# type in keywords: Data Scientist
search_box = driver.find_element(By.XPATH, '/html/body/div[5]/header/div/div/div/div[1]/input')
search_box.send_keys('"Data Scientist"')
search_box.send_keys(Keys.ENTER)
# search_box.clear()

driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div[2]/section/div/nav/div/ul/li[1]/button').click()
time.sleep(5)

driver.find_element(By.XPATH, "//button[contains(.,'Date posted')]").click()
time.sleep(3)
driver.find_element(By.XPATH, '//*[@id="artdeco-hoverable-artdeco-gen-57"]/div[1]/div/form/fieldset/div[1]/ul/li[2]/label').click()
time.sleep(3)
driver.find_element(By.XPATH, "//button[contains(.,'Show')]").click()

# scrape job links
job_lists = []
numPages = int(driver.find_elements(By.CSS_SELECTOR, 'ul[class="artdeco-pagination__pages artdeco-pagination__pages--number"] li')[-1].text) # number of pages

for page in range(2, numPages+2):
    for element in driver.find_elements(By.CSS_SELECTOR, "ul[class='scaffold-layout__list-container'] li.ember-view"):
        job_url = element.find_element(By.CSS_SELECTOR, "div[class='full-width artdeco-entity-lockup__title ember-view'] a")
        job_lists.append(job_url.get_attribute("href"))
        job_url.send_keys(Keys.PAGE_DOWN)

    if page <=8:
        driver.find_element(By.XPATH, "//button[contains(.,'{}')]".format(page)).click()
    elif page==9:
        driver.find_element(By.CSS_SELECTOR, "li[class='artdeco-pagination__indicator artdeco-pagination__indicator--number ember-view'] button[aria-label='Page 9']").click()
    else:
        driver.find_element(By.XPATH, "//button[contains(.,'{}')]".format(page-1)).click()
    
    time.sleep(3)

# save job urls to pandas dataframe
URLS = pd.DataFrame(columns=["job_url"])
URLS["job_url"] =  job_lists

# iterate through the job urls and fetch more information from its detail page
data = pd.DataFrame(columns=["job_title", "company", "location", "numApplicants", "skills", "salary_employment_seniority", "scale_industry", "details"])

for idx, row in URLS.iterrows():
    url = row["job_url"]
    driver.get(url)

    time.sleep(3)

    try:
        # click button "see more"
        driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div/div[1]/div[1]/div/div[2]/footer/button').click()

        # fetch more information from the detail page
        job_title = driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div/div[1]/div[1]/div/div[1]/div/div/div[1]/div[1]/h1').text
        company = driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div/div[1]/div[1]/div/div[1]/div/div/div[1]/div[2]/span[1]/span[1]/a').text
        location = driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div/div[1]/div[1]/div/div[1]/div/div/div[1]/div[2]/span[1]/span[2]').text
        try:
            numApplicants = driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div/div[1]/div[1]/div/div[1]/div/div/div[1]/div[2]/span[2]/span[2]/span').text
        except:
            numApplicants = "Over 200 applicants"
        
        try:
            driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div/div[1]/div[1]/div/div[1]/div/div/div[1]/div[3]/ul/div/button').click()
            skills = driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div[3]/button').text
            driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div[3]/button').click()
        except:
            skills = None

        salary_employment_seniority = driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div/div[1]/div[1]/div/div[1]/div/div/div[1]/div[3]/ul/li[1]/span').text
        scale_industry = driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div/div[1]/div[1]/div/div[1]/div/div/div[1]/div[3]/ul/li[2]/span').text
        details = driver.find_element(By.XPATH, '//*[@id="job-details"]').text

        row = {
            "job_title": job_title,
            "company": company,
            "location": location,
            "numApplicants": numApplicants,
            "skills": skills,
            "salary_employment_seniority": salary_employment_seniority,
            "scale_industry": scale_industry,
            "details": details
        }

        data = data.append(row, ignore_index=True)

        time.sleep(3)
        
    except:
        continue