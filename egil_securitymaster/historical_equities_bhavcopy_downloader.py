from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from datetime import datetime
from datetime import timedelta
import time
import os
from selenium.common.exceptions import TimeoutException,WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

class Daily_Report :

    def __init__(self,from_date,to_date) :
        
        self.from_date = from_date
        self.to_date=to_date

    def download(self,link) :
        c="C:\Users\Anusha\Downloads\chromedriver"        #enter the path for ChromeDriver
        os.environ["webdriver.chrome.driver"] = c
        driver=webdriver.Chrome(c)    
        driver.get(link)            #to get the website required
        
        select_report='//*[@id="h_filetype"]/option[2]'
        date='//*[@id="date"]'
        get_data='//*[@id="wrapper_btm"]/div[1]/div[4]/div/div[1]/div/div[4]/input[3]'
                               #to get the xpath

        select_report_Element =WebDriverWait(driver,30).until(lambda driver: driver.find_element_by_xpath(select_report))
        date_Element =WebDriverWait(driver,30).until(lambda driver: driver.find_element_by_xpath(date))
        date1=datetime.strptime(self.to_date, '%d-%m-%Y')
        date2=datetime.strptime(self.from_date, '%d-%m-%Y')

        while (date2 <= date1) :
            f_date=to_date=date2.strftime('%d-%m-%Y')
            date3=date2.date()
            if(date3.weekday() in [5,6]) :
                date2=date2 + timedelta(1)
                continue

            try :
                select_report_Element =WebDriverWait(driver,30).until(EC.presence_of_element_located((By.XPATH, select_report)))
                date_Element =WebDriverWait(driver,30).until(EC.presence_of_element_located((By.XPATH, date)))
                get_data_Element =WebDriverWait(driver,30).until(EC.presence_of_element_located((By.XPATH, get_data)))


                select_report_Element.click()
                date_Element.clear()
                date_Element.send_keys(f_date)
                date_Element.send_keys(Keys.ENTER)
                t=1
                time.sleep(4)
                get_data_Element =WebDriverWait(driver,30).until(EC.presence_of_element_located((By.XPATH, get_data)))
                get_data_Element.click()
                try:

                    link_for_file='//*[@id="spanDisplayBox"]/table/tbody/tr/td/a[1]'
                        
                    link_for_file_Element =WebDriverWait(driver,5).until(lambda driver: driver.find_element_by_xpath(link_for_file))

                    link_for_file_Element.click()
                    time .sleep(2)

                except TimeoutException:
                    date2=date2 + timedelta(1)
                    continue

                
            except :
                try :
                    select_report_Element =WebDriverWait(driver,30).until(EC.presence_of_element_located((By.XPATH, select_report)))
                    date_Element =WebDriverWait(driver,30).until(EC.presence_of_element_located((By.XPATH, date)))
                    get_data_Element =WebDriverWait(driver,30).until(EC.presence_of_element_located((By.XPATH, get_data)))

                    select_report_Element.click()
                    time.sleep(1)                    
                    date_Element.clear()
                    date_Element.send_keys(f_date)
                    date_Element.send_keys(Keys.ENTER)
                    t=1
                    time.sleep(t)
                except WebDriverException :
                    driver.get(link)
                        
                get_data_Element.click()
                try:

                    link_for_file='//*[@id="spanDisplayBox"]/table/tbody/tr/td/a[1]'
                        
                    link_for_file_Element =WebDriverWait(driver,5).until(lambda driver: driver.find_element_by_xpath(link_for_file))

                    link_for_file_Element.click()
                except TimeoutException:
                    date2=date2 + timedelta(1)
                    continue

                time .sleep(2)

            driver.get(link)
            date2=date2 + timedelta(1)
from_date = raw_input('enter the from date in dd-mm-yyyy format')
to_date = raw_input('enter the to date in dd-mm-yyyy format')
link='https://www.nseindia.com/products/content/equities/equities/archieve_eq.htm'
dr=Daily_Report(from_date,to_date)
dr.download(link)
print 'done'
            
