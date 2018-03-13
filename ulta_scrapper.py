
# coding: utf-8

# In[135]:

from selenium import webdriver
import time

driver = webdriver.Firefox("D:\\Data\\Dinesh\\Work\\revlon\\geckodriver-v0.19.1-win64");
driver.get("https://www.ulta.com")
driver.implicitly_wait(10)

links = {}
links["face"] = []
links["eyes"] = []
links["lips"] = []
list_elements = driver.find_elements_by_css_selector(".ch13-list-face a")
for e in list_elements:
    links["face"].append([e.get_attribute("data-nav-description"), e.get_attribute("href")])
list_elements = driver.find_elements_by_css_selector(".ch13-list-eyes a")
for e in list_elements:
    links["eyes"].append([e.get_attribute("data-nav-description"), e.get_attribute("href")])
list_elements = driver.find_elements_by_css_selector(".ch13-list-lips a")
for e in list_elements:
    links["lips"].append([e.get_attribute("data-nav-description"), e.get_attribute("href")])
print(links)
# links = {"face": [links["face"][0]]}


# In[ ]:

# from selenium.webdriver.support.select import Select
from selenium.common.exceptions import StaleElementReferenceException
import traceback
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
import re
from pathlib import Path
import os
import datetime

ts = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')

DATA_FOLDER = 'D:\\Data\\Dinesh\\Work\\revlon\\ulta_data'
if not os.path.exists(DATA_FOLDER + "\\files_by_category"):
    os.makedirs(DATA_FOLDER + "\\files_by_category")
if not os.path.exists(DATA_FOLDER + "\\files_by_category" + "\\" + ts):
    os.makedirs(DATA_FOLDER + "\\files_by_category" + "\\" + ts)


nth_file = 0
nth_file = nth_file + 1
file_writer = open(DATA_FOLDER + '\\ulta_comments_' + str(nth_file) + '.tsv', 'w', encoding="utf-8")
lines_written = 0

# driver = webdriver.Firefox("D:\\Data\\Dinesh\\Work\\revlon\\geckodriver-v0.19.1-win64");
# driver.implicitly_wait(10)
# links = { "face": [links["face"][0]], "eyes": [links["eyes"][0]] }

try:
    for key in links:
        print("=====================================FOR " + key + "=====================================")
        for link in links[key]:
            if not Path(DATA_FOLDER + "\\files_by_category" + "\\" + ts + "\\" + link[0].replace(":", "_") + ".tsv").is_file():
                category_file_writer = open(
                    DATA_FOLDER 
                        + "\\files_by_category" 
                        + "\\" + ts
                        + "\\" + link[0].replace(":", "_") 
                        + ".tsv"
                    , 'w'
                    , encoding="utf-8"
                )
            else:
                category_file_writer = None
            if not Path(DATA_FOLDER + "\\files_by_category" + "\\" + ts + "\\logs_" + link[0].replace(":", "_") + ".tsv").is_file():
                log_writer = open(
                    DATA_FOLDER 
                        + "\\files_by_category" 
                        + "\\" + ts
                        + "\\logs_" + link[0].replace(":", "_") 
                        + ".tsv"
                    , 'w'
                    , encoding="utf-8"
                )
            else:
                log_writer = log_file_writer = open(
                    DATA_FOLDER 
                        + "\\files_by_category" 
                        + "\\" + ts
                        + "\\logs_other" 
                        + ".tsv"
                    , 'a'
                    , encoding="utf-8"
                )
            log_writer.write("--------------------------------------------------------------------------------------" + "\n")
            print("--------------------------------------------------------------------------------------")
            log_writer.write("Fetching products for, " + link[0] + "\n")
            print("Fetching products for, " + link[0])
            log_writer.write("Here is the corresponding link, " + link[1] + "\n")
            print("Here is the corresponding link, " + link[1])
            driver.get(link[1] + "&Ns=product.bestseller%7C1");
            no_of_product_pages = len(
                driver.find_element_by_xpath(
                    "//select[@id='dropdown-measurement-select']"
                ).find_elements_by_xpath(".//*")
            )
            no_of_product_pages = int((driver.find_elements_by_css_selector(".upper-limit")[0]).text.split(" ")[1])
#             no_of_product_pages = 2
            log_writer.write("no of product pages, " + str(no_of_product_pages) + "\n")
            print("no of product pages, " + str(no_of_product_pages))
            product_links = []
            for page_index in range(0, no_of_product_pages):
                log_writer.write("visiting page " + str(page_index + 1) + "\n")
                driver.get(link[1] + "&Ns=product.bestseller%7C1" + "&No=" + str(page_index * 48) + "&Nrpp=48")
                product_elements = driver.find_elements_by_css_selector(".product")
                hrefs = [ product_element.get_attribute("href") for product_element in product_elements]
                product_links.extend(hrefs)
                log_writer.write("no of products at page " + str(page_index + 1) + ", " + str(len(hrefs)) + "\n")
            log_writer.write("total no of products, " + str(len(product_links)) + "\n")
            print("total no of products, " + str(len(product_links)))
#             product_links = product_links[0:2]
            for product_link in product_links:
                log_writer.write("visiting product at " + product_link + "\n")
                driver.get(product_link)
                product_name = driver.find_element_by_xpath("//h1[@itemprop='name']").text
                driver.find_element_by_xpath("//select[@id='pr-sort-reviews']/option[text()='Newest']").click()
                try:
                    WebDriverWait(driver, 5).until(
                        lambda driver=driver: 
                            driver.execute_script(
                                "return document.readyState==='complete'"
                            )
                    )
                    comments_count = 0
#                     comments = []
                    while comments_count<=150:
                        try:
                            comment_elements = driver.find_elements_by_css_selector(".pr-comments")
#                             print("no of comments, " + str(len(comment_elements)))
                        except StaleElementReferenceException:
                            comment_elements = []
                            log_writer.write("no comments" + "\n")
                            break
                        try:
                            next_page_element = driver.find_element_by_css_selector(".pr-page-next")
                        except StaleElementReferenceException:
                            next_page_element = None
                            pass
                        for i, comment_element in enumerate(comment_elements):
#                             print("comment enabled? :" + str(comment_element.is_enabled()))
                            try:
                                file_writer.write(
                                    link[0] + "\t" + product_name + "\t" 
                                    + str(comment_element.text).replace("\n", "").replace("\r", "").replace("\t", "")
                                    + "\n"
                                )
                                lines_written = lines_written + 1
                                if(lines_written >= 1500):
                                    file_writer.close()
                                    nth_file = nth_file + 1
                                    file_writer = open(
                                        'D:\\Data\\Dinesh\\Work\\revlon\\ulta_data\\ulta_comments_' 
                                        + str(nth_file) + '.tsv', 'w', encoding="utf-8"
                                    )
                                    lines_written = 0
    #                             comments.append(comment_element.text)
                                comments_count = comments_count + 1
                                if category_file_writer is not None:
                                    category_file_writer.write(
                                        link[0] + "\t" + product_name + "\t" 
                                        + str(comment_element.text).replace("\n", "").replace("\r", "").replace("\t", "")
                                        + "\n"
                                    )
                            except StaleElementReferenceException:
                                log_writer.write("for some reason could not fetch this comment, comment no: " + str(comments_count + i + 1) + "\n")
                        if(next_page_element):
                            try:
                                next_page_element.click()
                                time.sleep(5)
                                try:
                                    WebDriverWait(driver, 5).until(
                                        lambda driver=driver: 
                                            driver.execute_script(
                                                "return document.readyState==='complete'"
                                            )
                                    )
                                except:
                                    log_writer.write("could not go to next page, timeout after 5 secs" + "\n")
                                    break
                            except:
                                log_writer.write("no next page" + "\n")
                                break
                        else:
                            break
                    log_writer.write(link[0] + "," + product_name + "," + str(comments_count) + "\n")
                except TimeoutException:
                    log_writer.write("Could not sort by newest, timeout after 5 seconds" + "\n")
            if category_file_writer is not None:
                category_file_writer.close()
                category_file_writer = None
            log_writer.write("--------------------------------------------------------------------------------------" + "\n")
            log_file_writer.close()
    print("Your program is awesome, Done !!!")
    file_writer.close()
except Exception as e:
    log_writer.write(str(e))
    traceback.print_exc()
    log_writer.write(traceback.print_exc())
    pass
finally:
#     driver.quit()
    file_writer.close()
    if category_file_writer is not None:
        category_file_writer.close()
    log_file_writer.close()


# In[84]:

str('something')

