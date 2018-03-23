
# coding: utf-8

# In[ ]:

import time
import pymongo

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from pathlib import Path
import os
import datetime
import traceback

client = pymongo.MongoClient("localhost", 27017)
db = client.revlon
print("inserting into db, " + db.name)

driver = webdriver.Firefox("D:\\Data\\Dinesh\\Work\\revlon\\geckodriver-v0.19.1-win64");
driver.get("https://www.ulta.com")
driver.implicitly_wait(10)

db.categories_ulta_other.delete_many({})
traversed_links = []
links = []
# links = {}
# links["face"] = []
# links["eyes"] = []
# links["lips"] = []
cats = [
    "ch13-list-cleansers",
    "ch13-list-moisturizers",
    "ch13-list-treatment-serums",
    "ch13-list-eyetreatments",
    "ch13-list-suncare",

    "ch13-list-shampoo-conditioner",
    "ch13-list-treatment",
    "ch13-list-haircolor",

    "ch13-list-womensfragrance",
    "ch13-list-mensfragrance",

    "ch13-list-hairstyling-tools",
    "ch13-list-skincaretools",
    "ch13-list-hairremoval-tools",
    "ch13-list-makeupbrushestools",
    "ch13-list-hairbrushescombs"
]

for cat in cats:
    print("fetching for category, " + str("-".join(cat.split("-")[2:])))
    list_elements = driver.find_elements_by_css_selector("."+ cat + " a")
    for e in list_elements:
        print(e.get_attribute("data-nav-description"))
        if e.get_attribute("href") not in traversed_links:
            traversed_links.append(e.get_attribute("href"))
            links.append({
                "parent_category": "-".join(cat.split("-")[2:]),
                "category_name": e.get_attribute("data-nav-description"), 
                "category_page": e.get_attribute("href"),
                "fetch_status": 0
            })
print(len(traversed_links))
# list_elements = driver.find_elements_by_css_selector(".ch13-list-gelmanicure a")
# for e in list_elements:
#     links.append({
#         "parent_category": "gelmanicure",
#         "category_name": e.get_attribute("data-nav-description"), 
#         "category_page": e.get_attribute("href"),
#         "fetch_status": 0
#     })
category_inserts_result = db.categories_ulta_other.insert_many(links)
if(len(category_inserts_result.inserted_ids) == len(links)):
    print("inserted successfully!")
else:
    print("some problem with insertion")
# links = {"face": [links["face"][0]]}


# In[ ]:

db.products.update_many({}, { "$set": { "fetch_status": 0 } })


# In[8]:

import time
import pymongo

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from pathlib import Path
import os
import datetime
import traceback


RUN_TIMESTAMP = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
DATA_FOLDER = 'D:\\Data\\Dinesh\\Work\\revlon\\ulta_data\\skin'

if not os.path.exists(DATA_FOLDER + "\\files_by_category"):
    os.makedirs(DATA_FOLDER + "\\files_by_category")
if not os.path.exists(DATA_FOLDER + "\\files_by_category" + "\\" + RUN_TIMESTAMP):
    os.makedirs(DATA_FOLDER + "\\files_by_category" + "\\" + RUN_TIMESTAMP)
    
try:
    client = pymongo.MongoClient("localhost", 27017)
    db = client.revlon
    print("inserting into db, " + db.name)
except Exception as e:
    client = None
    print("error connecting to mongodb, " + str(e))
    traceback.print_exc()



db.products.update_many({}, { "$set": { "fetch_status": 0 } })


categories = list(db.categories_ulta_other.find({
    "category_name": { 
        "$nin": [
            "m - skin care:cleansers",
            "m - skin care:moisturizers",
            "m - skin care:treatment & serums",
            "m - skin care:eye treatments",
            "m - bath & body:suncare",
            "m - ulta collection:suncare",
            "m - hair:shampoo & conditioner",
            "m - hair:treatment",
            "m - fragrance:women's fragrance",
            "m - fragrance:men's fragrance",
            "m - hair:hair styling tools",
            "m - makeup:makeup brushes & tools"
        ]
    },
    "fetch_status": 0 
}))

print("no of categories not fetched, " + str(len(categories)))

driver = webdriver.Firefox("D:\\Data\\Dinesh\\Work\\revlon\\geckodriver-v0.19.1-win64");
driver.implicitly_wait(10)

category_file_writer = None
log_writer = None

def log_to_file_and_console(msg):
    log_writer.write(msg + "\n")
    print(msg)

try:
    for category in categories:
        print("=====================================for " + category["parent_category"] + "=====================================")
        category_data_file_path = (
            DATA_FOLDER
            + "\\files_by_category"
            + "\\" + RUN_TIMESTAMP
            + "\\" + category["category_name"].replace(":", "_") + ".tsv"
        )
#         print(category_data_file_path)
        if not Path(category_data_file_path).is_file():
            category_file_writer = open(category_data_file_path, 'w', encoding="utf-8")
        else:
            category_file_writer = None
        category_log_file_path = (
            DATA_FOLDER
            + "\\files_by_category"
            + "\\" + RUN_TIMESTAMP 
            + "\\logs_" + category["category_name"].replace(":", "_") + ".tsv"
        )
        if not Path(category_log_file_path).is_file():
            log_writer = open(category_log_file_path, 'w', encoding="utf-8")
        else:
            log_writer = open(
                DATA_FOLDER 
                    + "\\files_by_category" 
                    + "\\" + RUN_TIMESTAMP
                    + "\\logs_other" 
                    + ".tsv"
                , 'a'
                , encoding="utf-8"
            )
        log_to_file_and_console("--------------------------------------------------------------------------------------")
        log_to_file_and_console("Fetching products for, " + category["category_name"])
        log_to_file_and_console("Here is the corresponding link, " + category["category_page"])
        driver.get(category["category_page"] + "&Ns=product.bestseller%7C1");
        no_of_product_pages = len(
            driver.find_element_by_xpath(
                "//select[@id='dropdown-measurement-select']"
            ).find_elements_by_xpath(".//*")
        )
        no_of_product_pages = int((driver.find_elements_by_css_selector(".upper-limit")[0]).text.split(" ")[1])
        no_of_product_pages = 2
        log_to_file_and_console("no of product pages, " + str(no_of_product_pages))
        products = []
        for page_index in range(0, no_of_product_pages):
            log_writer.write("visiting page " + str(page_index + 1) + "\n")
            listing_page = category["category_page"] + "&Ns=product.bestseller%7C1" + "&No=" + str(page_index * 48) + "&Nrpp=48"
            driver.get(listing_page)
            product_elements = driver.find_elements_by_css_selector(".product")
            if page_index == 1:
                products_elements = product_elements[0:2]
            hrefs = [ 
                {
                    "category_name": category["category_name"],
                    "listing_page": listing_page,
                    "product_page": product_element.get_attribute("href"),
                    "fetch_status": 0,
                    "batch_no": page_index % 5
                }
                for product_element in product_elements
            ]
            products.extend(hrefs)
            log_writer.write("no of products at page " + str(page_index + 1) + ", " + str(len(hrefs)) + "\n")
        log_to_file_and_console("total no of products, " + str(len(products)))
        products_inserts_result = db.products_ulta_other.insert_many(products[0:50])
        if(len(products_inserts_result.inserted_ids) == len(products)):
            print("inserted successfully!")
            category_update_result = db.categories_ulta_other.update_one(
                {"_id": category["_id"] }, 
                { 
                    "$set": { "fetch_status": 1 }, 
                    "$currentDate": {"lastModified": True } 
                }
            )
            if(category_update_result.modified_count == 1):
                print("category updated")
            else:
                print("could not update category fetch status")
        else:
            print("something went wrong with product inserts for this category, " + category["category_page"])
except Exception as e:
    if log_writer is not None:
        log_writer.write(str(e))
    traceback.print_exc()
    pass
finally:
    if category_file_writer is not None:
        category_file_writer.close()
    if log_writer is not None:
        log_writer.close()
    if client is not None:
        client.close()


# In[ ]:

db.products.update_many(
    {}, 
    { 
        "$set": { "fetch_status": 0 }, 
        "$currentDate": {"lastModified": True } 
    }
).modified_count     


# In[ ]:

import time
import pymongo

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from pathlib import Path
import os
import datetime
import traceback

RUN_TIMESTAMP = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
DATA_FOLDER = 'D:\\Data\\Dinesh\\Work\\revlon\\ulta_data'

if not os.path.exists(DATA_FOLDER + "\\files_by_category"):
    os.makedirs(DATA_FOLDER + "\\files_by_category")
if not os.path.exists(DATA_FOLDER + "\\files_by_category" + "\\" + RUN_TIMESTAMP):
    os.makedirs(DATA_FOLDER + "\\files_by_category" + "\\" + RUN_TIMESTAMP)

if not os.path.exists(DATA_FOLDER + "\\" + RUN_TIMESTAMP):
    os.makedirs(DATA_FOLDER + "\\" + RUN_TIMESTAMP)

#master file writer
nth_file = 0
nth_file = nth_file + 1
file_writer = open(
    DATA_FOLDER 
        + "\\" + RUN_TIMESTAMP 
        + '\\ulta_comments_' 
        + str(nth_file) 
        + '.tsv', 
    'w', 
    encoding="utf-8"
)
lines_written = 0

try:
    client = pymongo.MongoClient("localhost", 27017)
    db = client.revlon
    print("inserting into db, " + db.name)
except Exception as e:
    client = None
    print("error connecting to mongodb, " + str(e))
    traceback.print_exc()

    
products = list(db.products.find({ "fetch_status": 0 }))
products = products[0:1]
print("no of products not fetched, " + str(len(products)))

driver = webdriver.Firefox("D:\\Data\\Dinesh\\Work\\revlon\\geckodriver-v0.19.1-win64");
driver.implicitly_wait(10)

product_file_writer = None
log_writer = None

def log_to_file_and_console(msg):
    log_writer.write(msg + "\n")
    print(msg)
try:
    for pi, product in enumerate(products):
        product_data_file_path = (
            DATA_FOLDER
            + "\\files_by_category"
            + "\\" + RUN_TIMESTAMP
            + "\\" + product["category_name"].replace(":", "_") + "___" + str(pi + 1) + ".tsv"
        )
        if not Path(product_data_file_path).is_file():
            product_file_writer = open(product_data_file_path, 'w', encoding="utf-8")
        else:
            product_file_writer = None
        product_log_file_path = (
            DATA_FOLDER
            + "\\files_by_category"
            + "\\" + RUN_TIMESTAMP 
            + "\\logs_" + product["category_name"].replace(":", "_") + "___" + str(pi + 1) + ".tsv"
        )
        if not Path(product_log_file_path).is_file():
            log_writer = open(product_log_file_path, 'w', encoding="utf-8")
        else:
            log_writer = open(
                DATA_FOLDER 
                    + "\\files_by_category" 
                    + "\\" + RUN_TIMESTAMP
                    + "\\logs_other_products" + ".tsv"
                , 'a'
                , encoding="utf-8"
            )
        log_writer.write("visiting product at " + product["product_page"] + "\n")
        driver.get(product["product_page"])
        product_name = driver.find_element_by_xpath("//h1[@itemprop='name']").text
        driver.find_element_by_xpath("//select[@id='pr-sort-reviews']/option[text()='Newest']").click()
        try:
            WebDriverWait(driver, 5).until(
                lambda driver=driver: 
                    driver.execute_script(
                        "return document.readyState==='complete'"
                    )
            )
            reviews_count = 0
            reviews = []
            while reviews_count<=15:
                try:
                    review_elements = driver.find_elements_by_css_selector(".pr-review-wrap")
    #               print("no of comments, " + str(len(comment_elements)))
                except Exception as e:
                    review_elements = []
                    log_writer.write("no reviews" + "\n")
                    break
                try:
                    next_page_element = driver.find_element_by_css_selector(".pr-page-next")
                except Exception as e:
                    next_page_element = None
                    pass
                next_page_element = None
                for i, review_element in enumerate(review_elements):
                    rating = (
                        review_element.find_element_by_css_selector(".pr-rating.pr-rounded")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    headline = (
                        review_element.find_element_by_css_selector(".pr-review-rating-headline")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    author_date = (
                        review_element.find_element_by_css_selector(".pr-review-author-date.pr-rounded")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    author_name = (
                        review_element.find_element_by_css_selector(".pr-review-author-name > span")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    author_location = (
                        review_element.find_element_by_css_selector(".pr-review-author-location > span")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    author_affinities = (
                        review_element.find_element_by_css_selector(".pr-review-author-affinities > span")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    is_verified_buyer = review_element.find_element_by_css_selector(".pr-badge.pr-verified-buyer").is_enabled
                    pros = (
                        review_element.find_element_by_css_selector(
                        ".pr-attribute-group.pr-rounded.pr-attribute-pros pr-attribute-value-list"
                        )
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    cons = (
                        review_element.find_element_by_css_selector(
                            "pr-attribute-group.pr-rounded.pr-attribute-cons pr-attribute-value-list"
                        )
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    bestuses= (
                        review_element.find_element_by_css_selector(
                            ".pr-attribute-group.pr-rounded.pr-attribute-bestuses pr-attribute-value-list"
                        )
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    brand_name= (
                        review_element.find_element_by_css_selector(".pr-brand-name")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    product_name= (
                        review_element.find_element_by_css_selector(".pr-product-name")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    comment = (
                        review_element.find_element_by_css_selector(".pr-comments")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    bottom_line = (
                        review_element.find_element_by_css_selector("..pr-review-bottom-line-wrapper")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                    print(
                        "rating: " + str(float(rating))
                        + "\t" + "headline: " + str(headline)
                        + "\t" + "author_location: " + str(author_location)
                        + "\t" + "author_affinities: " + str(author_affinities)
                        + "\t" + "author_date: " + str(author_date)
                        + "\t" + "author_name: " + str(author_name)
                        + "\t" + "is_verified_buyer: " + str(is_verified_buyer)
                        + "\t" + "pros: " + str(pros)
                        + "\t" + "cons: " + str(cons)
                        + "\t" + "bestuses: " + str(bestuses)
                        + "\t" + "bottom_line: " + str(bottom_line)
                        + "\t" + "product_name: " + str(product_name)
                        + "\t" + "brand_name: " + str(brand_name)
                        + "\t" + "category: " + product["category_name"]
                    )
    #                 try:
    #                     file_writer.write(
    #                         product["category_name"] 
    #                             + "\t" + product_name 
    #                             + "\t" + str(reviews_count + i + 1)
    #                             + "\t" + str(comment_element.text).replace("\n", ". ").replace("\r", "").replace("\t", " ")
    #                             + "\n"
    #                     )
    #                     lines_written = lines_written + 1
    #                     if(lines_written >= 1500):
    #                         file_writer.close()
    #                         nth_file = nth_file + 1
    #                         file_writer = open(
    #                             DATA_FOLDER 
    #                                 + "\\" + RUN_TIMESTAMP 
    #                                 + '\\ulta_comments_' 
    #                                 + str(nth_file) 
    #                                 + '.tsv', 
    #                             'w', 
    #                             encoding="utf-8"
    #                         )
    #                         lines_written = 0
    #                         comments.append({
    #                             "category_name": product["category_name"],
    #                             "product_name": product_name,
    #                             "product_listing_page": product["listing_page"],
    #                             "product_page": product["product_page"],
    #                             "comment_no": comments_count + i + 1,
    #                             "comment":comment_element.text
    #                         })
    #                     comments_count = comments_count + 1
    #                     if product_file_writer is not None:
    #                         product_file_writer.write(
    #                             product["category_name"] 
    #                                 + "\t" + product_name 
    #                                 + "\t" + str(comments_count + i + 1)
    #                                 + "\t" + str(comment_element.text).replace("\n", "").replace("\r", "").replace("\t", "")
    #                                 + "\n"
    #                         )
    #                 except Exception as e:
    #                     log_writer.write(
    #                         "for some reason could not fetch this comment, comment no: " 
    #                             + str(comments_count + i + 1) 
    #                         + "\n"
    #                     )
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
    #         reviews_inserts_result = db.comments.insert_many(reviews)
    #         if(len(reviews_inserts_result.inserted_ids) == len(reviews)):
    #             print('all comments written successfully onto db')
    #             product_update_result = db.products.update_one(
    #                 { "_id": product["_id"] }, 
    #                 { 
    #                     "$set": { "fetch_status": 1 }, 
    #                     "$currentDate": {"lastModified": True } 
    #                 }
    #             )
    #             if(product_update_result.modified_count == 1):
    #                 print("product status updated successfully")
    #             else:
    #                 print("product status could not be updated")
    #         else:
    #             print("comments could not be written to db")
    #         log_writer.write(link[0] + "," + product_name + "," + str(comments_count) + "\n")
            log_writer.write("--------------------------------------------------------------------------------------" + "\n")
        except TimeoutException:
            log_writer.write("Could not sort by newest, timeout after 5 seconds" + "\n")
        if product_file_writer is not None:
            product_file_writer.close()
            product_file_writer = None
        if log_writer is not None:
            log_writer.close()
            log_writer = None
    print("Your program is awesome, Done !!!")
    file_writer.close()
except Exception as e:
    log_writer.write(str(e))
    traceback.print_exc()
    pass
finally:
#     driver.quit()
    file_writer.close()
    if product_file_writer is not None:
        product_file_writer.close()
    if log_writer is not None:
        log_writer.close()
    client.close()


# In[ ]:



