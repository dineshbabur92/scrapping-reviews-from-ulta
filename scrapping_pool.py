# -*- coding: utf-8 -*-
"""
Created on Thu Mar 15 15:59:27 2018

@author: dineshbabu.rengasamy
"""
import time
import pymongo

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from pathlib import Path
import os
import datetime
import traceback
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

RUN_TIMESTAMP = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
DATA_FOLDER = 'D:\\Data\\Dinesh\\Work\\revlon\\ulta_data\\skin\\parallel'

if not os.path.exists(DATA_FOLDER + "\\files_by_category"):
    os.makedirs(DATA_FOLDER + "\\files_by_category")
if not os.path.exists(DATA_FOLDER + "\\files_by_category" + "\\" + RUN_TIMESTAMP):
    os.makedirs(DATA_FOLDER + "\\files_by_category" + "\\" + RUN_TIMESTAMP)

if not os.path.exists(DATA_FOLDER + "\\" + RUN_TIMESTAMP):
    os.makedirs(DATA_FOLDER + "\\" + RUN_TIMESTAMP)
    
#thread_pool = ThreadPoolExecutor(max_workers = 10)

try:
    client = pymongo.MongoClient("localhost", 27017)
    db = client.revlon
    print("inserting into db, " + db.name)
except Exception as e:
    client = None
    print("error connecting to mongodb, " + str(e))
    traceback.print_exc()

products = list(db.products_ulta_skin.find({ "fetch_status": 0 }))
#products = products[0:2]
#products = [
#    {
#        "_id" : "5ab3497710f9516ab07e75ec", 
#        "category_name" : "m - nails:top & base coats", 
#        "listing_page" : "https://www.ulta.com/nails-top-base-coats?N=27iq&Ns=product.bestseller%7C1&No=0&Nrpp=48", 
#        "product_page" : "https://www.ulta.com/gel-shine-top-coat?productId=xlsImpprod17741159", 
#        "fetch_status" : 0, 
#        "batch_no" : 0 
#    }
#]
#print("no of products not fetched, " + str(len(products)))

client.close()

global products_scraped
products_scraped = 0

def scrap_product(product):
    try:
        client = pymongo.MongoClient("localhost", 27017)
        db = client.revlon
#        print("inserting into db, " + db.name)
    except Exception as e:
        client = None
#        print("error connecting to mongodb, " + str(e))
        traceback.print_exc()
    print("scraping product, " + str(product["_id"]))
    driver = webdriver.Firefox("D:\\Data\\Dinesh\\Work\\revlon\\geckodriver-v0.19.1-win64")
    driver.implicitly_wait(3)
#    print("creating json file")
    product_data_json_file_path = (
        DATA_FOLDER
        + "\\files_by_category"
        + "\\" + RUN_TIMESTAMP
        + "\\" + product["category_name"].replace(":", "_") + "___" + str(product["_id"]) + ".json"
    )
    if not Path(product_data_json_file_path).is_file():
        product_json_file_writer = open(product_data_json_file_path, 'w', encoding="utf-8")
    else:
        product_json_file_writer = None
#    print("creating log file")
    product_log_file_path = (
        DATA_FOLDER
        + "\\files_by_category"
        + "\\" + RUN_TIMESTAMP 
        + "\\logs_" + product["category_name"].replace(":", "_") + "___" + str(product["_id"]) + ".tsv"
    )
    log_msg = ""
    if not Path(product_log_file_path).is_file():
        log_writer = open(product_log_file_path, 'w', encoding="utf-8")
    else:
        log_writer = None
        log_msg = ""
        log_msg += "\n" + ("visiting product at " + product["product_page"] + "\n")
#    print("getting page")
    driver.get(product["product_page"])
    product_name = driver.find_element_by_xpath("//h1[@itemprop='name']").text
    try:
        driver.find_element_by_xpath("//select[@id='pr-sort-reviews']/option[text()='Newest']").click()
    except:
        pass
    try:
#        print("web driver waiting after sorting by newest")
        WebDriverWait(driver, 5).until(
            lambda driver=driver: 
                driver.execute_script(
                    "return document.readyState==='complete'"
                )
        )
#        print('sorting done')
        reviews_count = 0
        reviews = []
        mongo_reviews = []
        while reviews_count<=150:
#            print("fetching reviews loop, waiting")
            time.sleep(3)
#            print("idleness got over")
            log_msg += "\n" + ("fetching comment page" + "\n")
#            print("fetching comment page")
            try:
                review_elements = driver.find_elements_by_css_selector(".pr-review-wrap")
                log_msg += "\n" + ("no of reviews, " + str(len(review_elements)) + "\n")
#                print("no of reviews, " + str(len(review_elements)))
            except Exception as e:
                review_elements = []
                log_msg += "\n" + ("no reviews" + "\n")
                break
            try:
                next_page_element = driver.find_element_by_css_selector("[data-pr-event='header-page-next-link']")
            except Exception as e:
                next_page_element = None
                pass
#                 next_page_element = None
#                 review_elements = review_elements[0:1]
#            print("looping through reviews")
            for i, review_element in enumerate(review_elements):
#                print("fetching review no. " + str(i))
                rating = headline = author_date = author_name = author_location = None
                author_affinities = is_verified_buyer = brand_name = product_name = bottom_line = None
                comment = pros = cons = bestuses = None
                try:
                    rating = (
                        review_element.find_element_by_css_selector(".pr-rating.pr-rounded")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    headline = (
                        review_element.find_element_by_css_selector(".pr-review-rating-headline")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    author_date = (
                        review_element.find_element_by_css_selector(".pr-review-author-date.pr-rounded")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    author_name = (
                        review_element.find_element_by_css_selector(".pr-review-author-name > span")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    author_location = (
                        review_element.find_element_by_css_selector(".pr-review-author-location > span")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    author_affinities = (
                        review_element.find_element_by_css_selector(".pr-review-author-affinities > span")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    is_verified_buyer = review_element.find_element_by_css_selector(".pr-badge.pr-verified-buyer").is_enabled
                    pros = (
                        review_element.find_element_by_css_selector(
                        ".pr-attribute-group.pr-rounded.pr-attribute-pros .pr-attribute-value-list"
                        )
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    cons = (
                        review_element.find_element_by_css_selector(
                            ".pr-attribute-group.pr-rounded.pr-attribute-cons .pr-attribute-value-list"
                        )
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    bestuses= (
                        review_element.find_element_by_css_selector(
                            ".pr-attribute-group.pr-rounded.pr-attribute-bestuses .pr-attribute-value-list"
                        )
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    brand_name= (
                        review_element.find_element_by_css_selector(".pr-brand-name")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    product_name= (
                        review_element.find_element_by_css_selector(".pr-product-name")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    comment = (
                        review_element.find_element_by_css_selector(".pr-comments")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                try:
                    bottom_line = (
                        review_element.find_element_by_css_selector(".pr-review-bottom-line-wrapper")
                        .text
                        .replace("\n", ". ").replace("\r", "").replace("\t", " ")
                    )
                except:
                    pass
                review = {
                    "rating": float(rating) if rating is not None else None,
                    "headline": str(headline) if headline is not None else None,
                    "author_location": str(author_location) if author_location is not None else None,
                    "author_affinities": str(author_affinities) if author_affinities is not None else None,
                    "author_date": datetime.datetime.strptime(author_date, "%m/%d/%Y").date().isoformat() if author_date is not None else None,
                    "author_name": str(author_name) if author_name is not None else None,
                    "is_verified_buyer": is_verified_buyer is not None,
                    "pros": pros.split(". ") if pros is not None else None,
                    "cons": cons.split(". ") if cons is not None else None,
                    "bestuses": bestuses.split(". ") if bestuses is not None else None,
                    "bottom_line": str(bottom_line).replace("BOTTOM LINE ", "") if bottom_line is not None else None,
                    "product_name": str(product_name) if product_name is not None else None,
                    "brand_name": str(brand_name) if brand_name is not None else None,
                    "category": product["category_name"],
                    "comment": str(comment) if comment is not None else None,
                    "product_id": str(product["_id"]),
                    "comment_no": str(i + 1)                  
                }
#                print(str(review))
#               print("\t".join([ str(value) for value in review.values()]))
                try:
                    log_msg += "\n" + ("fetched review" + "\n")
#                    print("appending review to product reviews list")
                    log_msg += "\n" + ("appending review to product reviews list" + "\n")
                    reviews.append({ key: str(value) for key, value in review.items() })
                    mongo_reviews.append(review)
#                    print("appending reviews to master list")
                    log_msg += "\n" + ("appending reviews to master reviews list" + "\n")
                    reviews_count = reviews_count + 1
                except Exception as e:
                    log_msg += "\n" + (
                        "for some reason could not fetch this comment, comment no: " 
                            + str(reviews_count + i + 1) 
                        + "\n"
                    )
#                    print(
#                     "for some reason could not fetch this comment, comment no: " 
#                         + str(reviews_count + i + 1) 
#                     + "\n"
#                    )
                    pass
            if(next_page_element):
                try:
                    next_page_element.click()
#                         time.sleep(5)
                    try:
                        WebDriverWait(driver, 5).until(
                            lambda driver=driver: 
                                driver.execute_script(
                                    "return document.readyState==='complete'"
                                )
                        )
                    except:
                        log_msg += "\n" + ("could not go to next page, timeout after 5 secs" + "\n")
#                            print("could not go to next page, timeout after 5 secs" + "\n")
                        break
                except:
                    log_msg += "\n" + ("no next page" + "\n")
#                    print("no next page" + "\n")
                    break
            else:
#                print("just breaking")
                break
#             print("==================all reviews================\n" + json.dumps(reviews))
        if product_json_file_writer is not None:
#                 print("writing to json file writer --- " + json.dumps(reviews))
            product_json_file_writer.write(json.dumps(reviews))
        try:
            reviews_inserts_result = db.reviews_ulta_skin.insert_many(mongo_reviews)
            if(len(reviews_inserts_result.inserted_ids) == len(reviews)):
                log_msg += "\n" + ('all comments written successfully onto db' + "\n")
#                    print('all comments written successfully onto db')
                product_update_result = db.products_ulta_skin.update_one(
                    { "_id": product["_id"] }, 
                    { 
                        "$set": { "fetch_status": 1 }, 
                        "$currentDate": {"lastModified": True } 
                    }
                )
                if(product_update_result.modified_count == 1):
                    log_msg += "\n" + ("product status updated successfully" + "\n")
#                        print("product status updated successfully")
                else:
                    log_msg += "\n" + ("product status could not be updated" + "\n")
#                        print("product status could not be updated")
            else:
                log_msg += "\n" + ("reviews could not be written to db" + "\n")
#                    print("reviews could not be written to db")
            client.close()
        except:
            log_msg += "\n" + ("reviews could not be written to db" + "\n")
#                print("reviews could not be written to db")
        log_msg += "\n" + (
            product["category_name"]
            + "," + product["product_page"] 
            + "," + str(product_name) 
            + "," + str(reviews_count) 
            + "\n"
        )
        log_msg += "\n" + ("--------------------------------------------------------------------------------------" + "\n")      
    except TimeoutException:
        log_msg += "\n" + ("Could not sort by newest, timeout after 5 seconds" + "\n")
    global products_scraped
    products_scraped += 1
    print("total products scraped so far, " + str(products_scraped))
    print("reviews fetched for " + str(product["_id"]) + ", no.of reviews: " + str(len(reviews)))
    if product_json_file_writer is not None:
        product_json_file_writer.close()
        product_json_file_writer = None
    if log_writer is not None:
        log_writer.write(log_msg)
        log_writer.close()
        log_writer = None
    if driver is not None:
        driver.quit()
        driver = None
    return 1

print("atleast processing reached here?")
try:
    with ThreadPoolExecutor(5) as thread_pool:
        futures = {thread_pool.submit(scrap_product, product) for product in products}
    for f in as_completed(futures):
        rs = f.result()
        print(str(rs))
except:
    pass