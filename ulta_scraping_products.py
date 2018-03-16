
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
file_msg = ""
file_msg += (
    "rating"
    + "\t" + "headline"
    + "\t" + "author_location"
    + "\t" + "author_affinities"
    + "\t" + "author_date"
    + "\t" + "author_name"
    + "\t" + "is_verified_buyer"
    + "\t" + "pros"
    + "\t" + "cons"
    + "\t" + "bestuses"
    + "\t" + "bottom_line"
    + "\t" + "product_name"
    + "\t" + "brand_name"
    + "\t" + "category"
    + "\t" + "comment"
    + "\n"
)
json_file_writer = open(
    DATA_FOLDER 
        + "\\" + RUN_TIMESTAMP 
        + '\\ulta_comments_' 
        + str(nth_file) 
        + '.json', 
    'w', 
    encoding="utf-8"
)
lines_written = 0
reviews_json_breakdown = []

try:
    client = pymongo.MongoClient("localhost", 27017)
    db = client.revlon
    print("inserting into db, " + db.name)
except Exception as e:
    client = None
    print("error connecting to mongodb, " + str(e))
    traceback.print_exc()

    
products = list(db.products.find({ "fetch_status": 0 }))
print("no of products not fetched, " + str(len(products)))
# print("but doing only for two products")
# products = products[0:2]

# driver = webdriver.Firefox("D:\\Data\\Dinesh\\Work\\revlon\\geckodriver-v0.19.1-win64");
# driver.implicitly_wait(3)
driver = None

product_file_writer = None
log_writer = None

def log_to_file_and_console(msg):
    log_msg += "\n" + (msg + "\n")
    print(msg)
try:
    for pi, product in enumerate(products):
#         print("doing for product no: " + str(pi + 1))
        driver = webdriver.Firefox("D:\\Data\\Dinesh\\Work\\revlon\\geckodriver-v0.19.1-win64");
        driver.implicitly_wait(3)
        product_data_file_path = (
            DATA_FOLDER
            + "\\files_by_category"
            + "\\" + RUN_TIMESTAMP
            + "\\" + product["category_name"].replace(":", "_") + "___" + str(product["_id"]) + ".tsv"
        )
        if not Path(product_data_file_path).is_file():
            product_file_writer = open(product_data_file_path, 'w', encoding="utf-8")
            product_file_msg = ""
            product_file_msg += (
                "rating"
                + "\t" + "headline"
                + "\t" + "author_location"
                + "\t" + "author_affinities"
                + "\t" + "author_date"
                + "\t" + "author_name"
                + "\t" + "is_verified_buyer"
                + "\t" + "pros"
                + "\t" + "cons"
                + "\t" + "bestuses"
                + "\t" + "bottom_line"
                + "\t" + "product_name"
                + "\t" + "brand_name"
                + "\t" + "category"
                + "\t" + "comment"
                + "\n"
            )
        else:
            product_file_writer = None
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
        product_log_file_path = (
            DATA_FOLDER
            + "\\files_by_category"
            + "\\" + RUN_TIMESTAMP 
            + "\\logs_" + product["category_name"].replace(":", "_") + "___" + str(product["_id"]) + ".tsv"
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
        log_msg = ""
        log_msg += "\n" + ("visiting product at " + product["product_page"] + "\n")
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
            mongo_reviews = []
            while reviews_count<=150:
                time.sleep(3)
                log_msg += "\n" + ("fetching comment page" + "\n")
#                 print("fetching comment page")
                try:
                    review_elements = driver.find_elements_by_css_selector(".pr-review-wrap")
                    log_msg += "\n" + ("no of reviews, " + str(len(review_elements)) + "\n")
#                     print("no of reviews, " + str(len(review_elements)))
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
                for i, review_element in enumerate(review_elements):
#                     print("fetching review no. " + str(i))
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
#                     print(
#                         "rating: " + str(rating)
#                         + "\t" + "headline: " + str(headline)
#                         + "\t" + "author_location: " + str(author_location)
#                         + "\t" + "author_affinities: " + str(author_affinities)
#                         + "\t" + "author_date: " + str(author_date)
#                         + "\t" + "author_name: " + str(author_name)
#                         + "\t" + "is_verified_buyer: " + str(is_verified_buyer is not None)
#                         + "\t" + "pros: " + str(pros)
#                         + "\t" + "cons: " + str(cons)
#                         + "\t" + "bestuses: " + str(bestuses)
#                         + "\t" + "bottom_line: " + str(bottom_line)
#                         + "\t" + "product_name: " + str(product_name)
#                         + "\t" + "brand_name: " + str(brand_name)
#                         + "\t" + "category: " + product["category_name"]
#                         + "\t" + "comment: " + str(comment)
#                     )
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
#                     print(str(review))
#                     print("\t".join([ str(value) for value in review.values()]))
                    try:
                        log_msg += "\n" + ("fetched review" + "\n")
                        log_msg += "\n" + ("writing review to master file" + "\n")
                        file_msg += ("\t".join([ str(value) for value in review.values()]) + "\n")
                        if product_file_writer is not None:
                            log_msg += "\n" + ("writing to product file" + "\n")
                            product_file_msg += ("\t".join([ str(value) for value in review.values()]) + "\n")
                        log_msg += "\n" + ("appending review to product reviews list" + "\n")
                        reviews.append({ key: str(value) for key, value in review.items() })
                        mongo_reviews.append(review)
                        log_msg += "\n" + ("appending reviews to master reviews list" + "\n")
                        reviews_json_breakdown.append({ key: str(value) for key, value in review.items() })
                        lines_written = lines_written + 1
                        if(lines_written >= 1500):
                            log_msg += "\n" + ("lines exceeded 1500" + "\n")
                            file_writer.write(file_msg)
                            file_writer.close()
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
                            file_msg = ""
                            file_msg += (
                                "rating"
                                + "\t" + "headline"
                                + "\t" + "author_location"
                                + "\t" + "author_affinities"
                                + "\t" + "author_date"
                                + "\t" + "author_name"
                                + "\t" + "is_verified_buyer"
                                + "\t" + "pros"
                                + "\t" + "cons"
                                + "\t" + "bestuses"
                                + "\t" + "bottom_line"
                                + "\t" + "product_name"
                                + "\t" + "brand_name"
                                + "\t" + "category"
                                + "\t" + "comment"
                                + "\n"
                            )
#                             print("writing to master json file writer --- " + json.dumps(reviews))
                            json_file_writer.write(json.dumps(reviews_json_breakdown))
                            reviews_json_breakdown = []
                            json_file_writer.close()
                            json_file_writer = open(
                                DATA_FOLDER 
                                    + "\\" + RUN_TIMESTAMP 
                                    + '\\ulta_comments_' 
                                    + str(nth_file) 
                                    + '.json', 
                                'w', 
                                encoding="utf-8"
                            )
                            lines_written = 0
                        reviews_count = reviews_count + 1
                    except Exception as e:
                        log_msg += "\n" + (
                            "for some reason could not fetch this comment, comment no: " 
                                + str(reviews_count + i + 1) 
                            + "\n"
                        )
#                         print(
#                             "for some reason could not fetch this comment, comment no: " 
#                                 + str(reviews_count + i + 1) 
#                             + "\n"
#                         )
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
                            break
                    except:
                        log_msg += "\n" + ("no next page" + "\n")
                        break
                else:
                    break
#             print("==================all reviews================\n" + json.dumps(reviews))
            if product_json_file_writer is not None:
#                 print("writing to json file writer --- " + json.dumps(reviews))
                product_json_file_writer.write(json.dumps(reviews))
            if product_file_writer is not None:
                product_file_writer.write(product_file_msg)
            reviews_inserts_result = db.reviews.insert_many(mongo_reviews)
            if(len(reviews_inserts_result.inserted_ids) == len(reviews)):
                log_msg += "\n" + ('all comments written successfully onto db' + "\n")
#                 print('all comments written successfully onto db')
                product_update_result = db.products.update_one(
                    { "_id": product["_id"] }, 
                    { 
                        "$set": { "fetch_status": 1 }, 
                        "$currentDate": {"lastModified": True } 
                    }
                )
                if(product_update_result.modified_count == 1):
                    log_msg += "\n" + ("product status updated successfully" + "\n")
                    print("product status updated successfully")
                else:
                    log_msg += "\n" + ("product status could not be updated" + "\n")
                    print("product status could not be updated")
            else:
                log_msg += "\n" + ("reviews could not be written to db" + "\n")
                print("reviews could not be written to db")
            log_msg += "\n" + (
                product["category_name"]
                + "," + product["product_page"] 
                + "," + str(product_name) 
                + "," + str(reviews_count) 
                + "\n"
            )
            log_msg += "\n" + ("--------------------------------------------------------------------------------------" + "\n")
            print("reviews fetched for product no. " + str(pi + 1) + ": " + str(len(reviews)))
        except TimeoutException:
            log_msg += "\n" + ("Could not sort by newest, timeout after 5 seconds" + "\n")
        if product_file_writer is not None:
            product_file_writer.close()
            product_file_writer = None
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
    print("Your program is awesome, Done !!!")
    file_writer.write(file_msg)
    file_writer.close()
    json_file_writer.write(json.dumps(reviews_json_breakdown))
    json_file_writer.close()
except Exception as e:
    log_msg += "\n" + (str(e))
    traceback.print_exc()
    pass
finally:
#     driver.quit()
    if file_writer is not None:
        file_writer.close()
    if json_file_writer is not None:
        json_file_writer.close()
    if product_file_writer is not None:
        product_file_writer.close()
    if log_writer is not None:
        log_writer.close()
    client.close()


# In[25]:

import json
temp = "something"
tempjson = { "temp": temp, "temp1": "temp1" }
json.loads("[" + json.dumps(tempjson) + ", " + json.dumps(tempjson) + "]")
"\t".join(list(tempjson.values()))

