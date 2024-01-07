from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import datetime
import re
import sqlalchemy
import pandas as pd
import clickhouse_driver

def get_review(app_id, lang, region):
    driver = webdriver.Chrome()

    url = f'https://play.google.com/store/apps/details?id={app_id}&hl={region}_{lang}'

    # вспомагательные переменные для поиска информации на странице

    xp_all_review = '//*[@id="yDmH0d"]/c-wiz[2]/div/div/div[2]/div[2]/div/div[1]/div[1]/c-wiz[4]/section/div/div[2]/div[5]/div/div/button/span'
    xp_new_review_part1 = '//*[@id="sortBy_1"]/div[2]/span[2]'
    xp_new_review_part2 = '//*[@id="yDmH0d"]/div[4]/div[2]/div/div/div/div/div[2]/div[2]/div/div/span[2]/div[2]/div[2]'

    class_review = 'RHo1pe'
    class_review_raiting = 'iXRFPc'
    class_review_date = 'bp9Aid'

    mounth_code = {
        'января': 1,
        'февраля': 2,
        'марта': 3,
        'апреля': 4,
        'мая': 5,
        'июня': 6,
        'июля': 7,
        'августа': 8,
        'сентября': 9,
        'октября': 10,
        'ноября': 11,
        'декабря': 12,
        'january': 1,
        'february': 2,
        'martha': 3,
        'april': 4,
        'may': 5,
        'june': 6,
        'july': 7,
        'august': 8,
        'september': 9,
        'october': 10,
        'november': 11,
        'december': 12,
    }

    driver.get(url)

    # открывает все отзывы и сортируем по дате

    driver.find_element(By.XPATH, xp_all_review).click()
    time.sleep(2)
    driver.find_element(By.XPATH, xp_new_review_part1).click()
    time.sleep(2)
    driver.find_element(By.XPATH, xp_new_review_part2).click()
    time.sleep(2)


    all_rewiew = driver.find_elements(By.CLASS_NAME, class_review)
    first_date = all_rewiew[0].find_element(By.CLASS_NAME, class_review_date).text

    # берём дату первого отзыва
    # и собираем все отзывы с такой же датой

    # скролим вниз пока дата последнего загруженного отзыва не будет отлична от первого

    while all_rewiew[-1].find_element(By.CLASS_NAME, class_review_date).text == first_date:
        all_rewiew[-1].click()
        time.sleep(2)
        all_rewiew = driver.find_elements(By.CLASS_NAME, class_review)

    all_rewiew = driver.find_elements(By.CLASS_NAME, class_review)
    
    # собираем рейтинг собранных отзывов
 
    score = []
    for i in all_rewiew:
        date_review = i.find_element(By.CLASS_NAME, class_review_date).text
        if date_review == first_date:
            score.append(re.findall('\d', i.find_element(By.CLASS_NAME, class_review_raiting).get_attribute("aria-label"))[0])
    df = pd.DataFrame({'score': score})

    # преобразуем дату в формат date

    event_date = first_date.split(' ')
    if lang != 'ru':
        current_day = int(event_date[1])
        current_month = mounth_code[event_date[0].lower()]
    else:
        current_day = int(event_date[0])
        current_month = mounth_code[event_date[1].lower()]
    current_year = int(event_date[2])
    df['event_date'] = datetime.date(year=current_year, month=current_month, day=current_day)
    df['language'] = lang
    driver.close()
    return df
