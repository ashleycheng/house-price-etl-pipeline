from google.cloud import storage
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import selenium.webdriver.support.ui as ui
import time
import zipfile
import glob
import os


def run(path):
    '''Build the web crawler to download all historical data in csv format from
    the website automatically.
    '''
    dir_name = path
    prefs = {'profile.default_content_settings.popups': 0,
             'download.default_directory': dir_name}
    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', prefs)

    driver = webdriver.Chrome(chrome_options=options)
    driver.get("http://plvr.land.moi.gov.tw/DownloadOpenData")
    driver.find_element_by_id("ui-id-2").click()

    wait = ui.WebDriverWait(driver, 10)
    wait.until(lambda driver: driver.find_element_by_id('fileFormatId'))
    # select the file format
    select = Select(driver.find_element_by_id('fileFormatId'))
    a = select.select_by_value("csv")

    # select the season and click download one by one
    select_box = driver.find_element_by_name("season")
    options = [x for x in select_box.find_elements_by_tag_name("option")]
    select_box = Select(select_box)
    season = []
    for element in options:
        value = element.get_attribute("value")
        season.append(value)
        select_box.select_by_value(value)

        driver.find_element_by_name("button9").click()

    #  wait for all download files to be completed
    dl_wait = True
    while dl_wait:
        time.sleep(2)
        dl_wait = False
        for fname in os.listdir(dir_name):
            if fname.endswith('.crdownload'):
                dl_wait = True
            elif fname.endswith('.zip'):
                file_name = dir_name+'/'+fname
                new_dir = dir_name+'/'+fname[:-4]
                os.makedirs(new_dir)
                zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
                zip_ref.extractall(new_dir)  # extract file to dir
                zip_ref.close()  # close file
                os.remove(file_name)  # delete zipped file

    driver.close()
    return season


def upload_to_GCS(path, season, project_id, bucket_name):
    '''Change the original file name to distinguish files
    of different seasons, and upload files to the GCS bucket.

    Example : g_lvr_land_a.csv -> 101S4_g_lvr_land_a.csv
    '''
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)

    dir_name = path
    list_files = os.listdir(dir_name)
    list_files = filter(os.path.isdir, glob.glob(dir_name + '*'))
    list_files = sorted(list_files, key=lambda x: x[-4:])

    season.append(season[0])
    del season[0]

    cnt = 0
    for dir in list_files:
        print(dir, season[cnt])
        for files in os.listdir(dir):
            if files.endswith('a.csv'):
                blob = bucket.blob('land_data/' + season[cnt] + '_' + files)
                blob.upload_from_filename(dir + '/' + files)
        cnt += 1


if __name__ == "__main__":
    # Set directory to store data
    path = 'YOUR_DIR_PATH'
    project_id = 'projectID'
    bucket_name = 'GCS_BUCKET_NAME'

    season = run(path)
    upload_to_GCS(path, season, project_id, bucket_name)
