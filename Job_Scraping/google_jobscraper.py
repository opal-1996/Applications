import json
import pandas as pd   
import requests
import os
from os.path import exists

class GoogleScraper:
    def __init__(self, base_url, collection_date, file_name="../Google/Joblist/data.csv"):
        self.base_url = base_url
        self.collection_date = collection_date
        self.file_name = file_name

        if exists(file_name):
            self.df = pd.read_csv(file_name)
        else:
            self.df = pd.DataFrame(columns=[
                    "job_id",
                    "job_title",
                    "categories",
                    "application_link",
                    "responsibilities",
                    "qualifications",
                    "location_is_remote",
                    "description",
                    "education_levels",
                    "post_date",
                    "collection_date"
                ])
            
    def get_totaljobscount(self):
        "Get the total job count."
        response = requests.get(self.base_url).json()
        totaljobscount = response["count"]
        page_size = response["page_size"]
        num_pages = (totaljobscount // page_size) + 1
        return num_pages
    
    def scraper(self):
        "Scrape and fetch job information."
        num_pages = self.get_totaljobscount()
        for page in range(1,num_pages+1):
            url = "https://careers.google.com/api/v3/search/?distance=50&hl=en_US&jlo=en_US&location=United%20States&page={}&q=data".format(page)
            joblist = requests.get(url).json()["jobs"]
            for job in joblist:
                job_id = job["id"][5:]
                job_title = job["title"]
                categories = job["categories"]
                application_link = job["apply_url"]
                responsibilities = job["responsibilities"]
                qualifications = job["qualifications"]
                locations_count = job["locations_count"]
                location_is_remote = [(job['locations'][i]["display"], job['locations'][i]["is_remote"]) for i in range(locations_count)]
                description = job["description"]
                education_levels = job["education_levels"]
                post_date = job["publish_date"]
                collection_date = self.collection_date
            
                self.df = self.df.append(
                    {
                    "job_id":job_id,
                    "job_title": job_title,
                    "categories": categories,
                    "application_link": application_link,
                    "responsibilities": responsibilities,
                    "qualifications": qualifications,
                    "location_is_remote": location_is_remote,
                    "description": description,
                    "education_levels": education_levels,
                    "post_date":post_date,
                    "collection_date": collection_date
                    },
                    ignore_index=True
                )
        
        self.df.to_csv(self.file_name)
        
if __name__ == "__main__":
    BASE_URL = "https://careers.google.com/api/v3/search/?distance=50&hl=en_US&jlo=en_US&location=United%20States&page=1&q=data"
    scraper = GoogleScraper(BASE_URL, "05-27-2023")
    scraper.scraper()