import json
import pandas as pd   
import requests
import os
from os.path import exists

class BOAScraper:
    def __init__(self, base_url, collection_date, file_name="../BOA/Joblist/data.csv"):
        self.base_url = base_url
        self.collection_date = collection_date
        self.file_name = file_name

        if exists(file_name):
            self.df = pd.read_csv(file_name)
        else:
            self.df = pd.DataFrame(columns=[
                "job_id",
                "job_title",
                "job_division",
                "post_date",
                "city",
                "state",
                "yearsOfExperience",
                "application_link",
                "collection_date"
                ])

    def get_totaljobscount(self):
        "Get the json file & total jobs count."
        response = requests.get(self.base_url).json()
        totaljobscount = response["totalMatches"]
        return response, totaljobscount

    def scraper(self):
        "Scrape and fetch job information."
        response, totaljobscount = self.get_totaljobscount()
        joblist = response["jobsList"]
        for i in range(totaljobscount):
            job_id = joblist[i]["jobRequisitionId"]
            job_title = joblist[i]["postingTitle"]
            job_division = joblist[i]["division"]
            post_date = joblist[i]["postedDate"]
            city = joblist[i]["city"]
            state = joblist[i]["state"]
            yearsOfExperience = joblist[i]["yearsOfExperience"]
            application_link = "https://careers.bankofamerica.com/" + joblist[i]["jcrURL"]
            collection_date = self.collection_date

            self.df = self.df.append(
                {   "job_id":job_id,
                    "job_title": job_title,
                    "job_division": job_division,
                    "post_date":post_date,
                    "city": city,
                    "state": state,
                    "yearsOfExperience": yearsOfExperience,
                    "application_link": application_link,
                    "collection_date": collection_date
                },
                ignore_index=True
            )
        self.df.to_csv(self.file_name)

if __name__ == "__main__":
    BASE_URL = "https://careers.bankofamerica.com/services/jobssearchservlet?term=data&start=0&rows=213&search=jobsByLocation&searchstring=United%20States&"
    scraper = BOAScraper(BASE_URL, "05-24-2023")
    scraper.scraper()