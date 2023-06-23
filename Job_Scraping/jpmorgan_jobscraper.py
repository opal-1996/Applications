import json
import pandas as pd   
import requests
import os
from os.path import exists

class JPMorganScraper:
    def __init__(self, base_url, latest_collection_time, offset=0, file_name="../JPMorgan/Joblist/data.csv"):
        self.base_url = base_url
        self.offset = offset
        self.file_name = file_name

        if exists(file_name):
            self.df = pd.read_csv(file_name)
        else:
            self.df = pd.DataFrame(columns=["job_id", "job_title", "post_date", "short_description", "primary_location", "primary_location_country", "relevancy", "secondary_locations", "application_link", "collection_date"])
        
        self.latest_collection_time = latest_collection_time # latest date collecting the job information

    def get_totaljobscount(self):
        "Get the total jobs count."
        response = requests.get(self.base_url + str(self.offset)).json()
        totaljobscount = response["items"][0]["TotalJobsCount"]

        return totaljobscount

    def scraper(self):
        "Fetch job information from web pages and save them as csv file."
        for i in range(0, self.get_totaljobscount(), 25):
            # json file
            contents = requests.get(self.base_url + str(self.offset + i)).json()
            
            # fetch related information, and save them into dataframe
            for job in contents["items"][0]["requisitionList"]:
                job_id = job["Id"]
                job_title = job["Title"]
                post_date = job["PostedDate"]
                short_description = job["ShortDescriptionStr"]
                primary_location = job["PrimaryLocation"]
                primary_location_country = job["PrimaryLocationCountry"]
                relevancy = job["Relevancy"]
                secondary_locations = []
                if job["secondaryLocations"]:
                    for i in range(len(job["secondaryLocations"])):
                        loc = job["secondaryLocations"][i]["Name"]
                        country = job["secondaryLocations"][i]["CountryCode"]
                        secondary_locations.append([loc, country])
                application_link = "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions/preview/" + str(job_id) + "/?keyword=data+science&lastSelectedFacet=POSTING_DATES&location=United+States&locationId=300000000289738&locationLevel=country&mode=location&selectedPostingDatesFacet=30"
                
                # detailed_page_url = "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails?expand=all&onlyData=true&finder=ById;Id=%" + str(job_id) + "%22,siteNumber=CX_1001"
                # salary = requests.get(detailed_page_url).json()["items"][0]["requisitionFlexFields"][0]["Value"]
                
                collection_date = self.latest_collection_time

                self.df = self.df.append({
                    "job_id": job_id,
                    "job_title": job_title,
                    "post_date": post_date,
                    "short_description": short_description,
                    "primary_location": primary_location,
                    "primary_location_country": primary_location_country,
                    "relevancy": relevancy,
                    "secondary_locations": secondary_locations,
                    "application_link": application_link,
                    "collection_date": collection_date
                }, ignore_index=True)

            
        self.df.to_csv(self.file_name)

if __name__ == "__main__":
    base_url = "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.secondaryLocations,flexFieldsFacet.values&finder=findReqs;siteNumber=CX_1001,facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,limit=25,keyword=%22data%22,lastSelectedFacet=POSTING_DATES,locationId=300000000289738,selectedPostingDatesFacet=30,sortBy=RELEVANCY,offset="
    latest_collection_time = "05-24-2023"

    # scrape data
    JPMorganScraper = JPMorganScraper(base_url=base_url, latest_collection_time=latest_collection_time )
    JPMorganScraper.scraper()