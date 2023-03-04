# DE_Project_Group_09 - Analysis Vietnam Jobs
This is our DE project from group 09, Master degree of Data Science K31, HCMUS, Ho Chi Minh, Vietnam.

Our group: Bui Thien Long (21C29008), Nguyen Tran Huu Thinh (21C29018), Bui Thi Hoang Yen (21C29030)

![Schema pic](https://user-images.githubusercontent.com/67893231/222876032-c60b63ed-11d9-4ce8-984d-b621160027d8.PNG)

In this project, we built a system that includes the following components:
1. Web scraping bot for job recruitments.
2. Processing pipeline.
3. Database (MySQL, PostgreSQL, DBeaver, Docker)
4. Data analysis (Power BI dashboard)

# Explain folder structure in this repository
In this repository we have 3 main components: scraping bot, data and pipeline

1. Scraping bot: lied in "vn_data_jobs" folder.
2. Data: including raw data and cleansed data, all lied in "Dataset" folder.
3. Pipeline: lied in "Pipeline" folder. it is "pipeline.py" Python file, and all of its back up versions lied in "pipeline bk" folder.

Go through more detail in "Dataset" folder, we have:
1. "cleansed data cb" folder: it contains general datasets, which will be loaded directly into dashboard (Power BI for example). Moreover, it also contains back up dataset folders, which is previous datasets in the past.
2. "r_cleansed_data_cb" folder: it contains cleansed datasets which were processed from its raw data. So general datasets will be merged with this cleansed datasets to have up-to-date data.
3. "raw data cb" folder: it contains raw data, categorized by which date it was scraped.



