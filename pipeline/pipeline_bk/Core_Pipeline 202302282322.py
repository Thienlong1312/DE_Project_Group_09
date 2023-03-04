#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import os
import numpy as np
import pandas as pd
from functools import reduce


# In[2]:


def filter_company_video_url(series_company_video_url):
    lst_company_video_url = list(filter(lambda x: (x != "Unknown") and bool(x) and (not isinstance(x, float)),
                                        series_company_video_url))
    if len(lst_company_video_url):
        return lst_company_video_url[0]
    else:
        return series_company_video_url.values[0]


# In[3]:


def processing_salary(x):
    '''
    x = 'Dưới50,8 Tr VND'
    y = 'Dưới500,000 VND'
    z = '9,2 Tr - 30,2 Tr VND'
    t = 'Trên 17,6 Tr VND'
    
    processing_salary(y)
    '''
    pattern_so = r"(^[\d]+(,[\d]+)? (Tr )?- [\d]+(,[\d]+)? (Tr )?VND)"
    pattern_duoi = r"^Dưới[\d]+(,[\d]+)? (Tr )?VND"
    pattern_tren = r"^Trên [\d]+(,[\d]+)? (Tr )?VND"
    if x == "Cạnh tranh":
        return ['Unknown', 'Unknown']
    elif bool(re.match(pattern_so, x)):
        try:
            re_match_obj = re.match(r"^[\d]+(,[\d]+)? (Tr )?", x)
            idx_s_lb_salary = re_match_obj.start()
            idx_e_lb_salary = re_match_obj.end()
            lb_salary = x[idx_s_lb_salary:idx_e_lb_salary].strip()
            lb_salary = float(lb_salary.replace(",", ""))/1_000_000 if (lb_salary[-3:]!=" Tr")                else float(lb_salary[:-3].replace(",", "."))
            re_search_obj = re.search(r"[\d]+(,[\d]+)? (Tr )?", x[idx_e_lb_salary:])
            idx_s_ub_salary = re_search_obj.start()
            idx_e_ub_salary = re_search_obj.end()
            ub_salary = x[idx_e_lb_salary+idx_s_ub_salary:idx_e_lb_salary+idx_e_ub_salary].strip()
            ub_salary = float(ub_salary.replace(",", ""))/1_000_000 if (ub_salary[-3:]!=" Tr")                else float(ub_salary[:-3].replace(",", "."))
            return [lb_salary, ub_salary]
        except:
            return [x, '']
    elif bool(re.match(pattern_duoi, x)):
        try:
            re_search_obj = re.search(r"[\d]+(,[\d]+)? (Tr )?", x)
            idx_s_ub_salary = re_search_obj.start()
            idx_e_ub_salary = re_search_obj.end()
            ub_salary = x[idx_s_ub_salary:idx_e_ub_salary].strip()
            ub_salary = float(ub_salary.replace(",", ""))/1_000_000 if (ub_salary[-3:]!=" Tr")                else float(ub_salary[:-3].replace(",", "."))
            lb_salary = ub_salary * 0.32
            return [lb_salary, ub_salary]
        except:
            return [x, '']
    elif bool(re.match(pattern_tren, x)):
        try:
            re_search_obj = re.search(r"[\d]+(,[\d]+)? (Tr )?", x)
            idx_s_lb_salary = re_search_obj.start()
            idx_e_lb_salary = re_search_obj.end()
            lb_salary = x[idx_s_lb_salary:idx_e_lb_salary].strip()
            lb_salary = float(lb_salary.replace(",", ""))/1_000_000 if (lb_salary[-3:]!=" Tr")                else float(lb_salary[:-3].replace(",", "."))
            ub_salary = lb_salary * 1.4
            return [lb_salary, ub_salary]
        except:
            return [x, '']
    else:
        return [x, '']


# In[4]:


def replace_col_by_col(df, replaced_col, replacing_col, idx_repaired, idx_repaired_2):
    print(f"Unique values of \"{replacing_col}\" to repair")
    print(df.loc[idx_repaired_2, replacing_col].unique())
    print()

    print(f"Unique values of \"{replaced_col}\" to be repaired")
    print(df.loc[idx_repaired_2, replaced_col].unique())
    print()

    print("Repairing")
    df.loc[idx_repaired_2, replaced_col] = df.loc[idx_repaired_2, replacing_col]
    df.loc[idx_repaired_2, replacing_col] = "Unknown"
    print()
    print('Done')
    print()

    print(f"Unique values of \"{replacing_col}\" to repair")
    print(df.loc[idx_repaired_2, replacing_col].unique())
    print()

    print(f"Unique values of \"{replaced_col}\" to be repaired")
    print(df.loc[idx_repaired_2, replaced_col].unique())
    print()

    print("Reduce idx_repaired")
    idx_repaired = list(set(idx_repaired) - set(idx_repaired_2))
    print("Done")
    print("Number of remaining error rows:", len(idx_repaired))
    print()
    
    return df, idx_repaired


# In[5]:


def processing_kinh_nghiem(x):
    """
    x = '10 - 11 Năm'
    y = '10 Năm'
    z = 'Dưới 10Năm'
    t = 'Trên 12 Năm'
    processing_kinh_nghiem(x)
    """
    
    pattern_1 = r"(^[\d]+ - [\d]+ Năm)"     # ? - ? Năm
    pattern_2 = r"(^[\d]+ Năm)"             # ? Năm
    pattern_3 = r"(^Dưới [\d]+Năm)"         # Dưới ?Năm
    pattern_4 = r"(^Trên [\d]+ Năm)"        # Trên ? Năm
    if x == "Chưa có kinh nghiệm":
        return [0, 0]
    elif bool(re.match(pattern_1, x)):
        try:
            re_match_obj = re.match(r"^[\d]+", x)
            idx_s_lb_kn = re_match_obj.start()
            idx_e_lb_kn = re_match_obj.end()
            lb_kn = x[idx_s_lb_kn:idx_e_lb_kn].strip()
            lb_kn = int(lb_kn)
            re_search_obj = re.search(r"[\d]+", x[idx_e_lb_kn:])
            idx_s_ub_kn = re_search_obj.start()
            idx_e_ub_kn = re_search_obj.end()
            ub_kn = x[idx_e_lb_kn+idx_s_ub_kn:idx_e_lb_kn+idx_e_ub_kn].strip()
            ub_kn = int(ub_kn)
            return [lb_kn, ub_kn]
        except:
            return [x, '']
    elif bool(re.match(pattern_2, x)):
        try:
            re_match_obj = re.match(r"^[\d]+", x)
            idx_s_lb_kn = re_match_obj.start()
            idx_e_lb_kn = re_match_obj.end()
            lb_kn = x[idx_s_lb_kn:idx_e_lb_kn].strip()
            lb_kn = int(lb_kn)
            return [lb_kn, lb_kn]
        except:
            return [x, '']
    elif bool(re.match(pattern_3, x)):
        try:
            re_match_obj = re.match(r"^[\d]+", x[5:])
            idx_s_ub_kn = re_match_obj.start()
            idx_e_ub_kn = re_match_obj.end()
            ub_kn = x[5+idx_s_ub_kn:5+idx_e_ub_kn].strip()
            ub_kn = int(ub_kn)
            return [0, ub_kn]
        except:
            return [x, '']
    elif bool(re.match(pattern_4, x)):
        try:
            re_match_obj = re.match(r"^[\d]+", x[5:])
            idx_s_lb_kn = re_match_obj.start()
            idx_e_lb_kn = re_match_obj.end()
            lb_kn = x[5+idx_s_lb_kn:5+idx_e_lb_kn].strip()
            lb_kn = int(lb_kn)
            return [lb_kn, 40]
        except:
            return [x, '']
    else:
        return [x, '']


# In[6]:

def processing_pipeline(link_to_data_files, link_to_output):
    # link_to_data_files = r"vn_data_jobs\data cb\raw data cb\all_jobs"
    # link_to_output = r"C:\Users\Admin\Downloads"


    # In[7]:


    print("Query df_companies_sv from server:")
    df_companies_sv = pd.read_csv(r"C:\Users\Admin\Downloads\DE 2nd folder\DE project\scrape component\vn_data_jobs\data cb\cleansed data cb\companies.csv",
                                usecols=['company_title', 'company_id'], keep_default_na=False)
    print(df_companies_sv)
    print()


    # In[8]:


    print("Number of company_id unique:", df_companies_sv['company_id'].unique().size, "unique values")
    print("Check number of company_id unique:")
    if len(df_companies_sv) == df_companies_sv['company_id'].unique().size:
        print(True)
    else:
        raise Exception("Number of company_id unique does not equal to total number of rows of df_companies_sv")
    print()


    # In[9]:


    print("Number of company_title unique:", df_companies_sv['company_title'].unique().size, "unique values")
    print("Check number of company_title unique:")
    if len(df_companies_sv) == df_companies_sv['company_title'].unique().size:
        print(True)
    else:
        raise Exception("Number of company_title unique does not equal to total number of rows of df_companies_sv")
    print()


    # In[10]:


    print("Query df_job_id from server:")
    df_job_id = pd.read_csv(r"C:\Users\Admin\Downloads\DE 2nd folder\DE project\scrape component\vn_data_jobs\data cb\cleansed data cb\jobs.csv",
                                usecols=['job_id'], keep_default_na=False)
    print(df_job_id)
    print()


    # In[11]:


    print("Number of job_id unique:", df_job_id['job_id'].unique().size, "unique values")
    print("Check number of job_id unique:")
    if len(df_job_id) == df_job_id['job_id'].unique().size:
        print(True)
    else:
        raise Exception("Number of job_id unique does not equal to total number of rows of df_job_id")
    print()


    # In[12]:


    print("Query df_ordered_cap_bac from server:")
    df_ordered_cap_bac = pd.read_csv(r"C:\Users\Admin\Downloads\DE 2nd folder\DE project\scrape component\vn_data_jobs\data cb\cleansed data cb\ordered_cap_bac.csv",
                                    keep_default_na=False)
    print(df_ordered_cap_bac)
    print()


    # In[13]:


    cap_bac = list(filter(lambda x: x!="Unknown", df_ordered_cap_bac['cap_bac'].unique()))


    # In[14]:


    print("Query city from server:")
    df_city = pd.read_csv(r"C:\Users\Admin\Downloads\DE 2nd folder\DE project\scrape component\vn_data_jobs\data cb\cleansed data cb\city_country.csv",
                                usecols=['City'], keep_default_na=False)
    print(df_city)
    print()


    # In[15]:


    print("Number of city unique:", df_city['City'].unique().size, "unique values")
    print("Check number of city unique:")
    if len(df_city) == df_city['City'].unique().size:
        print(True)
    else:
        raise Exception("Number of city unique does not equal to total number of rows of df_city")
    print()


    # In[16]:


    print("1) Processing raw data")
    print()


    # In[17]:


    lst_data_files = os.listdir(link_to_data_files)
    print("List raw data files:")
    for data_file in lst_data_files:
        print("\t", data_file)
    print()


    # In[18]:


    print("1.1) Combine raw data")
    print()
    total_rows = 0
    df = pd.DataFrame()
    for data_file in lst_data_files:
        sub_df = pd.read_csv(os.path.join(link_to_data_files, data_file))
        print("\t", data_file, "-", len(sub_df), "rows")
        total_rows += len(sub_df)
        df = pd.concat([df, sub_df], ignore_index=True)
    del data_file
    del sub_df
    print()
    print("Total rows:", total_rows)
    print()


    # In[19]:


    df.info()
    print()


    # In[20]:


    print("Drop NaN job_id")
    print(f"Total rows before drop: {len(df)}")
    df = df.dropna(subset=['job_id'])
    print("Done")
    print(f"Total rows before drop: {len(df)}")
    print()


    # In[21]:


    print("Fill Unknown for all NaN values")
    df = df.fillna("Unknown")
    print("Done")
    print()
    df.info()
    print()


    # In[22]:


    print("Strip all cell values")
    for col in df:
        df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    print("Done")
    print()


    # In[23]:


    print("Check total rows:")
    if len(df) == total_rows:
        print(True)
    else:
        raise Exception("Number of rows in combined_raw_data (df) does not equal to total_rows counted by summing from each raw data files")

    del total_rows
    print()


    # In[24]:


    # Assum has new raw data
    print("Filter existence job_id:")
    df = df[~df['job_id'].isin(df_job_id['job_id'])]
    print("Done")
    print()
    df.info()
    print()


    # # ======================================
    # # ==================================

    # In[25]:


    # Assum has new raw data


    # # ==================================
    # # ======================================

    # In[26]:


    if len(df):
        # In[27]:


        print("Number of job_id unique:", df['job_id'].unique().size, "unique values")
        print("Check number of job_id unique:")
        if len(df) == df['job_id'].unique().size:
            print(True)
        else:
            raise Exception("Number of rows in combined_raw_data (df) does not equal to number of job_id unique")

        print()


        # In[28]:


        print("1.2) Processing \"location\" column")
        print()


        # In[29]:


        print("Cities in raw data:")
        for city in set(reduce(lambda x, y: x+y, df['location'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | ")))).values)):
            print("  '" + city + "'")
        print()


        # In[30]:


        print("Process \"location\" column")
        df['location'] = df['location'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))
        print("Done")
        print()


        # In[31]:


        print('Explode location_job dataset')
        df_jobs = df[['location', 'job_id', 'company_title']].explode('location')    .rename(columns={"location": "City"})
        print("Done")
        print()
        print(df_jobs)
        print()


        # In[32]:


        print("Total number of cities in raw data:", len(reduce(lambda x, y: x+y, df['location'].values)), "cities")
        print("Check total number of cities in raw data:")
        if len(df_jobs) == len(reduce(lambda x, y: x+y, df['location'].values)):
            print(True)
        else:
            raise Exception("Number of rows in df_jobs does not equal to total number of cities in raw data")

        print()


        # In[33]:


        print("Fill NaN values with \"Unknown\"")
        df_jobs = df_jobs.fillna('Unknown')
        print()


        # In[34]:


        print("Check drop_duplicates makes difference")
        if len(df_jobs) == len(df_jobs.drop_duplicates()):
            print(True)
        else:
            raise Exception("Number of rows in df_jobs does not equal to number of rows in df_jobs.drop_duplicates()")

        print()


        # In[35]:


        print("Check for existence of company_title in raw data (Any new company title?)")
        df_jobs_unknown = df_jobs[~df_jobs['company_title'].isin(df_companies_sv['company_title'].values)]
        print(df_jobs_unknown)
        print()


        # In[36]:


        if len(df_jobs_unknown):
            print("There were some companies that their titles do not exists in database (df_companies_sv['company_title']) before")
            df_jobs_unknown = df_jobs_unknown[['company_title']].drop_duplicates()
            print()
            print(df_jobs_unknown)
            print()

            print("Number of companies needs to add to database:", len(df_jobs_unknown))
            print()

            print("df_companies_sv")
            print(df_companies_sv)
            print()

            print("Add company_id for new companies")
            max_value_company_id = df_companies_sv['company_id'].map(lambda x: int(x[1:])).max()
            df_jobs_unknown['company_id'] = range(max_value_company_id+1, max_value_company_id+1+len(df_jobs_unknown))
            df_jobs_unknown['company_id'] = df_jobs_unknown['company_id'].map(lambda x: "C" + "0"*(5-len(str(x))) + str(x))
            df_jobs_unknown = df_jobs_unknown[['company_id', 'company_title']]
            print("Done")
            print()
            print(df_jobs_unknown)
            print()

            print("Concatenate df_jobs_unknown to df_companies_sv")
            df_companies_sv = pd.concat([df_companies_sv, df_jobs_unknown], ignore_index=False)
            print("Done")
            print()
            print(df_companies_sv)
            print()

            print("Get additional columns: \"company_url\" and \"company_video_url\" columns")
            df_jobs_unknown = df_jobs_unknown.merge(df[['company_title', 'company_url', 'company_video_url']], on='company_title',
                                                    how='left')
            print()
            print(df_jobs_unknown)
            print()

            print("Processing \"company_url\" and \"company_video_url\" columns to get unique \"company_id\" and \"company_title\" columns")
            df_jobs_unknown = df_jobs_unknown.groupby(['company_id'], as_index=False).agg({"company_title": "first",
                                                                        "company_url": filter_company_video_url,
                                                                        "company_video_url": filter_company_video_url})
            df_jobs_unknown = df_jobs_unknown.fillna("Unknown")
            print()
            print(df_jobs_unknown)
            print()
        else:
            df_jobs_unknown = pd.DataFrame({"company_id": [], "company_title": [], "company_url": [], "company_video_url": [], })

        print("Save df_jobs_unknown to \"companies.csv\"")
        df_jobs_unknown.to_csv(os.path.join(link_to_output, r"companies.csv"), index=False)
        print()


        # In[37]:


        print("df_jobs merge with df_companies_sv")
        df_jobs = df_jobs.merge(df_companies_sv, on='company_title', how='left')
        print("Done")
        print()
        print(df_jobs)
        print()


        # In[38]:


        # Processing df_jobs is done -> save location_job.csv
        print("Save df_jobs to \"location_job.csv\"")
        df_jobs = df_jobs.fillna("Unknown")
        df_jobs.to_csv(os.path.join(link_to_output, r"location_job.csv"), index=False)
        print()


        # In[39]:


        print("Find unknown cities")
        set_unknown_cities = set(df_jobs['City'].unique()) - set(df_city['City'].unique())
        print(set_unknown_cities)
        print()


        # In[40]:


        print("Save unknown_cities to \"city_country.csv\"")
        pd.DataFrame({"City": list(set_unknown_cities), "Country": [np.NaN for i in range(len(list(set_unknown_cities)))]})    .to_csv(os.path.join(link_to_output, r"city_country.csv"), index=False)
        print()


        # In[41]:


        df_companies_sv


        # In[42]:


        print("Merge with df_companies_sv")
        df = df.merge(df_companies_sv, on='company_title',how='left')
        print("Done")
        print()
        df.info()
        print()


        # In[43]:


        print("Drop \"location\", \"company_title\", \"company_url\" and \"company_video_url\" columns")
        df = df.drop(['location', 'company_title', 'company_url', 'company_video_url'], axis=1)
        print("Done")
        print()
        df.info()
        print()


        # In[44]:


        print("1.3) Processing \"outstanding_welfare\" column")
        print()


        # In[45]:


        print("outstanding_welfare in raw data:")
        for outstanding_welfare in set(reduce(lambda x, y: x+y, df['outstanding_welfare'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | ")))).values)):
            print("  '" + outstanding_welfare + "'")
        print()


        # In[46]:


        print("Repair wrong text")
        df['outstanding_welfare'] = df['outstanding_welfare'].map(lambda x: x.replace("hiểểm", "hiểm"))
        print("Done")
        print()


        # In[47]:


        print("outstanding_welfare in raw data:")
        for outstanding_welfare in set(reduce(lambda x, y: x+y, df['outstanding_welfare'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | ")))).values)):
            print("  '" + outstanding_welfare + "'")
        print()


        # In[48]:


        print("Processing \"outstanding_welfare\" column")
        df['outstanding_welfare'] = df['outstanding_welfare'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))
        print("Done")
        print()


        # In[49]:


        print('Explode df_outstanding_welfare dataset')
        df_outstanding_welfare = df[['job_id', 'outstanding_welfare']].explode('outstanding_welfare')
        print("Done")
        print()
        print(df_outstanding_welfare)
        print()


        # In[50]:


        print("Save df_outstanding_welfare to \"outstanding_welfare_job.csv\"")
        df_outstanding_welfare.to_csv(os.path.join(link_to_output, r"outstanding_welfare_job.csv"), index=False)
        print()


        # In[51]:


        print("Drop \"outstanding_welfare\" column")
        df = df.drop(['outstanding_welfare'], axis=1)
        print("Done")
        print()
        df.info()
        print()


        # In[52]:


        print("1.4) Processing \"detailed_welfare\" column")
        print()


        # In[53]:


        print("detailed_welfare in raw data:")
        for detailed_welfare in set(reduce(lambda x, y: x+y, df['detailed_welfare'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | ")))).values)):
            print("  '" + detailed_welfare + "'")
        print()


        # In[54]:


        print("Processing \"detailed_welfare\" column")
        df['detailed_welfare'] = df['detailed_welfare'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))
        print("Done")
        print()


        # In[55]:


        print('Explode df_detailed_welfare dataset')
        df_detailed_welfare = df[['job_id', 'detailed_welfare']].explode('detailed_welfare')
        print("Done")
        print()
        print(df_detailed_welfare)
        print()


        # In[56]:


        print("Save df_detailed_welfare to \"detailed_welfare_job.csv\"")
        df_detailed_welfare.to_csv(os.path.join(link_to_output, r"detailed_welfare_job.csv"), index=False)
        print()


        # In[57]:


        print("Drop \"detailed_welfare\" column")
        df = df.drop(['detailed_welfare'], axis=1)
        print("Done")
        print()
        df.info()
        print()


        # In[58]:


        print("1.5) Processing \"nganh_nghe\" column")
        print()


        # In[59]:


        print("nganh_nghe in raw data:")
        for nganh_nghe in set(reduce(lambda x, y: x+y, df['nganh_nghe'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | ")))).values)):
            print("  '" + nganh_nghe + "'")
        print()


        # In[60]:


        print("Processing \"nganh_nghe\" column")
        df['nganh_nghe'] = df['nganh_nghe'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))
        print("Done")
        print()


        # In[61]:


        print('Explode df_nganh_nghe dataset')
        df_nganh_nghe = df[['job_id', 'nganh_nghe']].explode('nganh_nghe')
        print("Done")
        print()
        print(df_nganh_nghe)
        print()


        # In[62]:


        print("Save df_nganh_nghe to \"nganh_nghe_job.csv\"")
        df_nganh_nghe.to_csv(os.path.join(link_to_output, r"nganh_nghe_job.csv"), index=False)
        print()


        # In[63]:


        print("Drop \"nganh_nghe\" column")
        df = df.drop(['nganh_nghe'], axis=1)
        print("Done")
        print()
        df.info()
        print()


        # In[64]:


        print("1.6) Processing \"job_tags\" column")
        print()


        # In[65]:


        print("job_tags in raw data:")
        for job_tags in set(reduce(lambda x, y: x+y, df['job_tags'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | ")))).values)):
            print("  '" + job_tags + "'")
        print()


        # In[66]:


        print("Processing \"job_tags\" column")
        df['job_tags'] = df['job_tags'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))
        print("Done")
        print()


        # In[67]:


        print('Explode df_job_tags dataset')
        df_job_tags = df[['job_id', 'job_tags']].explode('job_tags')
        print("Done")
        print()
        print(df_job_tags)
        print()


        # In[68]:


        print("Save df_job_tags to \"job_tags_job.csv\"")
        df_job_tags.to_csv(os.path.join(link_to_output, r"job_tags_job.csv"), index=False)
        print()


        # In[69]:


        print("Drop \"job_tags\" column")
        df = df.drop(['job_tags'], axis=1)
        print("Done")
        print()
        df.info()
        print()


        # In[70]:


        print("1.7) Processing \"salary\" column")
        print()


        # In[71]:


        print("Test if all values in \"saraly\" column start with 'Lương: '")
        print(df[df['salary'].map(lambda x: x[:7]!='Lương: ')])
        print()


        # In[72]:


        print("Test if remove 7 characters at beginning (x[7:]), and get unique values")
        for i in sorted(df['salary'].map(lambda x: x[7:]).unique()):
            print(i)
        print()


        # In[73]:


        # Số...
        # Cạnh tranh
        # Dưới...
        # Trên...
        print("Processing \"salary\" column: remove 7 characters at beginning (x[7:])")
        df['salary'] = df['salary'].map(lambda x: x[7:])
        print("Done")
        print()
        print(df['salary'])
        print()


        # In[74]:


        print("Processing \"salary\" column: processing with processing_salary function")
        df['salary'] = df['salary'].map(processing_salary)
        print("Done")
        print()


        # In[75]:


        print("Split \"salary\" column into two columns: \"lb_salary\" column and \"ub_salary\" columns")
        df['lb_salary'] = df['salary'].map(lambda x: x[0])
        df['ub_salary'] = df['salary'].map(lambda x: x[1])


        # In[76]:


        print("Drop \"salary\" column")
        df = df.drop(['salary'], axis=1)
        print("Done")
        print()
        df.info()
        print()


        # In[77]:


        print("1.8) Processing \"announcement_date\" column")
        print()


        # In[78]:


        print("Check min and max of \"announcement_date\" column")
        print("Min:", df['announcement_date'].map(len).min())
        print("Max:", df['announcement_date'].map(len).max())


        # In[79]:


        print("Processing \"announcement_date\" column: rearrange date format YYYY-MM-DD")
        df['announcement_date'] = df['announcement_date'].map(lambda x: x[-4:] + "-" + x[3:5] + "-" + x[:2])
        print("Done")
        print()


        # In[80]:


        print(df['announcement_date'])
        print()


        # In[81]:


        print("1.9) Processing \"hinh_thuc\" column")
        print()


        # In[82]:


        print('Check unique values in \"hinh_thuc\" column')
        print(df['hinh_thuc'].unique())
        print()


        # In[83]:


        print("hinh_thuc in raw data:")
        for hinh_thuc in set(reduce(lambda x, y: x+y, df['hinh_thuc'].map(lambda x: list(map(lambda y: y.strip(), x.split(", ")))).values)):
            print("  '" + hinh_thuc + "'")
        print()


        # In[84]:


        print("Processing \"hinh_thuc\" column")
        df['hinh_thuc'] = df['hinh_thuc'].map(lambda x: list(map(lambda y: y.strip(), x.split(", "))))
        print("Done")
        print()


        # In[85]:


        print('Explode df_hinh_thuc dataset')
        df_hinh_thuc = df[['job_id', 'hinh_thuc']].explode('hinh_thuc')
        print("Done")
        print()
        print(df_hinh_thuc)
        print()


        # In[86]:


        print("Save df_hinh_thuc to \"hinh_thuc_job.csv\"")
        df_hinh_thuc.to_csv(os.path.join(link_to_output, r"hinh_thuc_job.csv"), index=False)
        print()


        # In[87]:


        print("Drop \"hinh_thuc\" column")
        df = df.drop(['hinh_thuc'], axis=1)
        print("Done")
        print()
        df.info()
        print()


        # In[88]:


        print("1.10) Processing \"expiration_date\" column")
        print()


        # In[89]:


        print("Copying \"expiration_date\" column")
        df['expiration_date_2'] = df['expiration_date']
        print("Done")
        print()


        # In[90]:


        print("Check \"expiration_date\" that does not have format DD/MM/YYYY")
        idx_repaired = df[df['expiration_date'].map(lambda x: not bool(re.match(r"(^[\d]{2}\/[\d]{2}\/[\d]{4}$)", x)))]['expiration_date'].index
        print("Number of error rows:", len(idx_repaired))
        print()


        # In[91]:


        if len(idx_repaired):
            print("Mode repairing \"expiration_date\"")
            print("Unique error values:")
            print(df.loc[idx_repaired, 'expiration_date'].unique())
            print()

            print("Try to repair from \"kinh_nghiem\"")
            df_2 = df.loc[idx_repaired, ['kinh_nghiem']]
            print()
            print(df_2)
            print()

            print("Check if any \"kinh_nghiem\" have format of DD/MM/YYYY")
            idx_repaired_2 = df_2[df_2['kinh_nghiem'].map(lambda x: bool(re.match(r"(^[\d]{2}\/[\d]{2}\/[\d]{4}$)", x)))].index
            print()
            print("Number of satisfied kinh_nghiem rows:", len(idx_repaired_2))
            print()

            if len(idx_repaired_2):
                df, idx_repaired = replace_col_by_col(df, 'expiration_date', 'kinh_nghiem', idx_repaired, idx_repaired_2)
            else:
                print("No satisfied kinh_nghiem row to repair")
            
            if len(idx_repaired):
                print("Try to repair from \"cap_bac\"")
                df_2 = df.loc[idx_repaired, ['cap_bac']]
                print()
                print(df_2)
                print()

                print("Check if any \"cap_bac\" have format of DD/MM/YYYY")
                idx_repaired_2 = df_2[df_2['cap_bac'].map(lambda x: bool(re.match(r"(^[\d]{2}\/[\d]{2}\/[\d]{4}$)", x)))].index
                print()
                print("Number of satisfied cap_bac rows:", len(idx_repaired_2))
                print()

                if len(idx_repaired_2):
                    df, idx_repaired = replace_col_by_col(df, 'expiration_date', 'cap_bac', idx_repaired, idx_repaired_2)
                else:
                    print("No satisfied cap_bac row to repair")
            else:
                pass
        else:
            print("No error format in \"expiration_date\"")


        # In[92]:


        print("Check min max of \"expiration_date\" column")
        print("Min:", df['expiration_date'].map(len).min())
        print("Max:", df['expiration_date'].map(len).max())
        print()


        # In[93]:


        print("Check len not 10 in \"expiration_date\" column")
        print(df[df['expiration_date'].map(lambda x: len(x) != 10)]['expiration_date'].unique())
        print()


        # In[94]:


        print("Check len 10 in \"expiration_date\" column")
        print(df[df['expiration_date'].map(lambda x: len(x) == 10)]['expiration_date'].unique())
        print()


        # In[95]:


        print("Processing \"expiration_date\" column:")
        df['expiration_date'] = df['expiration_date'].map(lambda x: (x[-4:] + "-" + x[3:5] + "-" + x[:2]) if (x!="Unknown") else x)
        print("Done")
        print()


        # In[96]:


        print(df['expiration_date'].unique())
        print()


        # In[97]:


        print("1.11) Processing \"cap_bac\" column")
        print()


        # In[98]:


        print("Existing cap_bac in server")
        for i in cap_bac:
            print(" '" + i + "'")
        print()


        # In[99]:


        print("Raw cap_bac in raw data")
        raw_cap_bac = list(filter(lambda x: x!="Unknown", df['cap_bac'].unique()))
        for i in raw_cap_bac:
            print(" '" + i + "'")
        print()


        # In[100]:


        print("Check Unknown \"cap_bac\"")
        idx_repaired = df[df['cap_bac']=='Unknown'].index
        print("Number of unknown \"cap_bac\":", len(idx_repaired))
        print()


        # In[101]:


        if len(idx_repaired):
            print("Mode repairing \"cap_bac\"")
            print("Try to repair from \"kinh_nghiem\"")
            df_2 = df.loc[idx_repaired, ['kinh_nghiem']]
            print()
            print(df_2)
            print()

            print("Check if any \"kinh_nghiem\" in list of raw_cap_bac")
            idx_repaired_2 = df_2[df_2['kinh_nghiem'].isin(raw_cap_bac)].index
            print()
            print("Number of satisfied kinh_nghiem rows:", len(idx_repaired_2))
            print()

            if len(idx_repaired_2):
                df, idx_repaired = replace_col_by_col(df, 'cap_bac', 'kinh_nghiem', idx_repaired, idx_repaired_2)
            else:
                print("No satisfied kinh_nghiem row to repair")

            if len(idx_repaired):
                print("Try to repair from \"expiration_date_2\"")
                df_2 = df.loc[idx_repaired, ['expiration_date_2']]
                print()
                print(df_2)
                print()

                print("Check if any \"expiration_date_2\" in list of raw_cap_bac")
                idx_repaired_2 = df_2[df_2['expiration_date_2'].isin(raw_cap_bac)].index
                print()
                print("Number of satisfied expiration_date_2 rows:", len(idx_repaired_2))
                print()

                if len(idx_repaired_2):
                    df, idx_repaired = replace_col_by_col(df, 'cap_bac', 'expiration_date_2', idx_repaired, idx_repaired_2)
                else:
                    print("No satisfied expiration_date_2 row to repair")
            else:
                print("No unknown \"cap_bac\"")
        else:
            print("No unknown \"cap_bac\"")


        # In[102]:


        print("Check unique values in \"cap_bac\"")
        print(df['cap_bac'].unique())
        print()


        # In[103]:


        cap_bac


        # In[104]:


        raw_cap_bac


        # In[105]:


        print("Check new cap_bac")
        new_cap_bac = list(set(raw_cap_bac) - set(cap_bac))
        print(new_cap_bac)
        print()


        # In[106]:


        if len(new_cap_bac):
            print("Some new_cap_bac exists")
            print("ordered_cap_bac in server")
            print(df_ordered_cap_bac)
            print()

            print("Create new ordered_cap_bac")
            df_ordered_cap_bac_new = pd.DataFrame({"cap_bac": new_cap_bac})
            print(df_ordered_cap_bac_new)
            print()

            print("Create STT for new cap_bac")
            df_ordered_cap_bac_new['STT'] = range(len(df_ordered_cap_bac)+1, len(df_ordered_cap_bac)+1+len(new_cap_bac))
            print(df_ordered_cap_bac_new)
            print()
        else:
            print("There is no new_cap_bac")
            print("Create empty new ordered_cap_bac")
            df_ordered_cap_bac_new = pd.DataFrame({"cap_bac": [], "STT": []})
            print(df_ordered_cap_bac_new)
            print()


        # In[107]:


        print("Save df_ordered_cap_bac_new to \"ordered_cap_bac.csv\"")
        df_ordered_cap_bac_new.to_csv(os.path.join(link_to_output, r"ordered_cap_bac.csv"), index=False)
        print()


        # In[108]:


        print("1.12) Processing \"kinh_nghiem\" column")
        print()


        # In[109]:


        print("Unique values in \"kinh_nghiem\" column")
        for i in sorted(df['kinh_nghiem'].unique()):
            print(" '" + i + "'")


        # In[110]:


        print("Check after removing all known patterns")
        for i in sorted(df[df['kinh_nghiem'].map(lambda x: not (bool(re.match(r"(^[\d]+ - [\d]+ Năm)", x))
                                                        or bool(re.match(r"(^[\d]+ Năm)", x))
                                                        or bool(re.match(r"(^Dưới [\d]+Năm)", x))
                                                        or bool(re.match(r"(^Trên [\d]+ Năm)", x))))]['kinh_nghiem'].unique()):
            print(" '" + i + "'")


        # In[111]:


        df[df['kinh_nghiem'].isin(['15/03/2023', '22/03/2023'])]


        # In[112]:


        print("Processing \"kinh_nghiem\" column")
        df['kinh_nghiem'] = df['kinh_nghiem'].map(processing_kinh_nghiem)
        print(df['kinh_nghiem'])
        print()


        # In[113]:


        print("Split \"kinh_nghiem\" column into two columns: \"lb_kinh_nghiem\" column and \"ub_kinh_nghiem\" column")
        df['lb_kinh_nghiem'] = df['kinh_nghiem'].map(lambda x: x[0])
        df['ub_kinh_nghiem'] = df['kinh_nghiem'].map(lambda x: x[1])
        print()


        # In[114]:


        print("Drop \"kinh_nghiem\" column")
        df = df.drop(['kinh_nghiem'], axis=1)


        # In[115]:


        df.info()
        print()


        # In[116]:


        print("Drop \"expiration_date_2\" column")
        df = df.drop(['expiration_date_2'], axis=1)
        df.info()
        print()


        # In[117]:


        print("Check fill Unknown for \"lb_kinh_nghiem\" column and \"ub_kinh_nghiem\" column")
        idx_repaired = df[df['ub_kinh_nghiem'].map(lambda x: True if not isinstance(x, int) else False)].index
        print("Number of unknown kinh nghiem:", len(idx_repaired))
        print()


        # In[118]:


        if len(idx_repaired):
            print("Unique unknown values in \"lb_kinh_nghiem\" column")
            print(df.loc[idx_repaired]['lb_kinh_nghiem'].unique())
            print()
            print("Unique unknown values in \"ub_kinh_nghiem\" column")
            print(df.loc[idx_repaired]['ub_kinh_nghiem'].unique())
            print()

            print("Fill Unknown for \"lb_kinh_nghiem\" column and \"ub_kinh_nghiem\" column")
            df.loc[idx_repaired, "lb_kinh_nghiem"] = "Unknown"
            df.loc[idx_repaired, "ub_kinh_nghiem"] = "Unknown"
            print("Done")
            print()
        else:
            print("There is no unknown kinh nghiem")



    else:
        print("All job_id in raw data have existed in server.")
        print(" -> No processing pipeline needed.")


    # In[120]:


    print("Summary warning")
    if len(set_unknown_cities):
        print(f"Exists {len(set_unknown_cities)} new cities: {set_unknown_cities}")
    if len(new_cap_bac):
        print(f"Exists {len(new_cap_bac)} new cap_bac: {new_cap_bac}")

