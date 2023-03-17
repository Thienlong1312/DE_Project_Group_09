import re
import os
import sys
from functools import reduce
import traceback
import pandas as pd

class DataTransformer():
    def __init__(self, df, df_database):
        self.df = df
        self.df_database = df_database

        '''
        x = 'Dưới50,8 Tr VND'
        y = 'Dưới500,000 VND'
        z = '9,2 Tr - 30,2 Tr VND'
        t = 'Trên 17,6 Tr VND'
        processing_salary(y)
        '''
        self.pattern_so = r"(^[\d]+(,[\d]+)? (Tr )?- [\d]+(,[\d]+)? (Tr )?VND)"
        self.pattern_duoi = r"^Dưới[\d]+(,[\d]+)? (Tr )?VND"
        self.pattern_tren = r"^Trên [\d]+(,[\d]+)? (Tr )?VND"
        self.unknown_salary = "Cạnh tranh"

        """
        x = '10 - 11 Năm'
        y = '10 Năm'
        z = 'Dưới 10Năm'
        t = 'Trên 12 Năm'
        processing_kinh_nghiem(x)
        """
        self.pattern_1 = r"(^[\d]+ - [\d]+ Năm)"     # ? - ? Năm
        self.pattern_2 = r"(^[\d]+ Năm)"             # ? Năm
        self.pattern_3 = r"(^Dưới [\d]+Năm)"         # Dưới ?Năm
        self.pattern_4 = r"(^Trên [\d]+ Năm)"        # Trên ? Năm
        self.zero_kn = "Chưa có kinh nghiệm"

    def filter_company_video_url(self, series_company_video_url):
        lst_company_video_url = list(filter(lambda x: (x != "Unknown") and bool(x) and (not isinstance(x, float)),
                                            series_company_video_url))
        if len(lst_company_video_url):
            return lst_company_video_url[0]
        else:
            return series_company_video_url.values[0]

    def processing_salary(self, x):
        if x == self.unknown_salary:
            return ['Unknown', 'Unknown']
        elif bool(re.match(self.pattern_so, x)):
            try:
                re_match_obj = re.match(r"^[\d]+(,[\d]+)? (Tr )?", x)
                idx_s_lb_salary = re_match_obj.start()
                idx_e_lb_salary = re_match_obj.end()
                lb_salary = x[idx_s_lb_salary:idx_e_lb_salary].strip()
                lb_salary = float(lb_salary.replace(",", ""))/1_000_000 if (lb_salary[-3:]!=" Tr")\
                    else float(lb_salary[:-3].replace(",", "."))
                re_search_obj = re.search(r"[\d]+(,[\d]+)? (Tr )?", x[idx_e_lb_salary:])
                idx_s_ub_salary = re_search_obj.start()
                idx_e_ub_salary = re_search_obj.end()
                ub_salary = x[idx_e_lb_salary+idx_s_ub_salary:idx_e_lb_salary+idx_e_ub_salary].strip()
                ub_salary = float(ub_salary.replace(",", ""))/1_000_000 if (ub_salary[-3:]!=" Tr")\
                    else float(ub_salary[:-3].replace(",", "."))
                return [lb_salary, ub_salary]
            except:
                return [x, '']
        elif bool(re.match(self.pattern_duoi, x)):
            try:
                re_search_obj = re.search(r"[\d]+(,[\d]+)? (Tr )?", x)
                idx_s_ub_salary = re_search_obj.start()
                idx_e_ub_salary = re_search_obj.end()
                ub_salary = x[idx_s_ub_salary:idx_e_ub_salary].strip()
                ub_salary = float(ub_salary.replace(",", ""))/1_000_000 if (ub_salary[-3:]!=" Tr")\
                    else float(ub_salary[:-3].replace(",", "."))
                lb_salary = ub_salary * 0.32
                return [lb_salary, ub_salary]
            except:
                return [x, '']
        elif bool(re.match(self.pattern_tren, x)):
            try:
                re_search_obj = re.search(r"[\d]+(,[\d]+)? (Tr )?", x)
                idx_s_lb_salary = re_search_obj.start()
                idx_e_lb_salary = re_search_obj.end()
                lb_salary = x[idx_s_lb_salary:idx_e_lb_salary].strip()
                lb_salary = float(lb_salary.replace(",", ""))/1_000_000 if (lb_salary[-3:]!=" Tr")\
                    else float(lb_salary[:-3].replace(",", "."))
                ub_salary = lb_salary * 1.4
                return [lb_salary, ub_salary]
            except:
                return [x, '']
        else:
            return [x, '']

    def replace_col_by_col(self, df, replaced_col, replacing_col, idx_repaired, idx_repaired_2):
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

    def processing_kinh_nghiem(self, x):
        if x == self.zero_kn:
            return [0, 0]
        elif bool(re.match(self.pattern_1, x)):
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
        elif bool(re.match(self.pattern_2, x)):
            try:
                re_match_obj = re.match(r"^[\d]+", x)
                idx_s_lb_kn = re_match_obj.start()
                idx_e_lb_kn = re_match_obj.end()
                lb_kn = x[idx_s_lb_kn:idx_e_lb_kn].strip()
                lb_kn = int(lb_kn)
                return [lb_kn, lb_kn]
            except:
                return [x, '']
        elif bool(re.match(self.pattern_3, x)):
            try:
                re_match_obj = re.match(r"^[\d]+", x[5:])
                idx_s_ub_kn = re_match_obj.start()
                idx_e_ub_kn = re_match_obj.end()
                ub_kn = x[5+idx_s_ub_kn:5+idx_e_ub_kn].strip()
                ub_kn = int(ub_kn)
                return [0, ub_kn]
            except:
                return [x, '']
        elif bool(re.match(self.pattern_4, x)):
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

    def processing_raw_data(self):
        try:
            
            df_database = self.df_database
            df_companies_sv = df_database.get("companies").copy()
            df_job_id = df_database.get("jobs").copy()
            df_ordered_cap_bac = df_database.get("ordered_cap_bac").copy()
            cap_bac = df_database.get("cap_bac").copy()
            df_city = df_database.get("city_country").copy()

            # choose_city_country_df = pd.read_csv(".\etl\cleansed_data_cb\city_country.csv")
            # choose_companies_df = pd.read_csv(".\etl\cleansed_data_cb\companies.csv")
            # choose_detailed_welfare_job_df = pd.read_csv(".\etl\cleansed_data_cb\detailed_welfare_job.csv")
            # choose_hinh_thuc_job_df = pd.read_csv(".\etl\cleansed_data_cb\hinh_thuc_job.csv")
            # choose_job_tags_job_df = pd.read_csv(".\etl\cleansed_data_cb\job_tags_job.csv")
            # choose_jobs_df = pd.read_csv(".\etl\cleansed_data_cb\jobs.csv")
            # choose_location_job_df = pd.read_csv(".\etl\cleansed_data_cb\location_job_df.csv")
            # choose_nganh_nghe_job_df = pd.read_csv(".\etl\cleansed_data_cb\nganh_nghe_job.csv")
            # choose_ordered_cap_bac_df = pd.read_csv(".\etl\cleansed_data_cb\ordered_cap_bac.csv")
            # choose_outstanding_welfare_df = pd.read_csv(".\etl\cleansed_data_cb\outstanding_welfare.csv")

            df = (self.df).copy()

            # print("ORIGINAL", df.head())

            df = df.dropna(subset=['job_id'])

            df = df.fillna("Unknown")

            for col in df:
                df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)

            # print("HEAD", df.head())

            # df.info()
            # print()

            # print("Drop NaN job_id")
            # print(f"Total rows before drop: {len(df)}")
            # df = df.dropna(subset=['job_id'])
            # print("Done")
            # print(f"Total rows before drop: {len(df)}")
            # print()

            # print("Fill Unknown for all NaN values")
            # df = df.fillna("Unknown")
            # print("Done")
            # print()
            # df.info()
            # print()

            # print("Strip all cell values")
            # for col in df:
            #     df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
            # print("Done")
            # print()

            
            # # Assum has new raw data
            # df = df[~df['job_id'].isin(df_job_id['job_id'])]

            # ======================================
            # ==================================
            # Assum has new raw data
            # ==================================
            # ======================================

            if len(df):
                print("Number of job_id unique:", df['job_id'].unique().size, "unique values")
                print("Check number of job_id unique:")
                if len(df) == df['job_id'].unique().size:
                    print(True)
                else:
                    raise Exception("Number of rows in combined_raw_data (df) does not equal to number of job_id unique")

                df['location'] = df['location'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))

                df_jobs = df[['location', 'job_id', 'company_title']].explode('location')\
                    .rename(columns={"location": "City"})
                
                if len(df_jobs) == len(reduce(lambda x, y: x+y, df['location'].values)):
                    print(True)
                else:
                    raise Exception("Number of rows in df_jobs does not equal to total number of cities in raw data")

                df_jobs = df_jobs.fillna('Unknown')

                if len(df_jobs) == len(df_jobs.drop_duplicates()):
                    print(True)
                else:
                    raise Exception("Number of rows in df_jobs does not equal to number of rows in df_jobs.drop_duplicates()")

                df_jobs_unknown = df_jobs[~df_jobs['company_title'].isin(df_companies_sv['company_title'].values)]

                if len(df_jobs_unknown):
                    df_jobs_unknown = df_jobs_unknown[['company_title']].drop_duplicates()
                    max_value_company_id = df_companies_sv['company_id'].map(lambda x: int(x[1:])).max()
                    max_value_company_id = max_value_company_id if not isinstance(max_value_company_id, float) else 0
                    df_jobs_unknown['company_id'] = range(max_value_company_id+1, max_value_company_id+1+len(df_jobs_unknown))
                    df_jobs_unknown['company_id'] = df_jobs_unknown['company_id'].map(lambda x: "C" + "0"*(5-len(str(x))) + str(x))
                    df_jobs_unknown = df_jobs_unknown[['company_id', 'company_title']]
                    df_companies_sv = pd.concat([df_companies_sv, df_jobs_unknown], ignore_index=False)
                    df_jobs_unknown = df_jobs_unknown.merge(df[['company_title', 'company_url', 'company_video_url']], on='company_title',
                                                            how='left')
                    df_jobs_unknown = df_jobs_unknown.groupby(['company_id'], as_index=False).agg({"company_title": "first",
                                                                                "company_url": self.filter_company_video_url,
                                                                                "company_video_url": self.filter_company_video_url})
                    df_jobs_unknown = df_jobs_unknown.fillna("Unknown")
                else:
                    df_jobs_unknown = pd.read_csv(".\etl\cleansed_data_cb\companies.csv")


                choose_companies_df = df_jobs_unknown.copy()

                df_jobs = df_jobs.merge(df_companies_sv, on='company_title', how='left')

                df_jobs = df_jobs.drop(['company_title'], axis=1)

                # Processing df_jobs is done -> save location_job.csv
                df_jobs = df_jobs.fillna("Unknown")
                
                choose_location_job_df = df_jobs.copy()

                set_unknown_cities = set(df_jobs['City'].unique()) - set(df_city['City'].unique())

                choose_city_country_df = pd.DataFrame({"City": list(set_unknown_cities), "Country": ["Vietnam" for i in range(len(list(set_unknown_cities)))]})

                df = df.merge(df_companies_sv, on='company_title',how='left')

                df = df.drop(['location', 'company_title', 'company_url', 'company_video_url'], axis=1)

                df['outstanding_welfare'] = df['outstanding_welfare'].map(lambda x: x.replace("hiểểm", "hiểm"))

                df['outstanding_welfare'] = df['outstanding_welfare'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))

                df_outstanding_welfare = df[['job_id', 'outstanding_welfare']].explode('outstanding_welfare')

                choose_outstanding_welfare_df = df_outstanding_welfare.copy()

                df = df.drop(['outstanding_welfare'], axis=1)

                df['detailed_welfare'] = df['detailed_welfare'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))
  
                df_detailed_welfare = df[['job_id', 'detailed_welfare']].explode('detailed_welfare')

                choose_detailed_welfare_job_df = df_detailed_welfare.copy()

                df = df.drop(['detailed_welfare'], axis=1)

                df['nganh_nghe'] = df['nganh_nghe'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))

                df_nganh_nghe = df[['job_id', 'nganh_nghe']].explode('nganh_nghe')

                choose_nganh_nghe_job_df = df_nganh_nghe.copy()

                df = df.drop(['nganh_nghe'], axis=1)

                df['job_tags'] = df['job_tags'].map(lambda x: list(map(lambda y: y.strip(), x.split(" | "))))

                df_job_tags = df[['job_id', 'job_tags']].explode('job_tags')

                choose_job_tags_job_df = df_job_tags.copy()

                df = df.drop(['job_tags'], axis=1)

                df['salary'] = df['salary'].map(lambda x: x[7:])

                df['salary'] = df['salary'].map(self.processing_salary)

                df['lb_salary'] = df['salary'].map(lambda x: x[0])
                df['ub_salary'] = df['salary'].map(lambda x: x[1])

                df = df.drop(['salary'], axis=1)
                
                df['announcement_date'] = df['announcement_date'].map(lambda x: x[-4:] + "-" + x[3:5] + "-" + x[:2])

                df['hinh_thuc'] = df['hinh_thuc'].map(lambda x: list(map(lambda y: y.strip(), x.split(", "))))

                df_hinh_thuc = df[['job_id', 'hinh_thuc']].explode('hinh_thuc')

                choose_hinh_thuc_job_df = df_hinh_thuc.copy()

                df = df.drop(['hinh_thuc'], axis=1)

                df['expiration_date_2'] = df['expiration_date']

                idx_repaired = df[df['expiration_date'].map(lambda x: not bool(re.match(r"(^[\d]{2}\/[\d]{2}\/[\d]{4}$)", x)))]['expiration_date'].index


                if len(idx_repaired):

                    df_2 = df.loc[idx_repaired, ['kinh_nghiem']]

                    idx_repaired_2 = df_2[df_2['kinh_nghiem'].map(lambda x: bool(re.match(r"(^[\d]{2}\/[\d]{2}\/[\d]{4}$)", x)))].index
          
                    if len(idx_repaired_2):
                        df, idx_repaired = self.replace_col_by_col(df, 'expiration_date', 'kinh_nghiem', idx_repaired, idx_repaired_2)
                    else:
                        print("No satisfied kinh_nghiem row to repair")

                    if len(idx_repaired):
                        df_2 = df.loc[idx_repaired, ['cap_bac']]
                        idx_repaired_2 = df_2[df_2['cap_bac'].map(lambda x: bool(re.match(r"(^[\d]{2}\/[\d]{2}\/[\d]{4}$)", x)))].index

                        if len(idx_repaired_2):
                            df, idx_repaired = self.replace_col_by_col(df, 'expiration_date', 'cap_bac', idx_repaired, idx_repaired_2)
                        else:
                            print("No satisfied cap_bac row to repair")
                    else:
                        pass
                else:
                    print("No error format in \"expiration_date\"")

                df['expiration_date'] = df['expiration_date'].map(lambda x: (x[-4:] + "-" + x[3:5] + "-" + x[:2]) if (x!="Unknown") else x)

                raw_cap_bac = list(filter(lambda x: x!="Unknown", df['cap_bac'].unique()))

                idx_repaired = df[df['cap_bac']=='Unknown'].index

                if len(idx_repaired):
                    df_2 = df.loc[idx_repaired, ['kinh_nghiem']]

                    idx_repaired_2 = df_2[df_2['kinh_nghiem'].isin(raw_cap_bac)].index
   

                    if len(idx_repaired_2):
                        df, idx_repaired = self.replace_col_by_col(df, 'cap_bac', 'kinh_nghiem', idx_repaired, idx_repaired_2)
                    else:
                        print("No satisfied kinh_nghiem row to repair")

                    if len(idx_repaired):
         
                        df_2 = df.loc[idx_repaired, ['expiration_date_2']]

               
                        idx_repaired_2 = df_2[df_2['expiration_date_2'].isin(raw_cap_bac)].index
              

                        if len(idx_repaired_2):
                            df, idx_repaired = self.replace_col_by_col(df, 'cap_bac', 'expiration_date_2', idx_repaired, idx_repaired_2)
                        else:
                            print("No satisfied expiration_date_2 row to repair")
                    else:
                        print("No unknown \"cap_bac\"")
                else:
                    print("No unknown \"cap_bac\"")

                new_cap_bac = list(set(raw_cap_bac) - set(cap_bac))

                if len(new_cap_bac):

                    df_ordered_cap_bac_new = pd.DataFrame({"cap_bac": new_cap_bac})

                    df_ordered_cap_bac_new['STT'] = range(len(df_ordered_cap_bac)+1, len(df_ordered_cap_bac)+1+len(new_cap_bac))

                else:
                    df_ordered_cap_bac_new = pd.DataFrame({"cap_bac": [], "STT": []})

                choose_ordered_cap_bac_df = df_ordered_cap_bac_new.copy()

                df['kinh_nghiem'] = df['kinh_nghiem'].map(self.processing_kinh_nghiem)

                df['lb_kinh_nghiem'] = df['kinh_nghiem'].map(lambda x: x[0])
                df['ub_kinh_nghiem'] = df['kinh_nghiem'].map(lambda x: x[1])

                df = df.drop(['kinh_nghiem'], axis=1)

                df = df.drop(['expiration_date_2'], axis=1)

                idx_repaired = df[df['ub_kinh_nghiem'].map(lambda x: True if not isinstance(x, int) else False)].index

                if len(idx_repaired):

                    df.loc[idx_repaired, "lb_kinh_nghiem"] = "Unknown"
                    df.loc[idx_repaired, "ub_kinh_nghiem"] = "Unknown"
                else:
                    print("There is no unknown kinh nghiem")
                
                print("Summary warning")
                if len(set_unknown_cities):
                    print(f"Exists {len(set_unknown_cities)} new cities: {set_unknown_cities}")
                if len(new_cap_bac):
                    print(f"Exists {len(new_cap_bac)} new cap_bac: {new_cap_bac}")

                choose_jobs_df = df.copy()

                choose_city_country_df = pd.read_csv("./etl/ops/utils/city_country.csv")
                choose_ordered_cap_bac_df = pd.read_csv("./etl/ops/utils/ordered_cap_bac.csv")
                
                df_dictionary = {"city_country": choose_city_country_df,
                            "companies": choose_companies_df,
                            "detailed_welfare_job": choose_detailed_welfare_job_df,
                            "hinh_thuc_job": choose_hinh_thuc_job_df,
                            "job_tags_job": choose_job_tags_job_df,
                            "jobs": choose_jobs_df,
                            "location_job": choose_location_job_df,
                            "nganh_nghe_job": choose_nganh_nghe_job_df,
                            "ordered_cap_bac": choose_ordered_cap_bac_df,
                            "outstanding_welfare_job": choose_outstanding_welfare_df
                }
                return df_dictionary

            else:
                print("All job_id in raw data have existed in server.")
                print(" -> No processing pipeline needed.")
                return "error"

        except Exception as err:
            print('!!! Error in processing pipeline => Turn off pipeline.')
            print(err)
            traceback.print_exc()
        finally:
            pass