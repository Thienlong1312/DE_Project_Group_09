CREATE TABLE city_country(
	city varchar(255),
	country varchar(255),
	PRIMARY KEY(city)
);

CREATE TABLE companies(
	company_title varchar(255),
	company_id varchar(255),
	company_url varchar(255),
	company_video_url varchar(255),
	PRIMARY KEY(company_id)
);

CREATE TABLE detailed_welfare_job(
	job_id varchar(255),
	detailed_welfare varchar(255)
);

CREATE TABLE hinh_thuc_job(
	job_id varchar(255),
	hinh_thuc varchar(255)
);

CREATE TABLE job_tags_job(
	job_id varchar(255),
	job_tags varchar(255)
);

CREATE TABLE jobs(
	job_title varchar(255),
	job_id varchar(255),
	job_url varchar(255),
	announcement_date varchar(255),
	cap_bac varchar(255),
	expiration_date varchar(255),
	job_description text,
	job_requirements text,
	other_info text,
	company_id varchar(255),
	lb_salary varchar(255),
	ub_salary varchar(255),
	lb_kinh_nghiem varchar(255),
	ub_kinh_nghiem varchar(255),
	PRIMARY KEY(job_id)
);

CREATE TABLE location_job(
	city varchar(255),
	job_id varchar(255),
	company_id varchar(255)
);

CREATE TABLE nganh_nghe_job(
	job_id varchar(255),
	nganh_nghe varchar(255)
);

CREATE TABLE ordered_cap_bac(
	cap_bac varchar(255),
	stt varchar(255),
	PRIMARY KEY(cap_bac)
);

CREATE TABLE outstanding_welfare_job(
	job_id varchar(255),
	outstanding_welfare varchar(255)
);

