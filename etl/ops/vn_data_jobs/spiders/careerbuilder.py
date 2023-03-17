# -*- coding: utf-8 -*-
import scrapy
import re

class CareerbuilderSpider(scrapy.Spider):
    # Problem: https://careerbuilder.vn/vi/tim-viec-lam/nhan-vien-ke-toan-thue.35BB1BAD.html can not be scraped (subset of 5% can not be scraped)
    #          https://careerbuilder.vn/vi/tim-viec-lam/tro-ly-kinh-doanh-tieng-trung.35BB74DD.html
    #          https://careerbuilder.vn/vi/tim-viec-lam/ke-toan-vien.35BB66AA.html

    # First, scrape entire, then, 3 days scrape once
    # 11/03 run 2 weeks ago, 16/03 run 3 days ago
    # scrapy crawl careerbuilder -o all_jobs_cb_202303110948.csv
    name = 'careerbuilder'
    allowed_domains = ['homeworkistrash.ml']

    def start_requests(self):
        yield scrapy.Request(url='https://homeworkistrash.ml/viec-lam/ngay-cap-nhat-d3-vi.html?__cpo=aHR0cHM6Ly9jYXJlZXJidWlsZGVyLnZu',
                             callback=self.parse,
                             headers={
                                 "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.1 Safari/537.36',
                             })

    def parse(self, response):
        for job in response.xpath("//div[@class='main-slide']/div[position() = 1]/div"):
            job_title = job.xpath(".//div/div[position() = 2]/div[position() = 1]/h2/a/@title").get()
            job_id = job.xpath(".//div/div[position() = 2]/div[position() = 1]/h2/a/@data-id").get()
            job_url = job.xpath(".//div/div[position() = 2]/div[position() = 1]/h2/a/@href").get()

            company_title = job.xpath(".//div/div[position() = 2]/div[position() = 2]/a[position() = 1]/@title").get()
            company_url = job.xpath(".//div/div[position() = 2]/div[position() = 2]/a[position() = 1]/@href").get()
            company_video_url = job.xpath(".//div/div[position() = 2]/div[position() = 3]/ul/li/a[@class='play-video']/@href").get()

            salary = job.xpath(".//div/div[position() = 2]/div[position() = 2]/a[position() = 2]/div[position() = 1]/p/text()").get()

            location = " |".join(job.xpath(".//div/div[position() = 2]/div[position() = 2]/a[position() = 2]/div[position() = 2]/ul/li/text()").getall())

            outstanding_welfare = " | ".join(job.xpath(".//div/div[position() = 2]/div[position() = 2]/a[position() = 2]/ul/li/text()").getall())

            announcement_date = job.xpath(".//div/div[position() = 2]/div[position() = 3]/div/time/text()").get()

            yield response.follow(
                url=job_url,
                callback=self.parse_job,
                meta={
                    "job_title": job_title,
                    "job_id": job_id,
                    "job_url": response.urljoin(job_url),
                    "company_title": company_title,
                    "company_url": company_url,
                    "company_video_url": company_video_url,
                    "salary": salary,
                    "location": location,
                    "outstanding_welfare": outstanding_welfare,
                    "announcement_date": announcement_date,
                },
            )

        next_page = response.xpath("//div[@class='main-slide']/div[position() = 2]/ul/li[@class = 'next-page']/a/@href").get()

        if next_page:
            print("\n\n"+(next_page.center(180, "=")+"\n")*7+"\n")
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse, headers={
                                    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
                                })

    def parse_job(self, response):
        job_title = response.request.meta['job_title']
        job_id = response.request.meta['job_id']
        job_url = response.request.meta['job_url']
        company_title = response.request.meta['company_title']
        company_url = response.request.meta['company_url']
        company_video_url = response.request.meta['company_video_url']
        salary = response.request.meta['salary']
        location = response.request.meta['location']
        outstanding_welfare = response.request.meta['outstanding_welfare']
        announcement_date = response.request.meta['announcement_date']


        nganh_nghe = " | ".join(list(map(lambda x: x.strip().strip("\r\n").strip(),
            response.xpath("//section[@class = 'job-detail-content']/div[@class = 'bg-blue']/div/div[position() = 2]/div/ul/li[position() = 2]/p/a/text()").getall())))
        if not nganh_nghe:
            nganh_nghe = " | ".join(response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'box-info']/div/div/table/tbody/tr[position() = 1]/td[position() = 2]/a/text()").getall())

        hinh_thuc = response.xpath("//section[@class = 'job-detail-content']/div[@class = 'bg-blue']/div/div[position() = 2]/div/ul/li[position() = 3]/p/text()").get()
        if not hinh_thuc:
            hinh_thuc = response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'box-info']/div/div/table/tbody/tr[position() = 3]/td[position() = 2]/p/text()").get()

        kinh_nghiem = response.xpath("//section[@class = 'job-detail-content']/div[@class = 'bg-blue']/div/div[position() = 3]/div/ul/li[position() = 2]/p/text()").get()
        if kinh_nghiem:
            kinh_nghiem = self.cleanse_kinh_nghiem(kinh_nghiem)
        else:
            kinh_nghiem = response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'box-info']/div/div/table/tbody/tr[position() = 6]/td[position() = 2]/p/text()").get()
            if kinh_nghiem:
                kinh_nghiem = self.cleanse_kinh_nghiem(kinh_nghiem)

        cap_bac = response.xpath("//section[@class = 'job-detail-content']/div[@class = 'bg-blue']/div/div[position() = 3]/div/ul/li[position() = 3]/p/text()").get()
        if not cap_bac:
            cap_bac = response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'box-info']/div/div/table/tbody/tr[position() = 5]/td[position() = 2]/p/text()").get()

        expiration_date = response.xpath("//section[@class = 'job-detail-content']/div[@class = 'bg-blue']/div/div[position() = 3]/div/ul/li[position() = 4]/p/text()").get()
        if not expiration_date:
            expiration_date = response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'box-info']/div/div/table/tbody/tr[position() = 7]/td[position() = 2]/p/text()").get()

        detailed_welfare = " | ".join(list(map(lambda x: x.strip(), response.xpath("//section[@class = 'job-detail-content']/div[position() = 2]/ul/li/text()").getall())))
        if not detailed_welfare:
            detailed_welfare = " | ".join(list(map(lambda x: x.strip(), response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'detail-row box-welfares']/div[position() = 2]/ul/li/text()").getall())))

        job_description = response.xpath("//section[@class = 'job-detail-content']/div[position() = 3]").get()
        if not job_description:
            job_description = response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'full-content']/div[position() = 1]").get()

        job_requirements = response.xpath("//section[@class = 'job-detail-content']/div[position() = 4]").get()
        if not job_requirements:
            job_requirements = response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'full-content']/div[position() = 2]").get()

        other_info = response.xpath("//section[@class = 'job-detail-content']/div[position() = 5]").get()
        if not other_info:
            other_info = response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'full-content']/div[position() = 3]").get()

        job_tags = " | ".join(response.xpath("//section[@class = 'job-detail-content']/div[position() = 7]/ul/li/a/text()").getall())
        if not job_tags:
            job_tags = " | ".join(list(map(lambda x: x.strip(), response.xpath("//div[@class = 'bottom-template']/div/div/div[position() = 1]/div[@class = 'full-content']/div[position() = 4]/ul/li/a/text()").getall())))


        yield {
            "job_title": job_title,
            "job_id": job_id,
            "job_url": job_url,
            "company_title": company_title,
            "company_url": company_url,
            "company_video_url": company_video_url,
            "salary": salary,
            "location": location,
            "outstanding_welfare": outstanding_welfare,
            "announcement_date": announcement_date,
            "nganh_nghe": nganh_nghe,
            "hinh_thuc": hinh_thuc,
            "kinh_nghiem": kinh_nghiem,
            "cap_bac": cap_bac,
            "expiration_date": expiration_date,
            "detailed_welfare": detailed_welfare,
            "job_description": job_description,
            "job_requirements": job_requirements,
            "other_info": other_info,
            "job_tags": job_tags,
        }

    def cleanse_kinh_nghiem(self, raw_string):
        """
        raw_string = '\r\n     Trên    2 \r\n        Năm\r\n          '
        
        Phase 1: '\r\n     Trên    2 \r\n        Năm\r\n          '
            --> 'Trên    2 Năm'
        
        Phase 2: 'Trên    2 Năm'
            --> 'Trên 2 Năm'
        """
        
        raw_string = raw_string.strip()
        pat = '(\\r\\n[\s]*)'
        for m_start_end in reversed([(m.start(0), m.end(0)) for m in re.finditer(pat, raw_string)]):
            raw_string = raw_string[:m_start_end[0]] + "" + raw_string[m_start_end[1]:]
        
        pat = '[\s]{2,}'
        for m_start_end in reversed([(m.start(0), m.end(0)) for m in re.finditer(pat, raw_string)]):
            raw_string = raw_string[:m_start_end[0]] + " " + raw_string[m_start_end[1]:]
        
        return raw_string
