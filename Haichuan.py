import scrapy
from scrapy.dupefilters import RFPDupeFilter
from scrapy.http import Request
from scrapy.selector import Selector
from Bysj_SE.items import HaichuanItem
from Bysj_SE.utils.haichuan_login_request import haichuan_login
from Bysj_SE.utils import common_params
import re
from datetime import datetime
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider,Rule
from scrapy_redis.spiders import RedisCrawlSpider


# class HaichuanSpider(RedisCrawlSpider):
# class HaichuanSpider(CrawlSpider):
class HaichuanSpider(scrapy.Spider):
    name = 'Haichuan'
    allowed_domains = ['bbs.hcbbs.com'] ###
    start_urls = ['https://bbs.hcbbs.com/forum-337-1.html'] ###

    # redis_key = 'haichuanspider:start_urls'


	a = 1
	b = 2

    def start_requests(self):  #  爬虫入口
        print("haichuan 起始url")
        #cookies = haichuan_login("Crown-w", "wsh2766659938")
        url = "https://bbs.hcbbs.com/forum-337-1.html"
        # yield Request( url= url,headers= common_params.common_headers,cookies= cookies,callback= self.parse)
        yield Request(url=url, headers=common_params.common_headers, callback=self.parse)

    # page_links = LinkExtractor(allow=(r"https://bbs.hcbbs.com/forum-337-\d+.html"))
    # article_links = LinkExtractor(allow=(r"https://bbs.hcbbs.com/thread-\d+-\d+-\d+.html"))
    #
    # rules = (
    #     Rule(page_links),
    #     Rule(article_links, callback="parse", follow=False)
    # )

    def parse(self, response):
        """
              爬取海川论坛的工艺-工艺技术部分
              :param response: 
              :return: 
              """

        print("haichuan parse函数")
        #  获取所有页面
        page_urls = Selector(response=response).xpath('//*[@id="fd_page_bottom"]/div/a')
        #  获取所有文章的 链接 与 标题
        article_urls = Selector(response=response).xpath( '//table[@id="threadlisttableid"]//th/a[re:test(@href,"https://bbs.hcbbs.com/thread-")]')


        for article_url in article_urls:
            href = article_url.xpath('@href').extract_first()
            title = article_url.xpath('text()').extract_first()
            yield Request(url= href, callback=self.empty_function, headers=common_params.common_headers)

        for page_url in page_urls:
            next_url = page_url.xpath('@href').extract_first()
            yield Request(url=next_url, callback=self.parse)

    def empty_function(self,response):
        start_time = datetime.now()
        href = response.url
        title = Selector(response= response).xpath('//*[@id="thread_subject"]/text()').extract_first()
        article = Selector(response= response).xpath('//*[@class="t_f"]/text()').extract_first()
        if not article is None:
            article = article.strip()
            article = article.replace(' ', '').replace('\n', '').replace('\r', '')
        else:
            article = ""
        if article =="":
            article = ""
            str_list = Selector(response= response).xpath('//*[@class="t_f"]//font/text()').extract()
            str_list.extend(Selector(response= response).xpath('//*[@class="t_f"]/div/text()').extract())
            for i in str_list:
                article = article + i
        temp = Selector(response= response).xpath('//*[@id="postlist"]/div[1]//div[@class="authi"]/em/span').extract_first()
        #  后期把 datetime 的格式处理移动到 items.py 里面
        if temp is None:
            date_time_str = Selector(response= response).xpath('//*[@id="postlist"]/div[1]//div[@class="authi"]/em/text()').extract_first()
        else:
            date_time_str = Selector(response= response).xpath('//*[@id="postlist"]/div[1]//div[@class="authi"]/em/span/@title').extract_first()
        print(date_time_str)
        # tu_date_time = re.search(r"(\d{4}-\d{1,2}-\d{1,2})",date_time_str).groups()[0]
        tu_date_time = date_time_str
        check = Selector(response= response).xpath('//div[@id="postlist"]/table//td[1]//span[2]/text()').extract_first()
        reply = Selector(response= response).xpath('//div[@id="postlist"]/table//td[1]//span[5]/text()').extract_first()
        item_obj = HaichuanItem(title= title, href= href, article= article, date_time= tu_date_time,check= check, reply= reply)
        end_time = datetime.now()
        last_seconds = (end_time - start_time).total_seconds()
        yield item_obj