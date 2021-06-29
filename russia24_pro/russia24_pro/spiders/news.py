import scrapy


class News(scrapy.Spider):
    """
    Get all links from main page
    Follows the news link
    Get title, datetime, news text
    """

    name = 'news'
    start_urls = ['https://russia24.pro/']

    def parse(self, response, **kwargs):
        # get all links from main page
        links = response.css('div.r24_item > a::attr(href)')
        for link in links:
            # follow the link
            yield response.follow(link.get(), callback=self.news_parse)

    @staticmethod
    def news_parse(response):
        # get title, datetime, news text
        article = response.css('article#r24MainArticle')
        title = article.css('h1::text').get().strip()
        date_time = article.css('div.r24_info > time::attr(datetime)').get()
        news = article.xpath('normalize-space(.//div[@class = "r24_text"])').get()

        yield {
            'title': title,
            'date_time': date_time,
            'news_text': news
        }
