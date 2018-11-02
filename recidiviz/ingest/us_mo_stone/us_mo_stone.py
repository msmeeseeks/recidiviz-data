import scrapy

class RosterSpider(scrapy.Spider):
    name = "roster"

    def start_requests(self):
        url = 'https://www.stonecountymosheriff.com/roster.php'
        yield scrapy.Request(url, self.parse_roster)

    def parse_roster(self, response):
        for person in response.css('.inmateTable'):
          person_link = person.css('a::attr(href)').extract_first()
          yield response.follow(person_link, self.parse_person)

        next_page = response.xpath('//a[text()=">>"]/@href').extract_first()
        if next_page:
            yield response.follow(next_page, self.parse_roster)

    def parse_person(self, response):
        name = response.css('span.ptitles::text').extract_first().strip()
        table = response.xpath('/html/body/table/tbody/tr/td[2]/table/tbody/tr[4]/td/table/tbody/tr[3]/td[2]/table/tbody/tr/td/div/div/table/tbody/tr[1]/td[2]/table/tbody')

        table_data = {}
        for row in table:
          key = table.css('span.tbold::text').extract_first().strip()
          value = table.css('td.text2::text').extract_first().strip()
          table_data[key] = value

        mapping = {
          'record_id': 'Booking #:',
          'age': 'Age:',
          'gender': 'Gender:',
          'race': 'Race:',
          'authority': 'Arresting Agency:',
          'date': 'Booking Date:',
          'charges': 'Charges:',
          'bond_amount': 'Bond:'
        }

        parsed_table_data = {key: table_data[table_key]
                             for key, table_key in mapping.items()}

        yield parsed_table_data
