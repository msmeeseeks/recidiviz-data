import scrapy

class UsMoStoneSpider(scrapy.Spider):
    name = 'us_mo_stone'

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
        table_entries = response.xpath(
            '//body//table//table//table//table//table//table/tr/td')

        # There are 17 `td` because "Charges:" has an empty placeholder.
        correct_length = (len(table_entries) == 17)
        assert correct_length, 'Table was not parsed correctly.'

        empty_td_in_row = (
            table_entries[13].xpath('text()').extract_first() == u'\xa0')
        assert empty_td_in_row, 'Empty `td` not found in "Charges:" row.'

        # Delete empty placeholder for "Charges:" row.
        del table_entries[13]

        # Bolded entries are table row "keys".
        table_keys = table_entries.css('.tbold::text').extract()
        table_keys = map(unicode.strip, table_keys)
        # 'text2' entries are table row "values".
        table_values = table_entries.css('.text2::text').extract()
        table_values = map(unicode.strip, table_values)

        table_data = dict(zip(table_keys, table_values))

        # Mapping from db keys to website table row keys.
        key_mapping = {
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
                             for key, table_key in key_mapping.items()}

        yield parsed_table_data
