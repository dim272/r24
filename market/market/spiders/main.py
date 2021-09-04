import datetime
import time

import scrapy

'''
Логика сбора данных

1. Загружаем начальную страницу 
2. Берём тайтлы, вычисляем ID, сравниваем с БД
3. Если в БД нет, то заходим 
4. Первым делом ищем на странице блок с информацией по ...., если он есть, закупку пропускаем
5. Если нет, собираем данные
6. Переходим на страницу заказчика, собираем данные
8. Ищем вкладку "....." и "....", если есть, то проходим по ним и собираем данные 
'''


class MainSpider(scrapy.Spider):
    name = 'main'

    def start_requests(self):
        # Определяем количество пройденных дней, которое нам нужно парсить (кроме сегодняшнего)
        days = 3
        urls = []
        while days > 0:
            # Здесь получаем сегодняшнюю дату минус количество дней => str '15.08.2021'
            yesterday = datetime.datetime.strftime((datetime.date.today() - datetime.timedelta(days=days)), '%d.%m.%Y')

            # При помощи этой переменной формируем ссылку на фильтр актуальных закупок по определённому дню
            url = f'http://www.example.com/market/?searching=1&date=1&' \
                  f'date_start_dmy={yesterday}&date_end_dmy={yesterday}&trade=buy&lot_type=0'
            urls.append(url)
            days -= 1

        # Идём по сформированным ссылкам
        for url in urls:
            self.logger.info(url)
            yield scrapy.Request(url, callback=self.pagination)
            time.sleep(3)

    def pagination(self, response):
        print(response.url)
        time.sleep(1)
        # Берём все ссылки на странице не содержащие в своём названии "Закрытый",
        # т.к. закрытые закупки нет возможности парсить
        urls_on_the_page = response.xpath('//a[contains(@class, "search-results-title") '
                                          'and not(contains(text(), "Закрытый"))]/@href').getall()

        # TODO: проверка на наличие ID в бд. Если есть, то прервать процесс

        print(len(urls_on_the_page))
        for url in urls_on_the_page:
            url = self.url_checker(response, url)
            yield scrapy.Request(url, callback=self.item_parsing)

        # TODO: пагинация

    def item_parsing(self, response):
        # Сразу проверяем наличие информации о выгрузке в ЕИС. Если есть, то пропускаем
        if response.xpath('//div[@class="common_block"]'):
            self.logger.info(f'EIS here {response.url}')
            return None

        item = {
            'id': self.get_id(response),
            'purchase_code_name': self.get_purchase_code_name(response),
            'title': self.get_title(response),
            'okpd2': self.get_okpd2_okved2(response, 'okpd2'),
            'okved2': self.get_okpd2_okved2(response, 'okved2'),
            'lots': self.get_lots_data(response),
            'lot_price': self.get_lot_price(response),
            'currency': response.xpath('//*[@id="trade-info-lot-price-currency"]/td[2]/text()').get(),
            'dates': self.get_dates(response),
            'placer': self.get_placer_additional_info(response),
            'delivery_place': self.get_delivery_place(response),
            'purchasing_positions': None,
            'purchasing_positions_url': self.get_purchasing_positions_link(response),
            'additional_info': self.get_additional_info(response),
            'url': response.url.split('/?error')[0]
        }

        placer_url = item['placer']['url']
        purchasing_positions_url = item['purchasing_positions_url']

        if placer_url:
            yield scrapy.Request(placer_url, callback=self.placer_parsing, meta={'item': item})
        elif purchasing_positions_url:
            yield scrapy.Request(purchasing_positions_url, callback=self.purchasing_positions_parsing, meta={'item': item})
        else:
            yield from self.item_generator(item)

    def placer_parsing(self, response):
        def get_data(value):
            xpath_request = f'//*[@class="box primaryBg menuBox"]//tbody[1]/tr/td[contains(text(), "{value}")]' \
                            f'/following-sibling::td/text()'
            result = response.xpath(xpath_request).get()

            if result:
                return result
            else:
                return None

        placer_info = {
            'short_name': get_data('Краткое наименование'),
            'full_name': get_data('Полное наименование'),
            'inn': self.remove_characters(get_data('ИНН')),
            'kpp': self.remove_characters(get_data('КПП')),
            'okpo': self.remove_characters(get_data('ОКПО')),
            'ogrn': self.remove_characters(get_data('ОГРН'))
        }

        item = response.meta['item']
        item['placer'] = {**item['placer'], **placer_info}

        purchasing_positions_url = item['purchasing_positions_url']

        if purchasing_positions_url:
            yield scrapy.Request(purchasing_positions_url, callback=self.purchasing_positions_parsing,
                                 meta={'item': item})
        else:
            yield from self.item_generator(item)

    def purchasing_positions_parsing(self, response):
        # Don't even try. It's magic here
        """
        https://www.example.com/market/postavka-elektromaterialov/2761308/positions
        """

        # Сюда мы пришли в последнюю очередь, когда вся возможная информация уже собрана в item
        # Здесь мы соберём все данные из таблицы "закупочные позиции"
        item = response.meta['item']
        # Возьмём данные о закупочных позициях из Item (если они есть)
        item_purchasing_positions = item['purchasing_positions']
        main_table_values = self.purchasing_positions_table_analyzer(response)
        additional_info = main_table_values['additional_info']
        result = {}

        # Возьмём все строки таблицы с данными
        table_rows = response.xpath('//tr[@class="thead"]/following-sibling::tr')
        purchasing_position_number = 1
        for row in table_rows:
            # Сюда соберём всю строку
            purchasing_position = {}
            # Сюда будем собирать данные из additional_info
            addit_info = {}

            # Теперь возьмём все клетки в данной строке
            cells = row.xpath('./td')
            cell_number = 1

            # И переберём
            for cell in cells:
                cell_text = cell.xpath('normalize-space(./text())').get()

                # Если номер клетки (слева направо) присутствует в словаре additional_info,
                # тогда значение клетки положим в словарь add_info,
                # а ключём назначим соответсвующий ключ из additional_info
                # Получим что-то вроде {'Единица измерения': 'кг'}
                if cell_number in additional_info.values():
                    # Здесь мы вынимаем ключ из additional_info по номеру клетки (слева направо)
                    key = list(additional_info.keys())[list(additional_info.values()).index(cell_number)]
                    addit_info[key] = cell_text

                # Если номер клетки (слева на право) не присутствует в словаре additional_info,
                # значит он из списка важных значений из первого ряда словаря main_table_values
                # В этом случае плоложим значение клетки в purchasing_position как value а
                # а ключём будет служить ключ номера этой клетки из основного словаря
                else:
                    # Здесь мы вынимаем ключ из основного словаря main_table_values по номеру клетки (слева направо)
                    key = list(main_table_values.keys())[list(main_table_values.values()).index(cell_number)]

                    # Если в таблице найдены ОКВЭД или ОКПД (что случается крайне редко),
                    # их нужно взять не просто текстом из клетки, как мы делаем выше,
                    # а разделить на код и расшифровку кода
                    if key in ['okved2', 'okpd2']:
                        key1 = key + '_code'
                        key2 = key + '_name'
                        value1 = cell.xpath('normalize-space(.//b/text())').get()
                        value2 = cell.xpath('normalize-space(./*/text())').get()
                        purchasing_position[key1] = value1
                        purchasing_position[key2] = value2

                    else:
                        purchasing_position[key] = cell_text

                cell_number += 1

            purchasing_position['additional_info'] = addit_info
            result[purchasing_position_number] = purchasing_position
            purchasing_position_number += 1

        # Если с прошлой страницы у нас уже были закупочные позиции, тогда объединим их с результатом
        if item_purchasing_positions:
            result = {**item_purchasing_positions, **result}

        # Положим все закупочные позиции обратно в Item
        item['purchasing_positions'] = result

        # В закупочных позиция быаают несколько страниц
        next_page = response.xpath('//a[contains(text(), "Следующая страница")]/@href').get()
        if next_page:
            url = self.url_checker(response, next_page)
            yield scrapy.Request(url, callback=self.purchasing_positions_parsing, meta={'item': item})
        else:
            yield from self.item_generator(item)
        # Низкий тебе поклон, если ты разобрался в этом методе.

    def item_generator(self, item):
        # Здесь мы соберём и вернём готовый Item

        external_id = item['id']
        url_vsrz = item['url']
        purchase_code_name = item['purchase_code_name']
        name = item['title']
        publication_date_time = item['dates']['publication_date_time']
        submission_open_date_time = item['dates']['submission_open_date_time']
        submission_close_date_time = item['dates']['submission_close_date_time']
        placer = self.placer_generator(item)
        additional_info = item['additional_info']
        purchase_positions = self.purchase_positions_generator(item)
        currency_name = item['currency']
        lot_price = item['lot_price']
        delivery_place = item['delivery_place']

        result = self.my_items.ItemTemplate(
            rejected=True,
            external_id=external_id,
            url_vsrz=url_vsrz,
            purchase_code_name=purchase_code_name,
            name=name,
            publication_date_time=publication_date_time,
            submission_open_date_time=submission_open_date_time,
            submission_close_date_time=submission_close_date_time,
            subject=name,
            initial_sum=lot_price,
            currency_name=currency_name,
            currency_code=None,
            joint_purchase=False,
            placer=placer,
            customer=placer,
            delivery_place=delivery_place,
            lot_items=purchase_positions,
            document_list=None,
            additional_info=additional_info
        )

        yield result

    def placer_generator(self, item):
        short_name = item['placer']['short_name']
        full_name = item['placer']['full_name']
        inn = item['placer']['inn']
        kpp = item['placer']['kpp']
        ogrn = item['placer']['ogrn']
        legal_address = item['placer']['post_address']
        postal_address = item['placer']['fact_address']
        contacts = item['placer']['contacts']

        return self.my_items.CompanyTemplate(
            short_name=short_name,
            full_name=full_name,
            inn=inn,
            kpp=kpp,
            ogrn=ogrn,
            legal_address=legal_address,  # Почтовый адрес заказчика:
            postal_address=postal_address,  # Местонахождение заказчика:
            okato=None,
            okopf=None,
            okopf_name=None,
            okpo=None,
            iko=None,
            customer_registration_date=None,
            customer_registration_date_specified=None,
            additional_info=None,
            contacts=contacts
        )

    def purchase_positions_generator(self, item):
        positions = item['purchasing_positions']
        print(f'\n\n{positions}\n\n')

        if not positions:
            print(f'POSITIONS IS NONE')
            return None

        item_okpd2 = item['okpd2']
        if item_okpd2:
            for key, val in item_okpd2.items():
                item_okpd2_code = key
                item_okpd2_name = val
        else:
            item_okpd2_code = None
            item_okpd2_name = None

        item_okved2 = item['okpd2']
        if item_okved2:
            for key, val in item_okved2.items():
                item_okved2_code = key
                item_okved2_name = val
        else:
            item_okved2_code = None
            item_okved2_name = None

        result = []
        for position in positions.values():
            try:
                ordinal_number = position['ordinal_number']
            except KeyError:
                ordinal_number = None

            try:
                name = position['name']
            except KeyError:
                name = None

            try:
                qty = position['qty']
            except KeyError:
                qty = None

            try:
                okpd2_code = position['okpd2_code']
                okpd2_name = position['okpd2_name']
            except KeyError:
                okpd2_code = item_okpd2_code
                okpd2_name = item_okpd2_name

            try:
                okved2_code = position['okpd2_code']
                okved2_name = position['okpd2_name']
            except KeyError:
                okved2_code = item_okved2_code
                okved2_name = item_okved2_name

            try:
                additional_info = position['additional_info']
            except KeyError:
                additional_info = None

            result.append(self.my_items.LotPosition(
                ordinal_number=ordinal_number,
                name=name,
                qty=qty,
                okpd2_name=okpd2_name,
                okpd2_code=okpd2_code,
                okved2_name=okved2_name,
                okved2_code=okved2_code,
                okei_name=None,
                okei_code=None,
                additional_info=additional_info
            ))

        return result

    # UTILS:

    def get_id(self, response):
        url = response.url
        _id = url.split('/')[-2]
        try:
            _id = _id.split('-')[-1]
        except Exception as e:
            self.logger.info(f'id error: {e}, here {response.url}')
            return None

        _id = self.remove_characters(_id)

        if _id and _id.isdigit():
            _id = int(_id)
        else:
            self.logger.info(f'get_id: id not .isdigit(): _id = "{_id}" from {response.url}')
            return None

        return _id

    def get_purchase_code_name(self, response):
        title = response.xpath('normalize-space(//h1/text())').get()
        if not title:
            self.logger.info(f'get_purchase_code_name(): not found title here {response.url}')
            return None
        purchase_code_name = title.split(' №')[0]
        return purchase_code_name.strip()

    def get_title(self, response):
        title = response.xpath('normalize-space(//h1/div/text())').get()
        if not title:
            self.logger.info(f'get_title(): not found title here {response.url}')
            return None
        return title

    @staticmethod
    def get_delivery_place(response):
        # Адрес места поставки
        delivery_place = response.xpath('normalize-space(//*[@id="trade_info_address"]/td[2]/text())').get()
        return delivery_place

    def get_okpd2_okved2(self, response, code_name):
        block = response.xpath(f'//*[@id="trade-info-{code_name}"]')
        if block is None:
            return None

        result = None

        multiple_block = block.xpath(
            './td[2]//*[@class="expandable-text full"]/*[@class="value"]/div')

        if multiple_block:
            code = multiple_block.xpath('./b/text()').get()
            name = multiple_block.xpath('normalize-space(./text())').get()
            if code and name:
                result = {code.strip(): name.strip()}
            else:
                self.logger.info(f'get_okpd2_okved2() error from multiple block: code = "{code}", name = "{name}"')

        else:
            single_block = block.xpath('./td[2]/div')

            if single_block:
                code = single_block.xpath('./b/text()').get()
                name = single_block.xpath('normalize-space(./text())').get()
                if code and name:
                    result = {code.strip(): name.strip()}
                else:
                    self.logger.info(f'get_okpd2_okved2() error from single block: code = "{code}", name = "{name}"')
            else:
                pass

        return result

    def get_lot_price(self, response):
        lot_price_none = response.xpath('normalize-space(//*[@id="trade-info-lot-price"]//td[2]/text())').get()
        if lot_price_none:
            if lot_price_none.strip() == 'Без указания цены':
                return None

        lot_price_exist = response.xpath('normalize-space(//*[@id="trade-info-lot-price"]//b/text())').get()
        if lot_price_exist:
            lot_price = lot_price_exist.strip().split("руб")[0]
            lot_price = lot_price.strip().split("грн")[0]
            lot_price = lot_price.strip().split("тенге")[0]
            lot_price = lot_price.replace("\xa0", "").replace(' ', '')
            lot_price = lot_price.replace("EUR", "").replace('USD', '')
            lot_price = lot_price.replace("лир", "").replace('тенге', '')
            lot_price = lot_price.replace("BYN", "").replace('GBP', '')
            lot_price = lot_price.replace("%", "").replace('AUD', '')

            try_val = lot_price.replace(',', '').replace('.', '')
            if try_val.isdigit():
                lot_price_float = lot_price.replace(',', '.')
                lot_price = float(lot_price_float)
            else:
                self.logger.info(f'get_lot_price float convert error: value = "{lot_price}" from {response.url}')
            return lot_price

        return None

    def get_dates(self, response):
        # Дата публикации:
        publication_date_time = response.xpath('//*[@id="trade_info_date_begin"]/td[2]/span/text()').get()
        if not publication_date_time:
            publication_date_time = response.xpath('//*[@id="trade_info_date_begin"]/td[2]/text()').get()

        # Дата начала подачи заявок. Отдельно указывается не часто, поэтому если не найдена, то равна дате публикации:
        submission_open_date_time = response.xpath('//*[@id="trade_info_date_start"]/td[2]/text()').get()
        if not submission_open_date_time:
            submission_open_date_time = publication_date_time

        # Дата окончания подачи заявок (встречается редко):
        submission_unsealing_date_time = response.xpath('//*[@id="trade_info_date_unsealing"]/td[2]/text()').get()

        # Дата рассмотрения заявок (встречается редко):
        submission_qualified_date_time = response.xpath('//*[@id="trade_info_date_qualified"]/td[2]/text()').get()

        # Дата завершения закупки:
        submission_close_date_time = response.xpath('//*[@id="trade_info_date_end"]/td[2]/text()').get()

        # Дата последнего редактирования:
        submission_update_date_time = response.xpath('//*[contains(text(), "Дата последнего редактирования:")]'
                                                    '/following-sibling::*/text()').get()


        return {
            'publication_date_time': publication_date_time,
            'submission_open_date_time': submission_open_date_time,
            'submission_unsealing_date_time': submission_unsealing_date_time,
            'submission_qualified_date_time': submission_qualified_date_time,
            'submission_close_date_time': submission_close_date_time,
            'submission_update_date_time': submission_update_date_time
        }

    def get_placer_url(self, response):
        url = response.xpath('//*[@id="trade-info-organizer-name"]/td[2]/a/@href').get()
        url = self.url_checker(response, url)
        return url

    def get_customer_url(self, response):
        url = response.xpath('//*[@id="trade-info-organizer-name"]/td[2]/a/@href').get()
        url = self.url_checker(response, url)
        return url

    def get_placer_additional_info(self, response):
        contacts = self.get_placer_contacts(response)

        additional_info = {
            'url': self.get_placer_url(response),
            # Бывают крайне редко
            'post_address': response.xpath('normalize-space(//*[@id="trade-info-organizer-post-address"]/td[2]/text())').get(),
            'fact_address': response.xpath('normalize-space(//*[@id="trade-info-organizer-fact-address"]/td[2]/text())').get(),
            'contacts': contacts
        }
        return additional_info

    @staticmethod
    def get_placer_contacts(response):
        contacts = {
            'responsible_person': response.xpath('normalize-space(//*[@id="trade-info-contact-person"]/td[2]/text())').get(),
            'email': response.xpath('normalize-space(//*[@id="trade-info-organizer-email"]/td[2]/a/text())').get(),
            'phone_number': response.xpath('normalize-space(//*[@id="trade-info-organizer-phone"]/td[2]/noindex/text())').get()
        }
        if contacts:
            return contacts
        else:
            return None

    def get_purchasing_positions_link(self, response):
        purchasing_positions_link = response.xpath('//a[contains(text(), "Закупочные позиции")]/@href').get()
        if purchasing_positions_link:
            purchasing_positions_link = self.url_checker(response, purchasing_positions_link)
        return purchasing_positions_link

    @staticmethod
    def purchasing_positions_table_analyzer(response):
        """
        Здесь мы проанализируем названия колонок и опеределим номер колонки для основных показателей:
        №   Наименование    Количество  ОКВЭД, ОКПД (если есть)
        Все остальные данные будем собирать в additional_info
        В результате мы должны получить словарь с номером колонки и значением,
        {
        ordinal_number: 1,
        name: 2,
        qty: 5,
        okpd2: 4,               если нет возьмём при формировании айтема закупочной позиции из общего окпд2
        okved2: 8,              если нет возьмём при формировании айтема закупочной позиции из общего оквэд2
        additional_info: {      всё остальное
            Единицы измерения: 3,
            Код ОКТМО: 6,
            Дата начала поставки: 7
            }
        }
        """

        def get_column_number(value):
            # Тут просто отнимаем количество оставшихся элементов в строке после искомого значения,
            # и получаем порядковы номер колонки слева направо
            if value and type(value) is list:
                return len_columns - len(value)
            else:
                return None

        # Определяем общее количество колонок в таблице
        columns = response.xpath('//div[@class="wideTable-wrap"]//tr[1]/td').getall()
        len_columns = len(columns)

        # Находим колонку с номером закупочной позиции (обычно он первый) и берём все колонки, идущие после
        columns_after_ordinal_number = response.xpath('//td[contains(text(), "№")]/following-sibling::td').getall()
        ordinal_number_position = get_column_number(columns_after_ordinal_number)  # номер колонки "№"
        if ordinal_number_position is None:
            ordinal_number_position = 1

        # Наименование
        columns_after_name = response.xpath('//td[contains(text(), "Наименование")]/following-sibling::td').getall()
        name_position = get_column_number(columns_after_name)  # номер колонки "Наименование"

        # Количество
        columns_after_quantity = response.xpath('//td[contains(text(), "Количество")]/following-sibling::td').getall()
        quantity_position = get_column_number(columns_after_quantity)  # номер колонки "Количество"

        # ОКВЭД
        columns_after_okved = response.xpath('//td[contains(text(), "ОКВЭД")]/following-sibling::td').getall()
        okved_position = get_column_number(columns_after_okved)  # номер колонки "ОКВЭД"

        # ОКПД
        columns_after_okved = response.xpath('//td[contains(text(), "ОКПД")]/following-sibling::td').getall()
        okpd_position = get_column_number(columns_after_okved)  # номер колонки "ОКПД"

        result = {
            'ordinal_number': ordinal_number_position,
            'name': name_position,
            'qty': quantity_position,
            'okved2': okved_position,
            'okpd2': okpd_position
        }

        # Теперь сформируем additional_info из того, что осталось
        additional_info = {}
        for column_number in range(1, len_columns + 1):
            if column_number in result.values():
                continue
            else:
                xpath_request = f'normalize-space(//div[@class="wideTable-wrap"]//tr[@class="thead"]/td[{column_number}]/text())'
                column_name = response.xpath(xpath_request).get()
                additional_info[column_name] = column_number

        result['additional_info'] = additional_info

        return result

    @staticmethod
    def get_additional_info(response):
        additional_info_block = response.xpath('//*[@id="auction_info_td"]/table/tbody/tr/'
                                               'td[contains(text(), "Дополнительная информация")]/parent::tr/'
                                               'following::tr/td/table//tr')
        if len(additional_info_block) == 0:
            additional_info_block = response.xpath('//*[@id="auction_info_td"]/table/tr/'
                                                   'td[contains(text(), "Дополнительная информация")]/'
                                                   'parent::tr/following::tr/td/table//tr')
        if len(additional_info_block) == 0:
            return None

        additional_info = {}
        for row in additional_info_block:
            key = row.xpath('normalize-space(./td[1]/span[1]/text())').get()
            if not key:
                key = row.xpath('normalize-space(./td[1]/text())').get()
            val = row.xpath('normalize-space(./td[2]/text())').get()
            if key and val:
                additional_info[key] = val
            elif key and not val:
                key = row.xpath('normalize-space(./td/b/text())').get()
                if key:
                    val_list = row.xpath('normalize-space(./td/text())').getall()
                    if val_list and len(val_list) > 1:
                        val = " ".join(val_list)
                        additional_info[key] = val
                    else:
                        continue
                else:
                    continue
            else:
                continue
        return additional_info

    def url_checker(self, response, url):
        try:
            iter(url)
        except Exception as e:
            self.logger.info(f'url_checker error: "{e}", from {response.url}, value "{url}"')
            return url
        else:
            if 'http' in url:
                return url
            else:
                return response.urljoin(url)

    def str_to_datetime(self, value):
        if not value:
            return None

        try:
            return datetime.datetime.strptime(value, '%d.%m.%Y %H:%M')
        except ValueError:
            return None
        except Exception as e:
            self.logger.info(f'str_to_datetime() error: "{e}" with value = "{value}"')
            return None

    @staticmethod
    def remove_characters(value):
        if type(value) is not str:
            return value

        new_value = value

        for item in value:
            if not item.isdigit():
                new_value = new_value.replace(item, '')

        return new_value
