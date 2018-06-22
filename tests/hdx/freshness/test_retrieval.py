# -*- coding: utf-8 -*-
'''
Unit tests for the retrieval class.

'''
from datetime import datetime

from hdx.freshness.retrieval import retrieve


class TestRetrieve:
    def test_retrieve(self):
        url1 = 'https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/now.pickle'
        url2 = 'https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html'
        url3 = 'https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound'
        url4 = 'file://lala'
        urls = [(url1, '1', False), (url2, '2', False), (url3, '3', False), (url4, '4', False), (url1, '5', True), (url2, '6', True)]
        result = retrieve(urls)
        assert result == {'1': (url1, None, None, '35757fc63e863d962dfc8d5f01d9d121', False),
                          '2': (url2, None, datetime(2004, 9, 1, 13, 24, 52), None, False),
                          '3': (url3, 'code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound', None, None, False),
                          '4': (url4,
                                'code= message=Cannot connect to host lala:None ssl:False [Name or service not known] raised=aiohttp.client_exceptions.ClientConnectorError url=file://lala',
                                None, None, False),
                          '5': (url1, None, None, '35757fc63e863d962dfc8d5f01d9d121', True),
                          '6': (url2, None, datetime(2004, 9, 1, 13, 24, 52), '982f40d035e618a332c287cdca7f3d0e', True)
                          }
