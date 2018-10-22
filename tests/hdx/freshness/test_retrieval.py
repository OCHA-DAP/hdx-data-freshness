# -*- coding: utf-8 -*-
'''
Unit tests for the retrieval class.

'''
import re
from datetime import datetime

from hdx.freshness.retrieval import retrieve


class TestRetrieve:
    def test_retrieve(self):
        url1 = 'https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/now.pickle'
        url2 = 'https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html'
        url3 = 'https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound'
        url4 = 'file://lala'
        urls = [(url1, '1', False), (url2, '2', False), (url3, '3', False), (url4, '4', False), (url1, '5', True), (url2, '6', True)]
        result = retrieve(urls, 'test')
        assert result['1'] == (url1, None, None, '35757fc63e863d962dfc8d5f01d9d121', False)
        assert result['2'] == (url2, None, datetime(2004, 9, 1, 13, 24, 52), None, False)
        assert result['3'] == (url3,
                               'code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound',
                               None, None, False)
        assert result['4'][0] == url4
        regexp = r'^code= message=Cannot connect to host lala:None ssl:None \[.*\] raised=aiohttp\.client_exceptions\.ClientConnectorError url=file:\/\/lala$'
        assert re.search(regexp, result['4'][1]) is not None
        assert result['4'][2] is None
        assert result['4'][3] is None
        assert result['4'][4] is False
        assert result['5'] == (url1, None, None, '35757fc63e863d962dfc8d5f01d9d121', True)
        assert result['6'] == (url2, None, datetime(2004, 9, 1, 13, 24, 52), '982f40d035e618a332c287cdca7f3d0e', True)
