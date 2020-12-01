# -*- coding: utf-8 -*-
'''
Unit tests for the retrieval class.

'''
import re
from datetime import datetime

from hdx.freshness.retrieval import retrieve


class TestRetrieve:
    def test_retrieve(self):
        url1 = 'https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html'
        url2 = 'https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound'
        url3 = 'file://lala:10'
        url4 = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/COD_MOZ_Admin0.geojson'
        url5 = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/hotosm_nic_airports_lines_shp.zip'
        url6 = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/Dataset.csv'
        url7 = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/list_one.xls'
        url8 = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/ACLED-Country-Coverage-and-ISO-Codes_8.2019.xlsx'
        url9 = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/response.html'
        urls = [(url1, '1', 'html'), (url2, '2', 'csv'), (url3, '3', 'csv'), (url4, '4', 'geojson'), (url5, '5', 'shp'),
                (url6, '6', 'csv'), (url7, '7', 'xls'), (url8, '8', 'xlsx'), (url9, '9', 'html'), (url9, '10', 'csv'),
                (url9, '11', 'xls')]
        result = retrieve(urls, 'test')
        assert result['1'] == (url1, 'html', None, datetime(2004, 9, 1, 13, 24, 52), '982f40d035e618a332c287cdca7f3d0e')
        assert result['2'] == (url2, 'csv',
                               'code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound',
                               None, None)
        assert result['3'][0] == url3
        regexp = r'^code= message=Cannot connect to host lala:10 ssl:default \[.*\] raised=aiohttp\.client_exceptions\.ClientConnectorError url=file:\/\/lala:10$'
        assert re.search(regexp, result['3'][2]) is not None
        assert result['3'][3] is None
        assert result['3'][4] is None
        assert result['4'] == (
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/COD_MOZ_Admin0.geojson',
            'geojson', None, None, 'c69a7ea486af00be2c409ef3b7894514')
        assert result['5'] == (
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/hotosm_nic_airports_lines_shp.zip',
            'shp', None, None, '681ac397411c62c3aba4c630a2fd27da')
        assert result['6'] == (
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/Dataset.csv',
            'csv', None, None, '50cd43aeecf3742f7fb3b8a7fb18af89')
        assert result['7'] == (
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/list_one.xls',
            'xls', None, None, 'd5177e2dadba53a36ae2323037e06c96')
        assert result['8'] == (
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/ACLED-Country-Coverage-and-ISO-Codes_8.2019.xlsx',
            'xlsx', None, None, '74f72b149defd3f1a3c9000600734a96')
        assert result['9'] == (
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/response.html',
            'html', None, None, '9070c22e0c8c41204c6d4e509929246e')
        assert result['10'] == (
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/response.html',
            'csv', 'File mimetype text/html; charset=utf-8 does not match HDX format csv!', None,
            '9070c22e0c8c41204c6d4e509929246e')
        assert result['11'] == (
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/tests/fixtures/retrieve/response.html',
            'xls',
            "File mimetype text/html; charset=utf-8 does not match HDX format xls! File signature b'<htm' does not match HDX format xls!",
            None, '9070c22e0c8c41204c6d4e509929246e')
