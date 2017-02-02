from datetime import datetime

from retrieval import retrieve


class TestRetrieve:
    def test_retrieve(self):
        url1 = 'https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/now.pickle'
        url2 = 'https://tools.ietf.org/rfc/rfc7230.txt'
        url3 = 'https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound'
        url4 = 'file://lala'
        urls = [(url1, '1', False), (url2, '2', False), (url3, '3', False), (url4, '4', False), (url1, '5', True), (url2, '6', True)]
        result = retrieve(urls)
        assert result == {'1': (url1, None, None, '35757fc63e863d962dfc8d5f01d9d121', False),
                          '2': (url2, None, datetime(2014, 6, 7, 0, 41, 49), None, False),
                          '3': (url3, 'code=404 message=Non-retryable response code raised=aiohttp.errors.HttpProcessingError url=https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound', None, None, False),
                          '4': (url4, 'code= message=[Errno -2] Cannot connect to host lala:None ssl:False [Name or service not known] raised=aiohttp.errors.ClientOSError url=file://lala', None, None, False),
                          '5': (url1, None, None, '35757fc63e863d962dfc8d5f01d9d121', True),
                          '6': (url2, None, datetime(2014, 6, 7, 0, 41, 49), 'c9313c2fe19195b91f00ab06cdc0f97b', True)
                          }