from retrieval import retrieve


class TestRetrieve:
    def test_retrieve(self):
        url1 = 'https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/now.pickle'
        url2 = 'https://tools.ietf.org/rfc/rfc7230.txt'
        url3 = 'https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound'
        url4 = 'file://lala'
        urls = [(url1, '1'), (url2, '2'), (url3, '3'), (url4, '4')]
        result = retrieve(urls)
        assert result == {'1': (url1, 2, '35757fc63e863d962dfc8d5f01d9d121'),
                          '2': (url2, 1, 'Sat, 07 Jun 2014 00:41:49 GMT'),
                          '3': (url3, 0, 'code:404 url=https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound message=Non-retryable response code raised=aiohttp.errors.HttpProcessingError'),
                          '4': (url4, 0, 'code: url=file://lala message=[Errno -2] Cannot connect to host lala:None ssl:False [Name or service not known] raised=aiohttp.errors.ClientOSError')}