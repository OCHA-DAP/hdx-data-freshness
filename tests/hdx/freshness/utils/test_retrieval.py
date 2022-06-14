"""
Unit tests for the retrieval class.

"""
import re
from datetime import datetime

from hdx.freshness.utils.retrieval import Retrieval


class TestRetrieve:
    def test_retrieve(self):
        url1 = "https://www.rfc-editor.org/rfc/rfc7235.txt"
        url2 = "https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound"
        url3 = "file://lala:10"
        url4 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/COD_MOZ_Admin0.geojson"
        url5 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/hotosm_nic_airports_lines_shp.zip"
        url6 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/Dataset.csv"
        url7 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/list_one.xls"
        url8 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/ACLED-Country-Coverage-and-ISO-Codes_8.2019.xlsx"
        url9 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/response.html"
        urls = [
            (url1, "1", "html"),
            (url2, "2", "csv"),
            (url3, "3", "csv"),
            (url4, "4", "geojson"),
            (url5, "5", "shp"),
            (url6, "6", "csv"),
            (url7, "7", "xls"),
            (url8, "8", "xlsx"),
            (url9, "9", "html"),
            (url9, "10", "csv"),
            (url9, "11", "xls"),
        ]
        result = Retrieval("test", url_ignore="data.humdata.org").retrieve(
            urls
        )
        assert result["1"] == (
            url1,
            "html",
            None,
            datetime(2014, 6, 7, 0, 53, 56),
            "c9ef27e27f0bb5a772e12af426102804",
            None,
        )
        assert result["2"] == (
            url2,
            "csv",
            "code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound",
            None,
            None,
            None,
        )
        assert result["3"][0] == url3
        regexp = r"^code= message=Cannot connect to host lala:10 ssl:default \[.*\] raised=aiohttp\.client_exceptions\.ClientConnectorError url=file:\/\/lala:10$"
        assert re.search(regexp, result["3"][2]) is not None
        assert result["3"][3] is None
        assert result["3"][4] is None
        assert result["4"][0] == url4
        assert result["4"][1] == "geojson"
        assert result["4"][4] == "c69a7ea486af00be2c409ef3b7894514"
        assert result["5"][0] == url5
        assert result["5"][1] == "shp"
        assert result["5"][4] == "681ac397411c62c3aba4c630a2fd27da"
        assert result["6"][0] == url6
        assert result["6"][1] == "csv"
        assert result["6"][4] == "50cd43aeecf3742f7fb3b8a7fb18af89"
        assert result["7"][0] == url7
        assert result["7"][1] == "xls"
        assert result["7"][4] == "d5177e2dadba53a36ae2323037e06c96"
        assert result["8"][0] == url8
        assert result["8"][1] == "xlsx"
        assert result["8"][4] == "74f72b149defd3f1a3c9000600734a96"
        assert result["8"][5] == "c3d51c5b077a48221e77797f7e771d1f"
        assert result["9"][0] == url9
        assert result["9"][1] == "html"
        assert result["9"][4] == "9070c22e0c8c41204c6d4e509929246e"
        assert result["10"][0] == url9
        assert result["10"][1] == "csv"
        assert (
            result["10"][2]
            == "File mimetype text/html; charset=utf-8 does not match HDX format csv!"
        )
        assert result["10"][4] == "9070c22e0c8c41204c6d4e509929246e"
        assert result["11"][0] == url9
        assert result["11"][1] == "xls"
        assert (
            result["11"][2]
            == "File mimetype text/html; charset=utf-8 does not match HDX format xls! File signature b'<htm' does not match HDX format xls!"
        )
        assert result["11"][4] == "9070c22e0c8c41204c6d4e509929246e"
