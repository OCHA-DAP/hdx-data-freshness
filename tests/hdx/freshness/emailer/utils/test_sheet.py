"""
Unit tests for sheet code.

"""
from os import getenv
from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.freshness.emailer.utils.sheet import Sheet
from hdx.utilities.dateparse import parse_date


class TestSheet:
    @pytest.fixture(scope="function")
    def configuration(self):
        project_config_yaml = join(
            "tests", "fixtures", "emailer", "project_configuration.yaml"
        )
        return Configuration(
            hdx_site="prod",
            user_agent="test",
            hdx_read_only=True,
            project_config_yaml=project_config_yaml,
        )

    @pytest.fixture(scope="function")
    def configuration_multiple(self):
        project_config_yaml = join(
            "tests",
            "fixtures",
            "emailer",
            "project_configuration_multiple.yaml",
        )
        return Configuration(
            hdx_site="prod",
            user_agent="test",
            hdx_read_only=True,
            project_config_yaml=project_config_yaml,
        )

    def test_setup_input(self, configuration):
        now = parse_date(
            "2019-10-24 19:07:30.333492", include_microseconds=True
        )
        sheet = Sheet(now)
        sheet.setup_gsheet(configuration, getenv("GSHEET_AUTH"), True, False)
        result = sheet.setup_input()
        if result:
            print(result)
        assert sheet.dutyofficer == {
            "name": "Davide Mmmmmm2",
            "email": "mmmmmmm2@ab.org",
        }
        assert sheet.datagridccs == ["godfrey@abc.org"]
        assert sheet.datagrids == {
            "afg": {
                "datagrid": "groups:afg",
                "administrative_divisions": '(vocab_Topics:"administrative divisions" AND has_geodata:True) ! (title:hotosm)',
                "idp": '(vocab_Topics:"internally displaced persons - idp" AND subnational:1) ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-conflict-induced-displacements-in-2018) ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-conflict-induced-displacements-in-2018) ! (name:afghanistan-protection-assessment-of-conflict-affected-populations-may-2018) ! (name:reach-dataset-afghanistan-north-flood-response-evaluation-assessment) ! (name:afghanistan-conflict-induced-displacements-in-2017) ! (name:reach-dataset-multi-cluster-needs-assessment-of-prolonged-idps-in-afghanistan) ! (name:afg-conflict-idps) ! (name:afghanistan-verified-estimates-of-idps-displaced-between-1-january-2014-and-1-march-2016) ! (name:afghanistan-joint-education-and-child-protection-needs-assessment)',
                "refugees_poc": '(vocab_Topics:"refugees" AND subnational:1) OR (vocab_Topics:"persons of concern - populations of concern - poc") ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-protection-assessment-of-conflict-affected-populations-may-2018) ! (name:afghan-voluntary-repatriation-2016) ! (name:afghanistan-joint-education-and-child-protection-needs-assessment)',
                "returnees": '(vocab_Topics:"returnees" AND subnational:1) ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-protection-assessment-of-conflict-affected-populations-may-2018)',
                "humanitarian_profile_locations": '(vocab_Topics:"displaced persons locations - camps - shelters" AND subnational:1) OR (name:"afghanistan-displacement-data-baseline-assessment-iom-dtm" AND subnational:1)',
                "casualties": '(vocab_Topics:"casualties" AND subnational:1) OR (vocab_Topics:"fatalities - deaths" AND subnational:1) OR (name:"acled-data-for-afghanistan" AND subnational:1) ! (name:natural-disaster-incidents-from-1-january-to-31-december-2015) ! (name:natural-disaster-incidents-from-1-january-to-31-december-2016) ! (name:natural-disaster-incidents-from-1-january-to-30-june-2015)',
                "missing_persons": '(vocab_Topics:"missing persons" AND subnational:1)',
                "3w": '(vocab_Topics:"who is doing what and where - 3w - 4w - 5w" AND subnational:1) ! (name:iati-afg) ! (name:afghanistan-who-does-what-where-july-to-september-2018) ! (name:afghanistan-who-does-what-where-october-to-december-2018) ! (name:afghanistan-who-does-what-where-january-to-march-2015) ! (name:afghanistan-who-does-what-where-july-to-september-2015) ! (name:afghanistan-who-does-what-where-april-to-june-2015) ! (name:afghanistan-who-does-what-where-october-to-december-2015) ! (name:afghanistan-who-does-what-where-april-to-june-2017) ! (name:afghanistan-who-does-what-where-october-to-december-2017) ! (name:afghanistan-who-does-what-where-july-to-september-2017) ! (name:afghanistan-who-does-what-where-january-to-march-2017) ! (name:afghanistan-who-does-what-where-october-to-december-2016) ! (name:afghanistan-who-does-what-where-april-to-june-2016) ! (name:afghanistan-who-does-what-where-january-to-march-2016) ! (name:afghanistan-who-does-what-where-july-to-september-2016) ! (name:afghanistan-ngo-presence-between-the-years-2000-and-2014) ! (name:afghanistan-who-does-what-where-april-to-june-2018) ! (name:afghanistan-who-does-what-where-january-to-march-2018) ! (name:afghanistan-who-does-what-where-january-to-march-2019)',
                "affected_areas": '(vocab_Topics:"affected area" AND subnational:1) ! (vocab_Topics:"damage assessment")',
                "conflict_events": '(vocab_Topics:"violence and conflict" AND subnational:1) OR (vocab_Topics:"security incidents" AND subnational:1) OR (vocab_Topics:"armed violence" AND subnational:1 OR (organization:acled) ! (name:security-incidents)',
                "humanitarian_access": '(vocab_Topics:"humanitarian access" AND subnational:1)',
                "transportation_status": '(vocab_Topics:"roadblocks - checkpoints - obstacles" AND subnational:1) OR (vocab_Topics:"transportation status" AND subnational:1)',
                "damaged_buildings": '(vocab_Topics:"damaged buildings" AND subnational:1) OR (vocab_Topics:"damage assessment" AND subnational:1)',
                "food_security": '(vocab_Topics:"integrated phase classification - ipc" AND subnational:1) OR (vocab_Topics:"food security" AND subnational:1) ! (name:coping-strategy-index-csi) ! (name:food-consumption-score-fcs) ! (name:afghanistan-rainfall-2013-2016-and-population-comparisons-2010-2020)',
                "gam": '(vocab_Topics:"global acute malnutrition - gam" AND subnational:1)',
                "sam": '(vocab_Topics:"severe acute malnutrition - sam" AND subnational:1)',
                "food_prices": '(vocab_Topics:"food prices" AND vocab_Topics:"food security" AND subnational:1) OR (name:"wfp-food-prices-for-afghanistan" AND subnational:1)',
                "populated_places": '(vocab_Topics:"populated places - settlements" AND subnational:1)',
                "roads": '(vocab_Topics:"roads" AND subnational:1) OR (name:"afghanistan-roads")',
                "airports": '(vocab_Topics:"airports" AND subnational:1)',
                "health_facilities": '(vocab_Topics:"health facilities" AND subnational:1) OR (vocab_Topics:hospitals AND subnational:1)',
                "education_facilities": '(vocab_Topics:"education facilities - schools" AND subnational:1)',
                "affected_schools": '(vocab_Topics:"affected facilities" AND vocab_Topics:"education facilities - schools" AND subnational:1)',
                "baseline_population": '(vocab_Topics:"baseline population" AND subnational:1) OR (vocab_Topics:"demographics" AND subnational:1) ! (vocab_Topics:"people in need - pin") ! (vocab_Topics:"affected population") ! (vocab_Topics:"displacement") ! (vocab_Topics:"internally displaced persons - idp") ! (vocab_Topics:"humanitarian needs overview - hno") ! (organization:"worldpop" AND title:" - Population") ! (organization:unhabitat-guo AND title:" - Demographic, Health, Education and Transport indicators") ! (organization:afdb) ! (name:estimated-population-of-afghanistan-2015-2016) ! (name:afghanistan-rainfall-2013-2016-and-population-comparisons-2010-2020)',
                "baseline_population_sadd": '(vocab_Topics:"baseline population" AND vocab_Topics:"sex and age disaggregated data - sadd" AND subnational:1) ! (organization:afdb) #African Development Bank Group datasets are not subnational ! (name:estimated-population-of-afghanistan-2015-2016)',
                "poverty_rate": "(vocab_Topics:poverty AND subnational:1)",
                "owner": {"name": "Peter", "email": "pete@abc.org"},
            },
            "sdn": {
                "datagrid": "groups:sdn",
                "idp": '(vocab_Topics:"internally displaced persons - idp" AND subnational:1) ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-conflict-induced-displacements-in-2018) ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-conflict-induced-displacements-in-2018) ! (name:afghanistan-protection-assessment-of-conflict-affected-populations-may-2018) ! (name:reach-dataset-afghanistan-north-flood-response-evaluation-assessment) ! (name:afghanistan-conflict-induced-displacements-in-2017) ! (name:reach-dataset-multi-cluster-needs-assessment-of-prolonged-idps-in-afghanistan) ! (name:afg-conflict-idps) ! (name:afghanistan-verified-estimates-of-idps-displaced-between-1-january-2014-and-1-march-2016) ! (name:afghanistan-joint-education-and-child-protection-needs-assessment)',
                "refugees_poc": '(vocab_Topics:"refugees" AND subnational:1) OR (vocab_Topics:"persons of concern - populations of concern - poc") ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-protection-assessment-of-conflict-affected-populations-may-2018) ! (name:afghan-voluntary-repatriation-2016) ! (name:afghanistan-joint-education-and-child-protection-needs-assessment)',
                "returnees": '(vocab_Topics:"returnees" AND subnational:1) ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-protection-assessment-of-conflict-affected-populations-may-2018)',
                "humanitarian_profile_locations": '(vocab_Topics:"displaced persons locations - camps - shelters" AND subnational:1) OR (name:"afghanistan-displacement-data-baseline-assessment-iom-dtm" AND subnational:1)',
                "casualties": '(vocab_Topics:"casualties" AND subnational:1) OR (vocab_Topics:"fatalities - deaths" AND subnational:1) OR (name:"acled-data-for-afghanistan" AND subnational:1) ! (name:natural-disaster-incidents-from-1-january-to-31-december-2015) ! (name:natural-disaster-incidents-from-1-january-to-31-december-2016) ! (name:natural-disaster-incidents-from-1-january-to-30-june-2015)',
                "missing_persons": '(vocab_Topics:"missing persons" AND subnational:1)',
                "3w": '(vocab_Topics:"who is doing what and where - 3w - 4w - 5w" AND subnational:1) ! (name:iati-afg) ! (name:afghanistan-who-does-what-where-july-to-september-2018) ! (name:afghanistan-who-does-what-where-october-to-december-2018) ! (name:afghanistan-who-does-what-where-january-to-march-2015) ! (name:afghanistan-who-does-what-where-july-to-september-2015) ! (name:afghanistan-who-does-what-where-april-to-june-2015) ! (name:afghanistan-who-does-what-where-october-to-december-2015) ! (name:afghanistan-who-does-what-where-april-to-june-2017) ! (name:afghanistan-who-does-what-where-october-to-december-2017) ! (name:afghanistan-who-does-what-where-july-to-september-2017) ! (name:afghanistan-who-does-what-where-january-to-march-2017) ! (name:afghanistan-who-does-what-where-october-to-december-2016) ! (name:afghanistan-who-does-what-where-april-to-june-2016) ! (name:afghanistan-who-does-what-where-january-to-march-2016) ! (name:afghanistan-who-does-what-where-july-to-september-2016) ! (name:afghanistan-ngo-presence-between-the-years-2000-and-2014) ! (name:afghanistan-who-does-what-where-april-to-june-2018) ! (name:afghanistan-who-does-what-where-january-to-march-2018) ! (name:afghanistan-who-does-what-where-january-to-march-2019)',
                "affected_areas": '(vocab_Topics:"affected area" AND subnational:1) ! (vocab_Topics:"damage assessment")',
                "conflict_events": '(vocab_Topics:"violence and conflict" AND subnational:1) OR (vocab_Topics:"security incidents" AND subnational:1) OR (vocab_Topics:"armed violence" AND subnational:1 OR (organization:acled) ! (name:security-incidents)',
                "humanitarian_access": '(vocab_Topics:"humanitarian access" AND subnational:1)',
                "transportation_status": '(vocab_Topics:"roadblocks - checkpoints - obstacles" AND subnational:1) OR (vocab_Topics:"transportation status" AND subnational:1)',
                "damaged_buildings": '(vocab_Topics:"damaged buildings" AND subnational:1) OR (vocab_Topics:"damage assessment" AND subnational:1)',
                "food_security": '(vocab_Topics:"integrated phase classification - ipc" AND subnational:1) OR (vocab_Topics:"food security" AND subnational:1) ! (name:coping-strategy-index-csi) ! (name:food-consumption-score-fcs) ! (name:afghanistan-rainfall-2013-2016-and-population-comparisons-2010-2020)',
                "gam": '(vocab_Topics:"global acute malnutrition - gam" AND subnational:1)',
                "sam": '(vocab_Topics:"severe acute malnutrition - sam" AND subnational:1)',
                "food_prices": '(vocab_Topics:"food prices" AND vocab_Topics:"food security" AND subnational:1) OR (name:"wfp-food-prices-for-afghanistan" AND subnational:1)',
                "administrative_divisions": '(vocab_Topics:"administrative divisions" AND vocab_Topics:"common operational dataset - cod" AND subnational:1)',
                "populated_places": '(vocab_Topics:"populated places - settlements" AND subnational:1)',
                "roads": '(vocab_Topics:"roads" AND subnational:1) OR (name:"afghanistan-roads")',
                "airports": '(vocab_Topics:"airports" AND subnational:1)',
                "health_facilities": '(vocab_Topics:"health facilities" AND subnational:1) OR (vocab_Topics:hospitals AND subnational:1)',
                "education_facilities": '(vocab_Topics:"education facilities - schools" AND subnational:1)',
                "affected_schools": '(vocab_Topics:"affected facilities" AND vocab_Topics:"education facilities - schools" AND subnational:1)',
                "baseline_population": '(vocab_Topics:"baseline population" AND subnational:1) OR (vocab_Topics:"demographics" AND subnational:1) ! (vocab_Topics:"people in need - pin") ! (vocab_Topics:"affected population") ! (vocab_Topics:"displacement") ! (vocab_Topics:"internally displaced persons - idp") ! (vocab_Topics:"humanitarian needs overview - hno") ! (organization:"worldpop" AND title:" - Population") ! (organization:unhabitat-guo AND title:" - Demographic, Health, Education and Transport indicators") ! (organization:afdb) ! (name:estimated-population-of-afghanistan-2015-2016) ! (name:afghanistan-rainfall-2013-2016-and-population-comparisons-2010-2020)',
                "baseline_population_sadd": '(vocab_Topics:"baseline population" AND vocab_Topics:"sex and age disaggregated data - sadd" AND subnational:1) ! (organization:afdb) #African Development Bank Group datasets are not subnational ! (name:estimated-population-of-afghanistan-2015-2016)',
                "poverty_rate": "(vocab_Topics:poverty AND subnational:1)",
                "owner": {"name": "Peter", "email": "pete@abc.org"},
            },
            "wsm": {
                "datagrid": "groups:wsm",
                "idp": '(vocab_Topics:"internally displaced persons - idp" AND subnational:1) ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-conflict-induced-displacements-in-2018) ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-conflict-induced-displacements-in-2018) ! (name:afghanistan-protection-assessment-of-conflict-affected-populations-may-2018) ! (name:reach-dataset-afghanistan-north-flood-response-evaluation-assessment) ! (name:afghanistan-conflict-induced-displacements-in-2017) ! (name:reach-dataset-multi-cluster-needs-assessment-of-prolonged-idps-in-afghanistan) ! (name:afg-conflict-idps) ! (name:afghanistan-verified-estimates-of-idps-displaced-between-1-january-2014-and-1-march-2016) ! (name:afghanistan-joint-education-and-child-protection-needs-assessment)',
                "refugees_poc": '(vocab_Topics:"refugees" AND subnational:1) OR (vocab_Topics:"persons of concern - populations of concern - poc") ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-protection-assessment-of-conflict-affected-populations-may-2018) ! (name:afghan-voluntary-repatriation-2016) ! (name:afghanistan-joint-education-and-child-protection-needs-assessment)',
                "returnees": '(vocab_Topics:"returnees" AND subnational:1) ! (name:whole-of-afghanistan-assessment-household-dataset-august-2018) ! (name:afghanistan-protection-assessment-of-conflict-affected-populations-may-2018)',
                "humanitarian_profile_locations": '(vocab_Topics:"displaced persons locations - camps - shelters" AND subnational:1) OR (name:"afghanistan-displacement-data-baseline-assessment-iom-dtm" AND subnational:1)',
                "casualties": '(vocab_Topics:"casualties" AND subnational:1) OR (vocab_Topics:"fatalities - deaths" AND subnational:1) OR (name:"acled-data-for-afghanistan" AND subnational:1) ! (name:natural-disaster-incidents-from-1-january-to-31-december-2015) ! (name:natural-disaster-incidents-from-1-january-to-31-december-2016) ! (name:natural-disaster-incidents-from-1-january-to-30-june-2015)',
                "missing_persons": '(vocab_Topics:"missing persons" AND subnational:1)',
                "3w": '(vocab_Topics:"who is doing what and where - 3w - 4w - 5w" AND subnational:1) ! (name:iati-afg) ! (name:afghanistan-who-does-what-where-july-to-september-2018) ! (name:afghanistan-who-does-what-where-october-to-december-2018) ! (name:afghanistan-who-does-what-where-january-to-march-2015) ! (name:afghanistan-who-does-what-where-july-to-september-2015) ! (name:afghanistan-who-does-what-where-april-to-june-2015) ! (name:afghanistan-who-does-what-where-october-to-december-2015) ! (name:afghanistan-who-does-what-where-april-to-june-2017) ! (name:afghanistan-who-does-what-where-october-to-december-2017) ! (name:afghanistan-who-does-what-where-july-to-september-2017) ! (name:afghanistan-who-does-what-where-january-to-march-2017) ! (name:afghanistan-who-does-what-where-october-to-december-2016) ! (name:afghanistan-who-does-what-where-april-to-june-2016) ! (name:afghanistan-who-does-what-where-january-to-march-2016) ! (name:afghanistan-who-does-what-where-july-to-september-2016) ! (name:afghanistan-ngo-presence-between-the-years-2000-and-2014) ! (name:afghanistan-who-does-what-where-april-to-june-2018) ! (name:afghanistan-who-does-what-where-january-to-march-2018) ! (name:afghanistan-who-does-what-where-january-to-march-2019)',
                "affected_areas": '(vocab_Topics:"affected area" AND subnational:1) ! (vocab_Topics:"damage assessment")',
                "conflict_events": '(vocab_Topics:"violence and conflict" AND subnational:1) OR (vocab_Topics:"security incidents" AND subnational:1) OR (vocab_Topics:"armed violence" AND subnational:1 OR (organization:acled) ! (name:security-incidents)',
                "humanitarian_access": '(vocab_Topics:"humanitarian access" AND subnational:1)',
                "transportation_status": '(vocab_Topics:"roadblocks - checkpoints - obstacles" AND subnational:1) OR (vocab_Topics:"transportation status" AND subnational:1)',
                "damaged_buildings": '(vocab_Topics:"damaged buildings" AND subnational:1) OR (vocab_Topics:"damage assessment" AND subnational:1)',
                "food_security": '(vocab_Topics:"integrated phase classification - ipc" AND subnational:1) OR (vocab_Topics:"food security" AND subnational:1) ! (name:coping-strategy-index-csi) ! (name:food-consumption-score-fcs) ! (name:afghanistan-rainfall-2013-2016-and-population-comparisons-2010-2020)',
                "gam": '(vocab_Topics:"global acute malnutrition - gam" AND subnational:1)',
                "sam": '(vocab_Topics:"severe acute malnutrition - sam" AND subnational:1)',
                "food_prices": '(vocab_Topics:"food prices" AND vocab_Topics:"food security" AND subnational:1) OR (name:"wfp-food-prices-for-afghanistan" AND subnational:1)',
                "administrative_divisions": '(vocab_Topics:"administrative divisions" AND vocab_Topics:"common operational dataset - cod" AND subnational:1)',
                "populated_places": '(vocab_Topics:"populated places - settlements" AND subnational:1)',
                "roads": '(vocab_Topics:"roads" AND subnational:1) OR (name:"afghanistan-roads")',
                "airports": '(vocab_Topics:"airports" AND subnational:1)',
                "health_facilities": '(vocab_Topics:"health facilities" AND subnational:1) OR (vocab_Topics:hospitals AND subnational:1)',
                "education_facilities": '(vocab_Topics:"education facilities - schools" AND subnational:1)',
                "affected_schools": '(vocab_Topics:"affected facilities" AND vocab_Topics:"education facilities - schools" AND subnational:1)',
                "baseline_population": '(vocab_Topics:"baseline population" AND subnational:1) OR (vocab_Topics:"demographics" AND subnational:1) ! (vocab_Topics:"people in need - pin") ! (vocab_Topics:"affected population") ! (vocab_Topics:"displacement") ! (vocab_Topics:"internally displaced persons - idp") ! (vocab_Topics:"humanitarian needs overview - hno") ! (organization:"worldpop" AND title:" - Population") ! (organization:unhabitat-guo AND title:" - Demographic, Health, Education and Transport indicators") ! (organization:afdb) ! (name:estimated-population-of-afghanistan-2015-2016) ! (name:afghanistan-rainfall-2013-2016-and-population-comparisons-2010-2020)',
                "baseline_population_sadd": '(vocab_Topics:"baseline population" AND vocab_Topics:"sex and age disaggregated data - sadd" AND subnational:1) ! (organization:afdb) #African Development Bank Group datasets are not subnational ! (name:estimated-population-of-afghanistan-2015-2016)',
                "poverty_rate": "(vocab_Topics:poverty AND subnational:1)",
                "owner": {"name": "Nafi", "email": "nafi@abc.org"},
            },
        }

    def test_setup_input_multiple(self, configuration_multiple):
        now = parse_date(
            "2019-10-24 19:07:30.333492", include_microseconds=True
        )
        sheet = Sheet(now)
        sheet.setup_gsheet(
            configuration_multiple, getenv("GSHEET_AUTH"), True, False
        )
        error = sheet.setup_input()
        assert error == "There is more than one owner of datagrid sdn!"
