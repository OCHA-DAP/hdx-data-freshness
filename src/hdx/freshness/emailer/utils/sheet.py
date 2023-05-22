"""Utilities to handle interaction with Google sheets
"""
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

import gspread
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date

from .hdxhelper import HDXHelper

logger = logging.getLogger(__name__)


def get_date(datestr):
    return datetime.strptime(datestr, "%Y-%m-%d")


class Sheet:
    """A class that provides functions to interact with a Google spreadsheet

    Args:
        now (datetime): Date to use for now
    """

    row_limit = 1000

    def __init__(self, now: datetime):
        self.now = now
        self.dutyofficers_spreadsheet = None
        self.datagrids_spreadsheet = None
        self.issues_spreadsheet = None
        self.dutyofficer = None
        self.datagrids = dict()
        self.datagridccs = list()

    @staticmethod
    def add_category_to_datagrid(
        hxltags: Dict[str, int], datagrid: Dict[str, str], row: List
    ) -> None:
        """Add a category to the datagrid from a supplied row read from the Google
        spreadsheet and map it to an HDX query

        Args:
            hxltags (Dict[str, int]): Mapping from HXL tag to column number
            datagrid (Dict[str, str]): Mapping from a category to a query
            row (List): Row from datagrids Google spreadsheet

        Returns:
            None
        """
        category = row[hxltags["#category"]].strip()
        include = row[hxltags["#include"]]
        if include:
            include = include.strip()
        exclude = row[hxltags["#exclude"]]
        if exclude:
            exclude = exclude.strip()
        query = datagrid.get(category, "")
        if query:
            queryparts = query.split(" ! ")
            query = queryparts[0]
            if include:
                query = f"{query} OR {include}"
            if len(queryparts) > 1:
                query = f"{query} ! {' ! '.join(queryparts[1:])}"
        elif include:
            query = include
        if exclude:
            query = f"{query} ! {exclude}"
        datagrid[category] = query

    def get_datagrid(
        self,
        hxltags: Dict[str, int],
        datagridname: str,
        rows: List[List],
        defaultgrid: Dict[str, str],
    ) -> Dict[str, str]:
        """Get datagrid with given name, constructing it if it does not already exist.
        defaultgrid contains defaults for all countries.

        Args:
            hxltags (Dict[str, int]): Mapping from HXL tag to column number
            datagridname (str): Name of datagrid
            rows (List[List]): Row from datagrids Google spreadsheet
            defaultgrid (Dict[str, str]): Default datagrid (defaults for all countries)

        Returns:
            Dict[str, str]: Datagrid
        """
        datagridname = datagridname.strip()
        if datagridname == "" or datagridname == "cc":
            return None
        datagrid = self.datagrids.get(datagridname)
        if datagrid is None:
            datagrid = dict()
            self.datagrids[datagridname] = datagrid
            for row in rows:
                if row[hxltags["#datagrid"]] == datagridname:
                    self.add_category_to_datagrid(hxltags, datagrid, row)
            for key in defaultgrid:
                if key not in datagrid:
                    if key == "datagrid":
                        datagrid[key] = defaultgrid[key].replace(
                            "$datagrid", datagridname
                        )
                    else:
                        datagrid[key] = defaultgrid[key]
        return datagrid

    def setup_gsheet(
        self,
        configuration: Configuration,
        gsheet_auth: Optional[str],
        spreadsheet_test: bool,
        no_spreadsheet: bool,
    ) -> Optional[str]:
        """Open the various Google spreadsheets. The Datasets with Issues spreadsheet
        is for output. The HDX Data Partnerships Team Duty Roster and DataGrid Curation
        filters spreadsheets are for input.

        Args:
            configuration (Configuration): Configuration object
            gsheet_auth (Optional[str]): Google Sheets authorisation
            spreadsheet_test (bool): Output to test Google spreadsheet
            no_spreadsheet (bool): Don't output to Google spreadsheet

        Returns:
            Optional[str]: Error message or None
        """

        if not gsheet_auth:
            return "No GSheet Credentials!"
        try:
            info = json.loads(gsheet_auth)
            scopes = ["https://www.googleapis.com/auth/spreadsheets"]
            gc = gspread.service_account_from_dict(info, scopes=scopes)
            if spreadsheet_test:  # use test not prod spreadsheet
                issues_spreadsheet = configuration[
                    "test_issues_spreadsheet_url"
                ]
            else:
                issues_spreadsheet = configuration[
                    "prod_issues_spreadsheet_url"
                ]
            logger.info("Opening duty officers gsheet")
            self.dutyofficers_spreadsheet = gc.open_by_url(
                configuration["dutyofficers_url"]
            )
            logger.info("Opening datagrids gsheet")
            self.datagrids_spreadsheet = gc.open_by_url(
                configuration["datagrids_url"]
            )
            if not no_spreadsheet:
                logger.info("Opening issues gsheet")
                self.issues_spreadsheet = gc.open_by_url(issues_spreadsheet)
            else:
                self.issues_spreadsheet = None
        except Exception as ex:
            return str(ex)
        return None

    def setup_input(self) -> Optional[str]:
        """Read in the input Google spreadsheets

        Returns:
            Optional[str]: Error message or None
        """
        logger.info("--------------------------------------------------")
        try:
            sheet = self.dutyofficers_spreadsheet.worksheet("DutyRoster")
            current_values = sheet.get_values()
            hxltags = {tag: i for i, tag in enumerate(current_values[1])}
            startdate_ind = hxltags["#date+start"]
            contactname_ind = hxltags["#contact+name"]
            contactemail_ind = hxltags["#contact+email"]
            for row in sorted(
                current_values[2:],
                key=lambda x: x[startdate_ind],
                reverse=True,
            ):
                startdate = row[startdate_ind].strip()
                if parse_date(startdate) <= self.now:
                    dutyofficer_name = row[contactname_ind]
                    if dutyofficer_name:
                        dutyofficer_name = dutyofficer_name.strip()
                        self.dutyofficer = {
                            "name": dutyofficer_name,
                            "email": row[contactemail_ind].strip(),
                        }
                        logger.info(f"Duty officer: {dutyofficer_name}")
                        break

            sheet = self.datagrids_spreadsheet.worksheet("DataGrids")
            current_values = sheet.get_values()
            hxltags = {tag: i for i, tag in enumerate(current_values[1])}
            rows = current_values[2:]
            defaultgrid = dict()
            for row in rows:
                if row[hxltags["#datagrid"]] == "default":
                    self.add_category_to_datagrid(hxltags, defaultgrid, row)

            sheet = self.datagrids_spreadsheet.worksheet("Curators")
            current_values = sheet.get_values()
            curators_hxltags = {
                tag: i for i, tag in enumerate(current_values[1])
            }
            curators = current_values[2:]
            for row in curators:
                curatoremail = row[curators_hxltags["#contact+email"]].strip()
                owner = row[curators_hxltags["#datagrid"]]
                for datagridname in owner.strip().split(","):
                    if datagridname.strip() == "cc":
                        self.datagridccs.append(curatoremail)
            for row in curators:
                curatorname = row[curators_hxltags["#contact+name"]].strip()
                curatoremail = row[curators_hxltags["#contact+email"]].strip()
                owner = row[curators_hxltags["#datagrid"]]
                if owner is not None:
                    for datagridname in owner.strip().split(","):
                        datagrid = self.get_datagrid(
                            hxltags, datagridname, rows, defaultgrid
                        )
                        if datagrid is None:
                            continue
                        if datagrid.get("owner"):
                            raise ValueError(
                                f"There is more than one owner of datagrid {datagridname}!"
                            )
                        datagrid["owner"] = {
                            "name": curatorname,
                            "email": curatoremail,
                        }
            for datagridname in self.datagrids:
                if "owner" not in self.datagrids[datagridname]:
                    raise ValueError(
                        f"Datagrid {datagridname} does not have an owner!"
                    )
        except Exception as ex:
            return str(ex)

    def update(
        self,
        sheetname: str,
        rows: List[Dict],
        dutyofficer_name: Optional[str] = None,
    ) -> None:
        """Update output Google spreadsheet (which must have been set up with
        setup_gsheet). The duty officer which is usually taken from the HDX Data
        Partnerships Team Duty Roster spreadsheet can be overridden by supplying
        dutyofficer_name.

        Args:
            sheetname (str): Name of tab in Google spreadsheet to output to
            rows (List[Dict]): Rows to add to Google spreadsheet
            dutyofficer_name (Optional[str]): Name of duty office. Defaults to None.

        Returns:
            None
        """

        if self.issues_spreadsheet is None or (
            self.dutyofficer is None and dutyofficer_name is None
        ):
            logger.warning("Cannot update Google spreadsheet!")
            return
        logger.info("Updating Google spreadsheet.")
        sheet = self.issues_spreadsheet.worksheet(sheetname)
        gsheet_rows = sheet.get_values()
        keys = gsheet_rows[0]
        url_ind = keys.index("URL")
        if "Update Frequency" in keys:
            update_frequency_ind = keys.index("Update Frequency")
        else:
            update_frequency_ind = None
        dateadded_ind = keys.index("Date Added")
        dateoccurred_ind = keys.index("Date Last Occurred")
        no_times_ind = keys.index("No. Times")
        assigned_ind = keys.index("Assigned")
        status_ind = keys.index("Status")
        headers = gsheet_rows[0]
        gsheet_rows = [row for row in gsheet_rows[1:] if row[url_ind]]
        urls = [x[url_ind] for x in gsheet_rows]
        if update_frequency_ind is not None:
            for gsheet_row in gsheet_rows:
                updatefreq = gsheet_row[update_frequency_ind]
                gsheet_row[update_frequency_ind] = int(
                    Dataset.transform_update_frequency(updatefreq)
                )
        updated_notimes = set()
        now = self.now.replace(tzinfo=None).isoformat()
        for row in rows:
            url = row["URL"]
            new_row = [row.get(key, "") for key in keys]
            new_row[dateoccurred_ind] = now
            try:
                rowno = urls.index(url)
                current_row = gsheet_rows[rowno]
                new_row[dateadded_ind] = current_row[dateadded_ind]
                no_times = current_row[no_times_ind]
                new_row[no_times_ind] = int(no_times)
                if url not in updated_notimes:
                    updated_notimes.add(url)
                    new_row[no_times_ind] += 1
                new_row[assigned_ind] = current_row[assigned_ind]
                new_row[status_ind] = current_row[status_ind]
                gsheet_rows[rowno] = new_row
            except ValueError:
                new_row[dateadded_ind] = now
                new_row[no_times_ind] = 1
                if dutyofficer_name is not None:
                    new_row[assigned_ind] = dutyofficer_name
                else:
                    new_row[assigned_ind] = self.dutyofficer["name"]
                gsheet_rows.append(new_row)
                urls.append(url)
                updated_notimes.add(url)
        if update_frequency_ind is None:
            gsheet_rows = sorted(
                gsheet_rows, key=lambda x: x[dateoccurred_ind], reverse=True
            )
        else:
            headers.append("sort")
            sort_ind = headers.index("sort")
            for gsheet_row in gsheet_rows:
                dateoccurred = gsheet_row[dateoccurred_ind]
                if dateoccurred == now:
                    sort_val = 0
                else:
                    nodays = self.now - parse_date(dateoccurred)
                    update_freq = gsheet_row[update_frequency_ind]
                    if update_freq == -1:
                        update_freq = 1000
                    elif update_freq == -2:
                        update_freq = 500
                    elif update_freq == 0:
                        update_freq = 0.5
                    sort_val = nodays.days / update_freq
                gsheet_row.append(sort_val)
            gsheet_rows = sorted(
                gsheet_rows,
                key=lambda x: (-x[sort_ind], x[dateoccurred_ind]),
                reverse=True,
            )
        no_rows = len(gsheet_rows)
        no_rows_to_remove = no_rows - self.row_limit
        gsheet_rows = gsheet_rows[:-no_rows_to_remove]

        if update_frequency_ind is not None:
            for gsheet_row in gsheet_rows:
                update_freq = gsheet_row[update_frequency_ind]
                gsheet_row[
                    update_frequency_ind
                ] = HDXHelper.get_update_frequency(update_freq)
                del gsheet_row[sort_ind]
            del headers[sort_ind]
        sheet.clear()
        sheet.update("A1", [headers] + gsheet_rows)

    @staticmethod
    def construct_row(
        hdxhelper: HDXHelper,
        dataset: Dict,
        maintainer: Dict[str, str],
        orgadmins: List[Dict[str, str]],
    ) -> Dict[str, str]:
        """Construct a Google spreadsheet dataset row from dataset, maintainer and
        organisation administrators.

        Args:
            hdxhelper (HDXHelper): HDX helper object
            dataset (Dict): Dataset to examine
            maintainer (Dict[str, str]): Maintainer information
            orgadmins (List[Dict[str, str]]): List of organisation administrator info

        Returns:
            Dict[str, str]: Spreadsheet row
        """
        url = hdxhelper.get_dataset_url(dataset)
        title = dataset["title"]
        org_title = dataset["organization_title"]
        if maintainer:
            maintainer_name = maintainer["name"]
            maintainer_email = maintainer["email"]
        else:
            maintainer_name, maintainer_email = "", ""
        orgadmin_names = ",".join([x["name"] for x in orgadmins])
        orgadmin_emails = ",".join([x["email"] for x in orgadmins])
        update_freq = dataset["update_frequency"]
        latest_of_modifieds = (
            dataset["latest_of_modifieds"].replace(tzinfo=None).isoformat()
        )
        row = {
            "URL": url,
            "Title": title,
            "Organisation": org_title,
            "Maintainer": maintainer_name,
            "Maintainer Email": maintainer_email,
            "Org Admins": orgadmin_names,
            "Org Admin Emails": orgadmin_emails,
            "Update Frequency": update_freq,
            "Latest of Modifieds": latest_of_modifieds,
        }
        return row
