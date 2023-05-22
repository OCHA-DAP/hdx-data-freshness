"""Outputs freshness statuses for datasets by email and to a Google sheet. Reports
overdue and delinquent datasets. Also reports datasets with broken or no resources
and/or invalid maintainers, organisations with invalid administrators and candidates for
the data grid.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Type

from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import dict_of_lists_add

from ..utils.databasequeries import DatabaseQueries
from ..utils.freshnessemail import Email
from ..utils.sheet import Sheet

logger = logging.getLogger(__name__)


class DataFreshnessStatus:
    """Data freshness emailer main class.

    Args:
        databasequeries (DatabaseQueries): DatabaseQueries object
        email (Email): Email object
        sheet (Sheet): Sheet object
    """

    object_output_limit = 2

    def __init__(
        self, databasequeries: DatabaseQueries, email: Email, sheet: Sheet
    ):
        self.databasequeries = databasequeries
        self.hdxhelper = databasequeries.hdxhelper
        self.email = email
        self.sheet = sheet

    def check_number_datasets(
        self,
        now: datetime,
        send_failures: List[str],
    ) -> bool:
        """Check the number of datasets in HDX today compared to yesterday and alert for
        failures like no run date today, no datasets today or a sizable fall in
        number of datasets compared to the previous day.


        Args:
            now (datetime): Date to use for now
            send_failures (List[str]): List of email addresses to send mails to

        Returns:
            bool: Whether to stop further processing due to a serious error
        """
        logger.info("\n\n*** Checking number of datasets ***")
        run_numbers = self.databasequeries.get_run_numbers()
        run_date = run_numbers[0][1]
        stop = True
        cc = None
        if now < run_date:
            subject = "FAILURE: Future run date!"
            msg = "Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n"
            to = send_failures
        elif now - run_date > timedelta(days=1):
            subject = "FAILURE: No run today!"
            msg = "Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n"
            to = send_failures
        elif len(run_numbers) == 2:
            (
                datasets_today,
                datasets_previous,
            ) = self.databasequeries.get_number_datasets()
            if datasets_today == 0:
                subject = "FAILURE: No datasets today!"
                msg = "Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n"
                to = send_failures
            elif datasets_previous == 0:
                subject = "FAILURE: Previous run corrupted!"
                msg = "Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n"
                to = send_failures
            else:
                diff_datasets = datasets_previous - datasets_today
                percentage_diff = diff_datasets / datasets_previous
                if percentage_diff <= 0.02:
                    logger.info("No issues with number of datasets.")
                    return False
                if percentage_diff > 0.5:
                    subject = "FAILURE: No datasets today!"
                    msg = "Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n"
                    to = send_failures
                else:
                    subject = "WARNING: Fall in datasets on HDX today!"
                    startmsg = f"Dear {Email.get_addressee(self.sheet.dutyofficer)},\n\n"
                    msg = f"{startmsg}There are {diff_datasets} ({percentage_diff * 100:.0f}%) fewer datasets today than yesterday on HDX which may indicate a serious problem so should be investigated!\n"
                    to, cc = self.email.get_to_cc(self.sheet.dutyofficer)
                    stop = False
        else:
            logger.info("No issues with number of datasets.")
            return False
        self.email.htmlify_send(to, subject, msg, cc=cc)
        return stop

    def process_broken(self, recipients: Optional[List[str]] = None) -> None:
        """Check for datasets that have resources with broken urls, update Google
        spreadsheet and email HDX administrators.

        Args:
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.

        Returns:
            None
        """
        logger.info("\n\n*** Checking for broken datasets ***")
        datasets = self.databasequeries.get_broken()
        if len(datasets) == 0:
            logger.info("No broken datasets found.")
            return
        startmsg = (
            "Dear {},\n\nThe following datasets have broken resources:\n\n"
        )
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.newline_to_br(startmsg))]

        def create_broken_dataset_string(ds, ma, oa):
            (
                dataset_string,
                dataset_html_string,
            ) = self.hdxhelper.create_dataset_string(
                ds,
                ma,
                oa,
                sysadmin=True,
                include_org=False,
                include_freshness=True,
            )
            Email.output_tabs(msg, htmlmsg, 2)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            newline = False
            for i, resource in enumerate(
                sorted(ds["resources"], key=lambda d: d["name"])
            ):
                if i == self.object_output_limit:
                    Email.output_tabs(msg, htmlmsg, 3)
                if i >= self.object_output_limit:
                    newline = True
                    Email.output_tabs(msg, htmlmsg, 1)
                    msg.append(f"{resource['name']} ({resource['id']})")
                    htmlmsg.append(f"{resource['name']} ({resource['id']})")
                    continue
                resource_string = f"Resource {resource['name']} ({resource['id']}) has error: {resource['error']}!"
                Email.output_tabs(msg, htmlmsg, 4)
                msg.append(f"{resource_string}\n")
                htmlmsg.append(f"{resource_string}<br>")
            if newline:
                Email.output_newline(msg, htmlmsg)

        def create_cut_down_broken_dataset_string(i, ds):
            if i == self.object_output_limit:
                Email.output_tabs(msg, htmlmsg, 1)
            if i >= self.object_output_limit:
                url = self.hdxhelper.get_dataset_url(ds)
                Email.output_tabs(msg, htmlmsg, 1)
                msg.append(f"{ds['title']} ({url})")
                htmlmsg.append(f"<a href=\"{url}\">{ds['title']}</a>")
                return True
            return False

        datasets_flat = list()
        for error_type in sorted(datasets):
            Email.output_error(msg, htmlmsg, error_type)
            datasets_error = datasets[error_type]
            for org_title in sorted(datasets_error):
                Email.output_org(msg, htmlmsg, org_title)
                org = datasets_error[org_title]
                newline = False
                for i, dataset_name in enumerate(sorted(org)):
                    dataset = org[dataset_name]
                    (
                        maintainer,
                        orgadmins,
                        _,
                    ) = self.hdxhelper.get_maintainer_orgadmins(dataset)
                    cut_down = create_cut_down_broken_dataset_string(
                        i, dataset
                    )
                    if cut_down:
                        newline = True
                    else:
                        create_broken_dataset_string(
                            dataset, maintainer, orgadmins
                        )
                    row = self.sheet.construct_row(
                        self.hdxhelper, dataset, maintainer, orgadmins
                    )
                    row["Freshness"] = self.hdxhelper.freshness_status.get(
                        dataset["fresh"], "None"
                    )
                    error = list()
                    for resource in sorted(
                        dataset["resources"], key=lambda d: d["name"]
                    ):
                        error.append(f"{resource['name']}:{resource['error']}")
                    row["Error Type"] = error_type
                    row["Error"] = "\n".join(error)
                    datasets_flat.append(row)
                if newline:
                    Email.output_newline(msg, htmlmsg)
            Email.output_newline(msg, htmlmsg)

        self.email.get_recipients_close_send(
            self.sheet.dutyofficer, recipients, "Broken datasets", msg, htmlmsg
        )
        self.sheet.update("Broken", datasets_flat)

    def process_delinquent(
        self, recipients: Optional[List[str]] = None
    ) -> None:
        """Check for datasets that have become delinquent, update Google spreadsheet
        and email HDX administrators.

        Args:
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.

        Returns:
            None
        """
        logger.info("\n\n*** Checking for delinquent datasets ***")
        nodatasetsmsg = "No delinquent datasets found."
        startmsg = "Dear {},\n\nThe following datasets have just become delinquent and their maintainers should be approached:\n\n"
        subject = "Delinquent datasets"
        sheetname = "Delinquent"
        datasets = self.databasequeries.get_status(3)
        self.email.email_admins(
            self.hdxhelper,
            datasets,
            nodatasetsmsg,
            startmsg,
            subject,
            self.sheet,
            sheetname,
            recipients,
        )

    def process_overdue(
        self,
        recipients: Optional[List[str]] = None,
        sysadmins: Optional[List[str]] = None,
    ) -> None:
        """Check for datasets that have become overdue, update Google spreadsheet
        and email maintainers, sending a summary of emails sent to HDX system
        administrators.

        Args:
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.
            sysadmins (Optional[List[str]]): HDX sysadmin emails. Defaults to None.

        Returns:
            None
        """
        logger.info("\n\n*** Checking for overdue datasets ***")
        datasets = self.databasequeries.get_status(2)
        nodatasetsmsg = "No overdue datasets found."
        startmsg = "Dear {},\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your $dashboard on HDX.\n\n"
        endmsg = '\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n'
        subject = "Time to update your datasets on HDX"
        summary_subject = "All overdue dataset emails"
        summary_startmsg = "Dear {},\n\nBelow are the emails which have been sent today to maintainers whose datasets are overdue. You may wish to follow up with them.\n\n"
        sheetname = None
        self.email.email_users_send_summary(
            self.hdxhelper,
            False,
            datasets,
            nodatasetsmsg,
            startmsg,
            endmsg,
            recipients,
            subject,
            summary_subject,
            summary_startmsg,
            self.sheet,
            sheetname,
            sysadmins=sysadmins,
        )

    def send_maintainer_email(
        self,
        invalid_maintainers: List[Dict],
        recipients: Optional[List[str]] = None,
    ) -> None:
        """Send invalid maintainer email.

        Args:
            invalid_maintainers (List[Dict]): Datasets with invalid maintainer
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.

        Returns:
            None
        """
        nodatasetsmsg = "No invalid maintainers found."
        startmsg = "Dear {},\n\nThe following datasets have an invalid maintainer and should be checked:\n\n"
        subject = "Datasets with invalid maintainer"
        sheetname = "Maintainer"
        self.email.email_admins(
            self.hdxhelper,
            invalid_maintainers,
            nodatasetsmsg,
            startmsg,
            subject,
            self.sheet,
            sheetname,
            recipients,
        )

    def send_orgadmins_email(
        self,
        invalid_orgadmins: Dict[str, Dict],
        recipients: Optional[List[str]] = None,
    ) -> None:
        """Send invalid organisation administrator email.

        Args:
            invalid_orgadmins (Dict[str, Dict]): Organisations with invalid admins
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.

        Returns:
            None
        """
        organizations_flat = list()
        if len(invalid_orgadmins) == 0:
            logger.info("No invalid organisation administrators found.")
            return
        startmsg = "Dear {},\n\nThe following organizations have an invalid administrator and should be checked:\n\n"
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.newline_to_br(startmsg))]
        for key in sorted(invalid_orgadmins):
            organization = invalid_orgadmins[key]
            url = self.hdxhelper.get_organization_url(organization)
            title = organization["title"]
            error = organization["error"]
            msg.append(f"{title} ({url})")
            htmlmsg.append(f'<a href="{url}">{title}</a>')
            msg.append(f" with error: {error}\n")
            htmlmsg.append(f" with error: {error}<br>")
            # URL	Title	Problem
            row = {"URL": url, "Title": title, "Error": error}
            organizations_flat.append(row)
        self.email.get_recipients_close_send(
            self.sheet.dutyofficer,
            recipients,
            "Organizations with invalid admins",
            msg,
            htmlmsg,
        )
        self.sheet.update("OrgAdmins", organizations_flat)

    def process_maintainer_orgadmins(
        self, recipients: Optional[List[str]] = None
    ) -> None:
        """Check for datasets that have an invalid maintainer or where the organisation
        administrators are invalid, update Google spreadsheet and email HDX system
        administrators.

        Args:
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.

        Returns:
            None
        """
        logger.info(
            "\n\n*** Checking for invalid maintainers and organisation administrators ***"
        )
        (
            invalid_maintainers,
            invalid_orgadmins,
        ) = self.databasequeries.get_invalid_maintainer_orgadmins()
        self.send_maintainer_email(invalid_maintainers, recipients)
        self.send_orgadmins_email(invalid_orgadmins, recipients)

    def process_datasets_noresources(
        self, recipients: Optional[List[str]] = None
    ) -> None:
        """Check for datasets that have no resources.

        Args:
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.

        Returns:
            None
        """

        logger.info("\n\n*** Checking for datasets with no resources ***")
        nodatasetsmsg = "No datasets with no resources found."
        startmsg = "Dear {},\n\nThe following datasets have no resources and should be checked:\n\n"
        subject = "Datasets with no resources"
        sheetname = "NoResources"
        datasets = self.databasequeries.get_datasets_noresources()
        self.email.email_admins(
            self.hdxhelper,
            datasets,
            nodatasetsmsg,
            startmsg,
            subject,
            self.sheet,
            sheetname,
            recipients,
        )

    def process_datasets_reference_period(
        self,
        recipients: Optional[List[str]] = None,
        sysadmins: Optional[List[str]] = None,
    ) -> None:
        """Check for datasets that have a reference period field that needs updating,
        update Google spreadsheet and email maintainers, sending a summary of emails
        sent to HDX system administrators.

        Args:
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.
            sysadmins (Optional[List[str]]): HDX sysadmin emails. Defaults to None.

        Returns:
            None
        """
        logger.info(
            "\n\n*** Checking for datasets where reference period has not been updated ***"
        )
        datasets = self.databasequeries.get_datasets_reference_period()
        nodatasetsmsg = (
            "No datasets with reference period needing update found."
        )
        startmsg = "Dear {},\n\nThe dataset(s) listed below have a reference period that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\n"
        endmsg = ""
        subject = "Check reference period for your datasets on HDX"
        summary_subject = "All reference period emails"
        summary_startmsg = "Dear {},\n\nBelow are the emails which have been sent today to maintainers whose datasets have a reference period that has not been updated. You may wish to follow up with them.\n\n"
        sheetname = "DateofDatasets"
        self.email.email_users_send_summary(
            self.hdxhelper,
            True,
            datasets,
            nodatasetsmsg,
            startmsg,
            endmsg,
            recipients,
            subject,
            summary_subject,
            summary_startmsg,
            self.sheet,
            sheetname,
            sysadmins=sysadmins,
        )

    def process_datasets_datagrid(
        self,
        recipients: Optional[List[str]] = None,
        datasetclass: Type[Dataset] = Dataset,
    ) -> None:
        """Check for datasets that are candidates for the datagrid.

        Args:
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.
            datasetclass (Type[Dataset]): Class with search_in_hdx. Defaults to Dataset.

        Returns:
            None
        """

        logger.info(
            "\n\n*** Checking for datasets that are candidates for the datagrid ***"
        )
        nodatasetsmsg = "No dataset candidates for the data grid {} found."
        startmsg = "Dear {},\n\nThe new datasets listed below are candidates for the data grid that you can investigate:\n\n"
        datagridstartmsg = "\nDatagrid {}:\n\n"
        subject = "Candidates for the datagrid"
        sheetname = "Datagrid"
        datasets_modified_yesterday = (
            self.databasequeries.get_datasets_modified_yesterday()
        )
        emails = dict()
        for datagridname in self.sheet.datagrids:
            datasets = list()
            datagrid = self.sheet.datagrids[datagridname]
            for category in datagrid:
                if category in ["datagrid", "owner"]:
                    continue
                runyesterday = self.databasequeries.run_numbers[1][1]
                runyesterday = runyesterday.replace(tzinfo=None)
                runyesterday = runyesterday.isoformat()
                runtoday = self.databasequeries.run_numbers[0][1]
                runtoday = runtoday.replace(tzinfo=None)
                runtoday = runtoday.isoformat()
                query = f'metadata_created:[{runyesterday}Z TO {runtoday}Z] AND {datagrid["datagrid"]} AND ({datagrid[category]})'
                datasetinfos = datasetclass.search_in_hdx(fq=query)
                for datasetinfo in datasetinfos:
                    dataset_id = datasetinfo["id"]
                    if dataset_id not in [
                        dataset["id"] for dataset in datasets
                    ]:
                        dataset = datasets_modified_yesterday.get(dataset_id)
                        if dataset is not None:
                            datasets.append(dataset)
            if len(datasets) == 0:
                logger.info(nodatasetsmsg.format(datagridname))
                continue
            owner = datagrid["owner"]
            datagridmsg = datagridstartmsg.format(datagridname)
            msg, htmlmsg = self.email.prepare_admin_emails(
                self.hdxhelper,
                datasets,
                datagridmsg,
                self.sheet,
                sheetname,
                dutyofficer=owner,
            )
            if msg is not None:
                ownertuple = (owner["name"], owner["email"])
                owneremails = emails.get(ownertuple, dict())
                for submsg in msg:
                    dict_of_lists_add(owneremails, "plain", submsg)
                for subhtmlmsg in htmlmsg:
                    dict_of_lists_add(owneremails, "html", subhtmlmsg)
                emails[ownertuple] = owneremails
        if recipients is None and len(self.sheet.datagridccs) != 0:
            users_to_email = self.sheet.datagridccs
        else:
            users_to_email = recipients
        for ownertuple in sorted(emails):
            owneremails = emails[ownertuple]
            owner = {"name": ownertuple[0], "email": ownertuple[1]}
            self.email.send_admin_summary(
                owner,
                users_to_email,
                owneremails,
                subject,
                startmsg,
                log=True,
                recipients_in_cc=True,
            )
