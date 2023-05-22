"""Utilities to handle creating and sending emails
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Union

from hdx.utilities.dictandlist import dict_of_lists_add

if TYPE_CHECKING:
    from .hdxhelper import HDXHelper
    from .sheet import Sheet

logger = logging.getLogger(__name__)


class Email:
    """A class providing functions for the creation and sending of email messages in
    raw text and HTML formats. Either sysadmins_to_email or configuration must be
    specified.

    Args:
        now (datetime): Date to use for now
        sysadmin_emails (List[str]): List of admins to email.
        send_emails (Optional[Callable]): Function to send emails. Defaults to None.
    """

    def __init__(
        self,
        now: datetime,
        sysadmin_emails: List[str] = None,
        send_emails: Optional[Callable] = None,
    ):
        self.now = now
        self.send_emails: Optional[Callable] = send_emails
        self.sysadmin_emails = sysadmin_emails

    def send(
        self,
        to: List[str],
        subject: str,
        text_body: str,
        html_body: str = None,
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
    ) -> None:
        """Send email

        Args:
            to (List[str]): Email addresses of recipient(s)
            subject (str): Email subject
            text_body (str): Plain text email body
            html_body (Optional[str]): HTML email body
            cc (Optional[List[str]]): Email addresses in cc. Defaults to None.
            bcc (Optional[List[str]]): Email addresses in bcc. Defaults to None.

        Returns:
            None
        """

        if self.send_emails is not None:
            subject = f"{subject} ({self.now.strftime('%d/%m/%Y')})"
            self.send_emails(
                to,
                subject,
                text_body,
                html_body=html_body,
                cc=cc,
                bcc=bcc,
            )
        else:
            logger.warning("Not sending any email!")

    def htmlify_send(
        self,
        to: List[str],
        subject: str,
        msg: str,
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
    ) -> None:
        """Get HTML version of message and send email

        Args:
            to (List[str]): Email addresses of recipient(s)
            subject (str): Email subject
            msg (str): Plain text email message
            cc (Optional[List[str]]): Email addresses in cc. Defaults to None.
            bcc (Optional[List[str]]): Email addresses in bcc. Defaults to None.

        Returns:
            None
        """
        text_body, html_body = self.htmlify(msg)
        self.send(to, subject, text_body, html_body, cc=cc, bcc=bcc)
        logger.info(text_body)

    def close_send(
        self,
        to: Union[str, List[str]],
        subject: str,
        msg: List[str],
        htmlmsg: List[str],
        endmsg: str = "",
        cc: Optional[Union[str, List[str]]] = None,
        bcc: Optional[Union[str, List[str]]] = None,
        log: bool = True,
    ) -> None:
        """Add the message close text to the email and send it

        Args:
            to (Union[str, List[str]]): Email recipient(s)
            subject (str): Email subject
            msg (List[str]): Lines of plain text email message
            htmlmsg (List[str]): Lines of HTML email message
            endmsg (str): Additional string to add to message end
            cc (Optional[Union[str, List[str]]]): Email cc. Defaults to None.
            bcc (Optional[Union[str, List[str]]]): Email bcc. Defaults to None.
            log (bool): Whether to output email contents to log. Defaults to True.

        Returns:
            None
        """
        text_body, html_body = Email.msg_close(msg, htmlmsg, endmsg)
        self.send(to, subject, text_body, html_body, cc=cc, bcc=bcc)
        if log:
            logger.info(text_body)

    @staticmethod
    def get_addressee(
        dutyofficer: Dict[str, str],
        recipients: Optional[List[str]] = None,
        recipients_in_cc: bool = False,
    ) -> str:
        """Get the addressee

        Args:
            dutyofficer (Dict[str, str]): Duty officer information
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.
            recipients_in_cc (bool): Put recipients in cc not to. Defaults to False.

        Returns:
            str: Addressee of email
        """
        if dutyofficer and (recipients is None or recipients_in_cc is True):
            return dutyofficer["name"]
        else:
            return "system administrator"

    @staticmethod
    def fill_addressee(
        msg: List[str],
        htmlmsg: List[str],
        dutyofficer: Dict[str, str],
        recipients: Optional[List[str]] = None,
        recipients_in_cc: bool = False,
    ) -> None:
        """Add the addressee into the appropriate place in the message template

        Args:
            msg (List[str]): Lines of plain text email message
            htmlmsg (List[str]): Lines of HTML email message
            dutyofficer (Dict[str, str]): Duty officer information
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.
            recipients_in_cc (bool): Put recipients in cc not to. Defaults to False.

        Returns:
            None
        """
        if "{}" not in msg[0]:
            return
        addressee = Email.get_addressee(
            dutyofficer, recipients, recipients_in_cc=recipients_in_cc
        )
        msg[0] = msg[0].format(addressee)
        htmlmsg[0] = htmlmsg[0].format(addressee)

    def get_to_cc(
        self,
        dutyofficer: Dict[str, str],
        recipients: Optional[List[str]] = None,
        recipients_in_cc: bool = False,
    ) -> Tuple[List[str], Optional[List[str]]]:
        """Get the to and cc email fields

        Args:
            dutyofficer (Dict[str, str]): Duty officer information
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.
            recipients_in_cc (bool): Put recipients in cc not to. Defaults to False.

        Returns:
            Tuple[List[str], Optional[List[str]]]:: To and cc email fields
        """
        if recipients is None:
            if dutyofficer:
                return [dutyofficer["email"]], self.sysadmin_emails
            else:
                return self.sysadmin_emails, None
        else:
            if recipients_in_cc:
                if dutyofficer:
                    return [dutyofficer["email"]], recipients
                else:
                    raise ValueError(
                        "Dutyofficer must be supplied if recipients are in cc!"
                    )
            else:
                return recipients, None

    def get_recipients_close_send(
        self,
        dutyofficer: Dict[str, str],
        recipients: Optional[List[str]],
        subject: str,
        msg: List[str],
        htmlmsg: List[str],
        endmsg: str = "",
        log: bool = True,
        recipients_in_cc: bool = False,
    ) -> None:
        """Add the message close text to the email and send it

        Args:
            dutyofficer (Dict[str, str]): Duty officer information
            recipients (Union[str, List[str]]): Email addresses of recipient(s)
            subject (str): Email subject
            msg (List[str]): Lines of plain text email message
            htmlmsg (List[str]): Lines of HTML email message
            endmsg (str): Additional string to add to message end. Defaults to "".
            log (bool): Whether to output email contents to log. Defaults to True.
            recipients_in_cc (bool): Put recipients in cc not to. Defaults to False.

        Returns:
            None
        """

        self.fill_addressee(
            msg,
            htmlmsg,
            dutyofficer,
            recipients,
            recipients_in_cc=recipients_in_cc,
        )
        to, cc = self.get_to_cc(
            dutyofficer, recipients, recipients_in_cc=recipients_in_cc
        )
        self.close_send(to, subject, msg, htmlmsg, endmsg, cc=cc, log=log)

    def send_admin_summary(
        self,
        dutyofficer: Dict[str, str],
        recipients: Optional[List[str]],
        emails: Dict[str, List[str]],
        subject: str,
        startmsg: str,
        log: bool = True,
        recipients_in_cc: bool = False,
    ) -> None:
        """Add the message close text to the summary email for administrators and send
        it

        Args:
            dutyofficer (Dict[str, str]): Duty officer information
            recipients (Union[str, List[str]]): Email recipient(s)
            emails (Dict[str, List[str]]): Lines from plain text and html emails
            subject (str): Email subject
            startmsg (str): Text for start of email
            log (bool): Whether to output email contents to log. Defaults to True.
            recipients_in_cc (bool): Put recipients in cc not to. Defaults to False.

        Returns:
            None
        """
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.newline_to_br(startmsg))]
        msg.extend(emails["plain"])
        htmlmsg.extend(emails["html"])
        self.get_recipients_close_send(
            dutyofficer,
            recipients,
            subject,
            msg,
            htmlmsg,
            log=log,
            recipients_in_cc=recipients_in_cc,
        )

    closure = "\nBest wishes,\nHDX Team"

    @classmethod
    def msg_close(
        cls,
        msg: Union[str, List[str]],
        htmlmsg: Union[str, List[str]],
        endmsg: str = "",
    ) -> Tuple[str, str]:
        """Add the message close text to the email and return plain text and html
        versions as strings. msg and htmlmsg are either a string or a list of strings
        where each string is a part of the message.

        Args:
            msg (Union[str, List[str]]): Plain text email message in str or list form
            htmlmsg (Union[str, List[str]]): HTML email message in str or list form
            endmsg (str): Additional string to add to message end. Defaults to "".

        Returns:
            Tuple[str, str]: Text email body and html email body
        """

        # The "".join treats a string and a list of strings the same
        text = "".join(msg)
        close = cls.closure
        text_body = f"{text}{endmsg}{close}"
        html = "".join(htmlmsg)
        endmsg = cls.newline_to_br(endmsg)
        close = cls.newline_to_br(close)
        html_body = cls.html_end(f"{html}{endmsg}{close}")
        return text_body, html_body

    @staticmethod
    def newline_to_br(msg: str) -> str:
        """Convert newlines in email into html line breaks

        Args:
            msg (str): Message string

        Returns:
            str: Message string with newlines converted into html line breaks
        """

        return msg.replace("\n", "<br>")

    @staticmethod
    def html_start(msg: str) -> str:
        """Add start of html to message

        Args:
            msg (str): Message string

        Returns:
            str: Message string with html start added
        """

        return f"""<html>
  <head></head>
  <body>
    <span>{msg}"""

    @staticmethod
    def html_end(msg: str) -> str:
        """Add end of html to message

        Args:
            msg (str): Message string

        Returns:
            str: Message string with html end added
        """

        return f"""{msg}
      <br/><br/>
      <small>
        <p>
          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>
        </p>
        <p>
          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>
        </p>
      </small>
    </span>
  </body>
</html>
"""

    @staticmethod
    def output_tabs(msg: List[str], htmlmsg: List[str], n: int = 1) -> None:
        """Add tabs to plain text and html versions of email

        Args:
            msg (List[str]): Lines of plain text email message
            htmlmsg (List[str]): Lines of HTML email message
            n (int): Number of tabs. Defaults to 1.

        Returns:
            None
        """

        for i in range(n):
            msg.append("  ")
            htmlmsg.append("&nbsp&nbsp")

    @staticmethod
    def output_newline(msg: List[str], htmlmsg: List[str]) -> None:
        """Add newline to plain text and html versions of email

        Args:
            msg (List[str]): Lines of plain text email message
            htmlmsg (List[str]): Lines of HTML email message

        Returns:
            None
        """

        msg.append("\n")
        htmlmsg.append("<br>")

    @classmethod
    def htmlify(cls, msg: str) -> Tuple[str, str]:
        """Take plain text email, produce an html version and add message close to both
        returning the plain text and html versions

        Args:
            msg (str): Text of email

        Returns:
            Tuple[str, str]: Text email body and html email body
        """

        htmlmsg = cls.html_start(cls.newline_to_br(msg))
        return cls.msg_close(msg, htmlmsg)

    @classmethod
    def output_error(
        cls, msg: List[str], htmlmsg: List[str], error: str
    ) -> None:
        """Add error message to plain text and html versions of email

        Args:
            msg (List[str]): Lines of plain text email message
            htmlmsg (List[str]): Lines of HTML email message
            error (str): Error message

        Returns:
            None
        """

        msg.append(error)
        htmlmsg.append(f"<b>{error}</b>")
        cls.output_newline(msg, htmlmsg)

    @classmethod
    def output_org(
        cls, msg: List[str], htmlmsg: List[str], title: str
    ) -> None:
        """Add organisation title to plain text and html versions of email

        Args:
            msg (List[str]): Lines of plain text email message
            htmlmsg (List[str]): Lines of HTML email message
            title (str): Organisation title

        Returns:
            None
        """

        msg.append(title)
        htmlmsg.append(f"<b><i>{title}</i></b>")
        cls.output_newline(msg, htmlmsg)

    @staticmethod
    def prepare_user_emails(
        hdxhelper: HDXHelper,
        include_reference_period: bool,
        datasets: List[Dict],
        sheet: Sheet,
        sheetname: str,
    ) -> Dict[str, List]:
        """Prepare emails to users

        Args:
            hdxhelper (HDXHelper): HDX helper object
            include_reference_period (bool): Whether to include reference period in output
            datasets (List[Dict]): List of datasets
            sheet (Sheet): Sheet object
            sheetname (str): Name of sheet

        Returns:
            Dict[str, List]: Emails to users
        """

        all_users_to_email = dict()
        datasets_flat = list()
        for dataset in sorted(
            datasets, key=lambda d: (d["organization_title"], d["name"])
        ):
            (
                maintainer,
                orgadmins,
                users_to_email,
            ) = hdxhelper.get_maintainer_orgadmins(dataset)
            (
                dataset_string,
                dataset_html_string,
            ) = hdxhelper.create_dataset_string(
                dataset,
                maintainer,
                orgadmins,
                include_reference_period=include_reference_period,
            )
            for user in users_to_email:
                id = user["id"]
                dict_of_lists_add(
                    all_users_to_email,
                    id,
                    (dataset_string, dataset_html_string),
                )
            row = sheet.construct_row(
                hdxhelper, dataset, maintainer, orgadmins
            )
            if include_reference_period:
                start_date, end_date = hdxhelper.get_reference_period(dataset)
                row["Dataset Start Date"] = start_date.isoformat()
                row["Dataset End Date"] = end_date.isoformat()
            datasets_flat.append(row)
        if sheetname is not None:
            sheet.update(sheetname, datasets_flat)
        return all_users_to_email

    def email_users_send_summary(
        self,
        hdxhelper: HDXHelper,
        include_reference_period: bool,
        datasets: List[Dict],
        nodatasetsmsg: str,
        startmsg: str,
        endmsg: str,
        recipients: Optional[List[str]],
        subject: str,
        summary_subject,
        summary_startmsg,
        sheet: Sheet,
        sheetname: str,
        sysadmins: Optional[List[str]] = None,
    ) -> None:
        """Email users and send a summary to HDX system administrators

        Args:
            hdxhelper (HDXHelper): HDX helper object
            include_reference_period (bool): Whether to include reference period in output
            datasets (List[Dict]): List of datasets
            nodatasetsmsg (str): Message for log when there are no datasets
            startmsg (str): Text for start of email to users
            endmsg (str): Additional string to add to message end
            recipients (Optional[List[str]]): Recipient emails
            subject (str): Subject of email to users
            summary_subject (str): Subject of summary email to admins
            summary_startmsg (str): Text for start of summary email to admins
            sheet (Sheet): Sheet object
            sheetname (str): Name of sheet
            sysadmins (Optional[List[str]]): HDX sysadmin emails. Defaults to None.

        Returns:
            None
        """

        if len(datasets) == 0:
            logger.info(nodatasetsmsg)
            return
        all_users_to_email = self.prepare_user_emails(
            hdxhelper, include_reference_period, datasets, sheet, sheetname
        )
        starthtmlmsg = self.html_start(self.newline_to_br(startmsg))
        if "$dashboard" in startmsg:
            startmsg = startmsg.replace("$dashboard", "dashboard")
            starthtmlmsg = starthtmlmsg.replace(
                "$dashboard",
                '<a href="https://data.humdata.org/dashboard/datasets">dashboard</a>',
            )
        emails = dict()
        for id in sorted(all_users_to_email.keys()):
            user = hdxhelper.users[id]
            username = hdxhelper.get_user_name(user)
            basemsg = startmsg.format(username)
            dict_of_lists_add(emails, "plain", basemsg)
            dict_of_lists_add(emails, "html", self.newline_to_br(basemsg))
            msg = [basemsg]
            htmlmsg = [starthtmlmsg.format(username)]
            for dataset_string, dataset_html_string in all_users_to_email[id]:
                msg.append(dataset_string)
                htmlmsg.append(dataset_html_string)
                dict_of_lists_add(emails, "plain", dataset_string)
                dict_of_lists_add(emails, "html", dataset_html_string)
            if recipients is None:
                users_to_email = [user["email"]]
            else:
                users_to_email = recipients
            self.close_send(users_to_email, subject, msg, htmlmsg, endmsg)
        self.send_admin_summary(
            sheet.dutyofficer,
            sysadmins,
            emails,
            summary_subject,
            summary_startmsg,
        )

    @staticmethod
    def prepare_admin_emails(
        hdxhelper: HDXHelper,
        datasets: List[Dict],
        startmsg: str,
        sheet: Sheet,
        sheetname: str,
        dutyofficer: Dict[str, str],
    ):
        """Prepare emails to HDX admins

        Args:
            hdxhelper (HDXHelper): HDX helper object
            datasets (List[Dict]): List of datasets
            startmsg (str): Text for start of email to admins
            sheet (Sheet): Sheet object
            sheetname (str): Name of sheet
            dutyofficer (Dict[str, str]): Duty officer information

        Returns:
            None
        """
        datasets_flat = list()
        msg = [startmsg]
        htmlmsg = [Email.newline_to_br(startmsg)]
        for dataset in sorted(
            datasets, key=lambda d: (d["organization_title"], d["name"])
        ):
            maintainer, orgadmins, _ = hdxhelper.get_maintainer_orgadmins(
                dataset
            )
            (
                dataset_string,
                dataset_html_string,
            ) = hdxhelper.create_dataset_string(
                dataset, maintainer, orgadmins, sysadmin=True
            )
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            datasets_flat.append(
                sheet.construct_row(hdxhelper, dataset, maintainer, orgadmins)
            )
        sheet.update(
            sheetname, datasets_flat, dutyofficer_name=dutyofficer["name"]
        )
        return msg, htmlmsg

    def email_admins(
        self,
        hdxhelper: HDXHelper,
        datasets: List[Dict],
        nodatasetsmsg,
        startmsg,
        subject,
        sheet: Sheet,
        sheetname: str,
        recipients=None,
        dutyofficer: Optional[Dict[str, str]] = None,
        recipients_in_cc: bool = False,
    ):
        """Send summary of emails sent to users to HDX admins

        Args:
            hdxhelper (HDXHelper): HDX helper object
            datasets (List[Dict]): List of datasets
            nodatasetsmsg (str): Message for log when there are no datasets
            startmsg (str): Text for start of email to users
            endmsg (str): Additional string to add to message end
            sheet (Sheet): Sheet object
            sheetname (str): Name of sheet
            recipients (Optional[List[str]]): Recipient emails. Defaults to None.
            dutyofficer (Optional[Dict[str, str]]): Duty officer. Defaults to None.
            recipients_in_cc (bool): Put recipients in cc not to. Defaults to False.

        Returns:
            None
        """
        if len(datasets) == 0:
            logger.info(nodatasetsmsg)
            return
        if not dutyofficer:
            dutyofficer = sheet.dutyofficer
        msg, htmlmsg = self.prepare_admin_emails(
            hdxhelper, datasets, startmsg, sheet, sheetname, dutyofficer
        )
        htmlmsg[0] = Email.html_start(htmlmsg[0])

        self.get_recipients_close_send(
            dutyofficer,
            recipients,
            subject,
            msg,
            htmlmsg,
            recipients_in_cc=recipients_in_cc,
        )
