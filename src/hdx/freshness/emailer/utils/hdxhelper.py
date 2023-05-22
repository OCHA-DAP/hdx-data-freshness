"""Helper functions for HDX datasets, users and organisations
"""
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from hdx.data.dataset import Dataset
from hdx.data.date_helper import DateHelper
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.utilities.dictandlist import dict_of_lists_add

from .freshnessemail import Email


class HDXHelper:
    """A class providing functions for retrieving information about HDX datasets,
    users and organisations

    Args:
        site_url (str): URL of HDX site
        users (Optional[List[Dict]]): List of users (for testing). Defaults to None.
        organizations (Optional[List[Dict]]): List of organizations. Defaults to None.
    """

    freshness_status = {0: "Fresh", 1: "Due", 2: "Overdue", 3: "Delinquent"}

    def __init__(
        self,
        site_url: str,
        users: Optional[List[User]] = None,
        organizations: Optional[List[Organization]] = None,
    ):
        self.site_url = site_url
        if users is None:  # pragma: no cover
            users = User.get_all_users()
        self.users: Dict[str, User] = dict()
        self.sysadmins = dict()
        for user in users:
            userid = user["id"]
            self.users[userid] = user
            if user["sysadmin"]:
                self.sysadmins[userid] = user

        self.organizations: Dict = dict()
        if organizations is None:  # pragma: no cover
            organizations: List = Organization.get_all_organization_names(
                all_fields=True, include_users=True
            )
        for organization in organizations:
            users_per_capacity = dict()
            for user in organization["users"]:
                dict_of_lists_add(
                    users_per_capacity, user["capacity"], user["id"]
                )
            self.organizations[organization["id"]] = users_per_capacity

    @staticmethod
    def get_reference_period(
        dataset: Dict,
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Return a tuple containing dataset reference period start and end
        or (None, None)

        Args:
            dataset (Dict): Dataset to examine

        Returns:
            Tuple[Optional[datetime], Optional[datetime]]:
            Reference period start and end or (None, None)
        """
        reference_period = dataset["dataset_date"]
        if not reference_period:
            return None, None
        date_info = DateHelper.get_date_info(reference_period)
        return date_info["startdate"], date_info["enddate"]

    def get_maintainer(self, dataset: Dict) -> User:
        """Get the maintainer of a dataset

        Args:
            dataset (Dict): Dataset to examine

        Returns:
            User: Maintainer of the dataset
        """
        maintainer = dataset["maintainer"]
        return self.users.get(maintainer)

    def get_org_admins(self, dataset: Dict) -> List[User]:
        """Get the administrators of the organisation of the dataset

        Args:
            dataset (Dict): Dataset to examine

        Returns:
            List[User]: Administrators of the organisation of the dataset
        """
        organization_id = dataset["organization_id"]
        orgadmins = list()
        organization = self.organizations[organization_id]
        if "admin" in organization:
            for userid in self.organizations[organization_id]["admin"]:
                user = self.users.get(userid)
                if user:
                    orgadmins.append(user)
        return orgadmins

    def get_maintainer_orgadmins(
        self, dataset: Dict
    ) -> Tuple[Dict[str, str], List[Dict[str, str]], List[User]]:
        """Get the maintainer of the dataset and the administrators of the organisation
        of the dataset as well as a list of users to email

        Args:
            dataset (Dict): Dataset to examine

        Returns:
            Tuple[Dict[str, str], List[Dict[str, str]], List[User]]:
            (maintainer info, list of org admin info, list of users to email)
        """
        users_to_email = list()
        maintainer = self.get_maintainer(dataset)
        if maintainer is not None:
            users_to_email.append(maintainer)
            maintainer_name = self.get_user_name(maintainer)
            maintainer = {
                "name": maintainer_name,
                "email": maintainer["email"],
            }
        orgadmins = list()
        for orgadmin in self.get_org_admins(dataset):
            if maintainer is None:
                users_to_email.append(orgadmin)
            username = self.get_user_name(orgadmin)
            orgadmins.append({"name": username, "email": orgadmin["email"]})
        return maintainer, orgadmins, users_to_email

    @staticmethod
    def get_update_frequency(update_freq: int) -> str:
        """Get the update frequency string as words from the numeric value

        Args:
            update_freq (int): Update frequency value

        Returns:
            str: Update frequency in words
        """
        if update_freq is None:
            return "NOT SET"
        else:
            return Dataset.transform_update_frequency(str(update_freq))

    @classmethod
    def get_update_frequency_from_dataset(cls, dataset: Dict) -> str:
        """Get the update frequency string as words from the dataset

        Args:
            dataset (Dict): Dataset to examine

        Returns:
            str: Update frequency in words
        """
        return cls.get_update_frequency(dataset["update_frequency"])

    @staticmethod
    def get_user_name(user: User) -> str:
        """Get the user name from the user

        Args:
            user (User): User to examine

        Returns:
            str: User name of User object
        """
        user_name = user.get("display_name")
        if not user_name:
            user_name = user["fullname"]
            if not user_name:
                user_name = user["name"]
        return user_name

    def get_dataset_url(self, dataset: Dict) -> str:
        """Get the dataset's URL

        Args:
            dataset (Dict): Dataset to examine

        Returns:
            str: URL of dataset
        """
        return f"{self.site_url}/dataset/{dataset['name']}"

    def get_organization_url(self, organization: Dict):
        """Get the organisation's URL

        Args:
            organization (Dict): Organisation to examine

        Returns:
            str: URL of organisation
        """
        return f"{self.site_url}/organization/{organization['name']}"

    def create_dataset_string(
        self,
        dataset: Dict,
        maintainer: Dict[str, str],
        orgadmins: List[Dict[str, str]],
        sysadmin: bool = False,
        include_org: bool = True,
        include_freshness: bool = False,
        include_reference_period: bool = False,
    ) -> Tuple[str, str]:
        """Create the string that will be output in an email, returning a plain text
        and HTML version, the latter including URL links

        Args:
            dataset (Dict): Dataset to examine
            maintainer (Dict[str, str]): Maintainer information
            orgadmins (List[Dict[str, str]]): List of organisation administrator info
            sysadmin (bool): Include additional info for sysadmins. Defaults to False.
            include_org (bool): Include additional org info in string. Defaults to True.
            include_freshness (bool): Include freshness status. Defaults to False.
            include_reference_period (bool): Include reference period. Defaults to False.

        Returns:
            Tuple[str, str]: (plain text string, HTML string) for output in email
        """
        url = self.get_dataset_url(dataset)
        msg = list()
        htmlmsg = list()
        msg.append(f"{dataset['title']} ({url})")
        htmlmsg.append(f"<a href=\"{url}\">{dataset['title']}</a>")
        if sysadmin and include_org:
            orgmsg = f" from {dataset['organization_title']}"
            msg.append(orgmsg)
            htmlmsg.append(orgmsg)
        if maintainer is not None:
            if sysadmin:
                user_name = maintainer["name"]
                user_email = maintainer["email"]
                msg.append(f" maintained by {user_name} ({user_email})")
                htmlmsg.append(
                    f' maintained by <a href="mailto:{user_email}">{user_name}</a>'
                )
        else:
            if sysadmin:
                missing_maintainer = (
                    " with missing maintainer and organization administrators "
                )
                msg.append(missing_maintainer)
                htmlmsg.append(missing_maintainer)

            usermsg = list()
            userhtmlmsg = list()
            for orgadmin in orgadmins:
                user_name = orgadmin["name"]
                user_email = orgadmin["email"]
                usermsg.append(f"{user_name} ({user_email})")
                userhtmlmsg.append(
                    f'<a href="mailto:{user_email}">{user_name}</a>'
                )
            if sysadmin:
                msg.append(", ".join(usermsg))
                htmlmsg.append(", ".join(userhtmlmsg))
        update_frequency = self.get_update_frequency_from_dataset(dataset)
        msg.append(f" with expected update frequency: {update_frequency}")
        htmlmsg.append(f" with expected update frequency: {update_frequency}")
        if include_freshness:
            fresh = self.freshness_status.get(dataset["fresh"], "None")
            msg.append(f" and freshness: {fresh}")
            htmlmsg.append(f" and freshness: {fresh}")
        if include_reference_period:
            reference_period = dataset["dataset_date"]
            msg.append(f" and reference period: {reference_period}")
            htmlmsg.append(f" and reference period: {reference_period}")
        Email.output_newline(msg, htmlmsg)

        return "".join(msg), "".join(htmlmsg)
