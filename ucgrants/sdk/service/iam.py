from collections.abc import Iterator
from typing import Final

from databricks.sdk.core import ApiClient
from databricks.sdk.service.iam import (
    GetSortOrder,
    Group,
    ListSortOrder,
    ServicePrincipal,
    User,
)

GROUP_API_PATH: Final = "/api/2.0/account/scim/v2/Groups"
SERVICE_PRINCIPAL_API_PATH: Final = "/api/2.0/account/scim/v2/ServicePrincipals"
USER_API_PATH: Final = "/api/2.0/account/scim/v2/Users"


class AccountGroupsWorkspaceApi:
    """API for managing Databricks Account groups using the workspace API.

    This class provides methods to interact with the Databricks account groups API,
    allowing you to retrieve and manage group information at the account level.

    This only does read operations (get and list) currently.
    """

    def __init__(self, api_client: ApiClient) -> None:
        self._api = api_client

    def get(self, id: str) -> Group:
        """Gets the information for a specific group in the Databricks account
        using the undocumented Workspace account users API.

        :param id: str
          Unique ID for a group in the Databricks account.

        :returns: :class:`Group`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"{GROUP_API_PATH}/{id}", headers=headers)
        return Group.from_dict(res)

    def list(
        self,
        *,
        attributes: str | None = None,
        count: int | None = None,
        excluded_attributes: str | None = None,
        filter: str | None = None,
        sort_by: str | None = None,
        sort_order: ListSortOrder | None = None,
        start_index: int | None = None,
    ) -> Iterator[Group]:
        """Gets all details of the groups associated with the Databricks account
        using the undocumented Workspace account users API.

        :param attributes: str (optional)
          Comma-separated list of attributes to return in response.
        :param count: int (optional)
          Desired number of results per page. Default is 10000.
        :param excluded_attributes: str (optional)
          Comma-separated list of attributes to exclude in response.
        :param filter: str (optional)
          Query by which the results have to be filtered. Supported operators are equals(`eq`),
          contains(`co`), starts with(`sw`) and not equals(`ne`). Additionally, simple expressions can be
          formed using logical operators - `and` and `or`. The [SCIM RFC] has more details but we currently
          only support simple expressions.

          [SCIM RFC]: https://tools.ietf.org/html/rfc7644#section-3.4.2.2
        :param sort_by: str (optional)
          Attribute to sort the results.
        :param sort_order: :class:`ListSortOrder` (optional)
          The order to sort the results.
        :param start_index: int (optional)
          Specifies the index of the first result. First item is number 1.

        :returns: Iterator over :class:`Group`
        """

        query: dict[str, str | int] = {}
        if attributes is not None:
            query["attributes"] = attributes
        if count is not None:
            query["count"] = count
        if excluded_attributes is not None:
            query["excludedAttributes"] = excluded_attributes
        if filter is not None:
            query["filter"] = filter
        if sort_by is not None:
            query["sortBy"] = sort_by
        if sort_order is not None:
            query["sortOrder"] = sort_order.value
        if start_index is not None:
            query["startIndex"] = start_index
        headers = {
            "Accept": "application/json",
        }

        # deduplicate items that may have been added during iteration
        seen = set()
        query["startIndex"] = 1
        if "count" not in query:
            query["count"] = 10000
        while True:
            json = self._api.do("GET", GROUP_API_PATH, query=query, headers=headers)
            if "Resources" in json:
                for v in json["Resources"]:
                    i = v["id"]
                    if i in seen:
                        continue
                    seen.add(i)
                    yield Group.from_dict(v)
            if "Resources" not in json or not json["Resources"]:
                return
            assert isinstance(query["startIndex"], int)
            query["startIndex"] += len(json["Resources"])


class AccountServicePrincipalsWorkspaceApi:
    """API for managing Databricks Account service principals using the workspace API.

    This class provides methods to interact with the Databricks account service principals API,
    allowing you to retrieve and manage service principals information at the account level.

    This only does read operations (get and list) currently.
    """

    def __init__(self, api_client: ApiClient) -> None:
        self._api = api_client

    def get(self, id: str) -> ServicePrincipal:
        """Gets the information for a specific service principal in the Databricks account
        using the undocumented Workspace account users API.
        """
        headers = {
            "Accept": "application/json",
        }
        res = self._api.do("GET", f"{SERVICE_PRINCIPAL_API_PATH}/{id}", headers=headers)
        return ServicePrincipal.from_dict(res)

    def list(
        self,
        *,
        attributes: str | None = None,
        count: int | None = None,
        excluded_attributes: str | None = None,
        filter: str | None = None,
        sort_by: str | None = None,
        sort_order: ListSortOrder | None = None,
        start_index: int | None = None,
    ) -> Iterator[ServicePrincipal]:
        """Gets the set of service principals associated with a Databricks account.

        :param attributes: str (optional)
          Comma-separated list of attributes to return in response.
        :param count: int (optional)
          Desired number of results per page. Default is 10000.
        :param excluded_attributes: str (optional)
          Comma-separated list of attributes to exclude in response.
        :param filter: str (optional)
          Query by which the results have to be filtered. Supported operators are equals(`eq`),
          contains(`co`), starts with(`sw`) and not equals(`ne`). Additionally, simple expressions can be
          formed using logical operators - `and` and `or`. The [SCIM RFC] has more details but we currently
          only support simple expressions.

          [SCIM RFC]: https://tools.ietf.org/html/rfc7644#section-3.4.2.2
        :param sort_by: str (optional)
          Attribute to sort the results.
        :param sort_order: :class:`ListSortOrder` (optional)
          The order to sort the results.
        :param start_index: int (optional)
          Specifies the index of the first result. First item is number 1.

        :returns: Iterator over :class:`ServicePrincipal`
        """

        query: dict[str, str | int] = {}
        if attributes is not None:
            query["attributes"] = attributes
        if count is not None:
            query["count"] = count
        if excluded_attributes is not None:
            query["excludedAttributes"] = excluded_attributes
        if filter is not None:
            query["filter"] = filter
        if sort_by is not None:
            query["sortBy"] = sort_by
        if sort_order is not None:
            query["sortOrder"] = sort_order.value
        if start_index is not None:
            query["startIndex"] = start_index
        headers = {
            "Accept": "application/json",
        }

        # deduplicate items that may have been added during iteration
        seen = set()
        query["startIndex"] = 1
        if "count" not in query:
            query["count"] = 10000
        while True:
            json = self._api.do(
                "GET",
                SERVICE_PRINCIPAL_API_PATH,
                query=query,
                headers=headers,
            )
            if "Resources" in json:
                for v in json["Resources"]:
                    i = v["id"]
                    if i in seen:
                        continue
                    seen.add(i)
                    yield ServicePrincipal.from_dict(v)
            if "Resources" not in json or not json["Resources"]:
                return
            assert isinstance(query["startIndex"], int)
            query["startIndex"] += len(json["Resources"])


class AccountUsersWorkspaceApi:
    """API for managing Databricks Account users using the workspace API.

    This class provides methods to interact with the Databricks account user API,
    allowing you to retrieve and manage users information at the account level.

    This only does read operations (get and list) currently.
    """

    def __init__(self, api_client: ApiClient) -> None:
        self._api = api_client

    def get(
        self,
        id: str,
        *,
        attributes: str | None = None,
        count: int | None = None,
        excluded_attributes: str | None = None,
        filter: str | None = None,
        sort_by: str | None = None,
        sort_order: GetSortOrder | None = None,
        start_index: int | None = None,
    ) -> User:
        """Gets information for a specific user in Databricks account
        using the undocumented Workspace account users API.

        :param id: str
          Unique ID for a user in the Databricks account.
        :param attributes: str (optional)
          Comma-separated list of attributes to return in response.
        :param count: int (optional)
          Desired number of results per page. Default is 10000.
        :param excluded_attributes: str (optional)
          Comma-separated list of attributes to exclude in response.
        :param filter: str (optional)
          Query by which the results have to be filtered. Supported operators are equals(`eq`),
          contains(`co`), starts with(`sw`) and not equals(`ne`). Additionally, simple expressions can be
          formed using logical operators - `and` and `or`. The [SCIM RFC] has more details but we currently
          only support simple expressions.

          [SCIM RFC]: https://tools.ietf.org/html/rfc7644#section-3.4.2.2
        :param sort_by: str (optional)
          Attribute to sort the results. Multi-part paths are supported. For example, `userName`,
          `name.givenName`, and `emails`.
        :param sort_order: :class:`GetSortOrder` (optional)
          The order to sort the results.
        :param start_index: int (optional)
          Specifies the index of the first result. First item is number 1.

        :returns: :class:`User`
        """
        query: dict[str, str | int] = {}
        if attributes is not None:
            query["attributes"] = attributes
        if count is not None:
            query["count"] = count
        if excluded_attributes is not None:
            query["excludedAttributes"] = excluded_attributes
        if filter is not None:
            query["filter"] = filter
        if sort_by is not None:
            query["sortBy"] = sort_by
        if sort_order is not None:
            query["sortOrder"] = sort_order.value
        if start_index is not None:
            query["startIndex"] = start_index
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"{USER_API_PATH}/{id}", query=query, headers=headers)
        return User.from_dict(res)

    def list(
        self,
        *,
        attributes: str | None = None,
        count: int | None = None,
        excluded_attributes: str | None = None,
        filter: str | None = None,
        sort_by: str | None = None,
        sort_order: ListSortOrder | None = None,
        start_index: int | None = None,
    ) -> Iterator[User]:
        """Gets details for all the users associated with a Databricks account using the
        undocumented Workspace account users API.

        :param attributes: str (optional)
          Comma-separated list of attributes to return in response.
        :param count: int (optional)
          Desired number of results per page. Default is 10000.
        :param excluded_attributes: str (optional)
          Comma-separated list of attributes to exclude in response.
        :param filter: str (optional)
          Query by which the results have to be filtered. Supported operators are equals(`eq`),
          contains(`co`), starts with(`sw`) and not equals(`ne`). Additionally, simple expressions can be
          formed using logical operators - `and` and `or`. The [SCIM RFC] has more details but we currently
          only support simple expressions.

          [SCIM RFC]: https://tools.ietf.org/html/rfc7644#section-3.4.2.2
        :param sort_by: str (optional)
          Attribute to sort the results. Multi-part paths are supported. For example, `userName`,
          `name.givenName`, and `emails`.
        :param sort_order: :class:`ListSortOrder` (optional)
          The order to sort the results.
        :param start_index: int (optional)
          Specifies the index of the first result. First item is number 1.

        :returns: Iterator over :class:`User`
        """

        query: dict[str, str | int] = {}
        if attributes is not None:
            query["attributes"] = attributes
        if count is not None:
            query["count"] = count
        if excluded_attributes is not None:
            query["excludedAttributes"] = excluded_attributes
        if filter is not None:
            query["filter"] = filter
        if sort_by is not None:
            query["sortBy"] = sort_by
        if sort_order is not None:
            query["sortOrder"] = sort_order.value
        if start_index is not None:
            if query.get("startIndex") is not None:
                assert isinstance(query["startIndex"], int)
            query["startIndex"] = start_index
        headers = {
            "Accept": "application/json",
        }

        # deduplicate items that may have been added during iteration
        seen = set()
        query["startIndex"] = 1
        if "count" not in query:
            query["count"] = 10000
        while True:
            json = self._api.do(
                "GET",
                USER_API_PATH,
                query=query,
                headers=headers,
            )
            if "Resources" in json:
                for v in json["Resources"]:
                    i = v["id"]
                    if i in seen:
                        continue
                    seen.add(i)
                    yield User.from_dict(v)
            if "Resources" not in json or not json["Resources"]:
                return
            assert isinstance(query["startIndex"], int)
            query["startIndex"] += len(json["Resources"])
