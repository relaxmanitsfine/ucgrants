from typing import Any

from databricks.sdk import WorkspaceClient

from ucgrants.sdk.service.iam import (
    AccountGroupsWorkspaceApi,
    AccountServicePrincipalsWorkspaceApi,
    AccountUsersWorkspaceApi,
)


class WorkspaceClientPlus(WorkspaceClient):
    """Extended DatabricksWorkspaceClient with additional functionality.

    This class provides additional functionality to the DatabricksWorkspaceClient,
    allowing you to retrieve and manage account groups, service principals, and users
    at the account level using the workspace API.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._account_groups = AccountGroupsWorkspaceApi(self.api_client)
        self._account_service_principals = AccountServicePrincipalsWorkspaceApi(
            self.api_client
        )
        self._account_users = AccountUsersWorkspaceApi(self.api_client)

    @property
    def account_groups(self) -> AccountGroupsWorkspaceApi:
        return self._account_groups

    @property
    def account_service_principals(self) -> AccountServicePrincipalsWorkspaceApi:
        return self._account_service_principals

    @property
    def account_users(self) -> AccountUsersWorkspaceApi:
        return self._account_users
