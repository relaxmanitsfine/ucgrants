import abc
import logging
import re
from enum import Enum

from databricks.sdk.errors.platform import NotFound
from databricks.sdk.service.iam import ComplexValue, Group, ServicePrincipal, User

from ucgrants import LOOKUP_WORKSPACE_GROUPS_DEFAULT, MAX_GROUP_MEMBERS
from ucgrants.sdk import WorkspaceClientPlus

logger = logging.getLogger(__name__)


class PrincipalType(Enum):
    USER = "user"
    GROUP = "group"
    SERVICE_PRINCIPAL = "service_principal"


class Grantee:
    """A grantee is a principal that can be granted privileges on a resource."""

    def __init__(
        self,
        principal_type: PrincipalType,
        principal_id: str,
        principal_name: str,
        principal_display_name: str,
        grantee_key: str,
    ):
        self.principal_type = principal_type
        self.principal_id = principal_id
        self.principal_name = principal_name
        self.principal_display_name = principal_display_name
        self.grantee_key = grantee_key

    def __str__(self) -> str:
        return f"{self.principal_type}: {self.principal_name} ({self.principal_id}) [{self.grantee_key}]"

    def to_dict(self) -> dict[str, str]:
        return {
            "principal_type": self.principal_type.value,
            "principal_id": self.principal_id,
            "principal": self.principal_name,
            "principal_display_name": self.principal_display_name,
            "grantee_key": self.grantee_key,
        }

    @classmethod
    def from_user(cls, user: User, grantee_key: str | None = None) -> "Grantee":
        if user.id is None:
            raise ValueError("User ID cannot be None")
        if user.user_name is None:
            raise ValueError("User name cannot be None")
        if user.display_name is None:
            display_name = ""
        else:
            display_name = user.display_name
        if grantee_key is None:
            grantee_key = user.user_name

        return cls(
            principal_type=PrincipalType.USER,
            principal_id=user.id,
            principal_name=user.user_name,
            principal_display_name=display_name,
            grantee_key=grantee_key,
        )

    @classmethod
    def from_service_principal(
        cls, service_principal: ServicePrincipal, grantee_key: str | None = None
    ) -> "Grantee":
        if service_principal.application_id is None:
            raise ValueError("Service principal application ID cannot be None")
        if service_principal.id is None:
            raise ValueError("Service principal ID cannot be None")
        if service_principal.display_name is None:
            display_name = ""
        else:
            display_name = service_principal.display_name
        if grantee_key is None:
            grantee_key = service_principal.application_id
        return cls(
            principal_type=PrincipalType.SERVICE_PRINCIPAL,
            principal_id=service_principal.id,
            principal_name=service_principal.application_id,
            principal_display_name=display_name,
            grantee_key=grantee_key,
        )

    @classmethod
    def from_group(cls, group: Group, grantee_key: str | None = None) -> "Grantee":
        if group.display_name is None:
            raise ValueError("Group display name cannot be None")
        if group.id is None:
            raise ValueError("Group ID cannot be None")
        if grantee_key is None:
            grantee_key = group.display_name
        return cls(
            principal_type=PrincipalType.GROUP,
            principal_id=group.id,
            principal_name=grantee_key,
            principal_display_name=grantee_key,
            grantee_key=grantee_key,
        )


def map_to_uc_by_default_group(group_name: str, workspace_id: int) -> str | None:
    admin_pattern = rf"^_workspace_admins_(\w+)_{workspace_id}$"
    if re.match(admin_pattern, group_name) is not None:
        logging.info(f"Group '{group_name}' is a UC by default admin group.")
        return "admins"
    users_pattern = rf"^_workspace_users_(\w+)_{workspace_id}$"
    if re.match(users_pattern, group_name) is not None:
        logging.info(f"Group '{group_name}' is a UC by default user group.")
        return "users"
    return None


class GranteeResolver(abc.ABC):
    @abc.abstractmethod
    def resolve(
        self,
        grantee_key: str,
        lookup_workspace_groups: bool = LOOKUP_WORKSPACE_GROUPS_DEFAULT,
        max_members: int = MAX_GROUP_MEMBERS,
    ) -> list[Grantee] | None:
        raise NotImplementedError

    @abc.abstractmethod
    def resolve_flat(
        self,
        grantee_keys: str,
        lookup_workspace_groups: bool = LOOKUP_WORKSPACE_GROUPS_DEFAULT,
        max_members: int = MAX_GROUP_MEMBERS,
    ) -> Grantee | None:
        raise NotImplementedError


class GranteeResolverWorkspaceClient(GranteeResolver):
    def __init__(self, databricks_client: WorkspaceClientPlus):
        self.databricks_client = databricks_client
        self._user_grantee_cache: dict[str, Grantee] = {}
        self._service_principal_grantee_cache: dict[str, Grantee] = {}
        self._group_grantee_cache: dict[str, list[Grantee]] = {}
        self._group_grantee_flat_cache: dict[str, Grantee] = {}

    def resolve(
        self,
        grantee_key: str,
        lookup_workspace_groups: bool = LOOKUP_WORKSPACE_GROUPS_DEFAULT,
        max_members: int = MAX_GROUP_MEMBERS,
    ) -> list[Grantee] | None:
        # Resolution order has to deal with the fact grantees are just strings
        # we try in this order:
        # 1. Try and resolve the grantee as a user
        # 2. If that fails try and resolve the grateen as a service principal app id
        # 3. If that fails assume we have a group and check if it is a uc by default group
        #    If so then map to the workspace group name and try and resolve that
        # 4. Failing that assume we have an account group name and try and resolve that
        # 5. Failing that the grantee can't be resolved
        #
        # 1. Resolve user
        if grantee_key in self._user_grantee_cache:
            logger.debug(f"User cache hit for {grantee_key}")
            return [self._user_grantee_cache[grantee_key]]
        maybe_user: User | None = self._get_account_user_by_name(grantee_key)
        if maybe_user is not None:
            try:
                user_grantee = Grantee.from_user(maybe_user)
                self._user_grantee_cache[grantee_key] = user_grantee
                return [user_grantee]
            except ValueError as e:
                logger.warning(f"Error creating Grantee from user {maybe_user}: {e}")

        # 2. Resolve service principal
        if grantee_key in self._service_principal_grantee_cache:
            logger.debug(f"Service principal cache hit for {grantee_key}")
            return [self._service_principal_grantee_cache[grantee_key]]
        maybe_service_principal: ServicePrincipal | None = (
            self._get_account_service_principal_by_name(grantee_key)
        )
        if maybe_service_principal is not None:
            try:
                service_principal_grantee = Grantee.from_service_principal(
                    maybe_service_principal
                )
                self._service_principal_grantee_cache[grantee_key] = (
                    service_principal_grantee
                )
                return [service_principal_grantee]
            except ValueError as e:
                logger.warning(
                    f"Error creating Grantee from service principal {maybe_service_principal}: {e}"
                )

        # 3. Resolve uc by defaultworkspace group
        workspace_id: int | None = self.databricks_client.get_workspace_id()
        assert workspace_id is not None, "Workspace ID is required"
        uc_by_default_group_name: str | None = map_to_uc_by_default_group(
            grantee_key, workspace_id
        )
        if uc_by_default_group_name is not None:
            if grantee_key in self._group_grantee_cache:
                logger.debug(f"Workspace group cache hit for {grantee_key}")
                return self._group_grantee_cache[grantee_key]
            if lookup_workspace_groups:
                maybe_full_workspace_group: Group | None = (
                    self._get_workspace_group_by_name_with_members(
                        uc_by_default_group_name
                    )
                )
                if maybe_full_workspace_group is not None:
                    workspace_group_members: list[Grantee] = self._get_group_members(
                        group=maybe_full_workspace_group,
                        max_members=max_members,
                        grantee_key=grantee_key,
                    )
                    self._group_grantee_cache[grantee_key] = workspace_group_members
                    return workspace_group_members
            else:
                logger.warning(
                    f"Group '{grantee_key}' is a UC by default group but lookup_workspace_group is False so skipping."
                )
                return None

        # 4. Resolve account group
        if grantee_key in self._group_grantee_cache:
            logger.debug(f"Account group cache hit for {grantee_key}")
            return self._group_grantee_cache[grantee_key]
        maybe_full_group: Group | None = self._get_account_group_by_name_with_members(
            grantee_key
        )
        if maybe_full_group is not None:
            group_members: list[Grantee] = self._get_group_members(
                group=maybe_full_group,
                max_members=max_members,
            )
            self._group_grantee_cache[grantee_key] = group_members
            return group_members
        else:
            logger.warning(
                f"Grantee '{grantee_key}' not found as user, service principal, or group."
            )

        return None

    def resolve_flat(
        self,
        grantee_key: str,
        lookup_workspace_groups: bool = LOOKUP_WORKSPACE_GROUPS_DEFAULT,
        max_members: int = MAX_GROUP_MEMBERS,
    ) -> Grantee | None:
        # Similar to resolve but we don't break down groups into their members
        # 1. Resolve user
        if grantee_key in self._user_grantee_cache:
            logger.debug(f"User cache hit for {grantee_key}")
            return self._user_grantee_cache[grantee_key]
        maybe_user: User | None = self._get_account_user_by_name(grantee_key)
        if maybe_user is not None:
            try:
                user_grantee = Grantee.from_user(maybe_user)
                self._user_grantee_cache[grantee_key] = user_grantee
                return user_grantee
            except ValueError as e:
                logger.warning(f"Error creating Grantee from user {maybe_user}: {e}")

        # 2. Resolve service principal
        if grantee_key in self._service_principal_grantee_cache:
            logger.debug(f"Service principal cache hit for {grantee_key}")
            return self._service_principal_grantee_cache[grantee_key]
        maybe_service_principal: ServicePrincipal | None = (
            self._get_account_service_principal_by_name(grantee_key)
        )
        if maybe_service_principal is not None:
            try:
                service_principal_grantee = Grantee.from_service_principal(
                    maybe_service_principal
                )
                self._service_principal_grantee_cache[grantee_key] = (
                    service_principal_grantee
                )
                return service_principal_grantee
            except ValueError as e:
                logger.warning(
                    f"Error creating Grantee from service principal {maybe_service_principal}: {e}"
                )

        # 3. Resolve uc by defaultworkspace group
        workspace_id: int | None = self.databricks_client.get_workspace_id()
        assert workspace_id is not None, "Workspace ID is required"
        uc_by_default_group_name: str | None = map_to_uc_by_default_group(
            grantee_key, workspace_id
        )
        if uc_by_default_group_name is not None:
            if grantee_key in self._group_grantee_flat_cache:
                logger.debug(f"Workspace group cache hit for {grantee_key}")
                return self._group_grantee_flat_cache[grantee_key]
            if lookup_workspace_groups:
                maybe_full_workspace_group: Group | None = (
                    self._get_workspace_group_by_name_with_members(
                        uc_by_default_group_name
                    )
                )
                if maybe_full_workspace_group is not None:
                    try:
                        workspace_group: Grantee = Grantee.from_group(
                            maybe_full_workspace_group,
                            grantee_key=grantee_key,
                        )
                        self._group_grantee_flat_cache[grantee_key] = workspace_group
                        return workspace_group
                    except ValueError as e:
                        logger.warning(
                            f"Error creating Grantee from workspace group {maybe_full_workspace_group}: {e}"
                        )
            else:
                logger.warning(
                    f"Group '{grantee_key}' is a UC by default group but lookup_workspace_group is False so skipping."
                )
                return None

        # 4. Resolve account group
        if grantee_key in self._group_grantee_flat_cache:
            logger.debug(f"Account group cache hit for {grantee_key}")
            return self._group_grantee_flat_cache[grantee_key]
        maybe_full_group: Group | None = self._get_account_group_by_name_with_members(
            grantee_key
        )
        if maybe_full_group is not None:
            try:
                group_grantee: Grantee = Grantee.from_group(maybe_full_group)
                self._group_grantee_flat_cache[grantee_key] = group_grantee
                return group_grantee
            except ValueError as e:
                logger.warning(
                    f"Error creating Grantee from account group {maybe_full_group}: {e}"
                )
        else:
            logger.warning(
                f"Grantee '{grantee_key}' not found as user, service principal, or group."
            )

        return None

    def _get_account_user_by_name(self, user_name: str) -> User | None:
        query_filter = f'userName eq "{user_name}"'
        try:
            users: list[User] = list(
                self.databricks_client.account_users.list(filter=query_filter)
            )
            if len(users) > 0:
                return users[0]
        except NotFound as e:
            logger.warning(f"Error retrieving user with name {user_name}: {e}")
        return None

    def _get_account_user_by_id(self, user_id: str) -> User | None:
        try:
            user: User = self.databricks_client.account_users.get(id=user_id)
            return user
        except NotFound as e:
            logger.warning(f"Error retrieving user with id {user_id}: {e}")
        return None

    def _get_account_service_principal_by_name(
        self, application_id: str
    ) -> ServicePrincipal | None:
        query_filter = f'applicationId eq "{application_id}"'
        try:
            service_principals: list[ServicePrincipal] = list(
                self.databricks_client.account_service_principals.list(
                    filter=query_filter
                )
            )
            if len(service_principals) > 0:
                return service_principals[0]
        except NotFound as e:
            logger.warning(
                f"Error retrieving service principal with applicationId {application_id}: {e}"
            )
        return None

    def _get_account_service_principal_by_id(
        self, service_principal_id: str
    ) -> ServicePrincipal | None:
        try:
            service_principal: ServicePrincipal = (
                self.databricks_client.account_service_principals.get(
                    id=service_principal_id
                )
            )
            return service_principal
        except NotFound as e:
            logger.warning(
                f"Error retrieving service principal with id {service_principal_id}: {e}"
            )
        return None

    def _get_account_group_by_name(self, group_name: str) -> Group | None:
        query_filter = f'displayName eq "{group_name}"'
        try:
            groups: list[Group] = list(
                self.databricks_client.account_groups.list(filter=query_filter)
            )
            if len(groups) > 0:
                return groups[0]
        except NotFound as e:
            logger.warning(f"Error retrieving group with displayName {group_name}: {e}")
        return None

    def _get_account_group_by_id(self, group_id: str) -> Group | None:
        try:
            group: Group = self.databricks_client.account_groups.get(id=group_id)
            return group
        except NotFound as e:
            logger.warning(f"Error retrieving group with id {group_id}: {e}")
        return None

    def _get_account_group_by_name_with_members(self, group_name: str) -> Group | None:
        maybe_group: Group | None = self._get_account_group_by_name(group_name)
        if maybe_group is not None:
            maybe_group_id: str | None = maybe_group.id
            if maybe_group_id is not None:
                maybe_full_group: Group | None = self._get_account_group_by_id(
                    maybe_group_id
                )
                return maybe_full_group
        return None

    def _get_workspace_group_by_name(self, group_name: str) -> Group | None:
        query_filter = f'displayName eq "{group_name}"'
        try:
            groups: list[Group] = list(
                self.databricks_client.groups.list(filter=query_filter)
            )
            if len(groups) > 0:
                return groups[0]
        except NotFound as e:
            logger.warning(f"Error retrieving group with displayName {group_name}: {e}")
        return None

    def _get_workspace_group_by_id(self, group_id: str) -> Group | None:
        try:
            group: Group = self.databricks_client.groups.get(id=group_id)
            return group
        except NotFound as e:
            logger.warning(f"Error retrieving group with id {group_id}: {e}")
        return None

    def _get_workspace_group_by_name_with_members(
        self, group_name: str
    ) -> Group | None:
        maybe_group: Group | None = self._get_workspace_group_by_name(group_name)
        if maybe_group is not None:
            maybe_group_id: str | None = maybe_group.id
            if maybe_group_id is not None:
                maybe_full_group: Group | None = self._get_workspace_group_by_id(
                    maybe_group_id
                )
                return maybe_full_group
        return None

    def _get_group_members(
        self,
        group: Group | None = None,
        grantee_key: str | None = None,
        members: list[Grantee] | None = None,
        max_members: int = MAX_GROUP_MEMBERS,
    ) -> list[Grantee]:
        if members is None:
            members = []
        if group is None and grantee_key is not None:
            maybe_full_group: Group | None = (
                self._get_account_group_by_name_with_members(grantee_key)
            )
            if maybe_full_group is not None:
                group = maybe_full_group
            else:
                return members
        if group is not None:
            if grantee_key is None:
                grantee_key = group.display_name
            group_members: list[ComplexValue] | None = group.members
            if group_members is not None:
                if len(group_members) > max_members:
                    logger.info(
                        f"Group '{grantee_key}' has more than {max_members} members, truncating"
                    )
                    group_members = group_members[:max_members]
                for member in group_members:
                    member_ref: str | None = member.ref
                    if member_ref is not None:
                        if member_ref.startswith("Users/"):
                            user_id: str | None = member.value
                            maybe_user_grantee: Grantee | None = self._user_from_ref(
                                user_id, grantee_key
                            )
                            if maybe_user_grantee is not None:
                                members.append(maybe_user_grantee)
                        elif member_ref.startswith("ServicePrincipals/"):
                            service_principal_id: str | None = member.value
                            maybe_service_principal_grantee: Grantee | None = (
                                self._service_principal_from_ref(
                                    service_principal_id, grantee_key
                                )
                            )
                            if maybe_service_principal_grantee is not None:
                                members.append(maybe_service_principal_grantee)
                        elif member_ref.startswith("Groups/"):
                            maybe_group_id = member.value
                            if maybe_group_id is not None:
                                nested_group_def: Group | None = (
                                    self._get_account_group_by_id(maybe_group_id)
                                )
                                if nested_group_def is not None:
                                    members = self._get_group_members(
                                        group=nested_group_def,
                                        grantee_key=grantee_key,
                                        members=members,
                                        max_members=max_members,
                                    )
        return members

    def _user_from_ref(
        self, user_id: str | None, grantee_key: str | None
    ) -> Grantee | None:
        if user_id is not None:
            user: User | None = self._get_account_user_by_id(user_id)
            if user is not None:
                try:
                    return Grantee.from_user(user, grantee_key=grantee_key)
                except ValueError as e:
                    logger.warning(f"Error creating Grantee from user {user}: {e}")
        return None

    def _service_principal_from_ref(
        self, service_principal_id: str | None, grantee_key: str | None
    ) -> Grantee | None:
        if service_principal_id is not None:
            service_principal: ServicePrincipal | None = (
                self._get_account_service_principal_by_id(service_principal_id)
            )
            if service_principal is not None:
                try:
                    return Grantee.from_service_principal(
                        service_principal, grantee_key=grantee_key
                    )
                except ValueError as e:
                    logger.warning(
                        f"Error creating Grantee from service principal {service_principal}: {e}"
                    )
        return None
