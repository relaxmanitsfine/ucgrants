import pytest

from ucgrants.grantees import map_to_uc_by_default_group


@pytest.mark.parametrize(
    "group_name, workspace_id, expected",
    [
        (
            "_workspace_admins_test_workspace_1234567890123456",
            1234567890123456,
            "admins",
        ),
        (
            "_workspace_users_test_workspace_1234567890123456",
            1234567890123456,
            "users",
        ),
        ("some_other_group", 1234567890123456, None),
        (
            "_workspace_admins_another_workspace_9876543210987654",
            9876543210987654,
            "admins",
        ),
        (
            "_workspace_users_another_workspace_9876543210987654",
            9876543210987654,
            "users",
        ),
    ],
)
def test_map_to_uc_by_default_group(
    group_name: str, workspace_id: int, expected: str | None
) -> None:
    assert (
        map_to_uc_by_default_group(group_name=group_name, workspace_id=workspace_id)
        == expected
    )
