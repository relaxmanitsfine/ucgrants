from typing import Final

# Some built-in groups like "account users" can have a lot of members
# so its safer to have some limits to stop API throttling
MAX_GROUP_MEMBERS: Final = 20

# Looking up Workspace groups for UC by default requires Workspace Admin rights
# toggle to disable that
LOOKUP_WORKSPACE_GROUPS_DEFAULT: Final = True
