from typing import Dict
import itertools

class GroupPipeline:
    """
    Represents a pipeline to manage and assign unique identifiers
    to user groups.

    This class maintains a cache of group IDs associated with
    user groups to ensure efficient lookup and assignment. The `group_cache`
    dictionary stores mappings between sets of user IDs and their corresponding
    unique identifiers. A unique identifier is assigned to a group only
    if it does not exist in the cache.

    :ivar group_cache: A dictionary mapping immutable sets of user IDs
        (frozensets) to their unique group identifiers.
    :type group_cache: Dict[frozenset, int]
    :ivar counter: An iterator generating unique integer identifiers for groups.
    :type counter: itertools.count
    """
    def __init__(self):
        self.group_cache: Dict[frozenset, int] = {}
        self.counter = itertools.count()

    def get_group_id(self, users: set[int]):
        """
        Generates a unique group ID for a given set of users. If the user group has
        already been processed and exists in the cache, the cached ID is returned.
        Otherwise, it generates and caches a new ID by using an internal counter.

        This function is useful for efficiently assigning and retrieving unique
        identifiers to non-empty sets of users, with caching to avoid redundant
        computations.

        :param users:
            A set of user IDs for which a unique group ID is required. Negative
            user IDs (e.g., `-1`) are excluded from the group. Only non-empty
            sets are processed.

        :return:
            An integer representing the unique group ID. If the resulting user
            set is empty after filtering out invalid user IDs, returns `-1`.
        """
        _users = frozenset({user for user in users if user != -1})
        if not _users:
            return -1
        if _users in self.group_cache:
            return self.group_cache[_users]
        id: int = next(self.counter)
        self.group_cache[_users] = id
        return id