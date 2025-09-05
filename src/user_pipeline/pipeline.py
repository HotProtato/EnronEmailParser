from typing import Dict, Set
from src.data_object.user_profile import UserProfile
import re
import itertools
from . import _helpers as helpers

class UserPipeline:
    def __init__(self):
        self.users: Dict[int, UserProfile] = {}
        self.alias_lookup: Dict[str, int] = {}
        self.counter = itertools.count()

    def get_user_id(self, alias: str) -> int:
        """
        Primary entry point. Takes a single alias string, finds or creates a user,
        and returns the user_id.
        """
        if not alias or not alias.strip():
            raise ValueError("Alias cannot be blank.")
        
        alias = alias.lower().strip()
        
        if alias in self.alias_lookup:
            return self.alias_lookup[alias]

        # If alias is unknown, treat it as a set of one and let the main function handle it.
        return self.get_user_id_from_set({alias})

    # Is used for both a single alias and a set.
    def get_user_id_from_set(self, aliases: Set[str]) -> int:
        """
        finds/creates/updates user profiles.
        """
        # 1. Clean aliases.
        cleaned_aliases = {a.lower().strip() for a in aliases if a}
        if not cleaned_aliases:
            raise ValueError("Cannot reconcile an empty set of aliases.")

        # 2. Find matches early.
        matches = {self.alias_lookup[alias] for alias in cleaned_aliases if alias in self.alias_lookup}

        # 3. If the initial was already found (required for 4 generated aliases), the result is returned.
        # otherwise, there's a potential opportunity to match the alias and update the generated aliases
        # by finding the initial.
        for id_ in matches:
            profile = self.users[id_]
            if len(profile.generated_aliases) == 4:
                return id_

        # 4. Attempt to find the name, initial and last name.
        best_parsed_info = {}
        for alias in cleaned_aliases:
            parsed = self._parse_alias(alias)
            if parsed.get("first_name") and parsed.get("last_name"):
                best_parsed_info = parsed
                break
        
        # 5. Creates a new user if no matches were found.
        if not matches:
            return self._create_user(cleaned_aliases, best_parsed_info)

        # 6. Updates the user if appropriate, any match is returned. Further logical matching is done in postprocessing.
        user_id = matches.pop()
        self._update_user(user_id, cleaned_aliases, best_parsed_info)
        return user_id

    def _parse_alias(self, alias: str) -> Dict:
        """Parses an alias string and returns a dictionary of its components."""
        # Try parsing as an email first
        email_filter = r"^([\w\.]+)@([\w\.]+)$"
        email_re = re.match(email_filter, alias)
        if email_re:
            name_part = email_re.group(1)
            domain_part = email_re.group(2)
            if domain_part == "enron.com" and not '..' in name_part:
                dot_count = name_part.count('.')
                if dot_count == 1:
                    name_re = re.match(r"^(\w+)\.(\w+)$", name_part)
                    if name_re:
                        return {"first_name": name_re.group(1), "initial": "", "last_name": name_re.group(2)}
                elif dot_count == 2:
                    name_re = re.match(r"^(\w+)\.(\w)\.(\w+)$", name_part)
                    if name_re:
                        return {"first_name": name_re.group(1), "initial": name_re.group(2), "last_name": name_re.group(3)}
        
        # Try parsing as "last, first"
        alias = re.sub(r"<[^>]+>", "", alias) # clean angle brackets
        split = alias.split(", ")
        if len(split) == 2:
            last_name, first_name_part = split
            
            if not last_name.strip() or not first_name_part.strip(): return {}

            first_name_spaces = first_name_part.split(" ")
            first_name = first_name_spaces[0].strip()
            initial = ""
            if len(first_name_spaces) > 1:
                initial = first_name_spaces[1].strip()

            last_name = re.sub(r'[^a-zA-Z]', '', last_name)
            first_name = re.sub(r'[^a-zA-Z]', '', first_name)
            initial = re.sub(r'[^a-zA-Z]', '', initial)

            if first_name and last_name:
                return {"first_name": first_name, "last_name": last_name, "initial": initial}

        return {}

    def _create_user(self, aliases: Set[str], parsed_info: Dict) -> int:
        """Creates a new user profile and returns the new user_id."""
        user_id = next(self.counter)
        
        first_name = parsed_info.get("first_name", "").lower()
        last_name = parsed_info.get("last_name", "").lower()
        initial = parsed_info.get("initial", "").lower()
        
        generated_aliases = set()
        if first_name and last_name:
            generated_aliases = helpers._generate_aliases(first_name, last_name, initial)

        all_aliases = aliases.union(generated_aliases)
        
        profile = UserProfile(
            id=user_id,
            first_name=first_name,
            last_name=last_name,
            generated_aliases=frozenset(generated_aliases),
            aliases=frozenset(all_aliases)
        )
        self.users[user_id] = profile
        
        for alias in all_aliases:
            self.alias_lookup[alias] = user_id
            
        return user_id

    def _update_user(self, user_id: int, new_aliases: Set[str], parsed_info: Dict):
        """Adds new aliases and potentially enriches an existing user's profile."""
        profile = self.users[user_id]

        # Enrich name if missing and now available
        if not profile.first_name and parsed_info.get("first_name"):
            first_name = parsed_info["first_name"]
            last_name = parsed_info["last_name"]
            profile.first_name = first_name
            profile.last_name = last_name
            initial = parsed_info["initial"]
            generated_aliases = helpers._generate_aliases(first_name, last_name, initial)
            profile.generated_aliases = profile.generated_aliases.union(generated_aliases)

        all_aliases = profile.aliases.union(new_aliases)
        profile.aliases = frozenset(all_aliases)

        for alias in all_aliases:
            if alias not in self.alias_lookup:
                self.alias_lookup[alias] = user_id
        if profile.generated_aliases:
            for alias in profile.generated_aliases:
                self.alias_lookup[alias] = user_id
