def _generate_aliases(first_name, last_name, initial="") -> set[str]:
    """
    Generates enron-related aliases that are well-established formats based on the name, initial and last name.
    :param first_name: Alias' first name
    :param last_name: Alias' last name
    :param initial: Alias' initial
    :return: A set of well-known aliases, given the name data.
    """
    aliases = set()
    if first_name == "" or last_name == "":
        return aliases
    aliases.add(first_name + "." + last_name + "@enron.com")
    aliases.add(first_name[0] + last_name + "@enron.com")

    if initial != "":
        aliases.add(first_name + "." + initial + "." + last_name + "@enron.com")
        aliases.add(initial + ".." + last_name + "@enron.com")
    return aliases