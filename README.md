This project is a parsing attempt at the Enron email dataset, found here: https://www.cs.cmu.edu/~enron/

My approach differs by:
1. Converting each file into a canon format (there is a potential difference for timezones) to de-duplicate and
allow for efficient parsing.
2. Separating parent and child emails, parsing both.
3. Logical matching of users by names, emails, any other alias format, and postprocessing matching.

**Instructions**

The default locations if you prefer not to change them, is to have the "maildir" folder in the "input" folder that's
a sibling to the "src" folder.

All output-related files will be generated within an "output" folder, that is also a sibling of the "src" folder by
default.

1. Configure the input and output location variables in main.py at your leisure.
2. Run main.py (NOTE: An array of errors are expected, as not every file can be parsed. This makes up a tiny portion of all files.)
3. Configure the output location variables if applicable within postprocessing_pipeline.py, as well as the desired output locations.
4. Run postprocessing_pipeline.py.

Enjoy!

**Limitations & Contributing**

1. To my knowledge, there are approximately 61 users with the alias of only one character; this forms a tiny percentage.
2. There are a little over 4,000 email items (approx 2.1% of the data) that have a -1 value for the "sender_id" field.
I may decide to address this in the future, otherwise all are welcome to contribute by way of a pull request.
3. The timezone of child emails are assumed to follow their parent timezone. If analysis that heavily relies on the time specifically is needed, I will
try to design a more rigorous approach for capturing timezones of child emails. For example, a child email somewhere could be a parent email elsewhere.

Moreover, a timezone can be associated with each user, and the mode timezone can be presumed to be that user's timezone, which is an approach that can be done now, through queries.

A suitable diagnostic approach would be to load the message hashes of all affected emails, record the text that follows the "From" or "X-From" field
as is applicable, and modify the parsing query or make a secondary one.

Finally, I opted not to distinguish CC and To, as users are generally inconsistent in how these fields are used by nature. Moreover, groups also contain the sender's id.
