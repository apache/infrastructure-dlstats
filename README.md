# Download stats tool for Apache projects

This service runs at https://logging1-he-de.apache.org/stats/ and displays download statistics for projects over a specified period of time.
It feeds off fastly's kibana logs and requires LDAP auth to work, primarily to prevent abuse.

It is installed as a pipservice on our logging cluster.
