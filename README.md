# KuraiStorage

A storage that utilizes MemoryStore and Datastores. It's a mega datastore but ignores the insane rate limits that Datastore has.
It does this by using MemoryStore as a temporary holder for the data and then in batches it uploads to the datastore (when it can).

It does not let two places modify at the same store and if one server tries to, it'll delay it.
Also uses MessagingService to message new data changes across all servers (might not be the most reliable as they have terrible limits)

Implements a queue and caches data to prevent repeat get requests.