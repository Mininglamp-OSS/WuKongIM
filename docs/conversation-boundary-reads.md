# Conversation boundary reads on the v2.2.5 branch

`clearUnread`, `setUnread`, and `delete` update conversations at the UID slot
leader. Their message boundary is read separately from the channel leader.

The metadata read captures a slot committed index and waits, within the request
budget, until that fixed index has been applied. Later unrelated writes do not
invalidate the read. Leadership changes still fail the read, and the caller
and serving node recheck the channel configuration around the boundary read.

For an active channel, the returned sequence is bounded by the committed index
in the post-read Raft snapshot. A message appended to disk but still waiting for
replication acknowledgements cannot advance the conversation boundary beyond
that index. A commit completed during the read may be included.

## Legacy recovery limitation

The committed-index bound is only as reliable as the old runtime's recovered
state. This branch initializes a channel's applied index from its durable message
tail, and initializes the runtime committed index from that applied index.
Although wkdb exposes a persistent channel applied-index key, the channel apply
pipeline does not maintain it. Existing records cannot be assumed to contain a
trustworthy independently persisted commit marker.

Consequently, a dormant-channel read can return a crash-residue uncommitted
suffix. Creating a runtime does not by itself resolve this: a freshly recovered
runtime also initially regards that durable suffix as committed. Only subsequent
replication/election reconciliation can establish the actual history. The active
read bound prevents new in-flight suffixes from being returned, but does not
repair this inherited recovery ambiguity. Resolving it requires commit-marker
maintenance and an explicit migration/recovery policy for existing data.

Dormant channels remain readable without being created or woken. This preserves
the v2.2.5 behavior and the above residual risk; it is not a claim of the stronger
committed-history guarantee provided by the refactored main branch. Missing
metadata returns an empty boundary without creating a channel. Dormant channels
with migration in progress are rejected.

These local role/configuration checks are not a quorum ReadIndex or a leader
lease. They do not guarantee linearizability across network partitions.

## Rolling upgrades and errors

The configuration and boundary RPCs use version 2 paths and responses. Version 1
conversation-read peers, including the earlier experimental implementation that
returned unbounded tails, are rejected. Existing unrelated RPCs are unchanged.
An upgraded caller does not fall back to the older boundary protocol. The full
fix requires upgrading participating nodes; requests served by old UID owners
retain the old behavior until those nodes are upgraded.

Each operation has one request budget and at most one route-refresh retry.
Unresolved readiness, transport, or ownership failures return HTTP 503 with
`{"msg":"retry required","status":503}`. Internal errors retain the channel,
attempt and available peer/slot details for diagnosis; remote serving failures
are also logged at debug level. Internal error text is not returned to API users.
