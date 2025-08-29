<img src="./logo.png">

## Introduction

A query engine over semi-structured (JSON) logs.

Similar to <a href="https://trino.io/">trino</a>, but doesn't require a table's schema (column & types) before executing a query.

While trino receives SQL and starts returning results once the entire query finishes (batch ETL), miso's query API receives a [KQL query](https://learn.microsoft.com/en-us/kusto/query/) (think SQL but with bash like pipes, see example below), and streams back the results using <a href="https://html.spec.whatwg.org/multipage/server-sent-events.html">SSE</a> (stream ETL).

It supports the same optimization based <a href="https://trino.io/docs/current/optimizer/pushdown.html">predicate pushdown</a> mechanism in trino, so a query transpiles as many query steps as its connector supports into the connector's query language, returning fewer documents over the network (which is usually the bottleneck), and utilizing the connector's indexes, making queries return much faster.

Here's an example of a query supported today by miso (`localqw` is a <a href="https://quickwit.io/">Quickwit</a> connector to `localhost:7280/`):

```sh
# curl supports SSE by adding the -N flag.
curl -N -H 'Content-Type: application/json' localhost:8080/query -d '{
  "query": "
    localqw.hdfs1
    | union (localqw.hdfs2)
    | where @time > now() - 1d
    | summarize minTid = min(tenant_id), maxTid = max(tenant_id), count = count()
      by severity_text
    | join (
        localqw.stackoverflow
        | where questionId > 80
      ) on $left.minTid == $right.questionId
    | top 10 by count desc
    | project-rename minimumTenantID = minTid
  "
}'
```
