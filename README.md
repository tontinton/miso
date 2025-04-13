<img src="./logo.png">

## Introduction

A query engine over semi-structured (JSON) logs.

Similar to <a href="https://trino.io/">trino</a>, but doesn't require a table's schema (column & types) before executing a query.

While trino receives SQL and starts returning results once the entire query finishes (batch ETL), miso's query API receives a sort of [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree) of the query plan (this was done to allow for any query language on the frontend), and streams back the results using <a href="https://html.spec.whatwg.org/multipage/server-sent-events.html">SSE</a> (stream ETL).

It supports the same optimization based <a href="https://trino.io/docs/current/optimizer/pushdown.html">predicate pushdown</a> mechanism in trino, so a query transpiles as many query steps as its connector supports into the connector's query language, returning fewer documents over the network (which is usually the bottleneck), and utilizing the connector's indexes, making queries return much faster.

Here's an example of a query supported today by miso (`localqw` is a <a href="https://quickwit.io/">Quickwit</a> connector to `localhost:7280/`):

```sh
# curl supports SSE by adding the -N flag.
curl -N -H 'Content-Type: application/json' localhost:8080/query -d '{
  "query": [
    { "scan": ["localqw", "hdfs1"] },
    { "union": [{ "scan": ["localqw", "hdfs2"] }] },
    {
      "summarize": {
        "aggs": {
          "min_tenant": {"min": "tenant_id"},
          "max_tenant": {"max": "tenant_id"},
          "count": "count"
        },
        "by": ["timestamp", "severity_text"]
      }
    },
    {
      "join": [
        {"on": ["min_tenant", "questionId"]},
        [
          { "scan": ["localqw", "stackoverflow"] },
          { "filter": {"gt": [{"id": "questionId"}, {"lit": 80}]} }
        ]
      ]
    }
    { "sort": [{"by": "count", "order": "desc"}] }
  ]
}'
```
