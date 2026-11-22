"""Query embedsync through the LlamaIndex retriever and print the nodes.

Exits 0 when nodes were returned, 1 when there were none, 2 on a request error.
"""

from __future__ import annotations

import argparse
import json
import sys

from embedsync_retriever import EmbedsyncError, EmbedsyncRetriever


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default="http://127.0.0.1:9187", help="embedsync metrics listener URL")
    p.add_argument("--query", required=True)
    p.add_argument("--mode", default="hybrid", choices=["vector", "hybrid", "text"])
    p.add_argument("--k", type=int, default=5)
    p.add_argument("--pipelines", help="comma-separated pipelines/views (default all)")
    p.add_argument("--filter", help='JSON filter, e.g. \'{"field":"lang","op":"eq","value":"en"}\'')
    p.add_argument("--header", action="append", default=[], help="extra header 'Name: value' (repeatable)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    headers = {}
    for h in args.header:
        name, sep, value = h.partition(":")
        if not sep:
            print(f"invalid --header {h!r}, want 'Name: value'", file=sys.stderr)
            return 2
        headers[name.strip()] = value.strip()
    retriever = EmbedsyncRetriever(
        args.url,
        k=args.k,
        mode=args.mode,
        pipelines=args.pipelines.split(",") if args.pipelines else None,
        filter=json.loads(args.filter) if args.filter else None,
        headers=headers or None,
    )
    try:
        nodes = retriever.retrieve(args.query)
    except EmbedsyncError as e:
        print(e, file=sys.stderr)
        return 2
    for i, n in enumerate(nodes, 1):
        print(f"{i}. [{n.node.node_id}] score={n.score:.4f} ranks={n.node.metadata['ranks']}")
        print(f"   {n.node.get_content()[:200]!r}")
    if not nodes:
        print("no results", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
