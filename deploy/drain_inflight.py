#!/usr/bin/env python3
"""
drain_inflight.py — Inspect and manage the inflight task queue.

When a worker crashes mid-task, the task stays in `agent_tasks_inflight`.
This tool lets you inspect, replay, archive, or discard those tasks.

Usage:
    python3 deploy/drain_inflight.py status           # Show current state
    python3 deploy/drain_inflight.py inspect           # List all stuck tasks
    python3 deploy/drain_inflight.py replay [--all]    # Move back to main queue
    python3 deploy/drain_inflight.py archive           # Move to DLQ archive
    python3 deploy/drain_inflight.py discard [--all]   # Remove permanently
"""
from __future__ import annotations
import os, sys, json, argparse, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MAIN_QUEUE = "agent_tasks"
INFLIGHT_QUEUE = "agent_tasks_inflight"
DLQ_QUEUE = "agent_tasks_dlq"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def cmd_status():
    """Show queue depths."""
    main = r.llen(MAIN_QUEUE)
    inflight = r.llen(INFLIGHT_QUEUE)
    dlq = r.llen(DLQ_QUEUE)
    print(f"Queue Status:")
    print(f"  {MAIN_QUEUE:30} {main:5} tasks")
    print(f"  {INFLIGHT_QUEUE:30} {inflight:5} tasks")
    print(f"  {DLQ_QUEUE:30} {dlq:5} tasks")
    if inflight > 0:
        print(f"\n⚠️  {inflight} task(s) stuck in inflight — run 'inspect' to review")
    else:
        print(f"\n✅ No stuck tasks")


def cmd_inspect():
    """List all items in the inflight queue."""
    items = r.lrange(INFLIGHT_QUEUE, 0, -1)
    if not items:
        print("Inflight queue is empty — nothing stuck.")
        return
    print(f"Inflight queue: {len(items)} item(s)\n")
    for i, item in enumerate(items):
        try:
            payload = json.loads(item)
            preview = json.dumps(payload, indent=2)[:300]
        except (json.JSONDecodeError, TypeError):
            preview = item[:300]
        print(f"[{i}] {preview}")
        print()


def cmd_replay(all_tasks: bool = False):
    """Move tasks from inflight back to main queue for re-execution."""
    count = r.llen(INFLIGHT_QUEUE)
    if count == 0:
        print("Nothing to replay.")
        return

    if not all_tasks:
        print(f"{count} task(s) in inflight. Use --all to replay all, or replay one at a time.")
        item = r.lindex(INFLIGHT_QUEUE, 0)
        try:
            preview = json.dumps(json.loads(item), indent=2)[:300]
        except Exception:
            preview = str(item)[:300]
        print(f"\nNext task:\n{preview}\n")
        confirm = input("Replay this task? [y/N] ").strip().lower()
        if confirm != "y":
            print("Skipped.")
            return
        r.lmove(INFLIGHT_QUEUE, MAIN_QUEUE, "LEFT", "RIGHT")
        print("✅ Task moved to main queue for re-execution.")
        return

    # Replay all
    replayed = 0
    while r.llen(INFLIGHT_QUEUE) > 0:
        r.lmove(INFLIGHT_QUEUE, MAIN_QUEUE, "LEFT", "RIGHT")
        replayed += 1
    print(f"✅ Replayed {replayed} task(s) back to main queue.")


def cmd_archive():
    """Move inflight tasks to the dead-letter queue for later analysis."""
    count = r.llen(INFLIGHT_QUEUE)
    if count == 0:
        print("Nothing to archive.")
        return
    archived = 0
    while r.llen(INFLIGHT_QUEUE) > 0:
        # Wrap with metadata before archiving
        item = r.lpop(INFLIGHT_QUEUE)
        envelope = json.dumps({
            "original_task": item,
            "archived_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "reason": "manual_archive",
        })
        r.rpush(DLQ_QUEUE, envelope)
        archived += 1
    print(f"✅ Archived {archived} task(s) to {DLQ_QUEUE}.")


def cmd_discard(all_tasks: bool = False):
    """Permanently remove tasks from inflight (data loss!)."""
    count = r.llen(INFLIGHT_QUEUE)
    if count == 0:
        print("Nothing to discard.")
        return

    if not all_tasks:
        print(f"⚠️  {count} task(s) in inflight. Use --all to discard all.")
        confirm = input(f"Discard all {count} task(s)? This is permanent. [y/N] ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return

    r.delete(INFLIGHT_QUEUE)
    print(f"🗑️  Discarded {count} task(s) from inflight.")


def main():
    parser = argparse.ArgumentParser(description="Inflight queue management")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Show queue depths")
    sub.add_parser("inspect", help="List stuck tasks")

    replay_p = sub.add_parser("replay", help="Move back to main queue")
    replay_p.add_argument("--all", action="store_true", help="Replay all tasks")

    sub.add_parser("archive", help="Move to dead-letter queue")

    discard_p = sub.add_parser("discard", help="Remove permanently")
    discard_p.add_argument("--all", action="store_true", help="Discard all")

    args = parser.parse_args()

    if args.command == "status":
        cmd_status()
    elif args.command == "inspect":
        cmd_inspect()
    elif args.command == "replay":
        cmd_replay(all_tasks=args.all)
    elif args.command == "archive":
        cmd_archive()
    elif args.command == "discard":
        cmd_discard(all_tasks=args.all)


if __name__ == "__main__":
    main()
