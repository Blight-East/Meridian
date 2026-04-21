from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from runtime.conversation.chat_engine import generate_chat_response
from runtime.conversation.chat_memory import save_message


DEFAULT_USER_ID = "meridian-direct-local"


def _print_intro(user_id: str) -> None:
    print("Meridian direct chat")
    print(f"user_id: {user_id}")
    print("Type /quit to exit.")
    print()


def _run_turn(user_id: str, message: str) -> str:
    save_message(user_id, "user", message)
    reply = generate_chat_response(user_id, message)
    save_message(user_id, "assistant", reply)
    return reply


def _interactive_chat(user_id: str) -> int:
    _print_intro(user_id)
    while True:
        try:
            message = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting Meridian chat.")
            return 0

        if not message:
            continue
        if message.lower() in {"/quit", "quit", "exit"}:
            print("Exiting Meridian chat.")
            return 0

        reply = _run_turn(user_id, message)
        print(f"\nMeridian: {reply}\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Talk directly to Meridian through the live Agent Flux conversation engine.",
    )
    parser.add_argument(
        "--user-id",
        default=DEFAULT_USER_ID,
        help="Conversation memory key to use for this direct Meridian session.",
    )
    parser.add_argument(
        "--message",
        default="",
        help="Run one message non-interactively and print Meridian's reply.",
    )
    args = parser.parse_args()

    user_id = str(args.user_id or DEFAULT_USER_ID).strip() or DEFAULT_USER_ID
    one_shot = str(args.message or "").strip()

    if one_shot:
        reply = _run_turn(user_id, one_shot)
        print(reply)
        return 0

    return _interactive_chat(user_id)


if __name__ == "__main__":
    raise SystemExit(main())
