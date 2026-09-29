# Copyright (c) Microsoft. All rights reserved.

"""Shared input receipt fixtures for history acceptance tests."""

from agent_framework import Message

PRIOR = ("occ-prior", "fingerprint-prior")
ACCEPTED = ("occ-B", "fingerprint-B")


def _input(text: str) -> Message:
    message = Message("user", [text])
    message._durable_ingestion_receipt = (f"occ-{text}", f"fingerprint-{text}")  # type: ignore[attr-defined]
    return message
