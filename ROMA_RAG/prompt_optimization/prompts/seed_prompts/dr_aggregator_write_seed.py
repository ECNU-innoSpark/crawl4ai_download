"""Aggregator instruction seed prompt for Deep Research (DR) — WRITE nodes.

This prompt is optimized for final long-form research report outputs:
- Report/whitepaper-level depth
- Full URL citations and bibliography
"""

# Reuse the existing DR report synthesizer prompt and demos as the WRITE variant.
# This keeps backward compatibility while enabling task-type routing via agent_mapping.aggregators.WRITE.

from .dr_aggregator_seed import (
    AGGREGATOR_DR_PROMPT as AGGREGATOR_DR_WRITE_PROMPT,
    AGGREGATOR_DR_DEMOS as AGGREGATOR_DR_WRITE_DEMOS,
)


