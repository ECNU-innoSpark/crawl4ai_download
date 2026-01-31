"""Seed prompts and few-shot demos for all ROMA agents.

This package provides instruction prompts and demos for use with DSPy examples
and prompt optimization workflows.
"""

from .aggregator_seed import AGGREGATOR_PROMPT, AGGREGATOR_DEMOS
from .atomizer_seed import ATOMIZER_PROMPT, ATOMIZER_DEMOS
from .executor_seed import EXECUTOR_PROMPT, EXECUTOR_DEMOS
from .planner_seed import PLANNER_PROMPT, PLANNER_DEMOS
from .verifier_seed import VERIFIER_PROMPT, VERIFIER_DEMOS

# Deep Research (DR) base prompts
from .dr_atomizer_seed import ATOMIZER_DR_PROMPT, ATOMIZER_DR_DEMOS
from .dr_planner_seed import PLANNER_DR_PROMPT, PLANNER_DR_DEMOS
from .dr_executor_seed import EXECUTOR_DR_PROMPT, EXECUTOR_DR_DEMOS
from .dr_aggregator_seed import AGGREGATOR_DR_PROMPT, AGGREGATOR_DR_DEMOS
from .dr_verifier_seed import VERIFIER_DR_PROMPT, VERIFIER_DR_DEMOS

# Deep Research (DR) task-routed planner variants
from .dr_planner_retrieve_seed import PLANNER_DR_RETRIEVE_PROMPT, PLANNER_DR_RETRIEVE_DEMOS
from .dr_planner_think_seed import PLANNER_DR_THINK_PROMPT, PLANNER_DR_THINK_DEMOS
from .dr_planner_write_seed import PLANNER_DR_WRITE_PROMPT, PLANNER_DR_WRITE_DEMOS

# Deep Research (DR) task-routed executor variants
# NOTE: DR executor variants use the same variable names as non-DR variants in their own modules.
# We export them with DR-prefixed aliases to avoid collisions in this package namespace.
from .dr_executor_retrieve_seed import (
    EXECUTOR_RETRIEVE_PROMPT as EXECUTOR_DR_RETRIEVE_PROMPT,
    EXECUTOR_RETRIEVE_DEMOS as EXECUTOR_DR_RETRIEVE_DEMOS,
)
from .dr_executor_think_seed import (
    EXECUTOR_THINK_PROMPT as EXECUTOR_DR_THINK_PROMPT,
    EXECUTOR_THINK_DEMOS as EXECUTOR_DR_THINK_DEMOS,
)
from .dr_executor_write_seed import (
    EXECUTOR_WRITE_PROMPT as EXECUTOR_DR_WRITE_PROMPT,
    EXECUTOR_WRITE_DEMOS as EXECUTOR_DR_WRITE_DEMOS,
)

# Deep Research (DR) aggregator variants (task-type routed)
from .dr_aggregator_retrieve_seed import (
    AGGREGATOR_DR_RETRIEVE_PROMPT,
    AGGREGATOR_DR_RETRIEVE_DEMOS,
)
from .dr_aggregator_think_seed import (
    AGGREGATOR_DR_THINK_PROMPT,
    AGGREGATOR_DR_THINK_DEMOS,
)
from .dr_aggregator_write_seed import (
    AGGREGATOR_DR_WRITE_PROMPT,
    AGGREGATOR_DR_WRITE_DEMOS,
)

# SWE-bench specific prompts
from .planner_swebench_seed import PLANNER_SWEBENCH_PROMPT, PLANNER_SWEBENCH_DEMOS
from .executor_swebench_seed import EXECUTOR_SWEBENCH_PROMPT, EXECUTOR_SWEBENCH_DEMOS

__all__ = [
    "AGGREGATOR_PROMPT",
    "AGGREGATOR_DEMOS",
    "ATOMIZER_PROMPT",
    "ATOMIZER_DEMOS",
    "EXECUTOR_PROMPT",
    "EXECUTOR_DEMOS",
    "PLANNER_PROMPT",
    "PLANNER_DEMOS",
    "VERIFIER_PROMPT",
    "VERIFIER_DEMOS",
    # DR base prompts
    "ATOMIZER_DR_PROMPT",
    "ATOMIZER_DR_DEMOS",
    "PLANNER_DR_PROMPT",
    "PLANNER_DR_DEMOS",
    "EXECUTOR_DR_PROMPT",
    "EXECUTOR_DR_DEMOS",
    "AGGREGATOR_DR_PROMPT",
    "AGGREGATOR_DR_DEMOS",
    "VERIFIER_DR_PROMPT",
    "VERIFIER_DR_DEMOS",
    # DR planner variants
    "PLANNER_DR_RETRIEVE_PROMPT",
    "PLANNER_DR_RETRIEVE_DEMOS",
    "PLANNER_DR_THINK_PROMPT",
    "PLANNER_DR_THINK_DEMOS",
    "PLANNER_DR_WRITE_PROMPT",
    "PLANNER_DR_WRITE_DEMOS",
    # DR executor variants (aliased to avoid collisions)
    "EXECUTOR_DR_RETRIEVE_PROMPT",
    "EXECUTOR_DR_RETRIEVE_DEMOS",
    "EXECUTOR_DR_THINK_PROMPT",
    "EXECUTOR_DR_THINK_DEMOS",
    "EXECUTOR_DR_WRITE_PROMPT",
    "EXECUTOR_DR_WRITE_DEMOS",
    # DR Aggregator variants
    "AGGREGATOR_DR_RETRIEVE_PROMPT",
    "AGGREGATOR_DR_RETRIEVE_DEMOS",
    "AGGREGATOR_DR_THINK_PROMPT",
    "AGGREGATOR_DR_THINK_DEMOS",
    "AGGREGATOR_DR_WRITE_PROMPT",
    "AGGREGATOR_DR_WRITE_DEMOS",
    # SWE-bench
    "PLANNER_SWEBENCH_PROMPT",
    "PLANNER_SWEBENCH_DEMOS",
    "EXECUTOR_SWEBENCH_PROMPT",
    "EXECUTOR_SWEBENCH_DEMOS",
]