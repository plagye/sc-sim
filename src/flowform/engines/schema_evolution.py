# Schema evolution engine: manages field appearance by sim day number.
# TODO: implement in Step 41
from __future__ import annotations

from datetime import date

from flowform.config import Config
from flowform.state import SimulationState


def run(state: SimulationState, config: Config, simdate: date) -> list:
    return []
