from dataclasses import dataclass
from typing import Any

@dataclass
class ProvisionExecutionResult:
    runtime_inventory: Any
    provision_report: Any
    blocked: Any
    artifacts: Any
    issues: Any
    checkpoint: Any
