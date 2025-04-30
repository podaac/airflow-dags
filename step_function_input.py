import json
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class StepFunctionInput:
    version: str
    run_type: str
    reach_subset_file: str
    temporal_range: str
    tolerated_failure_percentage: str
    run_gbpriors: str
    run_postdiagnostics: str
    counter: str


    @classmethod
    def from_json_file(cls, file_path: Optional[str] = None) -> 'StepFunctionInput':
        """Create a StepFunctionInput instance from a JSON file.
        
        Args:
            file_path: Path to the JSON file. If None, uses default path.
            
        Returns:
            StepFunctionInput instance with values from the JSON file.
        """
        if file_path is None:
            file_path = os.path.join(os.path.dirname(__file__), 'step-function-init.json')
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        return cls(**data)

    def to_dict(self) -> dict:
        """Convert the instance to a dictionary.
        
        Returns:
            Dictionary representation of the instance.
        """
        return {
            "version": self.version,
            "run_type": self.run_type,
            "reach_subset_file": self.reach_subset_file,
            "temporal_range": self.temporal_range,
            "tolerated_failure_percentage": self.tolerated_failure_percentage,
            "run_gbpriors": self.run_gbpriors,
            "run_postdiagnostics": self.run_postdiagnostics,
            "counter": self.counter
        }

    def increment_version(self) -> None:
        """Increment the version number by 1."""
        current_version = int(self.version)
        self.version = f"{current_version + 1:04d}"

    def increment_counter(self) -> None:
        """Increment the counter number by 1."""
        current_counter = int(self.counter.split('-')[-1])
        self.counter = f"test-run-{current_counter + 1:04d}"

    def save_to_file(self, file_path: Optional[str] = None) -> None:
        """Save the current state to a JSON file.
        
        Args:
            file_path: Path to save the JSON file. If None, uses default path.
        """
        if file_path is None:
            file_path = os.path.join(os.path.dirname(__file__), 'step-function-init.json')
        
        with open(file_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=4) 