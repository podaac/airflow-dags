import json
import os
import boto3
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
    run_ssc: str
    run_lakeflow: str
    counter: str

    @classmethod
    def from_s3(self, bucket_prefix: str) -> 'StepFunctionInput':
        """Create a StepFunctionInput instance from a JSON file in S3.
        
        Args:
            bucket_prefix: Prefix to be used for the bucket name (bucket will be prefix + "config")
            
        Returns:
            StepFunctionInput instance with values from the JSON file in S3.
        """
        bucket_name = f"{bucket_prefix}-config"
        s3_client = boto3.client('s3')
        
        try:
            response = s3_client.get_object(
                Bucket=bucket_name,
                Key='step-function-init.json'
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            return self(**data)
        except Exception as e:
            raise Exception(f"Failed to read step-function-init.json from S3 bucket {bucket_name}: {str(e)}")


    @classmethod
    def list_objects(self, bucket_prefix: str):
        bucket_name = f"{bucket_prefix}-config"

        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in response.get('Contents', []):
            print(f"📁 Found object: {obj['Key']}")

            
    @classmethod
    def from_json_file(self, file_path: Optional[str] = None) -> 'StepFunctionInput':
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
        
        return self(**data)

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
            "run_ssc": self.run_ssc,
            "run_lakeflow": self.run_lakeflow,
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

    def save_to_s3(self, bucket_prefix: str) -> None:
        """Save the current state to a JSON file in S3.
        
        Args:
            bucket_prefix: Prefix to be used for the bucket name (bucket will be prefix + "-config")
        """
        bucket_name = f"{bucket_prefix}-config"
        s3_client = boto3.client('s3')
        
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key='step-function-init.json',
                Body=json.dumps(self.to_dict(), indent=4)
            )
        except Exception as e:
            raise Exception(f"Failed to save step-function-init.json to S3 bucket {bucket_name}: {str(e)}") 