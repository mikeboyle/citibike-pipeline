import subprocess
from pathlib import Path
from typing import List, LiteralString
import os

def run_dbt_command(command_args: List[LiteralString] | List[str]) -> None:
    print(f"🌎 running with project_id={os.environ.get('GCP_PROJECT_ID')}")
    print(f"🌎 running with dataset={os.environ.get('BQ_DATASET')}")
    print(f"🌎 running with keyfile={os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")
    package_dir = Path(__file__).parent.parent
    dbt_dir = package_dir.parent / "dbt_transformations"
    
    command_args = command_args
    result = subprocess.run(
        command_args,
        cwd=dbt_dir,
        capture_output=True,
        check=False,
        text=True,
    )

    # Print output to logs
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise Exception(f"dbt command failed with exit code {result.returncode}. See output above for details.")
