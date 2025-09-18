import subprocess
from pathlib import Path
from typing import List, LiteralString

def run_dbt_command(command_args: List[LiteralString] | List[str]) -> None:
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
