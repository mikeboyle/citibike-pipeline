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
        capture_output=False,
        check=True,
        text=True,
    )

    if result.returncode != 0:
        raise Exception(result.stderr)
