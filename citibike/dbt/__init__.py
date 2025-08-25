import subprocess
from pathlib import Path

def run_dbt_command(command: str) -> None:
    package_dir = Path(__file__).parent.parent
    dbt_dir = package_dir.parent / "dbt_transformations"
    
    command_args = command.split(" ")
    result = subprocess.run(
        command_args,
        cwd=dbt_dir,
        capture_output=False,
        check=True,
        text=True,
    )

    if result.returncode != 0:
        raise Exception(result.stderr)
