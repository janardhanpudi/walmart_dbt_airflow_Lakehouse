import os
from pathlib import Path

from importlib import import_module


def load_postgres_driver():
    try:
        return import_module("psycopg2")
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "PostgreSQL driver is missing. Install it with: "
            "pip install psycopg2-binary"
        ) from error


DATA_DIR = Path(__file__).resolve().parent / "data"
CSV_FILES = {
    "customers.csv": "raw.customers",
    "stores.csv": "raw.stores",
    "products.csv": "raw.products",
    "employees.csv": "raw.employees",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items",
}


def load_env_file(path: Path) -> None:
    """Load simple KEY=value entries without overriding shell variables."""
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def main() -> None:
    psycopg2 = load_postgres_driver()
    project_dir = Path(__file__).resolve().parents[1]
    load_env_file(project_dir / ".env")
    load_env_file(project_dir / ".env.local")

    connection_string = (
        os.getenv("POSTGRES_CONNECTION_STRING")
        or os.getenv("DATABASE_URL")
    )
    if not connection_string:
        raise RuntimeError(
            "Set POSTGRES_CONNECTION_STRING or DATABASE_URL in the environment."
        )

    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            for csv_name, table_name in CSV_FILES.items():
                csv_path = DATA_DIR / csv_name
                if not csv_path.exists():
                    raise FileNotFoundError(f"CSV file not found: {csv_path}")

                print(f"Loading {csv_name} into {table_name}...")
                with csv_path.open("r", newline="") as csv_file:
                    cursor.copy_expert(
                        f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                        csv_file,
                    )
                print(f"Loaded {csv_name}")

    print("All CSV files loaded successfully.")


if __name__ == "__main__":
    main()
