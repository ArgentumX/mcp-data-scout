"""
Seed script: creates realistic sample data in SQLite and CSV,
then registers all sources (SQLite + CSV connectors) into the
shared manifest so the server picks them up on startup.

SQLite database:
  - customers      — client profiles
  - orders         — purchase orders
  - products       — product catalogue
  - order_items    — order line items
  - employees      — company staff

CSV files (written to UPLOADS_DIR, each gets its own CSVConnector):
  - sales_regions.csv       — regional sales data
  - marketing_campaigns.csv — campaign performance
  - inventory_snapshot.csv  — stock levels
"""

import csv
import os
import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

UPLOADS_DIR = Path(os.getenv("UPLOADS_DIR") or "/data/uploads")
SQLITE_FILENAME = "seeded_sqlite_main.db"
SEED = 42
random.seed(SEED)

def get_seed_sqlite_path() -> Path:
    return UPLOADS_DIR / SQLITE_FILENAME


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_source_seeded(filename: str) -> bool:
    """Check if a CSV file already exists"""
    path = UPLOADS_DIR / filename
    return path.exists()


def rand_date(start: date, end: date) -> str:
    delta = (end - start).days
    return str(start + timedelta(days=random.randint(0, delta)))


FIRST_NAMES = ["Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace",
               "Henry", "Iris", "Jack", "Karen", "Leo", "Maria", "Nick",
               "Olivia", "Peter", "Quinn", "Rachel", "Sam", "Tina"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
              "Miller", "Davis", "Martinez", "Wilson", "Moore", "Taylor",
              "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin"]
CITIES = ["Moscow", "Saint Petersburg", "Novosibirsk", "Yekaterinburg",
          "Kazan", "Samara", "Omsk", "Chelyabinsk", "Rostov-on-Don", "Ufa"]
CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports",
               "Books", "Toys", "Food & Beverages", "Automotive"]
PRODUCT_NAMES = [
    "Laptop Pro 15", "Wireless Headphones", "Smart Watch", "USB-C Hub",
    "Mechanical Keyboard", "Gaming Mouse", "4K Monitor", "Webcam HD",
    "Running Shoes", "Winter Jacket", "Yoga Mat", "Dumbbell Set",
    "Coffee Maker", "Blender Pro", "Air Purifier", "LED Desk Lamp",
    "Python Programming Book", "Data Science Handbook", "Design Patterns",
    "Lego City Set", "RC Car", "Board Game Classic",
]


# ---------------------------------------------------------------------------
# SQLite seed
# ---------------------------------------------------------------------------

def seed_sqlite():
    sqlite_path = get_seed_sqlite_path()
    
    # Skip if already seeded
    if is_source_seeded(SQLITE_FILENAME):
        print(f"SQLite already seeded: {sqlite_path}, skipping.")
        return

    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if sqlite_path.exists():
        sqlite_path.unlink()

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    # -- customers --
    cur.execute("""
        CREATE TABLE customers (
            customer_id   INTEGER PRIMARY KEY,
            first_name    TEXT NOT NULL,
            last_name     TEXT NOT NULL,
            email         TEXT UNIQUE NOT NULL,
            city          TEXT,
            signup_date   TEXT,
            is_premium    INTEGER DEFAULT 0
        )
    """)
    customers = []
    for i in range(1, 201):
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
        city = random.choice(CITIES)
        sd = rand_date(date(2020, 1, 1), date(2024, 12, 31))
        premium = random.randint(0, 1)
        customers.append((i, fn, ln, email, city, sd, premium))
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?,?,?)", customers
    )

    # -- products --
    cur.execute("""
        CREATE TABLE products (
            product_id    INTEGER PRIMARY KEY,
            name          TEXT NOT NULL,
            category      TEXT,
            price         REAL NOT NULL,
            stock_qty     INTEGER DEFAULT 0,
            created_at    TEXT
        )
    """)
    products = []
    for i, name in enumerate(PRODUCT_NAMES, 1):
        cat = random.choice(CATEGORIES)
        price = round(random.uniform(9.99, 1499.99), 2)
        stock = random.randint(0, 500)
        ca = rand_date(date(2019, 1, 1), date(2023, 12, 31))
        products.append((i, name, cat, price, stock, ca))
    cur.executemany(
        "INSERT INTO products VALUES (?,?,?,?,?,?)", products
    )

    # -- employees --
    cur.execute("""
        CREATE TABLE employees (
            employee_id   INTEGER PRIMARY KEY,
            full_name     TEXT NOT NULL,
            department    TEXT,
            position      TEXT,
            salary        REAL,
            hire_date     TEXT,
            manager_id    INTEGER
        )
    """)
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Support"]
    positions = ["Junior", "Middle", "Senior", "Lead", "Manager", "Director"]
    employees = []
    for i in range(1, 51):
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        dept = random.choice(departments)
        pos = random.choice(positions)
        salary = round(random.uniform(40000, 200000), 2)
        hd = rand_date(date(2015, 1, 1), date(2024, 6, 30))
        mgr = random.randint(1, 10) if i > 10 else None
        employees.append((i, f"{fn} {ln}", dept, pos, salary, hd, mgr))
    cur.executemany(
        "INSERT INTO employees VALUES (?,?,?,?,?,?,?)", employees
    )

    # -- orders --
    cur.execute("""
        CREATE TABLE orders (
            order_id      INTEGER PRIMARY KEY,
            customer_id   INTEGER NOT NULL,
            order_date    TEXT NOT NULL,
            status        TEXT DEFAULT 'pending',
            total_amount  REAL DEFAULT 0,
            shipping_city TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    orders = []
    for i in range(1, 501):
        cid = random.randint(1, 200)
        od = rand_date(date(2022, 1, 1), date(2025, 3, 1))
        status = random.choice(statuses)
        total = 0.0
        city = random.choice(CITIES)
        orders.append([i, cid, od, status, total, city])
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?,?)", orders
    )

    # -- order_items --
    cur.execute("""
        CREATE TABLE order_items (
            item_id       INTEGER PRIMARY KEY,
            order_id      INTEGER NOT NULL,
            product_id    INTEGER NOT NULL,
            quantity      INTEGER NOT NULL,
            unit_price    REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """)
    items = []
    item_id = 1
    order_totals: dict[int, float] = {}
    for order in orders:
        oid = order[0]
        n_items = random.randint(1, 5)
        for _ in range(n_items):
            pid = random.randint(1, len(PRODUCT_NAMES))
            qty = random.randint(1, 4)
            price = products[pid - 1][3]
            items.append((item_id, oid, pid, qty, price))
            order_totals[oid] = order_totals.get(oid, 0) + qty * price
            item_id += 1

    cur.executemany(
        "INSERT INTO order_items VALUES (?,?,?,?,?)", items
    )

    # Update order totals
    for oid, total in order_totals.items():
        cur.execute(
            "UPDATE orders SET total_amount = ? WHERE order_id = ?",
            (round(total, 2), oid),
        )

    conn.commit()
    conn.close()
    print(f"SQLite seeded: {sqlite_path}")


# ---------------------------------------------------------------------------
# CSV seed — written directly to UPLOADS_DIR
# ---------------------------------------------------------------------------

def write_csv(filename: str, headers: list[str], rows: list[list]) -> Path:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    path = UPLOADS_DIR / filename
    
    # Skip if already seeded
    if is_source_seeded(filename):
        print(f"CSV already seeded: {path}, skipping.")
        return path
    
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"CSV written: {path} ({len(rows)} rows)")
    return path


def seed_csv() -> list[tuple[str, Path, str]]:
    """
    Generate seed CSV files and return a list of
    (source_id, file_path, description) tuples ready for registration.
    """
    results: list[tuple[str, Path, str]] = []

    # sales_regions.csv
    regions = ["North", "South", "East", "West", "Central"]
    headers = ["region", "month", "year", "revenue", "units_sold",
               "new_customers", "return_rate_pct"]
    rows = []
    for year in [2023, 2024]:
        for month in range(1, 13):
            for region in regions:
                revenue = round(random.uniform(50000, 500000), 2)
                units = random.randint(100, 5000)
                new_cust = random.randint(10, 300)
                return_rate = round(random.uniform(0.5, 8.0), 2)
                rows.append([region, month, year, revenue, units, new_cust, return_rate])
    path = write_csv("seeded_sales_regions.csv", headers, rows)
    results.append(("seeded_sales_regions", path, "Regional sales data (2023–2024)"))

    # marketing_campaigns.csv
    channels = ["Email", "Social Media", "Search Ads", "Display Ads",
                "Affiliate", "Direct Mail"]
    headers = ["campaign_id", "campaign_name", "channel", "start_date",
               "end_date", "budget_usd", "impressions", "clicks",
               "conversions", "revenue_generated"]
    rows = []
    for i in range(1, 101):
        channel = random.choice(channels)
        name = f"{channel.replace(' ', '_')}_Campaign_{i}"
        sd = rand_date(date(2023, 1, 1), date(2024, 6, 30))
        ed = rand_date(date(2024, 7, 1), date(2025, 12, 31))
        budget = round(random.uniform(1000, 100000), 2)
        impressions = random.randint(5000, 1000000)
        clicks = random.randint(50, impressions // 5)
        conversions = random.randint(1, clicks // 3 + 1)
        revenue = round(conversions * random.uniform(20, 500), 2)
        rows.append([i, name, channel, sd, ed, budget, impressions,
                     clicks, conversions, revenue])
    path = write_csv("seeded_marketing_campaigns.csv", headers, rows)
    results.append(("seeded_marketing_campaigns", path, "Marketing campaign performance data"))

    # inventory_snapshot.csv
    headers = ["product_id", "product_name", "category", "warehouse",
               "quantity_on_hand", "quantity_reserved", "reorder_point",
               "last_restocked_date", "unit_cost"]
    warehouses = ["Moscow WH", "SPb WH", "Novosibirsk WH", "Kazan WH"]
    rows = []
    for pid, (_, name, cat, price, _, _) in enumerate(
        [
            (i, p[1], p[2], p[3], p[4], p[5])
            for i, p in enumerate(
                [
                    (i + 1, PRODUCT_NAMES[i], CATEGORIES[i % len(CATEGORIES)],
                     round(random.uniform(5, 800), 2), 0, "")
                    for i in range(len(PRODUCT_NAMES))
                ],
                1,
            )
        ],
        1,
    ):
        for wh in warehouses:
            qty = random.randint(0, 1000)
            reserved = random.randint(0, min(qty, 100))
            reorder = random.randint(10, 100)
            restock = rand_date(date(2024, 1, 1), date(2025, 3, 1))
            cost = round(price * random.uniform(0.3, 0.7), 2)
            rows.append([pid, name, cat, wh, qty, reserved, reorder, restock, cost])
    path = write_csv("seeded_inventory_snapshot.csv", headers, rows)
    results.append(("seeded_inventory_snapshot", path, "Inventory snapshot across warehouses"))

    return results


# ---------------------------------------------------------------------------
# Register all seeded sources into the manifest
# ---------------------------------------------------------------------------

def register_seeded_sources(csv_sources: list[tuple[str, Path, str]]) -> None:
    """
    Register seeded sources (SQLite + per-file CSVs) into the manifest so
    the server treats them the same as any user-uploaded source.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from server.source_registry import (
        SourceRegistry,
        SOURCES_MANIFEST,
        _indexing_rules_to_dict,
    )
    from connectors.sqlite_connector import SQLiteConnector
    from connectors.csv_connector import CSVConnector
    from connectors.abstraction.base import IndexingRules

    SQLITE_INDEXING_RULES = IndexingRules(
        exclude_tables={"sqlite_sequence"},
        exclude_columns={
            "customers": {"customer_id"},
            "employees": {"employee_id", "manager_id"},
            "order_items": {"item_id", "order_id", "product_id"},
            "orders": {"order_id", "customer_id"},
            "products": {"product_id"},
        },
        row_value_tables={"customers", "employees", "orders", "products"},
        row_value_columns={
            "customers": {"first_name", "last_name", "email", "city", "signup_date"},
            "employees": {"full_name", "department", "position", "hire_date"},
            "orders": {"order_date", "status", "shipping_city"},
            "products": {"name", "category", "created_at"},
        },
    )

    reg = SourceRegistry(manifest_path=SOURCES_MANIFEST)

    # Register SQLite source as dynamic so it appears in the manifest
    sqlite_connector = SQLiteConnector(
        source_id="seeded_sqlite_main",
        db_path=str(get_seed_sqlite_path()),
        description="Main SQLite database with business data",
        indexing_rules=SQLITE_INDEXING_RULES,
    )
    reg.register_dynamic(sqlite_connector)

    # Register each CSV file as its own dynamic source
    for source_id, file_path, description in csv_sources:
        connector = CSVConnector(
            source_id=source_id,
            file_path=str(file_path),
            description=description,
            indexing_rules=IndexingRules(),
        )
        reg.register_dynamic(connector)

    print(f"Registered {1 + len(csv_sources)} sources into manifest: {SOURCES_MANIFEST}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Seeding SQLite database...")
    seed_sqlite()
    print("Seeding CSV files...")
    csv_sources = seed_csv()
    print("Registering seeded sources into manifest...")
    register_seeded_sources(csv_sources)
    print("Done.")