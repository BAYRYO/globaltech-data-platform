"""Write the three landing drops and the sensor day of one profile."""

from pathlib import Path

from gtdp.datagen.customers import CustomersHistory, write_customers
from gtdp.datagen.engine import WrittenFile, write_manifest
from gtdp.datagen.model import DROPS, GENERATOR_VERSION, Profile
from gtdp.datagen.orders import OrdersHistory, write_orders
from gtdp.datagen.products import write_products
from gtdp.datagen.sensors import write_sensors
from gtdp.datagen.weblogs import write_weblogs
from gtdp.datagen.world import build_world, load_kit


def generate(profile: Profile, seed: int, out: Path, kit_dir: Path) -> list[WrittenFile]:
    """Drops are written in order because J2 and J3 resend rows and reuse ids of earlier drops."""
    world = build_world(load_kit(kit_dir), seed)
    root = out / profile.name
    orders = OrdersHistory()
    customers = CustomersHistory()
    written = []
    for drop in DROPS:
        landing = root / "landing" / drop.ingestion_date.isoformat()
        files = [
            write_orders(landing, seed, profile, drop, world, orders),
            write_customers(landing, seed, drop, world, customers),
            write_products(landing, seed, drop, world),
            write_weblogs(landing, seed, profile, drop, world),
        ]
        header = {
            "generator_version": GENERATOR_VERSION,
            "seed": seed,
            "profile": profile.name,
            "ingestion_date": drop.ingestion_date.isoformat(),
        }
        write_manifest(landing, header, files)
        written += files
    return written + write_sensors(root, seed, profile)
