# SPDX-License-Identifier: Apache-2.0

"""The pipeline's source tables: customers, their orders, each order's lines.

    <base>/raw/customers/part-000.parquet ...     customer_id, segment, country, signup_date
    <base>/raw/orders/part-000.parquet ...        order_id, customer_id, order_date, channel, status
    <base>/raw/order_lines/part-000.parquet ...   order_id, line_no, category, quantity, unit_price, discount
    <base>/raw/_manifest.json                     what was generated; written last

Synthetic and deterministic: file i of a table depends only on the profile,
the seed and i. So a table is made one file at a time (the most held in
memory is one file), any machine makes the same bytes, and tables without
their manifest are a generation that did not end.

There is something to learn in them. Each customer has three fixed traits,
functions of its id alone: how much it likes to buy (loyalty: it orders more
often and more units), how much it pays for a thing (means), and a favourite
category. And the year has a season. So what a customer did says something
about what it will do, which is what the models are asked to find.
"""

import numpy as np
import pyarrow as pa
import store
from profiles import START_YEAR

SEED = 7
TABLES = ("customers", "orders", "order_lines")
MANIFEST = "_manifest.json"

SEGMENTS = ("consumer", "business", "premium")
COUNTRIES = ("MX", "CO", "AR", "CL", "PE")
CHANNELS = ("web", "app", "store")
STATUSES = ("completed", "cancelled", "returned")
CATEGORIES = ("electronics", "home", "fashion", "grocery", "toys", "sports", "books", "beauty")
BASE_PRICE = np.array([120.0, 80.0, 45.0, 20.0, 25.0, 60.0, 18.0, 30.0])
CATEGORY_WEIGHT = np.array([0.08, 0.12, 0.18, 0.25, 0.08, 0.09, 0.10, 0.10])
DISCOUNTS = np.array([0.0, 0.05, 0.10, 0.20])
# What of a profile decides the data. Its box and its engine settings do not.
SPEC_FIELDS = ("customers", "orders", "lines_per_order", "months", "files")


def loyalty(customer_id):
    """A customer's taste for buying, in [0, 1): fixed, from its id alone."""
    return ((np.asarray(customer_id, dtype=np.int64) * 2654435761) % 1000) / 1000.0


def means(customer_id):
    """How much a customer pays for the same thing, in [0, 1). Its own
    trait: knowing a customer's loyalty says nothing of it."""
    return ((np.asarray(customer_id, dtype=np.int64) * 40503 + 12345) % 997) / 997.0


def favourite(customer_id):
    """The category half of a customer's lines are in."""
    return ((np.asarray(customer_id, dtype=np.int64) * 7919) >> 3) % len(CATEGORIES)


def _slice(total, files, index):
    return index * total // files, (index + 1) * total // files


def customers_file(profile, index, seed=SEED):
    low, high = _slice(profile.customers, profile.files, index)
    rng = np.random.default_rng([seed, 0, index])
    ids = np.arange(low, high, dtype=np.int64)
    taste = loyalty(ids)
    segment = np.where(taste >= 0.85, 2, (rng.random(len(ids)) < 0.25).astype(np.int64))
    signup = np.datetime64(f"{START_YEAR}-01-01") - rng.integers(30, 1500, len(ids)).astype("timedelta64[D]")
    return pa.table({
        "customer_id": ids,
        "segment": np.array(SEGMENTS)[segment],
        "country": np.array(COUNTRIES)[rng.integers(0, len(COUNTRIES), len(ids))],
        "signup_date": pa.array(signup, type=pa.date32()),
    })


def _month_weights(months):
    month = np.arange(months) % 12
    weight = 1.0 + 0.25 * np.sin(2 * np.pi * month / 12) + np.where(month >= 10, 0.5, 0.0)
    return weight / weight.sum()


def orders_file(profile, index, seed=SEED):
    """File `index` of orders, and the lines of exactly those orders."""
    low, high = _slice(profile.orders, profile.files, index)
    count = high - low
    rng = np.random.default_rng([seed, 1, index])
    ids = np.arange(low, high, dtype=np.int64)
    # Of three customers drawn at random, the order is the most loyal one's.
    drawn = rng.integers(0, profile.customers, (3, count))
    customer = drawn[np.argmax(loyalty(drawn), axis=0), np.arange(count)].astype(np.int64)
    month = rng.choice(profile.months, count, p=_month_weights(profile.months))
    date = (np.datetime64(f"{START_YEAR}-01") + month.astype("timedelta64[M]")).astype("datetime64[D]") \
        + rng.integers(0, 28, count).astype("timedelta64[D]")
    orders = pa.table({
        "order_id": ids,
        "customer_id": customer,
        "order_date": pa.array(date, type=pa.date32()),
        "channel": np.array(CHANNELS)[rng.integers(0, len(CHANNELS), count)],
        "status": np.array(STATUSES)[rng.choice(len(STATUSES), count, p=[0.92, 0.05, 0.03])],
    })

    per_order = 1 + rng.poisson(max(profile.lines_per_order - 1, 0), count)
    total = int(per_order.sum())
    first_line = np.repeat(np.cumsum(per_order) - per_order, per_order)
    category = np.where(rng.random(total) < 0.5, np.repeat(favourite(customer), per_order),
                        rng.choice(len(CATEGORIES), total, p=CATEGORY_WEIGHT))
    taste = np.repeat(loyalty(customer), per_order)
    price_level = np.repeat(0.6 + 1.6 * means(customer), per_order)
    lines = pa.table({
        "order_id": np.repeat(ids, per_order),
        "line_no": (np.arange(total) - first_line + 1).astype(np.int32),
        "category": np.array(CATEGORIES)[category],
        "quantity": (1 + rng.binomial(4, 0.1 + 0.7 * taste)).astype(np.int32),
        "unit_price": np.round(BASE_PRICE[category] * price_level * rng.lognormal(0.0, 0.25, total), 2),
        "discount": DISCOUNTS[rng.choice(len(DISCOUNTS), total, p=[0.60, 0.20, 0.15, 0.05])],
    })
    return orders, lines


def _part(index):
    return f"part-{index:03d}.parquet"


def generate(base, profile, seed=SEED, force=False):
    """Write the three tables under <base>/raw and return their manifest.
    Tables already there for the same profile and seed are kept."""
    raw = store.join(base, "raw")
    spec = {**{name: getattr(profile, name) for name in SPEC_FIELDS}, "seed": seed}
    found = store.read_json(store.join(raw, MANIFEST))
    if found and not force:
        if found.get("spec") == spec:
            return found
        raise ValueError(f"{raw} holds the tables of another profile or seed ({found.get('spec')}); "
                         "force=True replaces them")
    for name in TABLES:
        store.remove_dir(store.join(raw, name))
    rows = dict.fromkeys(TABLES, 0)
    for index in range(profile.files):
        customers = customers_file(profile, index, seed)
        orders, lines = orders_file(profile, index, seed)
        for name, table in zip(TABLES, (customers, orders, lines)):
            store.write_parquet(table, store.join(raw, name, _part(index)))
            rows[name] += table.num_rows
    manifest = {"spec": spec, "rows": rows}
    store.write_json(store.join(raw, MANIFEST), manifest)
    return manifest


if __name__ == "__main__":
    import sys

    import profiles

    print(generate(sys.argv[1], profiles.get(sys.argv[2] if len(sys.argv) > 2 else "")))
