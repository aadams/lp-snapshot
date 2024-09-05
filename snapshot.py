import sys
import polars as pl
from datetime import date, timedelta, datetime, timezone
import numpy as np
from time import gmtime, strftime
import json

sys.path.append("../v3-polars/")
from v3 import state

import os

os.environ['ALLIUM_POLARSV3_QUERY_ID'] = ''
os.environ['ALLIUM_POLARSV3_API_KEY'] = ''

if __name__ == "__main__":
    assert os.environ["ALLIUM_POLARSV3_API_KEY"] != "", "Please provide allium keys"

    # data given to code
    poolAddress = "0xBDB04e915B94FbFD6e8552ff7860E59Db7d4499a"
    as_of = 20500000

    update = True
    nfp_address = "0xc36442b4a4522e871399cd717abdd847ab11fe88"
    fp_error_bound = 1e8

    # start of script
    pool = state.v3Pool(poolAddress, "ethereum", update=update, update_from="allium")

    maxSupported = pool.max_supported
    if pool.max_supported < as_of:
        print(f"Data may not be updated by this point - pool max: {maxSupported} - calc at: {as_of}")
    
    # pull the nft position manager data
    _ = state.v3Pool(
        poolAddress,
        "ethereum",
        update=update,
        update_from="allium",
        tables=["nfp"],
        pull=False,
    )

    data = "nfp"
    nfp = (
        pl.scan_parquet(f"{pool.data_path}/{data}/*.parquet")
        .filter((pl.col("address") == pool.pool) & (pl.col("chain_name") == pool.chain))
        .collect()
    )

    tick = pool.getTickAt(as_of)
    lps = (
        pool.mb.with_columns(
            key=(
                pl.col("owner")
                + "_"
                + pl.col("tick_lower").cast(pl.Utf8)
                + "_"
                + pl.col("tick_upper").cast(pl.Utf8)
            ),
            liquidity_delta=pl.col("type_of_event") * pl.col("amount"),
        )
        .filter(pl.col("block_number") < as_of)
        .filter(
            # positions are in range if tl <= tick < tu
            (pl.col("tick_lower") <= tick)
            & (pl.col("tick_upper") > tick)
        )
        .select(["key", "liquidity_delta"])
        .group_by("key")
        .sum()
        # filter out the empty positions
        .filter(pl.col("liquidity_delta") >= fp_error_bound)
    )

    lps_to_nfp = nfp.filter(pl.col("block_number") < as_of).filter(
        # positions are in range if tl <= tick < tu
        (pl.col("tick_lower") <= tick)
        & (pl.col("tick_upper") > tick)
    )

    assert lps.filter(pl.col("liquidity_delta") <= 0).shape[0] == 0, "Negative LPs"
    # parse lps
    parsed_lps = {}

    for key, delta in lps.iter_rows():
        owner, lower, upper = key.split("_")

        lower, upper = int(lower), int(upper)
        # we want to pull the wallet and not the nft position manager
        if owner == nfp_address:
            nfp_events = (
                lps_to_nfp.filter(
                    (pl.col("tick_lower") == lower) & (pl.col("tick_upper") == upper)
                )
                .with_columns(
                    direction=pl.when(pl.col("name") == "IncreaseLiquidity")
                    .then(1)
                    .otherwise(-1),
                    amount=pl.col("amount").str.replace_all('"', ""),
                )
                .with_columns(
                    liquidity_delta=pl.col("direction")
                    * pl.col("amount").cast(pl.Float64)
                )
            )

            # we attribute the liquidity to the last person who touched the position
            nfp_in_range = (
                (
                    nfp_events.select(["tokenid", "liquidity_delta"])
                    .group_by("tokenid")
                    .sum()
                    # fp error
                    .filter(pl.col("liquidity_delta") >= fp_error_bound)
                )
                .join(
                    (
                        nfp_events.select("block_number", "tokenid", "from_address")
                        .group_by("tokenid", "from_address")
                        .max()
                    ),
                    on="tokenid",
                )
                .select("from_address", "liquidity_delta")
            )

            # do we find missing liquidity?
            if not np.isclose(
                nfp_in_range.select("liquidity_delta").sum().item(), delta
            ):
                # is it small? likely floating point error
                if delta <= fp_error_bound:
                    continue
                raise ValueError("Missing liquidity")

            # early return and avoid costly loop
            if nfp_in_range.shape[0] == 1:
                wallet, size = nfp_in_range.item(0, 0), nfp_in_range.item(0, 1)
            else:
                for wallet, size in nfp_in_range.iter_rows():
                    parsed_lps[wallet] = size
        else:
            parsed_lps[owner] = delta

    print(parsed_lps)

    path = strftime("snapshot_%Y-%m-%d_%H-%M-%S", gmtime())
    with open(f"{path}.json", "w") as f:
        json.dump(parsed_lps, f)
