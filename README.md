# lp-snapshot

Using [v3-polars](https://github.com/Uniswap/v3-polars), take snapshots of current in-range liquidity providers on arbitrary blocks.

This is valuable for research, business development, or creating incentivization strategies for Uniswap v3 LPs.

## How to use
1. Set up v3-polars according to its documentation
2. Place the v3-polars and lp-snapshot directories in the same folder
3. Update `poolAddress` and `as_of` to desired pool and block
4. Run and read json file!

## Caveats
The liquidity is attributed to the last address who initiated a transaction that touched (minted/burned) that position. This could mean a transfered position that is untouched will not get that liquidity attributed to them. Additionally, liquidity vaults that use the nft position manager may not have their liquidity correctly attributed.

In the future, support could be added for this, but is out of the scope of the current script.
