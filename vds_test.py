#!/usr/bin/env python3
"""CLI tool for running VDS (Virtual Dataset Service) tests against collections."""

import argparse
import sys
import xarray as xr
import earthaccess


COLLECTIONS = {
    "MUR25-JPL-L4-GLOB-v04.2": {
        "https_json": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-public/virtual_collections/MUR25-JPL-L4-GLOB-v04.2/MUR25-JPL-L4-GLOB-v04.2_virtual_https.json",
        "s3_json": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-public/virtual_collections/MUR25-JPL-L4-GLOB-v04.2/MUR25-JPL-L4-GLOB-v04.2_virtual_s3.json",
        "s3_endpoint": "https://archive.podaac.earthdata.nasa.gov/s3credentials",
        "variable": "analysed_sst",
        "slice_dims": {"time": slice(0, 10), "lat": slice(0, 10), "lon": slice(0, 10)},
    },
    "TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4": {
        "https_json": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-public/virtual_collections/TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4/TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4_virtual_https.json",
        "s3_json": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-public/virtual_collections/TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4/TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4_virtual_s3.json",
        "s3_endpoint": "https://archive.podaac.earthdata.nasa.gov/s3credentials",
        "variable": "lwe_thickness",
        "slice_dims": {"time": slice(0, 10), "lat": slice(0, 10), "lon": slice(0, 10)},
    },
    "CCMP_WINDS_10M6HR_L4_V3.1": {
        "https_json": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-public/virtual_collections/CCMP_WINDS_10M6HR_L4_V3.1/CCMP_WINDS_10M6HR_L4_V3.1_virtual_https.json",
        "s3_json": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-public/virtual_collections/CCMP_WINDS_10M6HR_L4_V3.1/CCMP_WINDS_10M6HR_L4_V3.1_virtual_s3.json",
        "s3_endpoint": "https://archive.podaac.earthdata.nasa.gov/s3credentials",
        "variable": "uwnd",
        "slice_dims": {"time": slice(0, 10), "latitude": slice(0, 10), "longitude": slice(0, 10)},
        "fallback_dims": {"time": slice(0, 10), "lat": slice(0, 10), "lon": slice(0, 10)},
    },
    "SWOT_L2_LR_SSH_Basic_D": {
        "https_json": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-public/virtual_collections/SWOT_L2_LR_SSH_Basic_D/SWOT_L2_LR_SSH_Basic_D_virtual_https.json",
        "s3_json": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-public/virtual_collections/SWOT_L2_LR_SSH_Basic_D/SWOT_L2_LR_SSH_Basic_D_virtual_s3.json",
        "s3_endpoint": "https://archive.swot.podaac.earthdata.nasa.gov/s3credentials",
        "variable": ["ssha_karin", "ssha"],
        "slice_dims": "auto",
    },
}


def get_storage_options(collection, protocol, https_fs, auth=None):
    if protocol == "https":
        json_url = collection["https_json"]
    else:
        json_url = collection["s3_json"]

    options = {
        "fo": json_url,
        "target_protocol": "https",
        "target_options": {**https_fs.storage_options, "asynchronous": False},
        "remote_protocol": protocol,
        "asynchronous": False,
    }

    if protocol == "s3":
        creds = auth.get_s3_credentials(endpoint=collection["s3_endpoint"])
        options["remote_options"] = {
            "key": creds["accessKeyId"],
            "secret": creds["secretAccessKey"],
            "token": creds["sessionToken"],
            "asynchronous": False,
        }
    else:
        options["remote_options"] = {**https_fs.storage_options, "asynchronous": False}

    return options


def resolve_variable(ds, variable_config):
    if isinstance(variable_config, list):
        for var in variable_config:
            if var in ds:
                return var
        raise KeyError(f"None of {variable_config} found in dataset")
    return variable_config


def resolve_slice_dims(ds, variable, slice_config):
    if slice_config == "auto":
        return {dim: slice(0, 10) for dim in ds[variable].dims}
    return slice_config


def run_test(short_name, collection, protocol):
    print(f"\n{'='*60}")
    print(f"Testing: {short_name}")
    print(f"Protocol: {protocol.upper()}")
    print(f"{'='*60}")

    print("\nAuthenticating...")
    auth = earthaccess.login()
    https_fs = earthaccess.get_fsspec_https_session()

    storage_options = get_storage_options(collection, protocol, https_fs, auth)

    print(f"Opening dataset metadata via {protocol.upper()}...")
    ds = xr.open_dataset(
        "reference://",
        engine="zarr",
        backend_kwargs={
            "consolidated": False,
            "storage_options": storage_options,
        },
        zarr_format=2,
    )
    print("\n✅ Metadata loaded successfully!")
    print(ds)

    variable = resolve_variable(ds, collection["variable"])
    slice_dims = resolve_slice_dims(ds, variable, collection["slice_dims"])

    print(f"\nFetching data slice from '{variable}'...")
    try:
        data_slice = ds[variable].isel(**slice_dims).values
        print(f"\n✅ Data fetched successfully!")
        print(data_slice)
    except KeyError:
        if "fallback_dims" in collection:
            fallback = collection["fallback_dims"]
            data_slice = ds[variable].isel(**fallback).values
            print(f"\n✅ Data fetched successfully (using fallback dimensions)!")
            print(data_slice)
        else:
            raise

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run VDS tests against PO.DAAC virtual collections"
    )
    parser.add_argument(
        "collections",
        nargs="*",
        help="Collection short names to test (default: interactive selection)",
    )
    parser.add_argument(
        "--protocol", "-p",
        choices=["https", "s3", "both"],
        default="both",
        help="Protocol to test (default: both)",
    )
    parser.add_argument(
        "--list", "-l", action="store_true", help="List available collections"
    )

    args = parser.parse_args()

    if args.list:
        print("Available collections:")
        for short_name in COLLECTIONS:
            print(f"  {short_name}")
        return

    if not args.collections:
        print("Available collections:")
        keys = list(COLLECTIONS.keys())
        for i, short_name in enumerate(keys, 1):
            print(f"  {i}. {short_name}")
        print(f"  {len(keys) + 1}. all")
        print()

        selection = input("Enter collection names or numbers (comma-separated): ").strip()
        if not selection:
            print("No selection made. Exiting.")
            return

        selected = []
        for item in selection.split(","):
            item = item.strip()
            if item == "all" or item == str(len(keys) + 1):
                selected = keys
                break
            elif item.isdigit():
                idx = int(item) - 1
                if 0 <= idx < len(keys):
                    selected.append(keys[idx])
                else:
                    print(f"Invalid number: {item}")
                    return
            elif item in COLLECTIONS:
                selected.append(item)
            else:
                print(f"Unknown collection: {item}")
                return
    else:
        selected = []
        for name in args.collections:
            if name == "all":
                selected = list(COLLECTIONS.keys())
                break
            elif name in COLLECTIONS:
                selected.append(name)
            else:
                print(f"Unknown collection: {name}")
                return

    protocols = ["https", "s3"] if args.protocol == "both" else [args.protocol]

    results = {}
    for short_name in selected:
        for protocol in protocols:
            key = f"{short_name} ({protocol})"
            try:
                run_test(short_name, COLLECTIONS[short_name], protocol)
                results[key] = "✅ PASSED"
            except Exception as e:
                print(f"\n❌ FAILED: {e}")
                results[key] = f"❌ FAILED: {e}"

    if len(results) > 1:
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        for key, result in results.items():
            print(f"  {key:<50} {result}")


if __name__ == "__main__":
    main()
