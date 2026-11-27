"""Recheck the M01 release inventory and its parent-grid metadata on Gadi."""

import hashlib
import json
import platform
import re
from pathlib import Path

import netCDF4 as nc
import numpy as np

PUBLICATION = Path("/g/data/im55/publications/MIP-DRS7/CMIP7")
ARCHIVE = Path("/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production")
MODEL = "ACCESS-ESM1-6"
MEMBER = "r1i1p1f1"
GRID = "g118"
LINES = ("fram_strait", "canadian_archipelago", "barents_opening", "bering_strait")
POINTS = ((0, 0), (60, 120), (240, 90), (270, 240), (299, 359))
GRID_SHAPE = (300, 360)
INVENTORY_SHA256 = "ceb2545c394f115d098c58bc1823245ffd0e92fa86996d1e2de285ef6b899a27"
MASS_TRANSPORT_RUNS = (
    ("C4MIP", "1pctCO2-bgc", "1pctCO2-bgc-01", "0201", "v20261019", 17),
    ("C4MIP", "1pctCO2-rad", "1pctCO2-rad-01", "0201", "v20261019", 17),
    ("C4MIP", "esm-flat10", "esm-flat10", "0201", "v20261019", 31),
    (
        "C4MIP",
        "esm-flat10-cdr",
        "esm-flat10-cdr/flat10-cdr-01-esm-flat10-cdr-2026-06-02-05321bca",
        "0301",
        "v20261019",
        31,
    ),
    ("CMIP", "1pctCO2", "1pctCO2-01-original", "0201", "v20261019", 17),
    ("CMIP", "abrupt-4xCO2", "abrupt-4xCO2", "0201", "v20261025", 33),
)
CONTROL_RUNS = (
    ("esm-piControl", "Ndep2-PI-CNP-emissions", "output100", "v20261006", 102),
    ("piControl", "Ndep2-PI-CNP-concentrations", "output000", "v20261001", 103),
)
CONTROL_PRODUCTS = (
    ("siareaacrossline", "glb", "ht"),
    ("siarea", "nh", "hm"),
    ("siarea", "sh", "hm"),
    ("siextent", "nh", "hm"),
    ("siextent", "sh", "hm"),
)


def publication_member(activity, experiment):
    return PUBLICATION / activity / "ACCESS-Consortium" / MODEL / experiment / MEMBER


def check_attrs(dataset, **expected):
    actual = {name: dataset.getncattr(name) for name in expected}
    assert actual == expected, (dataset.filepath(), actual, expected)


def check_parent(member, native, frequency, version):
    directory = member / "glb" / frequency / "siconc/tavg-u-hxy-u" / GRID / version
    published = sorted(directory.glob("*.nc"))[0]
    with nc.Dataset(native, "r") as source, nc.Dataset(published, "r") as parent:
        check_attrs(parent, grid_label=GRID, horizontal_label="hxy", region="glb")
        assert source["aice"].dimensions == ("time", "nj", "ni"), native
        assert source["aice"].shape[-2:] == GRID_SHAPE, native
        assert parent["siconc"].dimensions == ("time", "j", "i"), published
        assert parent["siconc"].shape[-2:] == GRID_SHAPE, published
        differences = {}
        for native_name, reported_name in (("TLAT", "latitude"), ("TLON", "longitude")):
            ncoord, pcoord = source[native_name], parent[reported_name]
            assert (
                ncoord.dimensions == ("nj", "ni") and ncoord.shape == GRID_SHAPE
            ), native
            assert (
                pcoord.dimensions == ("j", "i") and pcoord.shape == GRID_SHAPE
            ), published
            assert ncoord.units == pcoord.units, (native, published, reported_name)
            delta = np.array(
                [float(pcoord[j, i]) - float(ncoord[j, i]) for j, i in POINTS]
            )
            if reported_name == "longitude":
                delta = (delta + 180) % 360 - 180
            np.testing.assert_allclose(
                delta, 0, rtol=0, atol=1e-4, err_msg=str(published)
            )
            differences[reported_name] = float(np.abs(delta).max())
    return {
        "native_cice_file": str(native),
        "published_siconc_file": str(published),
        "grid_label": GRID,
        "horizontal_shape": GRID_SHAPE,
        "max_abs_probe_difference_degrees": differences,
    }


def check_reduced(member, product, version, file_count):
    variable, region, horizontal = product
    branding = f"tavg-u-{horizontal}-u"
    directory = member / region / "mon" / variable / branding / GRID / version
    files = sorted(directory.glob("*.nc"))
    assert len(files) == file_count, (directory, len(files), file_count)
    experiment = member.parent.name
    prefix = f"{variable}_{branding}_mon_{region}_{GRID}_{MODEL}_{experiment}_{MEMBER}_"
    pattern = re.compile(rf"{re.escape(prefix)}\d{{6}}-\d{{6}}\.nc")
    for path in files:
        assert pattern.fullmatch(path.name) and path.is_file(), path

    sample = files[0]
    with nc.Dataset(sample, "r") as dataset:
        check_attrs(
            dataset,
            source_id=MODEL,
            experiment_id=experiment,
            variant_label=MEMBER,
            variable_id=variable,
            region=region,
            frequency="mon",
            branding_suffix=branding,
            horizontal_label=horizontal,
            grid_label=GRID,
            nominal_resolution="100 km",
            access_moppy_version="1.9.1",
        )
        field = dataset[variable]
        dimensions = field.dimensions
        labels = ()
        if horizontal == "hm":
            assert dimensions == ("time",) and set(dataset.dimensions) == {
                "time",
                "bnds",
            }, sample
        else:
            assert dimensions == ("time", "line"), sample
            assert set(dataset.dimensions) == {"time", "line", "strlen", "bnds"}, sample
            assert field.coordinates == "line", sample
            line = dataset["line"]
            assert (
                line.dimensions == ("line", "strlen") and line.standard_name == "region"
            ), sample
            line.set_auto_mask(False)
            labels = tuple(nc.chartostring(line[:]).tolist())
            assert labels == LINES, (sample, labels)
    return files, {
        "sample_file": str(sample),
        "file_count": file_count,
        "horizontal_label": horizontal,
        "region": region,
        "grid_label": GRID,
        "dimensions": dimensions,
        "line_labels": labels,
    }


def main():
    parents, reduced, paths = {}, [], []
    for activity, experiment, run, year, parent_version, count in MASS_TRANSPORT_RUNS:
        member = publication_member(activity, experiment)
        native = ARCHIVE / run / f"output000/ice/iceh-1daily-mean_{year}-01.nc"
        parents[experiment] = check_parent(member, native, "day", parent_version)
        files, sample = check_reduced(
            member, ("simassacrossline", "glb", "ht"), "v20261119", count
        )
        reduced.append(sample)
        paths.extend(files)
    for experiment, run, output, parent_version, count in CONTROL_RUNS:
        member = publication_member("CMIP", experiment)
        native = ARCHIVE / run / output / "ice/iceh-1monthly-mean_0101-01.nc"
        parents[experiment] = check_parent(member, native, "mon", parent_version)
        for product in CONTROL_PRODUCTS:
            files, sample = check_reduced(member, product, "v20261117", count)
            reduced.append(sample)
            paths.extend(files)

    ordered_names = sorted(map(str, paths))
    digest = hashlib.sha256(("\n".join(ordered_names) + "\n").encode()).hexdigest()
    assert len(paths) == len(set(paths)) == 1171
    assert digest == INVENTORY_SHA256, digest
    print(
        json.dumps(
            {
                "dataset_versions_sampled": len(reduced),
                "file_paths_verified": len(paths),
                "file_list_sha256": digest,
                "python_version": platform.python_version(),
                "netcdf4_version": nc.__version__,
                "coordinate_sample_indices_zero_based_j_i": POINTS,
                "parent_grid_checks": parents,
                "reduced_product_samples": reduced,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
