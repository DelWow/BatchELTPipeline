import tomllib
from pathlib import Path
from urllib.parse import urlparse

REGISTRY_PATH = Path(__file__).resolve().parents[2] / "config/sources.toml"


def load_registry() -> dict:
    with REGISTRY_PATH.open("rb") as registry_file:
        return tomllib.load(registry_file)


def test_source_ids_and_product_ids_are_unique() -> None:
    sources = load_registry()["sources"]

    assert len({source["id"] for source in sources}) == len(sources)
    assert len({source["product_id"] for source in sources}) == len(sources)


def test_profiles_reference_only_configured_sources() -> None:
    registry = load_registry()
    source_ids = {source["id"] for source in registry["sources"]}

    for profile in registry["profiles"].values():
        assert set(profile["source_ids"]) <= source_ids


def test_source_urls_and_archive_members_match_product_id() -> None:
    for source in load_registry()["sources"]:
        product_id = source["product_id"]
        download_url = urlparse(source["expected_download_url"])

        assert download_url.scheme == "https"
        assert download_url.hostname == "www150.statcan.gc.ca"
        assert download_url.path.endswith(f"/{product_id}-eng.zip")
        assert source["data_member"] == f"{product_id}.csv"
        assert source["metadata_member"] == f"{product_id}_MetaData.csv"


def test_source_windows_are_ordered_and_intersect_full_profile() -> None:
    registry = load_registry()
    full_profile = registry["profiles"]["full"]

    for source in registry["sources"]:
        assert source["availability_start"] <= source["baseline_start"]
        assert source["baseline_start"] <= source["baseline_end"]
        assert source["baseline_start"] <= full_profile["reference_end"]
        assert source["baseline_end"] >= full_profile["reference_start"]
