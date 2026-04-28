"""
Unit tests for access_moppy.esmval.config_gen
"""

from __future__ import annotations

from pathlib import Path

import yaml

from access_moppy.esmval.config_gen import (
    DEFAULT_CONFIG_FILENAME,
    load_existing_config,
    merge_into_existing_config,
    write_esmval_config,
)


class TestWriteEsmvalConfig:
    def test_creates_file_at_default_location(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cache = tmp_path / "my_cache"
        result = write_esmval_config(cache)
        assert result == tmp_path / DEFAULT_CONFIG_FILENAME
        assert result.exists()

    def test_creates_file_at_explicit_location(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "subdir" / "config.yml"
        result = write_esmval_config(cache, output_path=out)
        assert result == out
        assert out.exists()

    def test_config_content_rootpath(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "config.yml"
        write_esmval_config(cache, output_path=out)
        data = yaml.safe_load(out.read_text())
        assert "rootpath" in data
        assert "CMIP6" in data["rootpath"]
        assert str(cache.resolve()) in data["rootpath"]["CMIP6"]

    def test_config_content_drs(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "config.yml"
        write_esmval_config(cache, output_path=out)
        data = yaml.safe_load(out.read_text())
        assert data["drs"]["CMIP6"] == "CMIP6"

    def test_extra_rootpaths_included(self, tmp_path):
        cache = tmp_path / "cache"
        extra = tmp_path / "extra"
        out = tmp_path / "config.yml"
        write_esmval_config(cache, output_path=out, extra_rootpaths=[extra])
        data = yaml.safe_load(out.read_text())
        paths = data["rootpath"]["CMIP6"]
        assert str(cache.resolve()) in paths
        assert str(extra.resolve()) in paths

    def test_tilde_expansion(self, tmp_path):
        """~ in cache_dir should be expanded."""
        out = tmp_path / "config.yml"
        write_esmval_config("~/fake_cache_xyz", output_path=out)
        data = yaml.safe_load(out.read_text())
        paths = data["rootpath"]["CMIP6"]
        assert all("~" not in p for p in paths)

    def test_returns_path_object(self, tmp_path):
        out = tmp_path / "config.yml"
        result = write_esmval_config(tmp_path / "cache", output_path=out)
        assert isinstance(result, Path)

    def test_creates_parent_directories(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "deep" / "nested" / "config.yml"
        write_esmval_config(cache, output_path=out)
        assert out.exists()


class TestLoadExistingConfig:
    def test_returns_dict_for_valid_yaml(self, tmp_path):
        cfg = tmp_path / "config.yml"
        cfg.write_text("rootpath:\n  CMIP6:\n    - /some/path\n")
        result = load_existing_config(cfg)
        assert isinstance(result, dict)
        assert "rootpath" in result

    def test_returns_empty_dict_for_missing_file(self, tmp_path):
        result = load_existing_config(tmp_path / "nonexistent.yml")
        assert result == {}

    def test_returns_empty_dict_for_yaml_list(self, tmp_path):
        cfg = tmp_path / "list.yml"
        cfg.write_text("- item1\n- item2\n")
        result = load_existing_config(cfg)
        assert result == {}

    def test_returns_empty_dict_for_empty_file(self, tmp_path):
        cfg = tmp_path / "empty.yml"
        cfg.write_text("")
        result = load_existing_config(cfg)
        assert result == {}


class TestMergeIntoExistingConfig:
    def test_prepends_cache_to_existing_cmip6(self, tmp_path):
        base = tmp_path / "config-user.yml"
        base.write_text("rootpath:\n  CMIP6:\n    - /existing/data\n")
        cache = tmp_path / "cache"
        out = tmp_path / "merged.yml"
        merge_into_existing_config(cache, base, output_path=out)

        data = yaml.safe_load(out.read_text())
        paths = data["rootpath"]["CMIP6"]
        assert paths[0] == str(cache.resolve())
        assert "/existing/data" in paths

    def test_creates_cmip6_key_when_absent(self, tmp_path):
        base = tmp_path / "config-user.yml"
        base.write_text("rootpath:\n  OBS: /some/obs\n")
        cache = tmp_path / "cache"
        out = tmp_path / "merged.yml"
        merge_into_existing_config(cache, base, output_path=out)

        data = yaml.safe_load(out.read_text())
        assert "CMIP6" in data["rootpath"]
        assert str(cache.resolve()) in data["rootpath"]["CMIP6"]

    def test_does_not_modify_base_config(self, tmp_path):
        base = tmp_path / "config-user.yml"
        original_content = "rootpath:\n  CMIP6:\n    - /original\n"
        base.write_text(original_content)
        cache = tmp_path / "cache"
        out = tmp_path / "merged.yml"
        merge_into_existing_config(cache, base, output_path=out)
        assert base.read_text() == original_content

    def test_missing_base_config_treated_as_empty(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "merged.yml"
        merge_into_existing_config(cache, tmp_path / "nonexistent.yml", output_path=out)
        data = yaml.safe_load(out.read_text())
        assert str(cache.resolve()) in data["rootpath"]["CMIP6"]

    def test_returns_path_object(self, tmp_path):
        base = tmp_path / "config.yml"
        base.write_text("")
        out = tmp_path / "out.yml"
        result = merge_into_existing_config(tmp_path / "cache", base, output_path=out)
        assert isinstance(result, Path)
