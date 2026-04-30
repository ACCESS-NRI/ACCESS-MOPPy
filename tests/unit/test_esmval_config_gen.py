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
    def test_creates_file_at_explicit_location(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "subdir" / "config.yml"
        result = write_esmval_config(cache, output_path=out)
        assert result == out
        assert out.exists()

    def test_config_content_has_projects_cmip6(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "config.yml"
        write_esmval_config(cache, output_path=out)
        data = yaml.safe_load(out.read_text())
        assert "projects" in data
        assert "CMIP6" in data["projects"]
        assert "data" in data["projects"]["CMIP6"]

    def test_config_content_moppy_cache_source(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "config.yml"
        write_esmval_config(cache, output_path=out)
        data = yaml.safe_load(out.read_text())
        sources = data["projects"]["CMIP6"]["data"]
        assert "moppy-cache" in sources
        src = sources["moppy-cache"]
        assert src["type"] == "esmvalcore.io.local.LocalDataSource"
        assert src["rootpath"] == str(cache.resolve())

    def test_config_content_dirname_template(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "config.yml"
        write_esmval_config(cache, output_path=out)
        data = yaml.safe_load(out.read_text())
        src = data["projects"]["CMIP6"]["data"]["moppy-cache"]
        assert "{project}" in src["dirname_template"]
        assert "{mip}" in src["dirname_template"]
        assert "{version}" in src["dirname_template"]

    def test_config_content_filename_template(self, tmp_path):
        cache = tmp_path / "cache"
        out = tmp_path / "config.yml"
        write_esmval_config(cache, output_path=out)
        data = yaml.safe_load(out.read_text())
        src = data["projects"]["CMIP6"]["data"]["moppy-cache"]
        assert "{short_name}" in src["filename_template"]
        assert "{grid}" in src["filename_template"]

    def test_extra_rootpaths_included(self, tmp_path):
        cache = tmp_path / "cache"
        extra = tmp_path / "extra"
        out = tmp_path / "config.yml"
        write_esmval_config(cache, output_path=out, extra_rootpaths=[extra])
        data = yaml.safe_load(out.read_text())
        sources = data["projects"]["CMIP6"]["data"]
        assert "moppy-cache" in sources
        assert "extra-0" in sources
        assert sources["extra-0"]["rootpath"] == str(extra.resolve())

    def test_tilde_expansion(self, tmp_path):
        """~ in cache_dir should be expanded in the rootpath."""
        out = tmp_path / "config.yml"
        write_esmval_config("~/fake_cache_xyz", output_path=out)
        data = yaml.safe_load(out.read_text())
        rootpath = data["projects"]["CMIP6"]["data"]["moppy-cache"]["rootpath"]
        assert "~" not in rootpath

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
        cfg.write_text("projects:\n  CMIP6:\n    data: {}\n")
        result = load_existing_config(cfg)
        assert isinstance(result, dict)
        assert "projects" in result

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
    def test_writes_data_source_file_next_to_base(self, tmp_path):
        base = tmp_path / "existing-config.yml"
        base.write_text("# existing config\n")
        cache = tmp_path / "cache"
        out = tmp_path / "moppy-esmval-data.yml"
        result = merge_into_existing_config(cache, base, output_path=out)
        assert result == out
        assert out.exists()

    def test_written_file_has_correct_rootpath(self, tmp_path):
        base = tmp_path / "any-config.yml"
        base.write_text("")
        cache = tmp_path / "cache"
        out = tmp_path / "merged.yml"
        merge_into_existing_config(cache, base, output_path=out)
        data = yaml.safe_load(out.read_text())
        assert data["projects"]["CMIP6"]["data"]["moppy-cache"]["rootpath"] == str(
            cache.resolve()
        )

    def test_does_not_modify_base_config(self, tmp_path):
        base = tmp_path / "config-user.yml"
        original_content = "# my config\n"
        base.write_text(original_content)
        cache = tmp_path / "cache"
        out = tmp_path / "merged.yml"
        merge_into_existing_config(cache, base, output_path=out)
        assert base.read_text() == original_content

    def test_default_output_path_is_next_to_base(self, tmp_path):
        base = tmp_path / "my-config.yml"
        base.write_text("")
        cache = tmp_path / "cache"
        result = merge_into_existing_config(cache, base)
        assert result == tmp_path / DEFAULT_CONFIG_FILENAME
        assert result.exists()

    def test_returns_path_object(self, tmp_path):
        base = tmp_path / "config.yml"
        base.write_text("")
        out = tmp_path / "out.yml"
        result = merge_into_existing_config(tmp_path / "cache", base, output_path=out)
        assert isinstance(result, Path)
