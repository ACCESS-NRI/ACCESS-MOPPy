# Time chunking and `split_years`

CMIP archives split long time series into several files rather than one file
per variable per experiment, mainly so that individual downloads stay a
manageable size (roughly 2–20 GB) and users can fetch only the years they
need. ACCESS-MOPPy reproduces this convention automatically through the
`split_years` parameter, rather than leaving each caller to slice the time
axis by hand. This page explains the policy behind the defaults and how the
splitting is actually carried out in `CMORiser.write()`
(`src/access_moppy/base.py`).

## The `split_years` parameter

`split_years` accepts three kinds of value:

- `"auto"` (the default) — the number of years per file is looked up from
  `DEFAULT_CHUNK_YEARS` (`src/access_moppy/defaults.py`), keyed by the
  variable's canonical CMIP frequency:

  | Frequency | Years per file |
  |---|---|
  | `1hr`, `3hr`, `6hr` | 1 |
  | `day` | 5 |
  | `mon` | 10 |
  | `yr`, `fx` | no split (single file) |

- a positive integer — overrides the default for every frequency, e.g.
  `split_years=1` always writes one file per calendar year.
- `None` — disables splitting entirely; the whole time series is written to
  a single file.

`fx` (time-independent) variables, and any dataset without a `time`
dimension, are always written as a single file regardless of `split_years` —
there is no time axis to chunk.

## How chunk boundaries are chosen

`_resolve_split_years()` first turns `split_years` into a concrete year
count (or `None`) using the table above. `_iter_time_chunks()` then decodes
the year of every timestep (handling `cftime`/`datetime64` coordinates, and
falling back to `cftime.num2date` for undecoded numeric time axes) and
assigns each timestep to a chunk via:

```
chunk_id = floor(year / split_years) * split_years
```

This aligns chunk boundaries to calendar-year multiples of `split_years`
rather than to the first timestep in the dataset — e.g. with
`split_years=5`, chunks always fall on 1850–1854, 1855–1859, … regardless of
whether the run itself starts mid-chunk. Each unique `chunk_id` becomes one
`ds.isel(time=...)` slice, and `write()` calls `_write_single()` once per
slice. The filename's time-range component is derived from the actual first
and last timestamp written, so it is always correct even when a chunk is
shorter than `split_years` (e.g. the final, partial chunk of a run).

## Why splitting happens before the expensive part

Everything up to `write()` is lazy dask, so slicing the dataset by time is
cheap — the cost only appears when each chunk is actually computed and
serialised. This means `split_years` does not add a separate pass over the
data: each chunk is read, computed, and written independently, and a failure
partway through a long run only needs the remaining chunks re-run rather
than the whole series.

## Bounding memory during the write: `write_prefetch`

Splitting by year keeps individual *files* a reasonable size, but a single
chunk's data still has to pass through Dask before it is written. Within
`_write_single()` / `_write_dask_slices()`, the underlying array is sliced
further into bounded chunks (`DatasetChunker`, sized against
`max_chunk_size_mb`), and `write_prefetch` controls how many of those
bounded slices are allowed to be computed *ahead* of the serial NetCDF
writer — the actual `netCDF4` write call is single-threaded, but the next
slice's Dask computation can run concurrently while the current slice is
being written.

- Default `write_prefetch=4`: up to four computed-but-not-yet-written slices
  are held in distributed memory at once, keeping the writer fed.
- `write_prefetch=1` disables prefetching — each slice is computed, written,
  and released before the next one starts, minimising memory at the cost of
  worker idle time.

Batch CMORisation (see {doc}`/howto/batch_processing`) accounts for
`write_prefetch` and `max_chunk_size_mb` together when sizing Dask workers:
if the requested PBS memory cannot fit at least one worker large enough for
the resulting write window, the job fails fast before the cluster starts,
rather than running out of memory partway through.

## Related pages

- {doc}`/reference/configuration` — the `split_years` and `write_prefetch`
  parameter reference.
- {doc}`/howto/batch_processing` — how chunk size and prefetch feed into PBS
  worker sizing for production runs.
- {doc}`/explanation/architecture` — where `write()` sits in the overall
  CMORisation lifecycle.
