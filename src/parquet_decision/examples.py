"""Generate the original combined synthetic newcomer fixture, never runtime code."""
from pathlib import Path
import argparse


def create_sources(root: Path) -> None:
    """Create a new source directory with two original producers and two conflicts."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    root.mkdir()  # No overwrite or reuse of an existing directory.
    tick = pa.timestamp('ns', tz='UTC')
    legacy = pa.schema([
        ('device', pa.int64()), ('tick', tick),
        ('payload', pa.struct([pa.field('distance', pa.int32(), metadata={b'unit': b'm'})]))
    ], metadata={b'producer': b'legacy'})
    new = pa.schema([
        ('device', pa.uint64()), ('tick', tick),
        ('payload', pa.struct([pa.field('distance', pa.int64(), metadata={b'unit': b'cm'}),
                               pa.field('status', pa.string())])), ('note', pa.string())
    ], metadata={b'producer': b'new'})
    rows = [[{'device': -1, 'tick': 1700000000000000001, 'payload': None},
             {'device': 42, 'tick': 1700000000000000999, 'payload': {'distance': 3}}],
            [{'device': 2**64 - 1, 'tick': 1700000000000001001,
              'payload': {'distance': 125, 'status': 'ok'}, 'note': 'new sensor'}]]
    for name, schema, data in zip(['legacy.parquet', 'new.parquet'], [legacy, new], rows, strict=True):
        with (root / name).open('xb') as output:
            pq.write_table(pa.Table.from_pylist(data, schema=schema), output, version='2.6', row_group_size=2)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    try:
        create_sources(args.destination)
    except FileExistsError:
        print(f'CONFLICT: example destination already exists; preserved: {args.destination}; choose a new directory')
        return 1
    print('Created original synthetic legacy/new sources; inspect both metadata conflicts explicitly.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
