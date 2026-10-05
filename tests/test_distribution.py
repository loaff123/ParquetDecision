"""Public release content contracts; external installed suites are recorded too."""
from pathlib import Path
import importlib.resources as resources
import json


def test_distribution_identity():
    root = Path(__file__).resolve().parents[1]
    assert (root / 'LICENSE').is_file(), 'distribution license is missing'
    assert (root / 'MANIFEST.in').is_file()
    for name in ['README.md', 'docs/profile.md', 'docs/limitations.md', 'docs/qualification.md']:
        assert (root / name).is_file(), name
    docs = resources.files('parquet_decision').joinpath('docs')
    assert docs.joinpath('README.md').is_file()
    assert resources.files('parquet_decision').joinpath('examples.py').is_file()
    assert resources.files('parquet_decision').joinpath('__main__.py').is_file()
    text = (root / 'pyproject.toml').read_text()
    assert '3.12.14' in text and 'license' in text
    assert 'pythonpath' not in text, 'installed tests must not redirect imports to src'


def test_public_frozen_evidence_preserves_forty_original_outcomes():
    root = Path(__file__).resolve().parents[1]
    evidence = root / 'evidence' / 'baselines'
    assert evidence.is_dir(), 'public original baseline packet missing'
    summary = json.loads((evidence / 'summary.json').read_text())
    assert summary
    assert len(list((root / 'examples' / 'original-fixtures').rglob('*.parquet'))) == 22


def relative_links(path):
    import re
    from urllib.parse import urlsplit
    text = path.read_text()
    return [target.split('#', 1)[0] for target in re.findall(r'\[[^]]*\]\(([^)]+)\)', text)
            if not urlsplit(target).scheme and not target.startswith('#')]


def test_all_source_and_installed_document_relative_links_resolve():
    root = Path(__file__).resolve().parents[1]
    installed = resources.files('parquet_decision').joinpath('docs')
    roots = [root / 'README.md', *sorted((root / 'docs').glob('*.md')),
             Path(str(installed.joinpath('README.md'))), *sorted(Path(str(installed)).glob('*.md'))]
    checked = []
    for document in roots:
        for link in relative_links(document):
            target = document.parent / link
            assert target.is_file(), f'{document.name}: unresolved relative link {link}'
            checked.append((document.name, link))
    assert len(relative_links(root / 'README.md')) == 7
    assert len(relative_links(Path(str(installed.joinpath('README.md'))))) == 7
    assert len(checked) >= 14


def test_installed_linked_baselines_preserve_forty_original_outcomes():
    import hashlib
    installed = resources.files('parquet_decision').joinpath('docs', 'evidence', 'baselines')
    root = Path(__file__).resolve().parents[1] / 'evidence' / 'baselines'
    for name in ['README.md', 'original-comparison.json', 'explicit-arrow-comparison.json', 'summary.json']:
        assert installed.joinpath(name).is_file(), 'missing linked installed baseline: ' + name
        assert installed.joinpath(name).read_bytes() == (root / name).read_bytes()
    summary = json.loads(installed.joinpath('summary.json').read_text())
    assert summary['outcome_count'] == 40
    for name, expected in summary['files'].items():
        assert hashlib.sha256(installed.joinpath(name).read_bytes()).hexdigest() == expected
