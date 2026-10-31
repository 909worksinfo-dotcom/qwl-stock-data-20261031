"""Immutable per-series successes, private weekly workers only"""
import hashlib
import json
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate(frame, ref, as_of, basis):
    from .calendar import latest_expected_session
    required = ['open', 'high', 'low', 'close', 'volume']
    if (frame is None or frame.empty or len(frame) < 60 or frame.index.has_duplicates
            or not frame.index.is_monotonic_increasing or not set(required) <= set(frame.columns)
            or frame.index.max().date() != latest_expected_session(ref.market, as_of)
            or frame.index.max().date() > as_of or frame.attrs.get('price_basis') != basis):
        return False
    import numpy as np
    values = frame[required].to_numpy(dtype=float)
    return bool(np.isfinite(values).all() and (values[:, :4] > 0).all()
        and (values[:, 4] >= 0).all() and (frame.high >= frame[['open','low','close']].max(axis=1)).all()
        and (frame.low <= frame[['open','high','close']].min(axis=1)).all())


def collect(root, ref, as_of, basis, signature, fetch):
    root = Path(root)
    directory = root / ref.symbol.replace('.', '_') / basis
    receipt = directory / 'receipt.json'
    if receipt.exists():
        try:
            value = json.loads(receipt.read_text())
        except (ValueError, OSError) as exc:
            raise ValueError('SERIES_INTEGRITY: invalid receipt') from exc
        sources = ({'tencent_kline_raw', 'akshare_tx_raw', 'akshare_hk_raw'} if basis == 'none' else
            {'tencent_kline_qfq', 'akshare_tx_qfq', 'akshare_hk_qfq', 'akshare_hk_sina_qfq'})
        name = value.get('data_file', '')
        if Path(name).name != name or not name.startswith('bars-') or not name.endswith('.parquet'):
            raise ValueError('SERIES_INTEGRITY: invalid member path')
        data = directory / name
        if (value.get('symbol') != ref.symbol or value.get('as_of') != as_of.isoformat()
                or value.get('basis') != basis or value.get('collector_sha256') != signature
                or value.get('source') not in sources
                or data.is_symlink() or not data.is_file() or digest(data) != value.get('sha256')):
            raise ValueError('SERIES_INTEGRITY: scope or SHA drift')
        frame = pd.read_parquet(data)
        if not validate(frame, ref, as_of, basis):
            raise ValueError('SERIES_INTEGRITY: invalid successful source')
        return frame, value['source']
    frame, source = fetch()
    if validate(frame, ref, as_of, basis) and source != 'none':
        directory.mkdir(parents=True, exist_ok=True)
        data = directory / ('bars-' + uuid.uuid4().hex + '.parquet')
        with data.open('xb') as handle:
            frame.to_parquet(handle)
            handle.flush(); os.fsync(handle.fileno())
        temporary = directory / ('.receipt-' + uuid.uuid4().hex + '.tmp')
        with temporary.open('x') as handle:
            json.dump({'symbol': ref.symbol, 'as_of': as_of.isoformat(), 'basis': basis,
                'source': source, 'data_file': data.name, 'sha256': digest(data), 'collector_sha256': signature,
                'observed_at': datetime.now(timezone.utc).isoformat()}, handle)
            handle.flush(); os.fsync(handle.fileno())
        os.link(temporary, receipt)
        temporary.unlink()
    return frame, source
