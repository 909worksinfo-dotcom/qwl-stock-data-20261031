"""Exact cross-provider margin comparison; never confuse missing values with zero."""
from datetime import date
from decimal import Decimal, InvalidOperation

SSE_FIELDS = frozenset({'margin_balance', 'margin_buy', 'margin_repay', 'short_volume', 'short_sell_volume'})
FULL_FIELDS = SSE_FIELDS | {'margin_net_buy', 'short_balance', 'short_repay_volume',
                            'short_net_sell_volume', 'margin_short_balance', 'float_market_cap', 'margin_balance_ratio'}

def day(value):
    text = str(value or '').strip().replace('/', '-')
    if len(text) == 8 and text.isdigit():
        text = f'{text[:4]}-{text[4:6]}-{text[6:]}'
    return date.fromisoformat(text).isoformat()

def same_number(left, right):
    if left is None or right is None or isinstance(left, bool) or isinstance(right, bool):
        return left is None and right is None
    try:
        a, b = Decimal(str(left)), Decimal(str(right))
        return a.is_finite() and b.is_finite() and a == b
    except InvalidOperation:
        return False

def compatible_subset(previous, candidate):
    """Only documented SSE daily fields may be absent; extra/PIT/unit changes fail."""
    if set(candidate) != SSE_FIELDS | {'date'}:
        return False
    try:
        return (day(previous['date']) == day(candidate['date'])
                and all(candidate[k] is not None and k in previous
                        and same_number(previous[k], candidate[k]) for k in SSE_FIELDS))
    except (KeyError, ValueError):
        return False

def current_shape_complete(records, expected):
    start = expected.fromordinal(expected.toordinal() - expected.weekday()).isoformat()
    try:
        current = [r for r in records if start <= day(r.get('date')) <= expected.isoformat()]
        return bool(current) and all(FULL_FIELDS <= r.keys() for r in current)
    except ValueError:
        return False

def _same_fields(left, right):
    common = (set(left) & set(right)) - {'date'}
    return all(same_number(left[k], right[k]) if k in FULL_FIELDS
               else left[k] == right[k] for k in common)

def _date_alias_equivalent(left, right):
    """Recognize only exact-shape or documented SSE/full aliases as one day."""
    if day(left.get('date')) != day(right.get('date')):
        return False
    if compatible_subset(left, right) or compatible_subset(right, left):
        return True
    return set(left) == set(right) and _same_fields(left, right)

def merge_working_records(existing, incoming, *, enrich=False):
    """Working cache only: retain richer rows, record real discrepancies separately."""
    rows = []
    positions = {}
    for source in existing:
        row = dict(source)
        key = day(row['date'])
        if key in positions:
            index = positions[key]
            previous = rows[index]
            if not _date_alias_equivalent(previous, row):
                raise ValueError(f'duplicate margin date conflict: {key}')
            if len(row) > len(previous):
                rows[index] = row
            continue
        positions[key] = len(rows)
        rows.append(row)
    conflicts, appended = [], 0
    for row in incoming:
        key = day(row['date'])
        if key not in positions:
            positions[key] = len(rows)
            rows.append({**row, 'date': key})
            appended += 1
            continue
        previous = rows[positions[key]]
        if compatible_subset(previous, row):
            continue
        equivalent = _same_fields(previous, row)
        if equivalent and set(previous) <= set(row) and enrich:
            rows[positions[key]] = {**previous, **{k:v for k,v in row.items() if k not in previous}, 'date': key}
        elif not (equivalent and set(previous) == set(row)):
            conflicts.append({'date': key, 'previous': previous, 'candidate': row})
    return sorted(rows, key=lambda r: day(r['date'])), conflicts, appended
