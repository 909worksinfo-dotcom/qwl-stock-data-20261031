"""Typed planning identities. Legacy formal replay retains its original semantics."""
import json
from datetime import date
from urllib.parse import parse_qs, urlsplit, urlunsplit

def announcement_identity(record):
    identity = next((str(record[k]) for k in ('announcement_id', 'notice_id', 'id') if record.get(k)), None)
    if identity:
        return identity
    url = urlsplit(str(record.get('url', '')).strip())
    if url.scheme not in {'http', 'https'} or not url.hostname:
        return None
    query = parse_qs(url.query)
    if url.hostname in {'www.cninfo.com.cn', 'cninfo.com.cn'}:
        values = query.get('announcementId', [])
        return values[0] if len(values) == 1 and values[0].isdigit() else None
    if url.hostname == 'data.eastmoney.com':
        import re
        match = re.fullmatch(r'/notices/detail/\d+/(AN\d+)\.html', url.path)
        return match.group(1) if match else None
    return None

def normalized_day(value):
    text = str(value or '').strip().replace('/', '-')
    if len(text) == 8 and text.isdigit():
        text = f'{text[:4]}-{text[4:6]}-{text[6:]}'
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return value  # Do not truncate a timestamp or repair an invalid date

def business_key(record, relative, fallback):
    domain = relative.parts[1] if len(relative.parts) > 1 else ''
    if domain == 'earnings_calendar' and record.get('event') == 'announcement_evidence':
        identity = announcement_identity(record)
        if identity:
            return json.dumps([domain, 'announcement_evidence', identity], ensure_ascii=False)
    if domain == 'hk_short_selling' and 'reportable_short_shares' in record:
        return json.dumps([domain, 'position', record.get('date')], ensure_ascii=False)
    if domain == 'announcements':
        identity = next((str(record[k]) for k in ('announcement_id', 'notice_id', 'id')
                         if record.get(k)), None)
        if identity is None and record.get('url'):
            url = urlsplit(str(record['url']).strip())
            if url.scheme in {'https', 'http'} and url.hostname:
                identity = urlunsplit((url.scheme.lower(), url.netloc.lower(),
                                       url.path, url.query, ''))
        if identity:
            # One issuer may publish multiple documents with the same generic
            # title on the same day.  The exchange document identity, not the
            # display title, distinguishes those valid announcements.
            return json.dumps([domain, identity], ensure_ascii=False)
    if domain == 'hk_action_evidence':
        identity = next((str(record[k]) for k in ('announcement_id', 'notice_id', 'id')
                         if record.get(k)), None)
        if identity is None and record.get('url'):
            url = urlsplit(str(record['url']).strip())
            if url.scheme in {'https', 'http'} and url.hostname:
                # Preserve query: it may carry a real announcement ID
                identity = urlunsplit((url.scheme.lower(), url.netloc.lower(), url.path, url.query, ''))
        if identity:
            return json.dumps([domain, identity, record.get('action_type'), record.get('phase')], ensure_ascii=False)
    return fallback(record)

def classification_record(record, relative):
    domain = relative.parts[1] if len(relative.parts) > 1 else ''
    if (domain in {'statements', 'statements_hk', 'hk_action_evidence'}
            or (domain == 'earnings_calendar' and record.get('event') == 'announcement_evidence'
                and announcement_identity(record))):
        # This dates announcement/report evidence, NOT corporate-action effectiveness
        if record.get('published_at'):
            return {**record, 'event_date': record['published_at']}
    return record

def equivalent(previous, candidate, relative, document):
    if len(relative.parts) > 1 and relative.parts[1] == 'margin':
        from qwl.datasources.margin_contract import compatible_subset
        # EM's append-only collector can retain earlier SSE-shaped rows while
        # the enclosing document is labelled EM. Restrict to Shanghai + these
        # two known sources + the exact five-field schema; retain formal data.
        if (relative.stem.startswith('6')
                and document.get('source') in {'stock_margin_detail_sse', 'RPTA_WEB_RZRQ_GGMX'}
                and compatible_subset(previous, candidate)):
            return True
        # No other domain, source, missing field or PIT field is waived here
        return ({**previous, 'date': normalized_day(previous.get('date'))}
                == {**candidate, 'date': normalized_day(candidate.get('date'))})
    return False
