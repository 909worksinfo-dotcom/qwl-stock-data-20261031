"""Fail-closed CNINFO transport and response evidence around native collectors."""
import hashlib
import json
import threading
from urllib.parse import urlsplit

PATHS = {'/new/data/szse_stock.json','/new/hisAnnouncement/query'}
HOSTS = {'www.cninfo.com.cn', 'webapi.cninfo.com.cn'}


class ProviderResponseError(RuntimeError):
    def __init__(self, code, evidence):
        self.code = code
        self.evidence = evidence
        super().__init__(f"{code}: {evidence.get('endpoint')} status={evidence.get('http_status')}")


def endpoint_kind(method, url):
    parsed = urlsplit(url)
    if parsed.scheme not in {'http', 'https'} or parsed.hostname not in HOSTS:
        return None
    key = (method.upper(), parsed.hostname, parsed.path)
    return {
        ('GET', 'www.cninfo.com.cn', '/new/data/szse_stock.json'): 'stock_map',
        ('POST', 'www.cninfo.com.cn', '/new/hisAnnouncement/query'): 'announcements',
        ('POST', 'webapi.cninfo.com.cn', '/api/stock/p_stock2215'): 'share_changes',
    }.get(key)

def secure_url(url):
    parsed=urlsplit(url)
    if parsed.scheme=='http' and parsed.netloc=='www.cninfo.com.cn' and parsed.path in PATHS:
        return parsed._replace(scheme='https').geturl()
    return url


def classify_response(kind, response, url, request=None):
    request = request or {}
    headers = getattr(response, 'headers', {}) or {}
    content = getattr(response, 'content', b'')
    evidence = {
        'kind': kind, 'endpoint': secure_url(url).split('?')[0],
        'http_status': response.status_code,
        'content_type': headers.get('Content-Type', ''),
        'response_sha256': hashlib.sha256(content).hexdigest(),
        'request_window': request.get('seDate') or {
            'start_date': request.get('sdate'), 'end_date': request.get('edate')},
        'page_num': str(request.get('pageNum', '')) if kind == 'announcements' else '',
    }
    if not 200 <= response.status_code < 300:
        code = ('CNINFO_REDIRECT_BLOCKED' if 300 <= response.status_code < 400
                else 'CNINFO_HTTP_DENIED' if response.status_code in {401, 403}
                else 'CNINFO_HTTP_FAILURE')
        raise ProviderResponseError(code, evidence)
    if 'html' in evidence['content_type'].lower():
        raise ProviderResponseError('CNINFO_NON_JSON', evidence)
    try:
        body = response.json()
    except (TypeError, ValueError) as exc:
        raise ProviderResponseError('CNINFO_NON_JSON', evidence) from exc
    if not isinstance(body, dict):
        raise ProviderResponseError('CNINFO_SCHEMA', evidence)
    if any(body.get(key) for key in ('error','errors','exception')) or body.get('success') is False:
        raise ProviderResponseError('CNINFO_SOURCE_ERROR', evidence)
    if kind == 'stock_map':
        if not isinstance(body.get('stockList'), list):
            raise ProviderResponseError('CNINFO_SCHEMA', evidence)
    elif kind == 'announcements':
        try:
            total = int(body['totalAnnouncement'])
        except (KeyError, ValueError, TypeError) as exc:
            raise ProviderResponseError('CNINFO_SCHEMA', evidence) from exc
        rows = body.get('announcements')
        if total < 0 or not isinstance(rows, (list, type(None))):
            raise ProviderResponseError('CNINFO_SCHEMA', evidence)
        evidence['total_announcement'] = total
        evidence['returned_rows'] = len(rows or [])
    elif kind == 'share_changes':
        if not isinstance(body.get('records'), list):
            raise ProviderResponseError('CNINFO_SCHEMA', evidence)
        if body.get('resultcode') is not None and str(body['resultcode']) != '200':
            raise ProviderResponseError('CNINFO_SOURCE_ERROR', evidence)
        evidence['returned_rows'] = len(body['records'])
    return body, evidence

def verified_empty(kind, status, body):
    if not 200<=status<300 or not isinstance(body,dict):
        return False
    if any(body.get(k) for k in ('error','errors','exception')) or body.get('success') is False:
        return False
    if kind=='share_changes':
        return (str(body.get('resultcode'))=='200' and body.get('records')==[]
                and all(str(body[k])=='0' for k in ('total','totalRecordNum','totalRecords') if k in body))
    if any(k in body and str(body[k]).upper() not in {'200','OK','SUCCESS'} for k in ('code','resultcode','status')):
        return False
    total=body.get('totalAnnouncement')
    return type(total) in {int,str} and str(total)=='0' and body.get('announcements') in ([],None)

def install(ak=None):
    import requests
    import pandas as pd
    if ak is None:
        import akshare as ak
    context=threading.local()
    events=[]
    original_request=requests.sessions.Session.request
    def request(self,method,url,**kwargs):
        kind=endpoint_kind(str(method),url)
        requested=secure_url(url)
        if kind:
            kwargs['allow_redirects']=False
        response=original_request(self,method,requested,**kwargs)
        capture=getattr(context,'capture',None)
        if kind and capture is not None:
            request_data=kwargs.get('data') or kwargs.get('params') or {}
            if not isinstance(request_data, dict):
                request_data={}
            try:
                body, evidence=classify_response(kind,response,url,request_data)
            except ProviderResponseError as exc:
                events.append({**exc.evidence, 'status':exc.code})
                raise
            evidence['status']='ok'
            if kind=='announcements':
                evidence['request_phase']='page' if kwargs.get('data') is not None else 'probe'
            events.append(evidence)
            if kind==capture['kind']:
                if verified_empty(kind,response.status_code,body):
                    capture['proof']=evidence
                else:
                    capture.pop('proof',None)
                if kind=='announcements':
                    if kwargs.get('data') is not None:
                        capture['page_rows']+=evidence['returned_rows']
                    else:
                        capture['expected_rows']=evidence['total_announcement']
        return response
    requests.sessions.Session.request=request

    def wrap(original,kind):
        def call(*args,**kwargs):
            prior=getattr(context,'capture',None)
            capture={'kind':kind,'page_rows':0};context.capture=capture
            try:
                result=original(*args,**kwargs)
                if kind=='announcements' and capture.get('expected_rows',0)!=capture['page_rows']:
                    raise ProviderResponseError('CNINFO_PAGINATION_INCOMPLETE', {
                        'endpoint':'https://www.cninfo.com.cn/new/hisAnnouncement/query',
                        'http_status':200, 'expected_rows':capture.get('expected_rows'),
                        'returned_rows':capture['page_rows']})
                return result
            except KeyError:
                # Never convert network errors, malformed JSON, non-empty
                # results, unknown schemas or denied requests into empty data
                if not capture.get('proof'):
                    raise
                events.append({'kind':kind,'symbol':kwargs.get('symbol',args[0] if args else None),
                    'start_date':kwargs.get('start_date'),'end_date':kwargs.get('end_date'),
                    **capture['proof'],'status':'verified_empty_source_window'})
                return pd.DataFrame()
            finally:
                context.capture=prior
        return call
    ak.stock_share_change_cninfo=wrap(ak.stock_share_change_cninfo,'share_changes')
    ak.stock_zh_a_disclosure_report_cninfo=wrap(ak.stock_zh_a_disclosure_report_cninfo,'announcements')
    return events
