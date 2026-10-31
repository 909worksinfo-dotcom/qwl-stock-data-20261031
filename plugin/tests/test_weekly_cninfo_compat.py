import json
from types import SimpleNamespace
import pandas as pd
import pytest
import requests
from scripts.weekly_cninfo_compat import (
    ProviderResponseError, classify_response, secure_url, verified_empty, install,
)

def test_only_two_official_endpoints_upgrade_transport():
    assert secure_url('http://www.cninfo.com.cn/new/data/szse_stock.json').startswith('https:')
    assert secure_url('http://www.cninfo.com.cn/new/hisAnnouncement/query?p=1')=='https://www.cninfo.com.cn/new/hisAnnouncement/query?p=1'
    for url in ['http://example.com/new/hisAnnouncement/query','http://www.cninfo.com.cn/other',
                'http://www.cninfo.com.cn.evil/new/hisAnnouncement/query']:
        assert secure_url(url)==url

@pytest.mark.parametrize('status,body',[(500,{'resultcode':200,'records':[]}),
    (200,{'resultcode':403,'records':[]}),(200,{'records':[]}),(200,{'resultcode':200,'records':[{'x':1}]}),
    (200,{'resultcode':200,'records':[],'error':'denied'}),
    (200,{'resultcode':200,'records':[],'total':10})])
def test_share_errors_never_become_empty(status,body):
    assert not verified_empty('share_changes',status,body)

def test_announcement_empty_requires_explicit_zero_and_no_failure():
    assert verified_empty('announcements',200,{'totalAnnouncement':0,'announcements':None})
    assert not verified_empty('announcements',200,{'announcements':[]})
    assert not verified_empty('announcements',200,{'totalAnnouncement':False,'announcements':[]})
    assert not verified_empty('announcements',200,{'totalAnnouncement':0,'announcements':[],'success':False})

def test_native_empty_dataframe_keyerror_is_handled_with_evidence(monkeypatch):
    body={'resultcode':200,'records':[]}
    def transport(self,method,url,**kwargs):
        return SimpleNamespace(status_code=200,content=json.dumps(body).encode(),json=lambda:body)
    monkeypatch.setattr(requests.sessions.Session,'request',transport)
    def native(**kwargs):
        response=requests.post('https://webapi.cninfo.com.cn/api/stock/p_stock2215')
        frame=pd.DataFrame(response.json()['records'])
        frame['公告日期']=pd.to_datetime(frame['公告日期'])
        return frame
    ak=SimpleNamespace(stock_share_change_cninfo=native,stock_zh_a_disclosure_report_cninfo=lambda:None)
    events=install(ak)
    assert ak.stock_share_change_cninfo(symbol='002594',start_date='20260831',end_date='20260904').empty
    assert events[-1]['status']=='verified_empty_source_window' and len(events[-1]['response_sha256'])==64
    # A later non-empty malformed response must not reuse the previous proof
    body['records']=[{'unexpected':'schema'}]
    with pytest.raises(KeyError):
        ak.stock_share_change_cninfo(symbol='002594')


def response(status, body, content_type='application/json'):
    return SimpleNamespace(status_code=status, content=json.dumps(body).encode(),
        headers={'Content-Type':content_type}, json=lambda:body)


@pytest.mark.parametrize('status,kind,body,expected', [
    (403,'stock_map',{},'CNINFO_HTTP_DENIED'),
    (200,'stock_map',{},'CNINFO_SCHEMA'),
    (200,'announcements',{'announcements':[]},'CNINFO_SCHEMA'),
    (200,'announcements',{'totalAnnouncement':2,'announcements':'bad'},'CNINFO_SCHEMA'),
    (302,'announcements',{},'CNINFO_REDIRECT_BLOCKED'),
    (200,'announcements',{'totalAnnouncement':0,'announcements':[],'success':False},'CNINFO_SOURCE_ERROR'),
])
def test_source_errors_are_typed_not_empty(status,kind,body,expected):
    url='https://www.cninfo.com.cn/new/data/szse_stock.json'
    with pytest.raises(ProviderResponseError) as error:
        classify_response(kind,response(status,body),url)
    assert error.value.code==expected


def test_html_response_cannot_be_treated_as_empty():
    with pytest.raises(ProviderResponseError,match='CNINFO_NON_JSON'):
        classify_response('announcements',response(200,{},'text/html'),
            'https://www.cninfo.com.cn/new/hisAnnouncement/query')


def test_paginated_announcement_must_match_reported_total(monkeypatch):
    def transport(self,method,url,**kwargs):
        page=bool(kwargs.get('data'))
        body={'totalAnnouncement':2,'announcements':[{'x':1}] if page else []}
        return response(200,body)
    monkeypatch.setattr(requests.sessions.Session,'request',transport)
    def native(**kwargs):
        requests.post('http://www.cninfo.com.cn/new/hisAnnouncement/query',
            params={'seDate':'2026-09-21~2026-09-25','pageNum':'1'})
        requests.post('http://www.cninfo.com.cn/new/hisAnnouncement/query',
            data={'seDate':'2026-09-21~2026-09-25','pageNum':'1'})
        return pd.DataFrame([{'x':1}])
    ak=SimpleNamespace(stock_share_change_cninfo=lambda **kw:None,
        stock_zh_a_disclosure_report_cninfo=native)
    events=install(ak)
    with pytest.raises(ProviderResponseError,match='CNINFO_PAGINATION_INCOMPLETE'):
        ak.stock_zh_a_disclosure_report_cninfo(symbol='000333')
    assert len(events)==2 and all(e['http_status']==200 for e in events)
