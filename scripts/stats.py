import ahapi
import typing
import elasticsearch
import elasticsearch_dsl
import re

MAX_HITS = 50

# Different indices have different field names, account for it here:
FIELD_NAMES = {
    "fastly": {
        "geo_country": "geo_country_code",
        "bytes": "response_body_size",
        "vhost": "vhost",
        "uri": "url",
        "timestamp": "timestamp",
        "_vhost_": "dlcdn.apache.org",
        "request_method": "request",
    },
    "loggy": {
        "geo_country": "geo_country",
        "bytes": "bytes",
        "vhost": "vhost",
        "uri": "uri",
        "timestamp": "@timestamp",
        "_vhost_": "downloads.apache.org",
        "request_method": "request_method",
    }
}


es_client = elasticsearch.AsyncElasticsearch(hosts=["http://localhost:9200/"], timeout=45)


async def process(state: typing.Any, request, formdata: dict) -> dict:
    duration = formdata.get('duration', '30d')
    project = formdata.get("project", "netbeans")

    downloads_by_requests = {}
    downloads_by_requests_unique = {}
    downloads_by_traffic = {}
    downloads_by_country = {}

    for provider, field_names in FIELD_NAMES.items():
        q = elasticsearch_dsl.Search(using=es_client)
        q = q.filter("range", **{field_names['timestamp']: {"gte": f"now-{duration}"}})
        q = q.filter("match", **{field_names['request_method']: "GET"})
        q = q.filter("range", bytes={"gt": 5000})
        q = q.filter("match", **{field_names['uri']: project})
        q = q.filter("regexp", **{field_names['uri'] + ".keyword": r".*\.[a-z0-9]+"})
        q = q.filter("match", **{field_names['vhost']: field_names['_vhost_']})

        q.aggs.bucket("request_per_url", elasticsearch_dsl.A("terms", field=f"{field_names['uri']}.keyword", size=MAX_HITS)) \
            .metric('unique_ips', 'cardinality', field='client_ip.keyword') \
            .pipeline('product_by_unique', 'bucket_sort', sort=[{'unique_ips': 'desc'}])
        q.aggs.bucket('by_country', 'terms', field=f"{field_names['geo_country']}.keyword", size=MAX_HITS) \
            .metric('unique_ips', 'cardinality', field='client_ip.keyword') \
            .pipeline('product_by_unique', 'bucket_sort', sort=[{'unique_ips': 'desc'}])

        q.aggs.bucket(
            "requests_by_traffic",
            elasticsearch_dsl.A("terms", field=f"{field_names['uri']}.keyword", size=MAX_HITS, order={"bytes_sum": "desc"}),
        ).metric("bytes_sum", "sum", field=field_names['bytes'])

        resp = await es_client.search(index=f"{provider}-*", body=q.to_dict(), size=0, timeout="60s")

        for entry in resp["aggregations"]["requests_by_traffic"]["buckets"]:
            if "bytes_sum" in entry:
                url = re.sub(r"/+", "/", entry["key"])
                url = re.sub("^/?" + project + "/", "", url, count=100)
                no_bytes = int(entry["bytes_sum"]["value"])
                if no_bytes:
                    downloads_by_traffic[url] = downloads_by_traffic.get(url, 0) + no_bytes
        for entry in resp["aggregations"]["request_per_url"]["buckets"]:
            url = re.sub(r"/+", "/", entry["key"])
            url = re.sub("^/?" + project + "/", "", url, count=100)
            if '.' in url and not url.endswith('/'):
                visits = int(entry["doc_count"])
                visits_unique = int(entry["unique_ips"]["value"])
                downloads_by_requests[url] = downloads_by_requests.get(url, 0) + visits
                downloads_by_requests_unique[url] = downloads_by_requests_unique.get(url, 0) + visits_unique
        for entry in resp["aggregations"]["by_country"]["buckets"]:
            cca2 = entry["key"]
            if 'cca2' and cca2 != '-':
                visits = int(entry["unique_ips"]["value"])
                downloads_by_country[cca2] = downloads_by_country.get(cca2, 0) + visits

    return {
        "timespan": duration,
        "by_requests": downloads_by_requests,
        "by_requests_unique": downloads_by_requests_unique,
        "by_bytes": downloads_by_traffic,
        "by_country": downloads_by_country,
    }


def register(state: typing.Any):
    return ahapi.endpoint(process)
