import json
import urllib.parse
import urllib.request

base_url = "https://publish.twitter.com/oembed"

default_params = {
    "url": "https://twitter.com/trace_map/status/",
    "hide_media": True,
    "hide_thread": True,
    "omit_script": True,
    "related": "trace_map",
    "link_color": "#9729ff",
    "dnt": True,
}


def get_html( tweetId):
    params = default_params.copy()
    params['url'] = params['url'] + tweetId
    params_string = urllib.parse.urlencode(params)
    url = base_url + "?" + params_string
    with urllib.request.urlopen(url) as response:
        data = response.read().decode('utf-8')
        data_json = json.loads( data)
        html = data_json['html']
        return html;
