import urllib
import urlparse

def get_webhdfs_url(hdfs_host, webhdfs_port, hdfs_user, op, path):
    path = urllib.quote(path.encode('utf-8'))
    url = 'http://%s:%s/webhdfs/v1/%s' % (hdfs_host, webhdfs_port, path)
    url_parts = urlparse.urlsplit(url)
    query_params = urlparse.parse_qs(url_parts.query)
    query_params['op'] = op
    if hdfs_user is not None:
        query_params['user.name'] = hdfs_user
    new_query_string = urllib.urlencode(query_params)
    return urlparse.urlunsplit(
        (url_parts.scheme, url_parts.netloc, url_parts.path,
         new_query_string, url_parts.fragment))
