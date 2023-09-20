# encoding: utf-8


class IPProxyMiddleware(object):
    """
    代理IP中间件
    """
    _proxy = ('i167.kdltps.com', '15818')

    def process_request(self, request, spider):
        """
        将代理IP添加到request请求中
        """
        username = "t19511624686191"
        password = "xg4bhmft"

        request.meta['proxy'] = "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password,
                                                                        "proxy": ':'.join(self._proxy)}
        request.headers["Connection"] = "close"

        return None

    def process_exception(self, request, exception, spider):
        """捕获407和302异常"""
        if "'status': 407" in exception.__str__() or "'status': 302" in exception.__str__():
            from scrapy.resolver import dnscache
            dnscache.__delitem__(self._proxy[0])

            # 使用新的代理尝试该请求
            # 你可以更新self._proxy为新的代理地址
            # 再次尝试原始请求
            return request.copy()

        # 对于其他异常，我们返回None，这样其他中间件可以处理它
        return None
