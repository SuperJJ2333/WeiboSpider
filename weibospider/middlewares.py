# encoding: utf-8
import logging


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

        # logging.info(f'Using proxy: {request.meta["proxy"]}')
        return None

    def process_exception(self, request, exception, spider):
        """捕获407和302异常"""
        if "'status': 407" in exception.__str__():
            from scrapy.resolver import dnscache
            dnscache.__delitem__(self._proxy[0])

            # 使用新的代理尝试该请求
            # 你可以更新self._proxy为新的代理地址
            # 再次尝试原始请求
            logging.debug(f'status: 407: {exception}')
            return exception
