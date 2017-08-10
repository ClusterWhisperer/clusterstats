"""Testcases for the clusterstats module."""
import unittest
import json
from pprint import pprint
import httpretty
from requests import HTTPError, Timeout
import pandas as pd
from clusterstats import http
from clusterstats import stats

class ClusterStatsTest(unittest.TestCase):

    def test_read_servers(self):
        """Test the _read_servers private method"""
        self.assertEquals(len(http._read_servers("data/servers.txt")), 1000)

    def test_transform_hostname_to_http_endpoint(self):
        """Test the _transform_hostname_to_http_endpoint"""
        hosts=["server1", "server2"]
        expected_out=["http://server1/status", "http://server2/status"] 
        self.assertEquals(http._transform_hostname_to_http_endpoint(hosts), expected_out) 

    @httpretty.activate
    def test_http_OK(self):
        """Test Http Connectivity - Success scenario""" 
        url='http://myserver/status' 
        content=('{"Application":"Webapp2","Version":"0.0.2",'
                 '"Uptime":8102471691,"Request_Count":4134752620,'
                 '"Error_Count":2772072365,"Success_Count":1362680255}')

        httpretty.register_uri(
            method=httpretty.GET,
            uri=url,
            status=200,
            body=content,
            content_type="application/json"
            )
        result=self._connect(url)
        self.assertEquals(result[0], 0)
        self.assertTrue(result[1]["Application"] == "Webapp2")

    @httpretty.activate    
    def test_http_404(self):
        """Test HTTP Error Condition"""
        def exception_callback(request, uri, headers):
            raise HTTPError("404 Page Not found.")

        url='http://myserver/status'
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url,
            status=404,
            body=exception_callback,
            content_type="application/json"
            )
        result=self._connect(url)
        self.assertEquals(-1,result[0])

    @httpretty.activate    
    def test_http_timeout(self):
        """Test Timeout Condition"""
        def exception_callback(request, uri, headers):
            raise Timeout("Connection Timeout.")

        url='http://myserver/status'
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url,
            status=504,
            body=exception_callback,
            content_type="application/json"
            )
        result=self._connect(url)
        self.assertEquals(-1,result[0])

    @httpretty.activate 
    def test_json_error(self):
        """Test ValueError if JSON not returned."""
        url='http://myserver/status'
        content="Hello World"
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url,
            status=200,
            body=content,
            content_type="application/json"
            )
        result=self._connect(url)
        self.assertEquals(-1,result[0])

    @httpretty.activate 
    def test_http_retries(self):
        """Test HTTP Session connection retries """
        def exception_callback(request, uri, headers):
            raise Timeout("Connection Timeout. - 2")

        url='http://myserver/status'
        content=('{"Application":"Webapp2","Version":"0.0.2",'
                 '"Uptime":8102471691,"Request_Count":4134752620,'
                 '"Error_Count":2772072365,"Success_Count":1362680255}')

        httpretty.register_uri(
            method=httpretty.GET,
            uri=url,
            responses=[
                httpretty.Response(body=exception_callback, status=504),
                httpretty.Response(body=exception_callback, status=504),
                httpretty.Response(body=content, status=200, content_type="application/json"),
            ])
        result=self._connect(url)
        self.assertEquals(result[0], 0)
        self.assertTrue(result[1]["Application"] == "Webapp2")

    @httpretty.activate
    def test_query_status(self):
        """ Test query_status """
        url1='http://myserver1/status'
        url2='http://myserver2/status'
        url3='http://myserver3/status'
        url4='http://myserver4/status'
        url5='http://myserver5/status'

        content=('{"Application":"Webapp2","Version":"0.0.2",'
                 '"Uptime":8102471691,"Request_Count":4134752620,'
                 '"Error_Count":2772072365,"Success_Count":1362680255}')
        bad_content="Hello World..."

        httpretty.register_uri(
            method=httpretty.GET,
            uri=url1,
            status=200,
            body=content,
            content_type="application/json"
            )
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url2,
            status=200,
            body=content,
            content_type="application/json"
            )
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url3,
            status=200,
            body=content,
            content_type="application/json"
            )
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url4,
            status=200,
            body=content,
            content_type="application/json"
            )
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url5,
            status=200,
            body=bad_content,
            content_type="application/json"
            )

        results=http.query_status([url1, url2, url3, url4, url5], 3, 2, 3)
        ##expecting success count = 4 and failure = 1
        success_list=filter(lambda x: x[0] == 0, results)
        failure_list=filter(lambda x: x[0] == -1, results)
        self.assertTrue(len(success_list) == 4)
        self.assertTrue(len(failure_list) == 1)


    def test_calc_qos(self):
        """Test Calculating QoS """
        self.assertTrue(stats.calc_qos(100,99), 99.0) 

    def test_check_qos(self):
        """Test Check QoS method """       
        self.assertTrue(stats.check_qos(99.0, 100, 99))
        self.assertFalse(stats.check_qos(99.1, 100, 99))

    def test_calc_stats(self):
        """Testing Stats Calculation"""
        d = [{"Application":"Webapp1","Version":"1.2.1","Uptime":9634484391,"Request_Count":7729359104,
              "Error_Count":3394574268,"Success_Count":4334784836},
             {"Application":"Webapp1","Version":"1.2.1","Uptime":9634484391,"Request_Count":7729359104,
              "Error_Count":3394574268,"Success_Count":4334784836},
             {"Application":"Database2","Version":"0.1.0","Uptime":8982039907,"Request_Count":2174448763,
              "Error_Count":2001963223,"Success_Count":172485540}] 

        df = stats.calc_stats(d, ['Application', 'Version'], 'Success_Count', stats.OPERATOR_ADD)        
        self.assertTrue(df.shape[0], 2) ## expecting two rows.
        
    @httpretty.activate        
    def test_success_flow(self):
        """Integration test of the http results and stats calculation."""
        url1='http://myserver1/status'
        url2='http://myserver2/status'
        url3='http://myserver3/status'

        content=('{"Application":"Webapp2","Version":"0.0.2",'
                 '"Uptime":8102471691,"Request_Count":4134752620,'
                 '"Error_Count":2772072365,"Success_Count":1362680255}')
        content2=('{"Application":"Database2","Version":"0.0.2",'
                 '"Uptime":8102471691,"Request_Count":172485540,'
                 '"Error_Count":2772072365,"Success_Count":1362680255}')
        
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url1,
            status=200,
            body=content,
            content_type="application/json"
            )
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url2,
            status=200,
            body=content,
            content_type="application/json"
            )
        httpretty.register_uri(
            method=httpretty.GET,
            uri=url3,
            status=200,
            body=content2,
            content_type="application/json"
            )

        results=http.query_status([url1, url2, url3], 3, 2, 3)

        success_list=filter(lambda x: x[0] == 0, results)
        failure_list=filter(lambda x: x[0] == -1, results) # expect no failure

        self.assertEquals(failure_list, [])

        data = [msg for (status, msg) in results]
        df = stats.calc_stats(data, [stats.FIELD_APPLICATION, stats.FIELD_VERSION],
                              stats.FIELD_SUCCESS_COUNT, stats.OPERATOR_ADD) 

        self.assertTrue(df.shape[0], 2)
        pprint(df)


    def _connect(self, url):
        result=http._get_server_status(url,10,3)
        # print result
        return result             

if __name__ == '__main__':
    unittest.main()