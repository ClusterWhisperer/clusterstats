"""
http.py
~~~~~~~~

Given input list of servers, query the status endpoint.
Features:
 - Handle concurrent requests.
 - Handle HttpSession retries on failure.
 - Handle Connection Timeout and other error conditions.
Operations:
 - get_status
 - query_status
"""
import json
from Queue import Queue
from threading import Thread
#from pprint import pprint
import requests
from requests.exceptions import RequestException

def _read_servers(filename):
    """Reads file with the server names.
    Returns a list of servers.
    """
    with open(filename) as file_obj:
        return [server.strip() for server in file_obj]

def _transform_hostname_to_http_endpoint(hostnames):
    """Convert hostnames to status page endpoint.
    Args:
        hostnames: List of server names
    Returns a list of urls in the format "http://<hostname>/status"
    """
    return ["http://{}/status".format(server) for server in hostnames]

STATUS_SUCCESS = 0
STATUS_FAILURE = -1

def _jsonify_exception(excep):
    """Given an exception object return json string {"error": str(e)} """
    return json.dumps({"error": str(excep)})

def _get_http_session(retries):
    """Create Http Session with overridden retries."""
    session = requests.Session()
    http_adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    session.mount('http://', http_adapter)
    session.mount('https://', http_adapter)
    return session

def _get_server_status(status_endpoint, tsecs, retries):
    """Query status endpoint.
    Args:
    status_endpoint: http url
    tsecs: Connection timeout
    retries: Max retries
    Returns tuple (STATUS_CODE, JSON Message)
    """
    try:
        response = _get_http_session(retries).get(status_endpoint, timeout=tsecs)
        response.raise_for_status() # if BAD http request
        return (STATUS_SUCCESS, response.json())
    except (ValueError, RequestException)  as ex: # if the response is not json.
        return (STATUS_FAILURE, _jsonify_exception(ex))

def query_status(endpoints, threads, timeout_secs, http_retries):
    """Queries a list of endpoints for status
    Args:
    endpoints: List of http endpoints.
    threads: # of threads
    timeout_secs: connection timeout in secs.
    http_retries: # of http retry attempts.

    Returns a list of tuples per endpoint with element containing the status
    and second element dictionary.if the status is success the second contains
    the response json, if failure then the excepton string."""
    tasks = Queue()
    results = []

    def worker():
        """Callable Object"""
        while True:
            endpoint = tasks.get()
            result = _get_server_status(endpoint, timeout_secs, http_retries)
            results.append(result) #synchronized operation
            tasks.task_done()

    #start worker threads.
    worker_threads = min(max(threads, 1), len(endpoints))
    for thread_id in range(worker_threads):
        thread = Thread(name=("ClusterStatsThread-"+str(thread_id)), target=worker)
        thread.daemon = True #kill the thread automatically
        thread.start()

    #Give work
    for item in endpoints:
        tasks.put(item)

    tasks.join() # blocks until work is complete
    return results

def get_status(server_inventory_file, threads, timeout_secs, http_retries):
    """ Given a file with host names, builds the status endpoints and queries.
    Args:
    endpoints: List of http endpoints.
    threads: # of threads
    timeout_secs: connection timeout in secs.
    http_retries: # of http retry attempts.

    Returns tuple (List(endpoints), List((STATUS, {RESULT}))
    RESULT: if STATUS == STATUS_SUCCESS then json content otherwise
    str(exception)"""
    urls = _transform_hostname_to_http_endpoint(_read_servers(server_inventory_file))
    return (urls, query_status(urls, threads, timeout_secs, http_retries))
