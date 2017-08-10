"""
Main entrypoint for the module.
Usage:
clusterstats --inventory_file=servers.txt -- --timeout=1 --threads=3 --verbose

"""
import os.path
from optparse import OptionParser, OptionGroup
import pprint
from clusterstats import http
from clusterstats import stats

def main():
    """Module entrypoint."""
    parser = OptionParser(usage=("usage: %prog -i <server_list_file> [-o output_directory] "
                                 "[options] [http options] [aggregation options]"))

    parser.add_option("-i", "--inventory_file", action="store", dest="inventory_file",
                      help="File path with host names to query status ")
    parser.add_option("-o", "--output_dir", action="store", dest="output_dir",
                      default="/tmp",
                      help="Directory to store the status output (default= /tmp).")
    parser.add_option("-q", "--qos", action="store", type=float, dest="qos", default="99.0",
                      help=("Guarantee atleast x% of the server status are collected to calculate"
                            " statistics, otherwise errors.(default 99%)"))
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Show error logs (default= turned off)")
    http_group = OptionGroup(parser, "HTTP Options")
    http_group.add_option("-t", "--timeout", action="store", type=float, dest="timeout",
                          default=1.0,
                          help="Connection timeout.(default 1sec)")
    http_group.add_option("-f", "--threads", action="store", type=int, dest="threads",
                          default=1,
                          help="Max concurrent http connections (default=1)")
    http_group.add_option("-r", "--retries", action="store", type=int, dest="http_retries",
                          default=3,
                          help="Http Session max-retries (default=3)")

    query_group = OptionGroup(parser, "Aggregation Options")
    query_group.add_option("-s", "--aggr_success_rate",
                           action="store_const", const="APP_VERSION_SUCCESS_CNT",
                           dest="query", default="APP_VERSION_SUCCESS_CNT",
                           help="Aggregate Success count by application and version.")


    parser.add_option_group(query_group)
    parser.add_option_group(http_group)
    (options, args) = parser.parse_args()

    if (options.inventory_file is None) or (not os.path.isfile(options.inventory_file)):
        parser.error("option -i inventory file does not exists.")

    if (options.output_dir is None) or (not os.path.isdir(options.output_dir)):
        parser.error("option -o output_dir does not exists or not a directory.")

    if not os.access(options.output_dir, os.W_OK):
        parser.error("option -o output_dir:{} not writable.".format(options.output_dir))

    if options.timeout <= 0.0:
        parser.error("option --timeout cannot be a negative number.")

    if options.threads < 1:
        parser.error("option --threads need to atleast 1.")

    if options.http_retries < 0:
        options.http_retries = 0

    if options.qos > 100.0 and options.qos <= 0.0:
        parser.error("option --qos expects percentage value > 0% <= 100%.")

    pp = pprint.PrettyPrinter(indent=4, width=80)

    if options.query == "APP_VERSION_SUCCESS_CNT":
        (endpoints, results) = http.get_status(options.inventory_file, options.threads,
                                               options.timeout, options.http_retries)
        success_queries = filter(lambda x: x[0] == http.STATUS_SUCCESS, results)

        try:
            pp.pprint('-'*80)

            qos_status = ["Total # of queries: {}".format(len(endpoints)),
                          "Successful queies:  {}".format(len(success_queries)),
                          "Expected QoS:       {}%".format(options.qos),
                          "Actual QoS:         {}%".format(stats.calc_qos(len(endpoints),
                                                                          len(success_queries)))]

            if not stats.check_qos(options.qos, len(endpoints), len(success_queries)):
                pp.pprint(["Status: FAILED"])
                pp.pprint(qos_status)

                return

            data = [msg for (status, msg) in results]
            data_frame = stats.calc_stats(data, [stats.FIELD_APPLICATION, stats.FIELD_VERSION],
                                          stats.FIELD_SUCCESS_COUNT, stats.OPERATOR_ADD)

            pp.pprint(["Status: OK"])
            pp.pprint(qos_status)

            pp.pprint(["Results:"])
            pp.pprint(data_frame)

            path = stats.write_stats(data_frame, options.output_dir)
            pp.pprint(["Output File: {}".format(path)])

        finally:
            pp.pprint('-'*80)
            if options.verbose:
                failed_queries = filter(lambda x: x[0] == http.STATUS_FAILURE, results)
                pp.pprint(['Failed Queries:'])
                pp.pprint([x[1] for x in failed_queries])
                pp.pprint('-'*80)
