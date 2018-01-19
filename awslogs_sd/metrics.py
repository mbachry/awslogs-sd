import logging
import gevent


logger = logging.getLogger('metrics')


class Metrics:
    def __init__(self):
        self.journal_queue = None
        self.cwl_queue = None
        self.n_logs_received = 0
        self.n_logs_sent = 0

    def __str__(self):
        assert self.journal_queue is not None
        assert self.cwl_queue is not None
        return (('received from journal: {}, sent to cloudwatch: {}, '
                 'journal queue size: {}, cloudwatch queue size: {}')
                .format(self.n_logs_received,
                        self.n_logs_sent,
                        self.journal_queue.qsize(),
                        self.cwl_queue.qsize()))


metrics = Metrics()


def metrics_task(delay=10):
    while True:
        gevent.sleep(delay)
        logger.info('Status: %s', metrics)
