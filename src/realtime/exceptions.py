class RealtimeException(Exception):
    pass


class ParseFailureException(Exception):
    """Failure to parse a logical replication test_decoding message"""
