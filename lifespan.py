"""
This code uses the concept of lifespan and maintains min and max timestamps.
As it turns out, address may have a larger lifespan but is active only for
few days in the span.

Have NOT implemented it here.
"""
from collections import defaultdict
from constants import DELIM
import datetime


def fmt_time(ts):
    # TODO: Move out of here to utils section
    dt_obj = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S UTC")
    return datetime.datetime.strftime(dt_obj, "%Y-%m-%d %H:%M:%S")


class Lifespan:
    def __init__(self):
        self.lifespan = defaultdict(list)

    def read_lifespan(self, fname):
        """
        Simulates address lifespan datastore
        """
        with open(fname) as fp:
            next(fp)  # skip header
            for line in fp:
                actor, min_ts, max_ts, ts_diff_days = line.strip().split(DELIM)
                self.lifespan[actor] = [fmt_time(min_ts), fmt_time(max_ts)]

    def have_overlap(self, actor_1, actor_2):
        # TODO: Add error checking here.
        # Sort timestamps by start time.
        ts = sorted([self.lifespan[actor_1], self.lifespan[actor_2]])

        # If end of first actor < start of new actor
        # They have overlap and thus *MAY* overlap
        if ts[0][1] <= ts[0][0]:
            # Search range:
            # max(start time) = ts[0][1], since sorted by start time
            # min(end_time) = min(ts[0][1], ts[1][1])
            return [ts[0][1], min(ts[0][1], ts[1][1])]
        return [None, None]

    def get_time(self, actor, start_ts, end_ts):
        # TODO: Add error checking here.
        actor_start, actor_end = self.lifespan[actor]
        return [max(actor_start, start_ts), min(actor_end, end_ts)]
