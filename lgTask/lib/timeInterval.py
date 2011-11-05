
from datetime import timedelta

class TimeInterval(timedelta):
    """Essentially a timedelta instance that allows instantiation from a 
    string describing the interval in the format:

    (count) (unit[s])

    e.g. 1 hour, 2 hours, 3 days, 1 day, 5 seconds

    Accepted units are days, hours, minutes, seconds.

    Can chain types: 1 hour 2 days
    """

    def __init__(self, desc):
        parts = desc.split(' ')
        kwargs = {}
        for i in range(0, len(parts), 2):
            qty = float(parts[i])
            unit = parts[i + 1]
            if unit[-1] == 's':
                unit = unit[:-1]
            unit = unit + "s"
            kwargs[unit] = qty
        timedelta.__init__(self, **kwargs)

