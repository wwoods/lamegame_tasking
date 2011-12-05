
from datetime import timedelta

class TimeInterval(timedelta):
    """Essentially a timedelta instance that allows instantiation from a 
    string describing the interval in the format:

    (count) (unit[s])

    e.g. 1 hour, 2 hours, 3 days, 1 day, 5 seconds

    Accepted units are days, hours, minutes, seconds.

    Can chain types: 1 hour 2 days

    Can use the "ago" word last to create a negative interval: 1 hour ago

    Can use "now" to indicate a zero interval: now
    """
    
    def __new__(cls, desc):
        """Since we derive from timedelta which is special, we must use 
        __new__.
        """
        if isinstance(desc, timedelta):
            kwargs = { 
                'days': desc.days
                , 'seconds': desc.seconds
                , 'microseconds': desc.microseconds
            }
        elif desc == 'now':
            kwargs = {}
        else:
            parts = desc.split(' ')
            ago = False
            if parts[-1] == 'ago':
                parts = parts[:-1]
                ago = True
            kwargs = {}
            for i in range(0, len(parts), 2):
                qty = float(parts[i])
                if ago:
                    qty = -qty
                unit = parts[i + 1]
                if unit[-1] == 's':
                    unit = unit[:-1]
                unit = unit + "s"
                kwargs[unit] = qty

        instance = timedelta.__new__(TimeInterval, **kwargs)
        return instance

    def __repr__(self):
        return 'TimeInterval("' + self.__str__() + '")'

    def __str__(self):
        days = self.days
        s = self.seconds
        secs = s % 60
        s //= 60
        mins = s % 60
        s //= 60
        hours = s
        msecs = self.microseconds

        result = ''
        if days: 
            result += ' {0} day{1}'.format(days, 's' if days != 1 else '')
        if hours: 
            result += ' {0} hour{1}'.format(hours, 's' if hours != 1 else '')
        if mins: 
            result += ' {0} minute{1}'.format(mins, 's' if mins != 1 else '')
        if secs: 
            result += ' {0} second{1}'.format(secs, 's' if secs != 1 else '')
        if msecs:
            result += ' {0} microsecond{1}'.format(
                msecs, 's' if msecs != 1 else ''
            )
        return result.strip()


