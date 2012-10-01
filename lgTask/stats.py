
import datetime
import pymongo

class StatsInterface(object):
    """Stats interface, for storing statistics in a round-robin type of 
    document within a mongoDB collection.

    Notes about implementation:
    Db info is stored in _schema, modifying _schema requires deleting all 
    matching statistics so it is wise to do that first.

    Schema outlines which prefixes will be stored for how long, and what the
    various suffixes for different types of stats are.

    Default stats are counters, meaning when you addStats() with a number, that
    number is added to whatever data was already in the stat.

    intervals -- stores a list of block sizes.  For storing data long-term, it
            is usually good to increase the period of time that a block 
            represents.  This cuts down on storage space and increases speed.
            Note that the first interval's block length is in second, and 
            each consecutive interval after that is the # of blocks in the
            previous layer to this layer.

    sampleSuffix -- any stats whose names end in this will end up being treated
            as "samples" of a value, rather than a counter.  That is, if we
            get two values for the same block, the most recent value will be
            used.
    """
    def __init__(self, statsCollection):
        self._epoch = datetime.datetime.utcfromtimestamp(0)
        self._col = statsCollection
        self._schema = self._col.find_one({ '_id': '_schema' })
        if self._schema is None:
            self._schema = { '_id': '_schema'
                    , 'prefixes': [ # Sorted in length-desc order
                        {
                            'prefix': ''
                            , 'intervals': [ 
                                [3*60, 1] # 3 min @ 1 day
                                , [5, 15] # 15 min @ 15 days
                                , [24, 365] # 6 hr @ 1 yr
                            ]
                            , 'sampleSuffix': '-total'
                        }
                    ]
                }
            self._col.save(self._schema)


    def addStat(self, name, value, time = None):
        """Adds a single statistic.

        name -- The key (period-delimited or a list of strings to be joined
                by periods) for the stat.
        value -- The numeric value for the stat.
        time -- A datetime.datetime object when the stat occurred, or None to
                use utcnow().  We use datetime rather than time.time since 
                time.time() comes back in local time...
        """
        timeVal = self._getTimeVal(time)
        if isinstance(name, list):
            name = '.'.join([ str(n).replace('.', '-') for n in name ])

        schema = self._getSchemaFor(name)
        myType = 'add'
        if name.endswith(schema['sampleSuffix']):
            myType = 'set'

        # index of each block we're writing to in each interval
        blocks = self._getBlocks(timeVal, schema['intervals'])

        # Mongodb syntax - to get fields, use 
        # find(fields = { 'valArray': { '$slice': [ idx, len ] }})

        # First see if we can find a document that is updated before or at the
        # same time as our requested update
        sets = { 'tsLatest': timeVal }
        deltas = { }
        updates = { '$set': sets, '$inc': deltas }
        fields = { 'tsLatest': 1 }
        for layer, blockInfo in enumerate(blocks):
            block = 'block.' + str(layer)
            key = block + '.' + str(blockInfo[0])
            if myType == 'set':
                sets[key] = value
            elif myType == 'add':
                deltas[key] = value
            else:
                raise ValueError("Unknown type: " + myType)

            fields[block] = { '$slice': [ blockInfo[0], 1 ] }

        r = self._col.find_and_modify(
            { '_id': name, 'tsLatest': { '$lte': timeVal }}
            , updates
            , fields = fields
        )
        if r is not None:
            # Are they from an old bucket?
            oldLatest = r['tsLatest']
            oldBlocks = self._getBlocks(oldLatest, schema['intervals'])
            if oldLatest + (oldBlocks[0][1] - 1) * oldBlocks[0][2] < timeVal:
                # They need a reset; the oldest data is older than all of the
                # current blocks could be.
                raise NotImplementedError()
            elif oldBlocks[0][0] != blocks[0][0]:
                # They need an update... something to keep in mind: since we've
                # replaced tsLatest with our tsLatest, no-one else should be
                # responsible for updating the missing buckets since they'll
                # see our tsLatest.

                # So, just zero out all of the unused buckets.  And then for
                # the current bucket, if we're using a counter, adjust off
                # the old value (from the last rollover).
                sets = {}
                incs = {}
                updates = { '$set': sets, '$inc': incs }
                for layer, oldBlock in enumerate(oldBlocks):
                    # We don't want to update the record that corresponds
                    # with the old tsLatest, since it's already up-to-date
                    oldIndex = oldBlock[0]
                    oldIndex += 1
                    while oldIndex != blocks[layer][0]:
                        block = 'block.' + str(layer)
                        key = block + '.' + str(oldIndex)
                        sets[key] = 0
                        # Rotate to next block
                        oldIndex = (oldIndex + 1) % oldBlock[1]
                    if myType == 'add':
                        # Custom update for add for the new slot, since we 
                        # don't want to get rid of our counter and 
                        # counters since
                        slayer = str(layer)
                        block = 'block.' + slayer
                        key = block + '.' + str(blocks[layer][0])
                        # Remove the old counter amount for this block
                        incs[key] = -r['block'][slayer][0]
                self._col.update(
                    { '_id': name }
                    , updates
                )
                        
            # If the blocks are the same or they've already been updated since
            # our stat, all is well
        else:
            # Can we update a document updated after us?
            r = self._col.find_and_modify(
                { '_id': name, 'tsLatest': { '$gte': timeVal }}
                , updates
                , fields = fields
            )
            if r is None:
                # r is None, meaning this stat doesn't exist.  Create a new
                # stat and insert it
                if not self._tryNewStat(schema, name, timeVal):
                    # Insert failed, meaning someone else beat us.  Run
                    # a normal update
                    return self.addStat(name, value, time = time)
                else:
                    # Still run a normal update to add our stat
                    return self.addStat(name, value, time = time)
            # if r is not None, then we're ok.  The document's already been
            # updated after us, meaning that the bucket logic has already
            # been applied.  So no worries


    def addStats(self, stats):
        """Add multiple statistics - stats is a list containing a dict, where
        the keys match the kwargs to addStat.
        """
        for s in stats:
            self.addStat(**s)


    def getStat(self, stat, start, stop):
        """Get stats between start and stop (datetime objects).  Returns a dict:

        { 'values': [ val1, val2, ... ], 'tsStart': start, 'tsInterval': time
                between buckets }
        """

        tsNow = self._getTimeVal(None)
        tsStart = self._getTimeVal(start)
        tsStop = self._getTimeVal(stop)

        schema = self._getSchemaFor(stat)
        blocksStart = self._getBlocks(tsStart, schema['intervals'])
        blocksStop = self._getBlocks(tsStop, schema['intervals'])
        for layer, (blockStart, blockStop) in enumerate(
                zip(blocksStart, blocksStop)):
            if tsNow - tsStart > (blockStart[1] - 1) * blockStart[2]:
                continue
            # This layer is OK to pull from.
            data = []
            tsLatest = None
            slayer = str(layer)
            block = 'block.' + slayer
            if blockStart[0] > blockStop[0]:
                # We need two slices
                d = self._col.find_one(
                    stat
                    , fields = { 'tsLatest': 1, block: { '$slice':
                        [ blockStart[0], 99999999 ] }}
                )
                if d is None:
                    raise ValueError("Stat not found: {0}".format(stat))
                data = d['block'][slayer]
                tsLatest = d['tsLatest']
                d = self._col.find_one(
                    stat
                    , fields = { block: { '$slice':
                        [ 0, blockStop[0] + 1 ] }}
                )
                if d is None:
                    raise ValueError("Stat error: {0}".format(stat))
                data.extend(d['block'][slayer])
            else:
                # Only one slice
                d = self._col.find_one(
                    stat
                    , fields = { 'tsLatest': 1, block: { '$slice': 
                        [ blockStart[0], blockStop[0] - blockStart[0] + 1 ] }}
                )
                tsLatest = d['tsLatest']
                data = d['block'][slayer]
                if d is None:
                    raise ValueError("Stat not found: {0}".format(stat))

            # We have our data, ok.  What of it isn't recent?
            blocksLatest = self._getBlocks(tsLatest, schema['intervals'])
            blockLatest = blocksLatest[layer]
            if blockStart[0] <= blockLatest[0] < blockStop[0]:
                # Latest stats between requested, need to set some zeroes
                for b in xrange(blockLatest[0] + 1, blockStop[0] + 1):
                    data[-1 + b - blockStop[0]] = 0
            elif blockStop[0] < blockStart[0] < blockLatest[0]:
                # Same
                for b in xrange(blockLatest[0] - blockStart[0] + 1, len(data)):
                    data[b] = 0
            elif blockLatest[0] < blockStop[0] < blockStart[0]:
                # Same
                for b in xrange(blockLatest[0] + 1, blockStop[0] + 1):
                    data[-1 + b - blockStop[0]] = 0

            return {
                'values': data
                , 'tsStart': blockStart[3]
                , 'tsInterval': blockStart[2]
            }
        raise ValueError("stop is too far in the past: {0}".format(past))


    def _getBlocks(self, timeVal, schemaIntervals):
        """For a given time value and schema intervals, return an array where
        each member corresponds to the given layer's block index.

        Returns [ (index, blockCountInLayer, blockLenSeconds, blockStartTime) ]
        """
        blocks = []
        realInterval = 1 # We get multiplied at each step
        for i in schemaIntervals:
            # Intervals are (blockLenSeconds, blockLifeDays)
            realInterval *= i[0]
            blockCount = int((i[1] * 24*60*60) / realInterval)
            blockIndexTotal = int(timeVal / realInterval)
            blockStart = blockIndexTotal * realInterval
            idx = blockIndexTotal % blockCount
            blocks.append((idx, blockCount, realInterval, blockStart))
        return blocks


    def _getSchemaFor(self, statName):
        for p in self._schema['prefixes']:
            if statName.startswith(p['prefix']):
                return p
        return None


    def _getTimeVal(self, time):
        """Converts a datetime.datetime object (or None for utcnow) to a 
        seconds since epoch number.
        """
        if time is None:
            time = datetime.datetime.utcnow()
        elif not isinstance(time, datetime.datetime):
            raise ValueError("time must be datetime.datetime")
        return (time - self._epoch).total_seconds()


    def _tryNewStat(self, schema, name, timeVal, randomize = False):
        """Try to insert a new document with the given schema and stat name.

        Return True if new stat made ok, False if it already was made.

        randomize -- For debugging.  Instead of filling with zeroes, fill with
                random data from 10 to 100.
        """
        blocks = {}
        blockData = self._getBlocks(timeVal, schema['intervals'])
        for layer, data in enumerate(blockData):
            slayer = str(layer)
            if not randomize:
                blocks[slayer] = [ 0 ] * data[1]
            else:
                blocks[slayer] = [ (i % 100) + 10 for i in xrange(data[1]) ]
            
        newDoc = {
            '_id': name
            , 'tsLatest': timeVal
            , 'block': blocks
        }
        try:
            self._col.insert(newDoc, safe = True)
        except pymongo.errors.DuplicateKeyError:
            return False
        return True


