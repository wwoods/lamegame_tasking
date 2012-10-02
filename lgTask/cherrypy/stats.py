
import cherrypy
import cherrypy.process
import json
from gzip import GzipFile
from io import BytesIO
import os
import threading
import time
import urllib, urllib2

class StatsRoot(object):
    def __init__(self, conn):
        self._conn = conn
        self._stats = conn.stats


    @cherrypy.expose
    def index(self):
        """Use d3 to render a basic page."""
        html = r"""<html>
                <head>
                    <title>D3 Test</title>
                    <link rel="stylesheet" type="text/css" href="../static/reset.css" />
                    <style type="text/css">
                        .display {
                            width: 100%;
                            height: 100%;
                            position: absolute;
                        }
                        .overtop {
                            position: absolute;
                            left: 200px;
                            top: 0;
                            color: #000;
                            background-color: #fff;
                        }
                        .graph {
                            width: 100%;
                            height: 100%;
                            position: absolute;
                        }
                        .controls {
                            width: 200px;
                            position: absolute;
                            left: 0;
                            top: 0;
                        }
                        .controls-collapse {
                            width: 100%;
                            height: 1.2em;
                            background-color: #efefff;
                            font-size: 0.8em;
                            cursor: pointer;
                        }
                    </style>
                    <script type="text/javascript" src="../static/json2.js"></script>
                    <script type="text/javascript" src="../static/jquery-1.7.2.min.js"></script>
                    <script type="text/javascript" src="../static/d3.v2.min.js"></script>
                    <script type="text/javascript" src="../static/class.js"></script>
                    <script type="text/javascript">
$.compareObjs = function(a, b) {
    //Returns true if a match, false otherwise
    if (typeof a === 'object' && typeof b === 'object') {
        for (var i in a) {
            if (!(i in b)) {
                return false;
            }
            if (!$.compareObjs(a[i], b[i])) {
                return false;
            }
        }
        for (var i in b) {
            if (!(i in a)) {
                return false;
            }
        }
    }
    else if (a !== b) {
        return false;
    }
    return true;
};

var UiBase = $.extend(
    Class.extend.call(
        $
        , {
            init: function(root) {
                if (typeof root === 'string') {
                    root = $(root);
                }
                root = root.domroot || root;
                this.length = 1;
                this[0] = root[0];
                this._root = root;
                this._root.addClass('d3-ui-base');
                this._root.data('ui-base', this);
            }
        })
        , {
            fromDom: function(dom) {
                if (dom instanceof UiBase) {
                    return dom;
                }
                else if (!(dom instanceof $)) {
                    dom = $([dom]);
                }
                return dom.data('ui-base') || null;
            }
        }
    );

var Graph = UiBase.extend({
    init: function(statsController, config) {
        this._super('<div class="graph"></div>');
    
        this._statsController = statsController;
        this._config = {
            stat: null,
            groups: [],
            smoothOver: 0, //Seconds to sum data over at each point.
            expectedPerGroup: 0, //Expected value (w/ sum) at each point.
            timeFrom: 0, //Epoch time (new Date().getTime() / 1000)
            timeTo: 'now', //Epoch time
            autoRefresh: 0, //Seconds between auto refresh after update()
                            //completes.
            graphPoints: 300, //Points on the graph
        };
        this._autoRefreshTimeout = null;
        this._autoRefreshNext = null;
        this.update(config);
    }
    ,
    update: function(configChanges) {
        var self = this;
        if (configChanges) {
            $.extend(this._config, configChanges);
        }
        
        //Clear old autorefresh
        if (this._autoRefreshTimeout !== null) {
            clearTimeout(this._autoRefreshTimeout);
            this._autoRefreshTimeout = null;
        }
        
        if (this._config.stat === null) {
            self.empty().append('<div class="overtop">No data selected</div>');
            return;
        }
        else {
            self.append('<div class="overtop">Loading data, please wait</div>');
        }
        
        //Set new autorefresh
        if (this._config.autoRefresh > 0) {
            this._autoRefreshNext = (new Date().getTime() / 1000) 
                    + this._config.autoRefresh;
        }
        else {
            this._autoRefreshNext = null;
        }

        var stat = this._config.stat;
        if (!(stat instanceof Stat)) {
            stat = this._statsController.stats[stat];
        }
        var targets = [];
        
        var groups = $.extend([], this._config.groups);
        var groupFiltersBase = self._statsController.groups;
        
        var groupFilters = {};
        for (var i = 0, m = groups.length; i < m; i++) {
            var group = groups[i];
            var baseValues = groupFiltersBase[group[0]];
            if (group[1] !== null) {
                var groupValues = [];
                groupFilters[group[0]] = groupValues;
                //group[1] is a regex that tells us to include a certain value
                for (var j = 0, k = baseValues.length; j < k; j++) {
                    if (group[1].test(baseValues[j])) {
                        groupValues.push(baseValues[j]);
                    }
                }
            }
            else {
                groupFilters[group[0]] = baseValues;
            }
        }
        for (var i = 0, m = stat.groups.length; i < m; i++) {
            var group = stat.groups[i];
            if (!(group in groupFilters)) {
                groups.push([ group ]);
                groupFilters[group] = groupFiltersBase[group];
            }
        }
        self._iterateTargets(targets, stat, {}, groups, 0, groupFilters);
        
        var timeTo = self._config.timeTo;
        if (timeTo === 'now') {
            timeTo = (new Date().getTime() / 1000);
        }
        $.ajax('getData', {
            type: 'POST',
            data: { targetListJson: JSON.stringify(targets), 
                    timeFrom: Math.floor(self._config.timeFrom 
                        - self._config.smoothOver),
                    timeTo: Math.floor(timeTo), },
            success: function(data) { self._onLoaded(data, timeTo); },
            error: function() { self.text("Failed to load data"); },
        });
    }
    ,
    _aggregateSourceData: function(rawData, pointTimes) {
        //Searches our statsController's stats for the stat matching 
        //rawLine, and aggregates it appropriately according to the stat
        //type.
        //
        //rawData - As per graphite's "raw" output:
        //statName, timestampFirst, timestampLast, timestampDelta, data...
        //
        //Returns a dict: 
        //{ stat: statName, groups: { group : value },
        //        values: [ array of values applicable at pointTimes ] }
        var self = this;
        
        var matchStat = null;
        var matchGroups = null;
        for (var statName in self._statsController.stats) {
            var stat = self._statsController.stats[statName];
            var match = stat.matchPath(rawData[0]);
            if (match !== null) {
                matchStat = stat;
                matchGroups = match;
                break;
            }
        }
        
        if (matchStat === null) {
            throw "Could not match stat: " + rawData[0];
        }
        
        var result = { stat: matchStat, groups: matchGroups };
        var values = [];
        result.values = values;
        
        var firstTime = self._config.timeFrom;
        //Don't add any post-smoothing points before timeFrom, since for
        //smoothing we had to request more data than we needed.
        //Note that srcIndex and srcTime are actually the index of the NEXT 
        //point to use
        var srcIndex = 4;
        var srcInterval = parseInt(rawData[3]);
        var srcTime = parseInt(rawData[1]); //beginning of data point, not end
        var srcTimeBase = srcTime;
        
        //Note that philosophically, we consider the time in each pointTimes
        //record to be from immediately after the last pointTime up to and 
        //including the next pointTime.
        var smoothSecs = self._config.smoothOver;
        //Keep track of originally requested smoothing for summations with
        //expected values
        var origSmooth = smoothSecs;
        if (smoothSecs < srcInterval) {
            //If no smoothing was specified, or at too small of a granularity,
            //use the data density.
            smoothSecs = srcInterval;
        }
        if (smoothSecs < pointTimes[1] - pointTimes[0]) {
            //Similarly, if smoothing is shorter than our point density, we
            //will use our point density instead
            smoothSecs = pointTimes[1] - pointTimes[0];
        }
        console.log("Final smooth: " + smoothSecs);
        
        var movingTotal = 0.0;
        var movingIndex = srcIndex;
        //we've removed up to this point in our current point:
        var movingTime = srcTime;
        //the start of our current point:
        var movingTimeBase = movingTime;
        for (var i = 0, m = pointTimes.length; i < m; i++) {
            //We're actually going to compute a moving sum of partial data 
            //points - that is, we assume our counters are uniformly distributed
            //between the two points they represent (dataTime through
            //dataTime + srcInterval, exclusive on the latter bound).  By
            //doing this, we avoid awkward discrete computations that can
            //really cause errors in certain situations (e.g. those with
            //expectedPerGroup or specific smoothing intervals)
            var newTail = pointTimes[i] - smoothSecs; 
            while (movingTime <= newTail) {
                //Take off of moving summation
                var timeLeft = newTail - movingTime;
                var partLeft = movingTimeBase + srcInterval - movingTime;
                if (timeLeft >= partLeft) {
                    //Take off the whole rest of the point
                    movingTotal -= (parseFloat(rawData[movingIndex]) * partLeft
                            / srcInterval);
                    movingTime = movingTimeBase + srcInterval;
                    movingTimeBase = movingTime;
                    movingIndex += 1;
                }
                else {
                    //Take off part of the point and we're done
                    movingTotal -= (parseFloat(rawData[movingIndex]) * timeLeft
                            / srcInterval);
                    movingTime = newTail;
                    break;
                }
            }
            while (srcIndex < rawData.length && srcTime <= pointTimes[i]) {
                //Moving summation
                var timeLeft = pointTimes[i] - srcTime;
                var partLeft = srcTimeBase + srcInterval - srcTime;
                if (timeLeft >= partLeft) {
                    //Rest of the point!
                    movingTotal += (parseFloat(rawData[srcIndex]) * partLeft
                            / srcInterval);
                    srcTime = srcTimeBase + srcInterval;
                    srcTimeBase = srcTime;
                    srcIndex += 1;
                }
                else {
                    //Partial point and done
                    movingTotal += (parseFloat(rawData[srcIndex]) * timeLeft
                            / srcInterval);
                    srcTime = pointTimes[i];
                    break;
                }
            }
            
            //Now, add!
            if (stat.type === 'count') {
                var value = movingTotal;
                if (self._config.expectedPerGroup > 0) {
                    //If we have an expected amount and we're not an average,
                    //then we'll need to scale according to the expected
                    //time scale.
                    value = value * origSmooth / smoothSecs;
                }
                values.push(value);
            }
            else if (stat.type === 'total') {
                values.push(movingTotal * srcInterval / smoothSecs);
            }
            else {
                throw "Stat type summation not defined: " + stat.type;
            }
        }
        
        //Return our data structure
        return result;
    }
    ,
    _iterateTargets: function(output, stat, statData, groups, groupIndex,
            groupValues) {
        if (groupIndex === groups.length) {
            output.push(stat.getTarget(statData));
            return;
        }
        
        var name = groups[groupIndex][0];
        var values = groupValues[name];
        for (var i = 0, m = values.length; i < m; i++) {
            statData[name] = values[i];
            this._iterateTargets(output, stat, statData, groups, 
                    groupIndex + 1, groupValues);
        }
    }
    ,
    _onLoaded: function(dataRaw, timeTo) {
        //timeTo is passed since it might be defined according to the request.
        var self = this;
        self.empty();
        var loadedText = $('<div class="overtop">Loaded, drawing graph</div>');
        loadedText.appendTo(self);
        
        //Set up next autorefresh first
        if (this._autoRefreshNext !== null) {
            var timeToGo = this._autoRefreshNext 
                    - (new Date().getTime() / 1000);
            if (timeToGo < 0) {
                this.update();
            }
            else {
                this._autoRefreshTimeout = setTimeout(function() {
                    self.update();
                }, timeToGo * 1000);
            }
        }
        
        //STEP 1 - Parse the data returned to us into datasets
        var dataSetsIn = dataRaw.split(/\n/g);
        var dataSetsRaw = [];
        for (var i = 0, m = dataSetsIn.length; i < m; i++) {
            var newSet = dataSetsIn[i].split(/[,\|]/g);
            if (newSet.length < 4) {
                //Empty / bad line
                continue;
            }
            dataSetsRaw.push(newSet);
        }
        
        //STEP 2 - Group those datasets into uniform collections of points
        //aggregated into each group(s) bucket
        //In other words, the data should look like this afterwards:
        //pointTimes = [ timeStampFor0, timeStampFor1, ... ]
        //data = { values: {...}, outerGroupValue : { values: { stat : valueList }, innerGroupValue : {...} } }
        //where each "values" is an array of the aggregate stats at that point
        //with timestamps matching pointTimes
        var pointTimes = [];
        var lastPoint = timeTo;
        var pointDiff = ((timeTo - self._config.timeFrom) 
                / self._config.graphPoints);
        for (var i = 0, m = self._config.graphPoints; i < m; i++) {
            pointTimes.unshift(lastPoint);
            lastPoint -= pointDiff; 
        }
        
        var data = { values: {} };
        for (var i = 0, m = dataSetsRaw.length; i < m; i++) {
            var dataSetData = self._aggregateSourceData(dataSetsRaw[i],
                    pointTimes);
            var dataSetName = dataSetData.stat.name;
            var myGroups = self._config.groups.slice();
            var dataOutput = data;
            while (true) {
                //Merge at this level
                if (!(dataSetName in dataOutput.values)) {
                    dataOutput.values[dataSetName] = dataSetData.values.slice();
                }
                else {
                    var valuesOut = dataOutput.values[dataSetName];
                    for (var j = 0, k = dataSetData.values.length; j < k; j++) {
                        valuesOut[j] += dataSetData.values[j];
                    }
                }
            
                //Look for the next group that needs the data
                var next = myGroups.shift();
                if (next === undefined) {
                    break;
                }
                var nextValue = dataSetData.groups[next[0]];
                if (nextValue === undefined) {
                    //This stat doesn't have our next group, so stop here.
                    break;
                }
                if (!(nextValue in dataOutput)) {
                    dataOutput[nextValue] = { values: {} };
                }
                dataOutput = dataOutput[nextValue];
            }
        }
        console.log(data);
        
        //==================ADAPTER CODE====================
        var dataSets = [];
        for (var groupVal in data) {
            var dataSet = data[groupVal];
            if (groupVal === 'values') {
                if (self._config.groups.length !== 0) {
                    //We have data to display, don't include the "values" set
                    continue;
                }
                dataSet = data;
            }
            var values = dataSet.values[self._config.stat.name];
            dataSets.push(values.map(function(a, i) {
                var value = a;
                if (self._config.expectedPerGroup != 0) {
                    value = Math.max(
                            self._config.expectedPerGroup - value,
                            0);
                }
                return { x: pointTimes[i], y: value, title: groupVal };
            }));
        }
        
        //--------------------------OLD (relevant) CODE-----------------------
        
        var display = self;
        var width = display.width();
        var height = display.height() - 32;
        var vis = d3.select(display[0])
            .append('svg')
                .attr('width', width).attr('height', height);
    
        var xmin = dataSets[0][0].x;
        var xmax = dataSets[0][dataSets[0].length - 1].x;
        var intervalMax = xmax;
        var intervalLength = 60;
        if ((xmax - xmin) / intervalLength > 20) {
            intervalLength *= 5;
        }
        if ((xmax - xmin) / intervalLength > 20) {
            intervalLength *= 4;
        }
        //An hour?
        if ((xmax - xmin) / intervalLength > 20) {
            intervalLength *= 3;
        }
        //4 hours?
        if ((xmax - xmin) / intervalLength > 20) {
            intervalLength *= 4;
        }
        //A day?
        if ((xmax - xmin) / intervalLength > 20) {
            intervalLength *= 6;
        }
        //5 Days?
        if ((xmax - xmin) / intervalLength > 20) {
            intervalLength *= 5;
        }
        //30 days?
        if ((xmax - xmin) / intervalLength > 20) {
            intervalLength *= 6;
        }
        //Now that we have an "optimal" length, try to align to the nearest 
        //whole time units!
        var intervalShift = 0;
        var maxDate = new Date();
        maxDate.setTime(intervalMax * 1000);
        var nextReset;
        
        nextReset = maxDate.getSeconds();
        if (intervalShift + nextReset < intervalLength) {
            intervalShift += nextReset;
            maxDate.setTime((intervalMax - intervalShift) * 1000);
        }
        nextReset = (maxDate.getMinutes() % 5) * 60;
        if (intervalShift + nextReset < intervalLength) {
            intervalShift += nextReset;
            maxDate.setTime((intervalMax - intervalShift) * 1000);
        }
        nextReset = (maxDate.getMinutes() % 20) * 60;
        if (intervalShift + nextReset < intervalLength) {
            intervalShift += nextReset;
            maxDate.setTime((intervalMax - intervalShift) * 1000);
        }
        nextReset = (maxDate.getMinutes() % 30) * 60;
        if (intervalShift + nextReset < intervalLength) {
            intervalShift += nextReset;
            maxDate.setTime((intervalMax - intervalShift) * 1000);
        }
        nextReset = (maxDate.getMinutes()) * 60;
        if (intervalShift + nextReset < intervalLength) {
            intervalShift += nextReset;
            maxDate.setTime((intervalMax - intervalShift) * 1000);
        }
        nextReset = (maxDate.getHours() % 2) * 60 * 60;
        if (intervalShift + nextReset < intervalLength) {
            intervalShift += nextReset;
            maxDate.setTime((intervalMax - intervalShift) * 1000);
        }
        nextReset = (maxDate.getHours() % 6) * 60 * 60;
        if (intervalShift + nextReset < intervalLength) {
            intervalShift += nextReset;
            maxDate.setTime((intervalMax - intervalShift) * 1000);
        }
        nextReset = (maxDate.getHours()) * 60 * 60;
        if (intervalShift + nextReset < intervalLength) {
            intervalShift += nextReset;
            maxDate.setTime((intervalMax - intervalShift) * 1000);
        }
        //Effect the interval
        intervalMax -= intervalShift;
        
        var intervals = [];
        while (intervalMax > xmin) {
            var d = new Date();
            d.setTime(intervalMax * 1000);
            
            var label;
            if (d.getHours() == 0 && d.getMinutes() == 0) {
                //Month : day timestamps
                var months = {
                    1: 'Jan',
                    2: 'Feb',
                    3: 'Mar',
                    4: 'Apr',
                    5: 'May',
                    6: 'Jun',
                    7: 'Jul',
                    8: 'Aug',
                    9: 'Sep',
                    10: 'Oct',
                    11: 'Nov',
                    12: 'Dec',
                };
                label = months[d.getMonth()] + d.getDate().toString();
            }
            else {
                //Hour : minute timestamps
                var hrs = d.getHours().toString();
                if (hrs.length < 2) {
                    hrs = '0' + hrs;
                }
                var mins = d.getMinutes().toString();
                if (mins.length < 2) {
                    mins = '0' + mins;
                }
                label = hrs + ':' + mins;
            }
            
            intervals.push({ 
                x: (intervalMax - xmin) * width / (xmax - xmin)
                , label: label
                });
                
            intervalMax -= intervalLength;
        }
        var axis = d3.select(display[0])
            .append('svg')
                .attr('width', width).attr('height', 32);
        var axisContext = axis.selectAll()
            .data(intervals)
            .enter();
        axisContext
            .append("text")
                .attr("text-anchor", "middle")
                .attr("x", function(d) { return d.x; })
                .attr("y", "18")
                .text(function(d) { return d.label; })
            ;
        axisContext
            .append("svg:line")
                .attr("x1", function(d) { return d.x; })
                .attr("x2", function(d) { return d.x; })
                .attr("y1", "0")
                .attr("y2", "5")
                .attr("stroke", "black")
            ;
        
        console.log(dataSets);
        //stack adds the "y0" property to datapoints, and stacks them
        var stack = d3.layout.stack().offset("wiggle")(dataSets);
        var ymax = Math.max.apply(Math, stack.map(
            function(a) {
                return a.reduce(
                    function(last, d) { return Math.max(d.y + d.y0, last); },
                    0);
            }
        ));
        window.stack = stack;
            
        function onMouseOver(d) {
            var x = d3.mouse(this)[0];
            var svgWidth = $(this).closest('svg').width();
            var idx = Math.floor(x * d.length / svgWidth);
            console.log("OVER " + d[0].title + ", " + d[idx].y);
        }
        function onMouseMove(d) {
            //onMouseOver.apply(this, arguments);
        }
        function onMouseOut() {
        }
        function onMouseDown() {
        }
        
        var relativeToAll = true;
        if (self._config.expectedPerGroup == 0) {
            relativeToAll = false;
        }
        function getAreaMethod() {
            var useYmax = dataSets.length * self._config.expectedPerGroup;
            if (!relativeToAll) {
                useYmax = ymax;
            }
            var area = d3.svg.area()
                .x(function(d) { return (d.x - xmin) * width / (xmax - xmin); })
                .y0(function(d) { return height - d.y0 * height / useYmax; })
                .y1(function(d) { return height - (d.y + d.y0) * height / useYmax; })
                ;
            return area;
        }
        
        //First render
        var color = d3.interpolateRgb("#aad", "#556");
        vis.selectAll("path")
            .data(stack).enter()
                .append("path")
                .style("fill", function() { return color(Math.random()); })
                .attr("d", getAreaMethod())
                .attr("title", function(d, i) { return d[0].title; })
                .on("mouseover", onMouseOver)
                .on("mousemove", onMouseMove)
                .on("mouseout", onMouseOut)
                .on("mousedown", onMouseDown)
                ;
                
        //Remove loaded message since we've drawn the SVG
        loadedText.remove();
        
        function redraw() {
            if (self._config.expectedPerGroup > 0) {
                relativeToAll = !relativeToAll;
            }
            vis.selectAll("path")
                .data(stack)
                .transition()
                    .duration(1000)
                    .attr("d", getAreaMethod())
                    ;
        }
        
        $('body').unbind('click').bind('click', function() {
            redraw();
        });
    }
});
    
var ListBox = UiBase.extend({
    init: function(isMultiple) {
        this._super('<select></select>');
        if (isMultiple) {
            this.attr('multiple', 'multiple');
        }
    }
    ,
    add: function(caption, value) {
        value = value || caption;
        $('<option></option>')
            .attr('value', value)
            .text(caption)
            .appendTo(this)
            ;
        if (this.children().length === 1) {
            this.trigger('change');
        }
    }
    ,
    reset: function() {
        this.empty();
    }
    ,
    select: function(value) {
        this.val(value);
        this.trigger('change');
    }
});

var Stat = Class.extend({
    init: function(params) {
        //See constructor...
        //type = 'count' | 'total'
        this.name = params.name;
        this.groups = params.groups;
        this.path = params.path;
        this.pathRegex = params.pathRegex;
        this.type = params.type;
        if (this.type == undefined) {
            this.type = 'count';
            if (/-sample$/.test(this.path)) {
                this.type = 'total';
            }
        }
    }
    ,
    getTarget: function(groupValues) {
        //groupValues - { group : value }
        //Returns a graphite-suitable target path (*'s for unspecified values)
        var target = this.path;
        for (var i = 0, m = this.groups.length; i < m; i++) {
            var group = this.groups[i];
            var toReplace = '{' + group + '}';
            var value = '*';
            if (group in groupValues) {
                value = groupValues[group];
            }
            target = target.replace(toReplace, value);
        }
        if (true) {
            //We're not using graphite, don't need these.
        }
        else if (this.type === 'count') {
            target = 'aliasSub(aliasSub(transformNull(sumSeries('
                    + target
                    + '), 0), ".*sumSeries\\(", ""), "\\).*", "")';
        }
        else if (this.type === 'total') {
            target = 'aliasSub(aliasSub(sumSeries(transformNull(keepLastValue('
                    + target
                    + '), 0)), ".*keepLastValue\\(", ""), "\\).*", "")';
        }
        else {
            throw 'Unknown type: ' + this.type;
        }
        return target;
    }
    ,
    matchPath: function(path) {
        //Returns null if this stat does not match the given path.
        //If this stat does match the given path, returns a dict:
        //{ group : value }
        var match = this.pathRegex.exec(path);
        this.pathRegex.lastIndex = 0; //Reset the regex
        
        var result = null;
        if (match) {
            result = {};
            for (var i = 0, m = this.groups.length; i < m; i++) {
                result[this.groups[i]] = match[i + 1];
            }
        }
        return result;
    }
});

var StatPath = Class.extend({
    init: function(path, statOptions) {
        this.statOptions = statOptions;
        //Find groups
        this.groups = [];
        var findGroup = /{([^}]*)}/g;
        var next = null;
        while ((next = findGroup.exec(path)) !== null) {
            this.groups.push(next[1]);
        }
        this.path = path;
        var isDir = false;
        if (/\.\*$/.test(this.path)) {
            isDir = true;
            this.path = this.path.substring(0, this.path.length - 2);
        }
        this.isDir = isDir;
        this.pathRegex = this.getRegexForPath(this.path, isDir);
    }
    ,
    getRegexForPath: function(path, isDir) {
        //Returns a regex for the given path
        var findGroup = /{([^}]*)}/g;
        var reString = '^' 
                + path.replace('.', '\\.').replace(findGroup, '([^\\.]*)');
        if (isDir) {
            reString += '\\.[^\\.]*';
        }
        else {
            reString += '$';
        }
        return new RegExp(reString, 'g');
    }
    ,
    matchStat: function(path) {
        //Returns null for no match or a dict:
        //{ name: 'statName', groups: [ ['group', 'value'] ], 
        //        path: 'myPathWithStatName', pathRegex: 'regex for stat' }
        var match = null;
        var result = { name: null, groups: [], path: null };
        var stat = null;
        
        match = this.pathRegex.exec(path);
        //Reset the regex so that exec works the next time
        this.pathRegex.lastIndex = 0;
        if (match === null) {
            return match;
        }
        
        for (var i = 0, m = this.groups.length; i < m; i++) {
            result.groups.push([ this.groups[i], match[i + 1] ]);
        }
        var name = match[0];
        for (var i = 1, m = this.groups.length + 1; i < m; i++) {
            var toReplace = match[i];
            if (i === 1 && this.path[0] === '{') {
                toReplace += '.';
            }
            else {
                toReplace = '.' + toReplace;
            }
            //Javascript replace() with strings only does first instance...
            //which is exactly what we want.
            name = name.replace(toReplace, '');
        }
        result.name = name;
        if (this.isDir) {
            var statPart = result.name.substring(
                    result.name.lastIndexOf('.') + 1);
            result.path = this.path + '.' + statPart;
        }
        else {
            result.path = this.path;
        }
        result.pathRegex = this.getRegexForPath(result.path);
        
        return result;
    }
});

var StatController = Class.extend({
    init: function() {
        this.stats = {};
        this.stats_doc = "{ name : { path : graphitePath w/ groups, groups: [ group names ] } }";
        this.groups = {};
        this.groups_doc = "{ name : [ possible values ] }";
        this.statPaths = [];
    }
    ,
    addStats: function(statPath, statOptions) {
        if (typeof statPath === 'string') {
            statPath = new StatPath(statPath, statOptions);
        }
        this.statPaths.push(statPath);
    }
    ,
    parseStats: function(stats) {
        for (var i = 0, m = stats.length; i < m; i++) {
            var stat = stats[i];
            for (var j = 0, k = this.statPaths.length; j < k; j++) {
                var path = this.statPaths[j];
                var result = path.matchStat(stat);
                if (result === null) {
                    continue;
                }
                
                var statInit = { name: result.name, path: result.path, 
                        pathRegex: result.pathRegex, groups: [] };
                $.extend(statInit, path.statOptions);
                var statDef = new Stat(statInit);
                for (var g = 0, h = result.groups.length; g < h; g++) {
                    var group = result.groups[g];
                    statDef.groups.push(group[0]);
                    if (!(group[0] in this.groups)) {
                        this.groups[group[0]] = [];
                    }
                    if ($.inArray(group[1], this.groups[group[0]]) < 0) {
                        this.groups[group[0]].push(group[1]);
                    }
                }
                if (result.name in this.stats) {
                    if (!$.compareObjs(statDef, this.stats[result.name])) {
                        console.log("Showing new def, then old def");
                        console.log(statDef);
                        console.log(this.stats[result.name]);
                        throw "Same stat name with different properties";
                    }
                }
                else {
                    this.stats[result.name] = statDef;
                } 
            }
        }
    }
});

var Controls = UiBase.extend({
    init: function() {
        var self = this;
        this._super('<div class="controls"></div>');
        this._content = $('<div class="controls-content"></div>');
        this.append(this._content);
        this._expandCollapse = $('<div class="controls-collapse">(click to collapse)</div>');
        this.append(this._expandCollapse);
        
        this.bind('click', function() {
            //Stop it from affecting graph
            return false;
        });
        
        this._expandCollapse.bind('click', function() {
            self._content.toggle();
        });
        
        var timeDiv = $('<div>Show last </div>');
        var timeDivAmt = $('<input type="text" />').appendTo(timeDiv);
        this._content.append(timeDiv);
        
        var lb = new ListBox();
        this._content.append(lb);
        lb.bind('change', function() {
            groupsActive.empty();
            groupList.reset();
            var stat = self._statsController.stats[lb.val()];
            for (var i = 0, m = stat.groups.length; i < m; i++) {
                groupList.add(stat.groups[i]);
            }
        });
        
        var groupsActive = new UiBase('<div></div>');
        groupsActive.delegate('div', 'click', function() {
            $(this).remove();
        });
        this._content.append('<div>Groups</div>');
        this._content.append(groupsActive);
        this._content.append('<div>Groups Available</div>');
        var groupList = new ListBox(true);
        this._content.append(groupList);
        groupList.delegate('option', 'click', function() {
            var g = $('<div></div>').text($(this).val()).appendTo(groupsActive);
            $('<input class="regex" type="text" />')
                //Stop click from removing the row
                .bind('click', function() { return false; })
                .appendTo(g);
        });
        
        var smootherDiv = $('<div></div>').appendTo(this._content);
        smootherDiv.append('Smooth hours: ');
        var smoother = $('<input type="text" />').appendTo(smootherDiv);
        smoother.val('1');
        
        var expectedDiv = $('<div></div>').appendTo(this._content);
        expectedDiv.append('Expected / grp (eval): ');
        var expected = $('<input type="text" />').appendTo(expectedDiv);
        expected.val('1');
        
        self._statsController = new StatController();
        var graph = new Graph(self._statsController);
        
        var ok = new UiBase('<input type="button" value="Refresh" />');
        this._content.append(ok);
        ok.bind('click', function() {
            var stat = self._statsController.stats[lb.val()];
            var groups = [];
            groupsActive.children().each(function() {
                var filter = $('.regex', $(this)).val();
                if (filter === '') {
                    filter = null;
                }
                else {
                    filter = new RegExp(filter);
                }
                groups.push([ $(this).text(), filter ]);
            });
            var smoothOver = parseFloat(smoother.val()) || 0;
            smoothOver = smoothOver * 60 * 60;
            var expectedVal = eval(expected.val()) || 0;
            var timeAmt = timeDivAmt.val();
            var timeFrom = new Date().getTime() / 1000;
            if (timeAmt === '') {
                timeFrom -= 12 * 24 * 60 * 60;
            }
            else if (/d(y|ay)?s?$/.test(timeAmt)) {
                //Days
                timeFrom -= parseFloat(timeAmt) * 24 * 60 * 60;
            }
            else if (/m(in|inute)?s?$/.test(timeAmt)) {
                //Minutes
                timeFrom -= parseFloat(timeAmt) * 60;
            }
            else {
                //Hours
                timeFrom -= parseFloat(timeAmt) * 60 * 60;
            }
            var options = { stat: stat, groups: groups, 
                    smoothOver: smoothOver, expectedPerGroup: expectedVal,
                    timeFrom: timeFrom, autoRefresh: 300 };
            graph.update(options);
        });
        
        $.ajax('getStats', {
            success: function(data) {
                if (typeof data === 'string') {
                    data = JSON.parse(data);
                }
                var stats = data.stats;
                window._stats = stats;

                for (var i = 0, m = data.paths.length; i < m; i++) {
                    var p = data.paths[i];
                    self._statsController.addStats(p.path, p.options);
                }

                self._statsController.parseStats(stats);
                console.log(self._statsController);
                
                for (var name in self._statsController.stats) {
                    lb.add(name);
                }
                
                //lb.select('repricedFreshPercent');
                //$('[value=dealer]', groupList).click();

                $('.display').empty().append(graph);
            }
        });
    }
});

$(function() {
    $('body').empty();
    var display = $('<div class="display"></div>').appendTo('body');
    var controls = window.controls = new Controls();
    $('body').append(controls);
    display.text("Loading stats, please wait");
});
                    </script>
                </head>
            <body>
                JS fail?
            </body></html>"""
        return html
    
    
    @cherrypy.expose
    def getData(self, targetListJson, timeFrom, timeTo):
        """Note that timeFrom and timeTo should be epoch seconds (UTC).

        Essentially because it is a really compressed format, we dump like so:
        stat,start,end,interval|data1,data2,data3,data4

        Each stat will be on a new line.
        """
        if '.' in timeFrom or '.' in timeTo:
            raise ValueError("timeFrom and timeTo may not have decimals")

        targetList = json.loads(targetListJson)
        results = []
        timeFrom = float(timeFrom)
        timeTo = float(timeTo)
        for target in targetList:
            stat = self._stats.getStat(target, timeFrom, timeTo
                , timesAreUtcSeconds = True
            )
            results.append(
                ','.join(
                    [ target, str(stat['tsStart']), str(stat['tsStop'])
                        , str(stat['tsInterval']) ]
                )
                + '|'
                + ','.join([ str(v) for v in stat['values'] ])
            )
        return '\n'.join(results)
        
        url = '/render'
        targetList = json.loads(targetListJson)
        urlParms = [
                # OLD : 'aliasSub(aliasByNode(offset(scale(movingAverage(sumSeriesWithWildcards(transformNull(dev.repricing.*.*.*.repricedFreshPercent, 0),3,4),360), -360), 1.0), 2), "^", "% SLA fail dealer ")',
                #('target', 'aliasSub(aliasByNode(sumSeriesWithWildcards(transformNull(dev.repricing.*.*.*.repricedFreshPercent, 0),3,4), 2), "^", "% SLA fail dealer ")'),
                ('format', 'raw'),
                ('from', timeFrom),
                ('until', timeTo),
                ]
        for target in targetList:
            # Graphite takes multiple "target" params for different lines
            urlParms.append(('target', target))
        return self._graphiteRequest(url, urlParms)
    
    
    @cherrypy.expose
    def getStats(self):
        """Returns a JSON blob about available stats.
        """
        cherrypy.response.headers['Content-Type'] = "application/json"
        result = {
            'stats': self._stats.listStats()
            , 'paths': [ 
                { 'path': 'sc-usage.{db}.*' }
                , { 'path': 'sc-usage-user.{user}.loads' }
                , { 'path': 'processors.{host}.*' }
            ]
        }
        return json.dumps(result)
    
    
    def _graphiteRequest(self, url, params = None, keepGzip = True):
        """Make a request to graphite using our credentials.
        
        keepGzip [True] - If True, set the response header content-encoding
                to GZIP and return it like we get it.
        """
        urlBase = 'http://rtg.picohost.net'
        # urlBase = 'http://vmpm.taofield.com'
        # urlBase = 'http://prodpm.taofield.com'
        data = params
        if data is not None:
            data = urllib.urlencode(data)
        req = urllib2.Request(urlBase + url, data = data,
                headers = { 'Authorization': 'Basic ZGV2OlBhcm9sYTg4MzQz',
                        'Accept-Encoding': 'gzip,deflate,sdch', }
                ) 
        result = urllib2.urlopen(req)
        data = result.read()
        headers = result.headers
        if keepGzip and headers.get('content-encoding') == 'gzip':
            # Don't bother extracting the information, just forward it
            cherrypy.response.headers['Content-Encoding'] = 'gzip'
        return data
    

