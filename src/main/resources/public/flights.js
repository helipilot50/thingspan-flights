//
//     Copyright © 2011-2016 Cambridge Intelligence Limited. 
//     All rights reserved.
//
//     Sample Code modified to display flight graph
//

var chart;
var timebar;

var linkitems;

var baseColor = '#c0c0c0';
var selectionColours = ['rgb(114, 179, 0)', 'rgb(255, 153, 0)', 'rgb(178, 38, 9)'];

function linkWidth(count) {
  var limit = 40;
  var result =  1 + 0.05 * count;
  return Math.min(limit, result);
}

function prepareItems() {
  var nodeids = {};
  var links = {};
  
  _.each(flightData, function (flight) {
	  console.log(flight)
      var from, to;

      from = flight.origin;
      to = flight.destination;

      var id = flight.flightDate + "-" + from + '-' + to + '-' + flight.carrier + '-' + flight.flightNumber;
      console.log(id)
      var dt = new Date(flight.flightDate + " " + flight.departureTime);

      nodeids[from] = true;
      nodeids[to] = true;

      if (links[id]) {
        links[id].dt.push(dt);
      } else {
        links[id] = {
          type: 'link',
          id: id,
          id1: from,
          id2: to,
          dt: [dt],
          w: 1,
          a2 : true
        };
      }
  });

  var nodeitems = _.map(nodeids, function(v, k) {
    return {
      type: 'node',
      c: '#c0c0c0',
      id: k,
      t: k
    };
  });

  linkitems = _.values(links);

  _.each(linkitems, function(link) {
    link.w = linkWidth(link.dt.length);
  });

  var chartData = {
    type: 'LinkChart',
    items: nodeitems.concat(linkitems)
  };

  timebar.load(chartData, function () {
    timebar.zoom('fit', { animate: false });
  });
  chart.load(chartData, chart.layout);
}

// Find the neighbours of the selection to n levels deep
function computeNeighboursOfSelection() {
  var items = chart.selection();

  if (items.length > 0) {
    var ids = {};
    var levels = 2;

    function addItems() {
      for (var i = 0; i < items.length; i++) {
        ids[items[i]] = true;
      }
    }

    addItems();

    while (levels-- > 0) {
      var n = chart.graph().neighbours(items);
      items = n.links.concat(n.nodes);
      addItems();
    }

    return ids;
  } else {
    return null;
  }
}

function afterFiltering(nodesShown, nodesHidden, selBefore) {

  function finish() {

    //if the nodes displayed have changed then run a tweak layout
    if ((nodesShown.length > 0) || (nodesHidden.length > 0)) {
      chart.layout('tweak', {animate: true, time: 1000, easing: 'linear'});
    }

    if (nodesShown.length > 0) {
      chart.ping(nodesShown, {time: 1500, c:'rgb(178, 38, 9)'});
    }

    // If our filtering has hidden any selected items, then we need to update
    // the selection lines in the timebar
    if (selBefore.length !== chart.selection().length) {
      showSelection();
    }

  }

  var connectedItems = $('#selonly').is(':checked') ? computeNeighboursOfSelection() : null;

  function isConnected(item) {
    return connectedItems[item.id];
  }

  if (connectedItems) {
    chart.filter(isConnected, { animate: false }, function (result) {
      //add any newly shown nodes to the list so we get haloes on them too
      nodesShown = nodesShown.concat(result.shown.nodes);
      finish();
    });
  } else {
    finish();
  }

}

function timebarChanged() {
  if ($('#hide').is(':checked')) {
    var selBefore = chart.selection();
    chart.filter(timebar.inRange, { animate: false, type: 'link' }, function (result) {
      afterFiltering(result.shown.nodes, result.hidden.nodes, selBefore);
    });
  } else {
    chart.foreground(timebar.inRange, { type: 'link' });
  }
}

// When the user changes the hide checkbox, swap items from
// being hidden to background items
function hideToggled() {
  var hi = $('#hide').is(':checked');
  var bg = !hi;
  var was = hi ? 'bg' : 'hi';
  var items = [];

  chart.each({ type: 'all' }, function (item) {
    if (item[was]) {
      items.push({id:item.id, bg: bg, hi: hi});
    }
  });

  if (items.length > 0) {
    chart.setProperties(items);
  }
}

function showSelection() {

  var visualProperties = {};

  chart.each({type:'all'}, function(item) {
    visualProperties[item.id] = {id: item.id, c: baseColor, ha1: false};
  });

  var selectedIds = chart.selection();

  //we only colour the first few items of the selection
  selectedIds = selectedIds.slice(0, selectionColours.length);
  
  // clear all selections
  timebar.selection([]);

  var items = [];

  _.each(selectedIds, function (id, index) {

    var fromNodes = [];
    var links = [];
    var toNodes = [];

    var item = chart.getItem(id);

    if (item.type === 'node') {

      fromNodes.push(id);

      //note we use all: true so that we apply styles also to
      //hidden nodes and links that aren't in the current time range
      var toNeighbours = chart.graph().neighbours(id, {direction: 'from', all: true});
      links = toNeighbours.links;
      toNodes = toNeighbours.nodes;

    }
    else {

      //it is a link
      links.push(id);

      //use the arrows to decide which list to put the nodes on
      (item.a1 ? toNodes : fromNodes).push(item.id1);
      (item.a2 ? toNodes : fromNodes).push(item.id2);

    }

    _.each(fromNodes.concat(links), function (id) {
      visualProperties[id].c = selectionColours[index];
    });

    _.each(toNodes, function (id) {
      visualProperties[id].ha1 = {w: 5, r: 27, c: selectionColours[index]};
    });

    //and select in the timebar
    //be careful to include the id of the thing selected too - this is necessary when selecting links only
    items.push({id: links, index: index, c: selectionColours[index]});

  });

  timebar.selection(items);

  chart.setProperties(_.values(visualProperties));
}

function klReady(err, ready) {

  chart = ready[0];
  timebar = ready[1];

  // Chart events to react to
  chart.bind('selectionchange', timebarChanged);
  chart.bind('selectionchange', showSelection);

  //Time bar events to react to
  timebar.bind('change', timebarChanged);
  timebar.bind('hover', showTooltip);

  // load the data into timebar and chart
  prepareItems();

  $('#hide').click(hideToggled);
  $('#selonly').click(timebarChanged);

  $('#hide').click(function () {
    var checked = $('#hide').is(':checked');
    $('#selonly').prop('disabled', !checked);
    $('#solab').css('color', checked ? '' : 'silver');
  });
}

function showTooltip(type, index, value, x, y, t1, t2){

  // the offseet is taken from the top-left corner of the chart
  // calculate the height of the chart to start from the top-left of the timebar
  var offset = {top: $('#kl').height(), left: 0};
  
  var toShow = (type === 'bar' || type === 'selection');

  if (toShow) {

    // change the content
    $('#tooltipText').text( "Value: "+ value );

    // selection colour or default colour for bar hover
    var tooltipColour = selectionColours[index] || '';
    
    // style both the tooltip body and the arrow
    $('.tooltip-arrow').css('border-top-color', tooltipColour);
    $('.tooltip-inner').css('background-color', tooltipColour);
    
    // in case of default colour or red (selection 2)
    var isBetterWhite = tooltipColour.length === 0 || index > 1;

    // white works better in same cases
    $('.tooltip-inner').css('color', isBetterWhite ? 'white' : 'black');

    // the top needs to be adjusted to accommodate the height of the tooltip
    offset.top  += y - $('#tooltip')[0].offsetHeight;

    // shift left by half width to centre it
    offset.left = x - $('#tooltip')[0].offsetWidth / 2;
  }
  
  // set the position and toggle the "in" class to show/hide the tooltip
  $('#tooltip')
    .css('left', offset.left)
    .css('top', offset.top)
    .toggleClass('in', toShow);
    
}

var chartOptions = {
  handMode: true,
  logo: 'assets/Logo.png',
  overview: {icon: false, shown: false},
  selectionColour: 'rgba(255, 255, 255, 0)', 
  selectionFontColour: 'white'
};

//Note: use window.load for HTML5 canvas to ensure fonts are loaded
$(window).load(function () {
  KeyLines.paths({ assets: 'assets/',
    flash: { swf: 'swf/keylines.swf', swfObject: 'js/swfobject.js', expressInstall: 'swf/expressInstall.swf' }});
  
  KeyLines.create([{id:'kl', type:'chart', options: chartOptions}, {id: 'tl', type: 'timebar'}], klReady);
});

