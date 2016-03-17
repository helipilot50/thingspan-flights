<!DOCTYPE html>
<html lang="en">
  <head>
    <title>Flight Graph - Thingspan</title>
    <link rel="stylesheet" type="text/css" href="css/bootstrap.css">
    <link rel="stylesheet" type="text/css" href="css/keylines.css">
    <link rel="stylesheet" type="text/css" href="css/minimalsdk.css">
    <link rel="stylesheet" type="text/css" href="flights.css">
  </head>
  <body>
  <%
    double num = Math.random();
    if (num > 0.95) {
  %>
      <h2>You'll have a luck day!</h2><p>(<%= num %>)</p>
  <%
    } else {
  %>
      <h2>Well, life goes on ... </h2><p>(<%= num %>)</p>
  <%
    }
  %>
  <a href="<%= request.getRequestURI() %>"><h3>Try Again</h3></a>
    <div class="container">
      <h2 id="title">Flight Graph - Thingspan</h2>
      <div class="row">
        <div id="lhs" class="span8">
          <div class="cichart">
            <div id="kl" style="width: 768px; height: 580px;"></div>
          </div>
          <div class="citimebar">
            <div id="tl" style="width:768px;height:170px;"></div>
            <div id="tooltip" style="position: absolute; pointer-events: none;" class="tooltip top">
              <div class="tooltip-arrow"></div>
              <div style="white-space:pre-wrap;" class="tooltip-inner">
                <p id="tooltipText"></p>
              </div>
            </div>
          </div>
        </div>
        <div id="rhs" class="span4">
          <div id="rhscontent" style="border-width: 0 0 2px 2px; padding: 5px;" class="citext border">
            <p></p>
            <div class="cicontent cipad">
              <fieldset>
                <p>
                  Click on nodes or links in the chart to see their activity over time in the time bar. Shift-click to select multiple
                  items in the chart and compare their time profiles - the time bar displays up to three different selections.
                </p>
              </fieldset>
              <legend>Options</legend>
              <p>
                The first check-box below controls whether items outside the time bar's time range are hidden or backgrounded. When the
                second checkbox is checked, only items within two steps of a selected item are shown in the chart. This allows you
                to focus on activity in the immediate neighbourhood of an item of interest.
              </p>
              <label class="checkbox">
                <input id="hide" type="checkbox" checked="checked">Hide items outside the current time range
              </label>
              <label style="margin-left:20px" class="checkbox">
                <input id="selonly" type="checkbox" checked="checked">
                <p id="solab">and show only neighbours of selection</p>
              </label>
              <form class="dateRange" id="dateRange">
    			<legend>Date range:</legend>
    				<div class="field">
    				<span>From:</span><input name="fromDate" type="date"><input name="fromTime" type="time"></div>
     				<div class="field"><span>To:</span><input name="toDate" type="date"><input name="toTime" type="time"></div>
     				<input id="flightDates" type="button" value="Click Me">
  			 </form>
            </div>
          </div>
        </div>
        <div class="clearfix"></div>
      </div>
    </div>
    <div id="moreParent">
      <div id="moreContainer">
      </div>
    </div>
        <script src="vendor/json2.js" type="text/javascript"></script>
    <script src="vendor/jquery.js" type="text/javascript"></script>
    <script src="js/keylines.js" type="text/javascript"></script>
    <script src="js/demo.js" type="text/javascript"></script>
    <script src="vendor/underscore.js" type="text/javascript"></script>
    <script src="flights-rest.js" type="text/javascript"></script>
    <script src="flights.js" type="text/javascript"></script>
    
  </body>
</html>