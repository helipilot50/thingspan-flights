var flightData;

var flightsController = (function() {
	var context = 'http://localhost:8080/flights/';
	var getFlightData = function(fromDate, toDate) {
		function pad(datePart, len) {
			if (!len) {
				len = 2;
			}
			if (typeof datePart !== 'string') {
				datePart = ''+datePart;
			}
			var x = '0000' + datePart;
			return x.substr(-len);
		}
		function dateToStr(date) {
			if(typeof date === 'string') {
				return date;
			}
			else {
				return date.getFullYear() +
					pad(1+date.getMonth()) + 
					pad(date.getDate()) +
					pad(date.getHours()) + 
					pad(date.getMinutes());
			}
		}
		var url = context + dateToStr(fromDate) + "/" +dateToStr(toDate);
		console.log(url);
		return $.ajax({
			url: url
		})
	}
	return {
		getFlightData: getFlightData
	};
})();

