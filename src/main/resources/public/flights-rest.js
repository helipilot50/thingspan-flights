var flightData

$.ajax({
    
    url: 'http://localhost:8080/flights/201201170000/201201171200',
    success: function(flights) {
    	flightData = flights;
//    	console.log(flightData.length)
//    	console.log(flightData[0]);
//    	console.log(flightData[1]);
//    	console.log(flightData[2]);
//    	console.log(flightData[3]);
//    	console.log(flightData[4]);
//    	console.log(flightData[5]);
    }
});