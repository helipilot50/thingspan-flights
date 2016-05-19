objy startlockserver
objy exportSchema -outFile flightsSchema.xml -over -bootfile data/flights.boot
objy deletefd -bootfile data/flights.boot
objy createfd -fdname flights -fdDirPath data
