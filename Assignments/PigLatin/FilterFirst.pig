REGISTER file:/home/hadoop/lib/pig/piggybank.jar;
DEFINE  CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
--load the flights data file as Flights1
Flights1 = LOAD '$INPUT' USING CSVLoader() AS 
(Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier,
	AirlineID,Carrier,TailNum,FlightNum,Origin,OriginCityName,
	OriginState,OriginStateFips,OriginStateName,OriginWac,Dest,
	DestCityName,DestState,DestStateFips,DestStateName,DestWac,
	CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,
	DepartureDelayGroups,DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,
	TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes,ArrDel15,
	ArrivalDelayGroups,ArrTimeBlk,Cancelled,CancellationCode,
	Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Flights,
	Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,
	SecurityDelay,LateAircraftDelay);
--load the flights data file as Flights2
Flights2 = LOAD '$INPUT' USING CSVLoader() AS 
(Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier,
	AirlineID,Carrier,TailNum,FlightNum,Origin,OriginCityName,
	OriginState,OriginStateFips,OriginStateName,OriginWac,Dest,
	DestCityName,DestState,DestStateFips,DestStateName,DestWac,
	CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,
	DepartureDelayGroups,DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,
	TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes,ArrDel15,
	ArrivalDelayGroups,ArrTimeBlk,Cancelled,CancellationCode,
	Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Flights,
	Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,
	SecurityDelay,LateAircraftDelay);
--remove the records in Flights1 that don't have Origin "ORD" and 
--are not cancelled or diverted and 
--are not in the required range (June 2007-May 2008)
filterOrigin = FILTER Flights1 BY Origin eq 'ORD' AND Dest neq 'JFK' 
				AND Cancelled eq '0.00' AND Diverted eq '0.00' 
				AND ((Year eq '2007' and (not (Month < 6))) 
					or (Year eq '2008' and (not (Month > 5))));
--remove the records in Flights2 that don't have Dest "JFK" 
--and are not cancelled or diverted
--and are not in the required range (June 2007-May 2008)
filterDest = FILTER  Flights2 BY Origin neq 'ORD' AND Dest eq 'JFK' 
				AND Cancelled eq '0.00' AND Diverted eq '0.00'
				AND ((Year eq '2007' and (not (Month < 6))) 
					or (Year eq '2008' and (not (Month > 5))));
--remove the attributes that are not needed from both Flights1 and Flights2 
NeededInfo1 = FOREACH filterOrigin GENERATE FlightDate, Dest, ArrTime, ArrDelayMinutes;
NeededInfo2 = FOREACH filterDest GENERATE FlightDate, Origin, DepTime, ArrDelayMinutes;
--join Flights1 and Flights2 that have the same flightDate and
-- the dest of Flights1 matches the origin of Flights2
Joined_results = JOIN NeededInfo1 BY (FlightDate, Dest), NeededInfo2 
									BY (FlightDate, Origin) PARALLEL 10;
--filter out the tuples where the departure time in Flights2 
--is not after the arrival time in Flights1, find the two-leg flights
filter_twoleg = FILTER Joined_results BY ArrTime < DepTime;
--calcute the delay time for each two-leg flight
calcu_delays = FOREACH filter_twoleg GENERATE ($3 + $7) AS delayMinutes;
--group all the results
grouped = GROUP calcu_delays ALL PARALLEL 10;
--calculate the average delay time
avg_delay = FOREACH grouped GENERATE AVG(calcu_delays.delayMinutes);
--output the average delay time
STORE avg_delay INTO '$OUTPUT' USING PigStorage(',');
