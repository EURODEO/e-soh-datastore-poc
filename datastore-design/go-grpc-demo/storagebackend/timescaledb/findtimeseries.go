package timescaledb

import (
	"datastore/datastore"

	_ "github.com/lib/pq"
)

/* OBSOLETE
// retrieveObsForTS retrieves into obs observations for time series tsID in
// open-ended range [fromTime, toTime>.
// Returns nil upon success, otherwise error.
func retrieveObsForTS(
	db *sql.DB, tsID int64, fromTime, toTime *timestamppb.Timestamp, obs *[]*datastore.Observation) error {

	rows, err := db.Query(`
		SELECT EXTRACT(EPOCH FROM tstamp), value, field1, field2 FROM observations
		WHERE ts_id = $1
		AND tstamp >= to_timestamp($2)
		AND tstamp <  to_timestamp($3)
		ORDER BY tstamp ASC
	`, tsID, float64(fromTime.Seconds)+float64(fromTime.Nanos)/1e9, float64(toTime.Seconds)+float64(toTime.Nanos)/1e9)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	for rows.Next() {
		var (
			obsTime        float64
			obsVal         float64
			field1, field2 string
		)
		if err := rows.Scan(&obsTime, &obsVal, &field1, &field2); err != nil {
			return fmt.Errorf("rows.Scan() failed: %v", err)
		}
		intpart, div := math.Modf(obsTime)
		(*obs) = append(*obs, &datastore.Observation{
			Time:  &timestamppb.Timestamp{Seconds: int64(intpart), Nanos: int32(div * 1e9)},
			Value: obsVal,
			Metadata: &datastore.ObsMetadata{
				Field1: field1,
				Field2: field2,
			},
		})
	}

	return nil
}
*/

// FindTimeSeries ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) FindTimeSeries(request *datastore.FindTSRequest) (
	*datastore.FindTSResponse, error) {

	// TODO: parse request ...

	// // TODO: validate request.Tsids (ensure it doesn't contains duplicates etc.)
	// tsObs := make([]*datastore.TSObservations, len(request.Tsids))
	// for i, tsID := range request.Tsids {
	// 	obs := []*datastore.Observation{}
	// 	if err := retrieveObsForTS(
	// 		sbe.Db, tsID, request.Fromtime, request.Totime, &obs); err != nil {
	// 		return nil, fmt.Errorf("retrieveObsForTS() failed (i: %d, tsID: %d): %v", i, tsID, err)
	// 	}
	// 	tsObs[i] = &datastore.TSObservations{
	// 		Tsid: tsID,
	// 		Obs:  obs,
	// 	}
	// }

	// FOR NOW RETURN EMPTY RESULT
	return &datastore.FindTSResponse{}, nil
}
