package postgresql

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/cridenour/go-postgis"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	tsStringMdataGoNames []string // Go names for time series metadata of type string
	tsStringMdataPBNames []string // protobuf names for time series metadata of type string

	obspb2go map[string]string // association between observation specific protobuf name and
	// (generated by protoc) Go name.
	// NOTES:
	// - Protobuf names are field names of the ObsMetadata message in datastore.proto.
	// - Only fields of string type are included
	// - The observation value field is not included
	// - Protobuf names are identical to corresponding database column names.
	// - Protobuf names are snake case (aaa_bbb) whereas Go names are camel case (aaaBbb or AaaBbb).

	obsStringMdataGoNames []string // Go names for observation metadata of type string
	obsStringMdataCols []string // column names qualified with table name 'observation'
)

func init() {
	tsStringMdataGoNames = []string{}
	tsStringMdataPBNames = []string{}
	for _, field := range reflect.VisibleFields(reflect.TypeOf(datastore.TSMetadata{})) {
		if field.IsExported() && (field.Type.Kind() == reflect.String) {
			goName := field.Name
			tsStringMdataGoNames = append(tsStringMdataGoNames, goName)
			tsStringMdataPBNames = append(tsStringMdataPBNames, common.ToSnakeCase(goName))
		}
	}

	obspb2go = map[string]string{}
	obsStringMdataGoNames = []string{}
	obsStringMdataCols = []string{}
	for _, field := range reflect.VisibleFields(reflect.TypeOf(datastore.ObsMetadata{})) {
		if field.IsExported() && (field.Type.Kind() == reflect.String) &&
		(strings.ToLower(field.Name) != "value") { // (obs value not considered metadata here)
			goName := field.Name
			pbName := common.ToSnakeCase(goName)
			obspb2go[pbName] = goName
			obsStringMdataGoNames = append(obsStringMdataGoNames, goName)
			obsStringMdataCols = append(obsStringMdataCols, fmt.Sprintf("observation.%s", pbName))
		}
	}
}

// addStringMdata ... (TODO: document!)
func addStringMdata(rv reflect.Value, stringMdataGoNames []string, colVals []interface{}) error {
	for i, goName := range stringMdataGoNames {
		val, ok := colVals[i].(string)
		if !ok {
			return fmt.Errorf("colVals[%d] not string: %v (type: %T)", i, colVals[i], colVals[i])
		}

		field := rv.Elem().FieldByName(goName)

		// NOTE: we assume the following assignemnt will never panic, hence we don't do
		// any pre-validation of field
		field.SetString(val)
	}

	return nil
}

// addWhereCondMatchAnyPattern appends to whereExpr an expression of the form
// "(cond1 OR cond2 OR ... OR condN)" where condi tests if the ith pattern in patterns matches
// colName. Matching is case-insensitive and an asterisk in a pattern matches zero or more
// arbitrary characters. The patterns with '*' replaced with '%' are appended to phVals.
func addWhereCondMatchAnyPattern(
	colName string, patterns []string, whereExpr *[]string, phVals *[]interface{}) {

	if (patterns == nil) || (len(patterns) == 0) {
		return
	}

	whereExprOR := []string{}

	index := len(*phVals)
	for _, ptn := range patterns {
		index++
		expr := fmt.Sprintf("(lower(%s) LIKE lower($%d))", colName, index)
		whereExprOR = append(whereExprOR, expr)
		*phVals = append(*phVals, strings.ReplaceAll(ptn, "*", "%"))
	}

	*whereExpr = append(*whereExpr, fmt.Sprintf("(%s)", strings.Join(whereExprOR, " OR ")))
}

// scanTSRow ... (TODO: document!)
func scanTSRow(rows *sql.Rows) (*datastore.TSMetadata, int64, error) {

	var (
		tsID int64
		linkHref pq.StringArray
		linkRel  pq.StringArray
		linkType pq.StringArray
		linkHrefLang pq.StringArray
		linkTitle pq.StringArray
	)

	// initialize colValPtrs
	colValPtrs := []interface{}{
		&tsID,
		&linkHref,
		&linkRel,
		&linkType,
		&linkHrefLang,
		&linkTitle,
	}

	// complete colValPtrs with string metadata
	colVals0 := make([]interface{}, len(tsStringMdataGoNames))
	for i := range tsStringMdataGoNames {
		colValPtrs = append(colValPtrs, &colVals0[i])
	}

	// scan row into column value pointers
	if err := rows.Scan(colValPtrs...); err != nil {
		return nil, -1, fmt.Errorf("rows.Scan() failed: %v", err)
	}

	// initialize tsMdata with non-string metadata
	links := []*datastore.Link{}
	for i := 0; i < len(linkHref); i++ {
		links = append(links, &datastore.Link{
			Href:     linkHref[i],
			Rel:      linkRel[i],
			Type:     linkType[i],
			Hreflang: linkHrefLang[i],
			Title:    linkTitle[i],
		})
	}
	tsMdata := datastore.TSMetadata{
		Links: links,
	}

	// complete tsMdata with string metadata
	err := addStringMdata(reflect.ValueOf(&tsMdata), tsStringMdataGoNames, colVals0)
	if err != nil {
		return nil, -1, fmt.Errorf("addStringMdata() failed: %v", err)
	}

	return &tsMdata, tsID, nil
}

// getTSMetadata retrieves into tsMdatas metadata of time series in table time_series that match
// tsIDs. The keys of tsMdatas are the time series IDs.
// Returns nil upon success, otherwise error
func getTSMetadata(db *sql.DB, tsIDs []string, tsMdatas map[int64]*datastore.TSMetadata) error {

	query := fmt.Sprintf(
		`SELECT id, %s FROM time_series WHERE %s`,
		strings.Join(getTSMdataCols(), ","),
		createSetFilter("id", tsIDs),
	)
	fmt.Printf("getTSMetadata(): query: %s\n", query)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		tsMdata, tsID, err := scanTSRow(rows)
		if err != nil {
			return fmt.Errorf("scanTSRow() failed: %v", err)
		}

		tsMdatas[tsID] = tsMdata
	}

	return nil
}

// getTimeFilter derives from ti the expression used in a WHERE clause for filtering on obs time.
// Returns expression.
func getTimeFilter(ti *datastore.TimeInterval) string {
	timeExpr := "TRUE" // by default, don't filter on obs time at all

	if ti != nil {
		timeExprs := []string{}
		if start := ti.GetStart(); start != nil {
			timeExprs = append(timeExprs, fmt.Sprintf(
				"obstime_instant >= to_timestamp(%f)", common.Tstamp2float64Secs(start)))
		}
		if end := ti.GetEnd(); end != nil {
			timeExprs = append(timeExprs, fmt.Sprintf(
				"obstime_instant < to_timestamp(%f)", common.Tstamp2float64Secs(end)))
		}
		if len(timeExprs) > 0 {
			timeExpr = fmt.Sprintf("(%s)", strings.Join(timeExprs, " AND "))
		}
	}

	// restrict to current valid time range
	loTime, hiTime := common.GetValidTimeRange()
	timeExpr += fmt.Sprintf(" AND (obstime_instant >= to_timestamp(%d))", loTime.Unix())
	timeExpr += fmt.Sprintf(" AND (obstime_instant <= to_timestamp(%d))", hiTime.Unix())

	return timeExpr
}

type stringFilterInfo struct {
	colName  string
	patterns []string
}
// TODO: add filter infos for other types than string

// getMdataFilter derives from stringFilterInfos the expression used in a WHERE clause for
// "match any" filtering on a set of attributes.
//
// The expression will be of the form
//
//	(
//	  ((<attr1 matches pattern1,1>) OR (<attr1 matches pattern1,2>) OR ...) AND
//	  ((<attr2 matches pattern2,1>) OR (<attr1 matches pattern2,2>) OR ...) AND
//	  ...
//	)
//
// Values to be used for query placeholders are appended to phVals.
//
// Returns expression.
func getMdataFilter(stringFilterInfos []stringFilterInfo, phVals *[]interface{}) string {

	whereExprAND := []string{}

	for _, sfi := range stringFilterInfos {
		addWhereCondMatchAnyPattern(
			sfi.colName, sfi.patterns, &whereExprAND, phVals)
	}

	whereExpr := "TRUE" // by default, don't filter
	if len(whereExprAND) > 0 {
		whereExpr = fmt.Sprintf("(%s)", strings.Join(whereExprAND, " AND "))
	}

	return whereExpr
}

// getGeoFilter derives from 'inside' the expression used in a WHERE clause for keeping
// observations inside this polygon.
// Returns expression.
func getGeoFilter(inside *datastore.Polygon, phVals *[]interface{}) (string, error) {
	whereExpr := "TRUE" // by default, don't filter
	if inside != nil {  // get all points
		points := inside.Points

		equal := func(p1, p2 *datastore.Point) bool {
			return (p1.Lat == p2.Lat) && (p1.Lon == p2.Lon)
		}

		if (len(points) > 0) && !equal(points[0], points[len(points)-1]) {
			points = append(points, points[0]) // close polygon
		}

		if len(points) < 4 {
			return "", fmt.Errorf("polygon contains too few points")
		}

		// construct the polygon ring of the WKT representation
		// (see https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry;
		// note that only a single ring is supported for now)
		polygonRing := []string{}
		for _, point := range points {
			polygonRing = append(polygonRing, fmt.Sprintf("%f %f", point.Lon, point.Lat))
		}

		srid := "4326" // spatial reference system ID

		index := len(*phVals) + 1
		whereExpr = fmt.Sprintf(
			"ST_DWITHIN(point, ST_GeomFromText($%d, %s)::geography, 0.0)", index, srid)
		*phVals = append(*phVals, fmt.Sprintf("polygon((%s))", strings.Join(polygonRing, ",")))
	}

	return whereExpr, nil
}

type stringFieldInfo struct {
	field reflect.StructField
	tableName string
	method reflect.Value
	methodName string
}

// getStringMdataFilter ... (TODO: document)
func getStringMdataFilter(
	request *datastore.GetObsRequest, phVals *[]interface{}) (string, error) {

	rv := reflect.ValueOf(request)

	stringFilterInfos := []stringFilterInfo{}

	stringFieldInfos := []stringFieldInfo{}

	addStringFields := func(s interface{}, tableName string) {
		for _, field := range reflect.VisibleFields(reflect.TypeOf(s)) {
			mtdName := fmt.Sprintf("Get%s", field.Name)
			mtd := rv.MethodByName(mtdName)
			if field.IsExported() && (field.Type.Kind() == reflect.String) && (mtd.IsValid()) {
				stringFieldInfos = append(stringFieldInfos, stringFieldInfo{
					field: field,
					tableName: tableName,
					method: mtd,
					methodName: mtdName,
				})
			}
		}
	}
	addStringFields(datastore.TSMetadata{}, "time_series")
	addStringFields(datastore.ObsMetadata{}, "observation")

	for _, sfInfo := range stringFieldInfos {
		patterns, ok := sfInfo.method.Call([]reflect.Value{})[0].Interface().([]string)
		if !ok {
			return "", fmt.Errorf(
				"sfInfo.method.Call() failed for method %s; failed to return []string",
				sfInfo.methodName)
		}
		if len(patterns) > 0 {
			stringFilterInfos = append(stringFilterInfos, stringFilterInfo{
				colName: fmt.Sprintf(
					"%s.%s", sfInfo.tableName, common.ToSnakeCase(sfInfo.field.Name)),
				patterns: patterns,
			})
		}
	}

	return getMdataFilter(stringFilterInfos, phVals), nil
}

// createObsQueryVals creates values used for querying observations.
// Upon success returns:
// - time filter used in 'WHERE ... AND ...' clause (possibly just 'TRUE')
// - geo filter ... ditto
// - string metadata ... ditto
// - nil
// Upon failure returns ..., ..., ..., error
func createObsQueryVals(
	request *datastore.GetObsRequest, phVals *[]interface{}) (string, string, string, error) {

	timeFilter := getTimeFilter(request.GetInterval())

	geoFilter, err := getGeoFilter(request.Inside, phVals)
	if err != nil {
		return "", "", "", fmt.Errorf("getGeoFilter() failed: %v", err)
	}

	stringMdataFilter, err := getStringMdataFilter(request, phVals)
	if err != nil {
		return "", "", "", fmt.Errorf("getStringMdataFilter() failed: %v", err)
	}

	return timeFilter, geoFilter, stringMdataFilter, nil
}

// scanObsRow ... (TODO: document!)
func scanObsRow(rows *sql.Rows) (*datastore.ObsMetadata, int64, error) {

	var (
		tsID            int64
		obsTimeInstant0 time.Time
		pubTime0        time.Time
		value           string
		point           postgis.PointS
	)

	// initialize colValPtrs
	colValPtrs := []interface{}{
		&tsID,
		&obsTimeInstant0,
		&pubTime0,
		&value,
		&point,
	}

	// complete colValPtrs with string metadata
	colVals0 := make([]interface{}, len(obsStringMdataGoNames))
	for i := range obsStringMdataGoNames {
		colValPtrs = append(colValPtrs, &colVals0[i])
	}

	// scan row into column value pointers
	if err := rows.Scan(colValPtrs...); err != nil {
		return nil, -1, fmt.Errorf("rows.Scan() failed: %v", err)
	}

	// initialize obsMdata with obs value and non-string metadata
	obsMdata := datastore.ObsMetadata{
		Geometry: &datastore.ObsMetadata_GeoPoint{
			GeoPoint: &datastore.Point{
				Lon: point.X,
				Lat: point.Y,
			},
		},
		Obstime: &datastore.ObsMetadata_ObstimeInstant{
			ObstimeInstant: timestamppb.New(obsTimeInstant0),
		},
		Pubtime: timestamppb.New(pubTime0),
		Value: value,
	}

	// complete obsMdata with string metadata
	err := addStringMdata(reflect.ValueOf(&obsMdata), obsStringMdataGoNames, colVals0)
	if err != nil {
		return nil, -1, fmt.Errorf("addStringMdata() failed: %v", err)
	}

	return &obsMdata, tsID, nil
}

// getObs gets into obs all observations that match request.
// Returns nil upon success, otherwise error.
func getObs(db *sql.DB, request *datastore.GetObsRequest, obs *[]*datastore.Metadata2) (retErr error) {

	// get values needed for query
	phVals := []interface{}{} // placeholder values
	timeFilter, geoFilter, stringMdataFilter, err := createObsQueryVals(request, &phVals)
	if err != nil {
		return fmt.Errorf("createQueryVals() failed: %v", err)
	}

	// define and execute query
	query := fmt.Sprintf(`
		SELECT
		    ts_id,
			obstime_instant,
			pubtime,
			value,
			point,
			%s
		FROM observation
		JOIN time_series on time_series.id = observation.ts_id
		JOIN geo_point ON observation.geo_point_id = geo_point.id
		WHERE %s AND %s AND %s
		ORDER BY ts_id, obstime_instant
	`, strings.Join(obsStringMdataCols, ","), timeFilter, geoFilter, stringMdataFilter)

	rows, err := db.Query(query, phVals...)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	obsMdatas := make(map[int64][]*datastore.ObsMetadata) // observations per time series ID

	// scan rows
	for rows.Next() {
		obsMdata, tsID, err := scanObsRow(rows)
		if err != nil {
			return fmt.Errorf("scanObsRow() failed: %v", err)
		}

		obsMdatas[tsID] = append(obsMdatas[tsID], obsMdata)
	}

	// get time series
	tsMdatas := map[int64]*datastore.TSMetadata{}
	tsIDs := []string{}
	for tsID := range obsMdatas {
		tsIDs = append(tsIDs, fmt.Sprintf("%d", tsID))
	}
	if err = getTSMetadata(db, tsIDs, tsMdatas); err != nil {
		return fmt.Errorf("getTSMetadata() failed: %v", err)
	}

	// assemble final output
	for tsID, obsMdata := range obsMdatas {
		*obs = append(*obs, &datastore.Metadata2{
			TsMdata:  tsMdatas[tsID],
			ObsMdata: obsMdata,
		})
	}

	return nil
}

// GetObservations ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetObservations(request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	var err error

	obs := []*datastore.Metadata2{}
	if err = getObs(
		sbe.Db, request, &obs); err != nil {
		return nil, fmt.Errorf("getObs() failed: %v", err)
	}

	return &datastore.GetObsResponse{Observations: obs}, nil
}
