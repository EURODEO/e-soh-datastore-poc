package postgresql

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/lib/pq"
)

var (
	tspb2go map[string]string // association between time series specific protobuf name and
	// (generated by protoc) Go name.
	// NOTES:
	// - Protobuf names are field names the TSMetadata message in datastore.proto.
	// - Protobuf names are identical to corresponding database column names.
	// - Protobuf names are snake case (aaa_bbb) whereas Go names are camel case (aaaBbb or AaaBbb).
)

func init() {
	tspb2go = map[string]string{}
	for _, f := range reflect.VisibleFields(reflect.TypeOf(datastore.TSMetadata{})) {
		if f.IsExported() && (f.Type.Kind() == reflect.String) {
			// TODO: support non-string types, like the 'links' attribute
			goName := f.Name
			pbName := common.ToSnakeCase(goName)
			tspb2go[pbName] = goName
		}
	}
}

// getTSGoNamesFromPBNames returns Go names names corresponding to pbNames.
func getTSGoNamesFromPBNames(pbNames []string) ([]string, error) {
	goNames := []string{}
	for _, pbName := range pbNames {
		goName, found := tspb2go[pbName]
		if !found {
			return nil, fmt.Errorf("pb2go: no value found for key >%s<", pbName)
		}
		goNames = append(goNames, goName)
	}
	return goNames, nil
}

// getTSAllPBNames returns names of all database columns.
func getTSAllPBNames() []string {
	pbNames := []string{}
	for pbName := range tspb2go {
		pbNames = append(pbNames, pbName)
	}
	return pbNames
}

// getTSDBColumns returns names of database columns corresponding to pbNames.
func getTSDBColumns(pbNames []string) ([]string, error) {
	seen := map[string]struct{}{}
	cols := []string{}

	for _, pbName := range pbNames {
		if _, found := tspb2go[pbName]; !found {
			return nil, fmt.Errorf(
				"attribute not found: %s; supported attributes: %s",
				pbName, strings.Join(getTSAllPBNames(), ", "))
		}

		if _, found := seen[pbName]; found {
			return nil, fmt.Errorf("attribute %s specified more than once", pbName)
		}

		cols = append(cols, pbName)
		seen[pbName] = struct{}{}
	}

	return cols, nil
}

// getTSMdata returns a TSMetadata object initialized from colVals.
func getTSMdata(colVals map[string]interface{}) (*datastore.TSMetadata, error) {

	tsMData := datastore.TSMetadata{}
	tp := reflect.ValueOf(&tsMData)

	for pbName, val := range colVals {
		// NOTE: column names are (assumed to be) identical to protobuf names, hence the
		// key to the colVals map can be pbName instead of colName
		goName, found := tspb2go[pbName]
		if !found {
			return nil, fmt.Errorf(
				"key not found in tspb2go: %s (existing contents: %v)", pbName, tspb2go)
		}

		field := tp.Elem().FieldByName(goName)
		if !field.IsValid() {
			return nil, fmt.Errorf("invalid field (goName: %s, pbName: %s)", goName, pbName)
		}
		if !field.CanSet() {
			return nil, fmt.Errorf("unassignable field (goName: %s, pbName: %s)", goName, pbName)
		}

		switch field.Kind() {
		case reflect.String:
			val0, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf(
					"value not string: %v (type: %T; goName: %s, pbName: %s",
					val, val, goName, pbName)
			}
			field.SetString(val0)
		default:
			return nil, fmt.Errorf(
				"unsupported type: %v (val: %v; type: %T; goName: %s; pbName: %s)",
				field.Kind(), val, val, goName, pbName)
		}
	}

	return &tsMData, nil
}

// scanTsMdata scans column cols from current result row in rows and converts to a
// TSMetadata object.
// Returns (TSMetadata object, nil) upon success, otherwise (..., error).
func scanTsMdata(rows *sql.Rows, cols []string) (*datastore.TSMetadata, error) {
	colVals0 := make([]interface{}, len(cols))   // column values
	colValPtrs := make([]interface{}, len(cols)) // pointers to column values
	for i := range colVals0 {
		colValPtrs[i] = &colVals0[i]
	}
	// scan row into column value pointers
	if err := rows.Scan(colValPtrs...); err != nil {
		return nil, fmt.Errorf("rows.Scan() failed: %v", err)
	}
	// combine column names and -values into a map
	colVals := map[string]interface{}{}
	for i, col := range cols {
		colVals[col] = colVals0[i]
	}
	// convert to a TSMetadata object
	tsMdata, err := getTSMdata(colVals)
	if err != nil {
		return nil, fmt.Errorf("getTSMdata failed(): %v", err)
	}

	return tsMdata, nil
}

// getTSMdataEqual returns upon success (true, nil) iff tsmd1 and tsmd2 are equal wrt. goNames,
// otherwise (false, nil). If an error occurs, (..., error) is returned.
func tsMdataEqual(tsmd1, tsmd2 *datastore.TSMetadata, goNames []string) (bool, error) {
	tp1 := reflect.ValueOf(tsmd1)
	tp2 := reflect.ValueOf(tsmd2)

	for _, goName := range goNames {
		field1 := tp1.Elem().FieldByName(goName)
		if !field1.IsValid() {
			return false, fmt.Errorf("tp1.Elem().FieldByName(%s) returned invalid value", goName)
		}
		field2 := tp2.Elem().FieldByName(goName)
		if !field2.IsValid() {
			return false, fmt.Errorf("tp2.Elem().FieldByName(%s) returned invalid value", goName)
		}

		if !field1.Equal(field2) {
			return false, nil // at least one difference found
		}
	}

	return true, nil // no differences found
}

// getCombo gets a version of tsMdata1 where only the given attributes (in goNames) have been set
// (all other attributes having the default values).
// Returns (TSMetadata, nil) upon success, otherwise (..., error).
func getCombo(tsMdata1 *datastore.TSMetadata, goNames []string) (*datastore.TSMetadata, error) {
	tsMdata2 := datastore.TSMetadata{}
	tp1 := reflect.ValueOf(tsMdata1)
	tp2 := reflect.ValueOf(&tsMdata2)

	for _, goName := range goNames {
		field1 := tp1.Elem().FieldByName(goName)
		if !field1.IsValid() {
			return nil, fmt.Errorf("invalid field (1) (goName: %s)", goName)
		}
		field2 := tp2.Elem().FieldByName(goName)
		if !field2.IsValid() {
			return nil, fmt.Errorf("invalid field (2) (goName: %s)", goName)
		}
		if !field2.CanSet() {
			return nil, fmt.Errorf("unassignable field (2) (goName: %s)", goName)
		}

		switch field1.Kind() {
		case reflect.String:
			field2.SetString(field1.String())
		default:
			return nil, fmt.Errorf("unsupported type: %v (goName: %s)", field1.Kind(), goName)
		}
	}

	return &tsMdata2, nil
}

// getTSAttrGroupsIncInstances populates groups from cols such that each group contains all
// instances that match a unique combination of database values corresponding to cols.
// All attributes, including those in cols, are set to the actual values found in the database.
// Returns nil upon success, otherwise error.
func getTSAttrGroupsIncInstances(
	db *sql.DB, cols []string, groups *[]*datastore.TSMdataGroup) error {
	allCols := getTSAllPBNames() // get all protobuf names of TSMetadata message

	goNames, err := getTSGoNamesFromPBNames(cols)
	if err != nil {
		return fmt.Errorf("getTSGoNamesFromPBNames() failed: %v", err)
	}

	// query database for all columns in time_series, ordered by cols
	allColsS := strings.Join(allCols, ",")
	colsS := strings.Join(cols, ",")
	query := fmt.Sprintf("SELECT %s FROM time_series ORDER BY %s", allColsS, colsS)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	// aggregate rows into groups
	currInstances := []*datastore.TSMetadata{} // initial current instance set
	for rows.Next() {
		// extract tsMdata from current result row
		tsMdata, err := scanTsMdata(rows, allCols)
		if err != nil {
			return fmt.Errorf("scanTsMdata() failed: %v", err)
		}

		if len(currInstances) > 0 { // check if we should create a new current instance set
			equal, err := tsMdataEqual(tsMdata, currInstances[0], goNames)
			if err != nil {
				return fmt.Errorf("tsMdataEqual() failed: %v", err)
			}

			if !equal { // ts metadata changed wrt. cols
				// add next group with current instance set
				currCombo, err := getCombo(currInstances[0], goNames)
				if err != nil {
					return fmt.Errorf("getCombo() failed (1): %v", err)
				}
				*groups = append(*groups, &datastore.TSMdataGroup{
					Combo:     currCombo,
					Instances: currInstances,
				})
				currInstances = []*datastore.TSMetadata{} // create a new current instance set
			}
		}

		currInstances = append(currInstances, tsMdata) // add tsMdata to current instance set
	}

	// assert(len(currInstances) > 0)
	// add final group with current instance set
	currCombo, err := getCombo(currInstances[0], goNames)
	if err != nil {
		return fmt.Errorf("getCombo() failed (2): %v", err)
	}
	*groups = append(*groups, &datastore.TSMdataGroup{
		Combo:     currCombo,
		Instances: currInstances,
	})

	return nil
}

// getTSAttrGroupsComboOnly populates groups from cols such that each group contains a single,
// unique combination of database values corresponding to cols. Other attributes than those in cols
// have the default value for the type (i.e. "" for string, etc.).
// Returns nil upon success, otherwise error.
func getTSAttrGroupsComboOnly(db *sql.DB, cols []string, groups *[]*datastore.TSMdataGroup) error {
	// query database for unique combinations of cols in time_series, ordered by cols
	colsS := strings.Join(cols, ",")
	query := fmt.Sprintf("SELECT DISTINCT %s FROM time_series ORDER BY %s", colsS, colsS)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	// aggregate rows into groups
	for rows.Next() {
		// extract tsMdata from current result row
		tsMdata, err := scanTsMdata(rows, cols)
		if err != nil {
			return fmt.Errorf("scanTsMdata() failed: %v", err)
		}

		// add new group with tsMData as the combo (and leaving the instances
		// array unset)
		*groups = append(*groups, &datastore.TSMdataGroup{Combo: tsMdata})
	}

	return nil
}

// GetTSAttrGroups ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetTSAttrGroups(request *datastore.GetTSAGRequest) (
	*datastore.GetTSAGResponse, error) {

	cols, err := getTSDBColumns(request.Attrs) // get database column names for requested attributes
	if err != nil {
		return nil, fmt.Errorf("getTSAttrCols() failed: %v", err)
	}

	groups := []*datastore.TSMdataGroup{}

	if request.IncludeInstances {
		if err := getTSAttrGroupsIncInstances(sbe.Db, cols, &groups); err != nil {
			return nil, fmt.Errorf("getTSAGroupsIncInstances() failed: %v", err)
		}
	} else {
		if err := getTSAttrGroupsComboOnly(sbe.Db, cols, &groups); err != nil {
			return nil, fmt.Errorf("getTSAGroupsComboOnly() failed: %v", err)
		}
	}

	return &datastore.GetTSAGResponse{Groups: groups}, nil
}
