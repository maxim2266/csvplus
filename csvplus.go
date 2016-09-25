/*
Copyright (c) 2016, Maxim Konakov
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors
   may be used to endorse or promote products derived from this software without
   specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// Package csvplus extends the standard Go encoding/csv package with fluent
// interface, lazy stream processing operations, indices and joins.
package csvplus

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

/*
Row represents one line from a data source like .csv file.

Each Row is a map from column names to the string values under that columns on the current line.
It is assumed that each column has a unique name.
In a .csv file, the column names may either come from the first line of the file ("expected header"),
or they can be set-up via configuration of the reader object ("assumed header").

Using meaningful column names instead of indices is usually more convenient when the columns get rearranged
during the execution of the processing pipeline.
*/
type Row map[string]string

// HasColumn is a predicate returning 'true' when the specified column is present.
func (row Row) HasColumn(col string) (found bool) {
	_, found = row[col]
	return
}

// SafeGetValue returns the value under the specified column, if present, otherwise it returns the
// substitution value.
func (row Row) SafeGetValue(col, subst string) string {
	if value, found := row[col]; found {
		return value
	}

	return subst
}

// Header returns a slice of all column names, sorted via sort.Strings.
func (row Row) Header() []string {
	r := make([]string, 0, len(row))

	for col := range row {
		r = append(r, col)
	}

	sort.Strings(r)
	return r
}

// String returns a string representation of the Row.
func (row Row) String() string {
	if len(row) == 0 {
		return "{}"
	}

	header := row.Header() // make order predictable

	var buff bytes.Buffer

	buff.WriteString(`{ "`)
	buff.WriteString(header[0])
	buff.WriteString(`" : "`)
	buff.WriteString(row[header[0]])
	buff.WriteByte('"')

	for _, col := range header[1:] {
		buff.WriteString(`, "`)
		buff.WriteString(col)
		buff.WriteString(`" : "`)
		buff.WriteString(row[col])
		buff.WriteByte('"')
	}

	buff.WriteString(" }")
	return buff.String()
}

// SelectExisting takes a list of column names and returns a new Row
// containing only those columns from the list that are present in the current Row.
func (row Row) SelectExisting(cols ...string) Row {
	r := make(map[string]string, len(cols))

	for _, name := range cols {
		if val, found := row[name]; found {
			r[name] = val
		}
	}

	return r
}

// Select takes a list of column names and returns a new Row
// containing only the specified columns, or an error if any column is not present.
func (row Row) Select(cols ...string) (Row, error) {
	r := make(map[string]string, len(cols))

	for _, name := range cols {
		var found bool

		if r[name], found = row[name]; !found {
			return nil, fmt.Errorf(`Missing column "%s"`, name)
		}
	}

	return r, nil
}

// SelectValues takes a list of column names and returns a slice of their
// corresponding values, or an error if any column is not present.
func (row Row) SelectValues(cols ...string) ([]string, error) {
	r := make([]string, len(cols))

	for i, name := range cols {
		var found bool

		if r[i], found = row[name]; !found {
			return nil, fmt.Errorf(`Missing column "%s"`, name)
		}
	}

	return r, nil
}

// Clone returns a copy of the current Row.
func (row Row) Clone() Row {
	r := make(map[string]string, len(row))

	for k, v := range row {
		r[k] = v
	}

	return r
}

// ColumnAsInt returns the value of the given column converted to integer type, or an error.
// The column must be present on the row.
func (row Row) ColumnAsInt(column string) (res int, err error) {
	var val string
	var found bool

	if val, found = row[column]; !found {
		err = fmt.Errorf(`Missing column "%s"`, column)
		return
	}

	if res, err = strconv.Atoi(val); err != nil {
		if e, ok := err.(*strconv.NumError); ok {
			err = fmt.Errorf(`Column "%s": Cannot convert "%s" to integer: %s`, column, val, e.Err)
		} else {
			err = fmt.Errorf(`Column "%s": %s`, column, err.Error())
		}
	}

	return
}

// ColumnAsFloat returns the value of the given column converted to floating point type, or an error.
// The column must be present on the row.
func (row Row) ColumnAsFloat64(column string) (res float64, err error) {
	var val string
	var found bool

	if val, found = row[column]; !found {
		err = fmt.Errorf(`Missing column "%s"`, column)
		return
	}

	if res, err = strconv.ParseFloat(val, 64); err != nil {
		if e, ok := err.(*strconv.NumError); ok {
			err = fmt.Errorf(`Column "%s": Cannot convert "%s" to float: %s`, column, val, e.Err)
		} else {
			err = fmt.Errorf(`Column "%s": %s`, column, err.Error())
		}
	}

	return
}

// RowFunc is the function type used when iterating Rows via ForEach() method.
type RowFunc func(Row) error

// DataSource is the interface to any data that can be represented as a sequence of Rows.
type DataSource interface {
	// ForEach should call the given RowFunc once per each Row. The iteration should
	// continue for as long as the RowFunc returns 'nil'. When RowFunc returns
	// a non-nil error, this function should stop iteration and return an error,
	// which may be either the original one, or some other error. The special
	// value of io.EOF should be treated as a 'stop iteration' command, in which
	// case this function should return 'nil' error. Given that Rows can be modified
	// by the RowFunc, the implementations should only pass copies of their
	// underlying rows to the supplied RowFunc.
	ForEach(RowFunc) error
}

// Table implements sequential operations on a given data source as well as
// the DataSource interface itself and other iterating methods. All sequential
// operations are 'lazy', i.e. they are not invoked immediately, but instead
// they return a new table which, when iterated over, invokes the particular
// operation. The operations can be chained using so called fluent interface.
type Table struct {
	exec func(RowFunc) error
	wrap func(RowFunc) RowFunc
}

// ForEach iterates over the Table invoking all the operations in the processing pipeline,
// and calls the specified RowFunc on each resulting Row.
func (t Table) ForEach(fn RowFunc) error {
	return t.exec(t.wrap(fn))
}

// Take converts any DataSource into a Table.
func Take(source DataSource) Table {
	return Table{
		exec: source.ForEach,
		wrap: passWrap,
	}
}

// TakeRows converts a slice of Rows into a Table.
func TakeRows(rows []Row) Table {
	return Table{
		exec: func(fn RowFunc) error {
			for i, row := range rows {
				switch err := fn(row.Clone()); err {
				case nil:
					continue
				case io.EOF:
					return nil
				default:
					return &DataSourceError{
						Name: "In-memory table of Rows",
						Line: uint64(uint(i)),
						Err:  err,
					}
				}
			}

			return nil
		},
		wrap: passWrap,
	}
}

func passWrap(fn RowFunc) RowFunc { return fn }

// Transform is the most generic operation on a Row. It takes a function which
// maps a Row to another Row or returns an error. Any error returned from that function
// stops the iteration, otherwise the returned Row, if not empty, gets passed
// down to the next stage of the processing pipeline.
func (t Table) Transform(trans func(Row) (Row, error)) Table {
	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			return func(row Row) (err error) {
				if row, err = trans(row); err == nil && len(row) > 0 {
					err = fn(row)
				}

				return
			}
		},
	}
}

// Filter takes a predicate which, when applied to a Row, decides if that Row
// should be passed down for further processing. The predicate should return 'true' to pass the Row.
func (t Table) Filter(pred func(Row) bool) Table {
	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			return func(row Row) (err error) {
				if pred(row) {
					err = fn(row)
				}

				return
			}
		},
	}
}

// Map takes a function which gets applied to each Row when the source is iterated over. The function
// may return a modified input Row, or an entirely new Row.
func (t Table) Map(mf func(Row) Row) Table {
	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc { return func(row Row) error { return fn(mf(row)) } },
	}
}

// Validate takes a function which checks every Row upon iteration and returns an error
// if the validation fails. The iteration stops at the first error encountered.
func (t Table) Validate(vf func(Row) error) Table {
	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			return func(row Row) (err error) {
				if err = vf(row); err == nil {
					err = fn(row)
				}

				return
			}
		},
	}
}

// Top specifies the number of Rows to pass down the pipeline before stopping the iteration.
func (t Table) Top(n int) Table {
	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			counter := n

			return func(row Row) error {
				if counter <= 0 {
					return io.EOF
				}

				counter--
				return fn(row)
			}
		},
	}
}

// Drop specifies the number of Rows to ignore before passing the remaining rows down the pipeline.
func (t Table) Drop(n int) Table {
	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			counter := n

			return func(row Row) error {
				if counter <= 0 {
					return fn(row)
				}

				counter--
				return nil
			}
		},
	}
}

// TakeWhile takes a predicate which gets applied to each Row upon iteration.
// The iteration stops when the predicate returns 'false' for the first time.
func (t Table) TakeWhile(pred func(Row) bool) Table {
	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			var done bool

			return func(row Row) error {
				if done = done || !pred(row); done {
					return io.EOF
				}

				return fn(row)
			}
		},
	}
}

// DropWhile ignores all the Rows for as long as the specified predicate is true;
// afterwards all the remaining Rows are passed down the pipeline.
func (t Table) DropWhile(pred func(Row) bool) Table {
	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			var yield bool

			return func(row Row) (err error) {
				if yield = yield || !pred(row); yield {
					err = fn(row)
				}

				return
			}
		},
	}
}

// ToCsvFile iterates the input source  writing the selected columns to the file with the given name,
// in "canonical" form with the header on the first line and with all the lines having the same number of fields,
// using default settings for the underlying Writer from the encoding/csv package.
func (t Table) ToCsvFile(fileName string, columns ...string) error {
	if len(columns) == 0 {
		panic("Empty columns list in ToCsvFile()")
	}

	return withCsvFileWriter(fileName, func(out *csv.Writer) error {
		// header
		if err := out.Write(columns); err != nil {
			return fmt.Errorf(`Error writing file "%s": %s`, fileName, err)
		}

		// body
		return t.ForEach(func(row Row) (e error) {
			var values []string

			if values, e = row.SelectValues(columns...); e == nil {
				e = out.Write(values)
			} else {
				e = fmt.Errorf(`Error writing file "%s": %s`, fileName, e)
			}

			return
		})
	})
}

// ToRows iterates the Table storing the result in a slice of Rows.
func (t Table) ToRows() (rows []Row, err error) {
	err = t.ForEach(func(row Row) error {
		rows = append(rows, row)
		return nil
	})

	return rows, err
}

// IndexOn iterates the input source building index on the specified columns.
// Columns are taken from the specified list from left to the right.
func (t Table) IndexOn(columns ...string) (*Index, error) {
	return createIndex(t, columns)
}

// UniqueIndexOn iterates the input source building unique index on the specified columns.
// Columns are taken from the specified list from left to the right.
func (t Table) UniqueIndexOn(columns ...string) (*Index, error) {
	return createUniqueIndex(t, columns)
}

// Join returns a Table which is a join between the current Table and the specified
// Index. The specified columns are matched against those from the index, in the order of specification.
// Empty 'columns' list yields a join on the columns from the Index (aka "natural join") which all must
// exist in the current Table.
// Each row in the resulting table contains all the columns from both the current table and the index.
// This is a lazy operation, the actual join is performed only when the resulting table is iterated over.
func (t Table) Join(index *Index, columns ...string) Table {
	if len(columns) == 0 {
		columns = index.impl.columns
	} else if len(columns) > len(index.impl.columns) {
		panic("Too many source columns in Join()")
	}

	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			return func(row Row) (err error) {
				var values []string

				if values, err = row.SelectValues(columns...); err == nil {
					n := len(index.impl.rows)

					for i := index.impl.first(values); i < n && !index.impl.cmp(i, values, false); i++ {
						if err = fn(mergeRows(index.impl.rows[i], row)); err != nil {
							break
						}
					}
				}

				return
			}
		},
	}
}

func mergeRows(left, right Row) Row {
	r := make(map[string]string, len(left)+len(right))

	for k, v := range left {
		r[k] = v
	}

	for k, v := range right {
		r[k] = v
	}

	return r
}

// Except returns a table containing all the rows not in the specified Index, unchanged. The specified
// columns are matched against those from the index, in the order of specification. If no columns
// are specified then the columns list is taken from the index.
func (t Table) Except(index *Index, columns ...string) Table {
	if len(columns) == 0 {
		columns = index.impl.columns
	} else if len(columns) > len(index.impl.columns) {
		panic("Too many source columns in Except()")
	}

	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			return func(row Row) (err error) {
				var values []string

				if values, err = row.SelectValues(columns...); err == nil {
					if !index.impl.has(values) {
						err = fn(row)
					}
				}

				return
			}
		},
	}
}

// helper function for handling resources associated with an open .csv file
func withCsvFileWriter(name string, fn func(*csv.Writer) error) (err error) {
	var file *os.File

	if file, err = os.Create(name); err != nil {
		return err
	}

	defer func() {
		if e := file.Close(); e != nil && err == nil {
			err = e
		}

		if err != nil {
			os.Remove(name)
		}
	}()

	out := csv.NewWriter(file)

	defer func() {
		if err == nil {
			out.Flush()
			err = out.Error()
		}
	}()

	err = fn(out)
	return
}

// DropColumns removes the specifies columns from each row.
func (t Table) DropColumns(columns ...string) Table {
	if len(columns) == 0 {
		panic("No columns specified in DropColumns()")
	}

	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			return func(row Row) error {
				for _, col := range columns {
					delete(row, col)
				}

				return fn(row)
			}
		},
	}
}

// SelectColumns leaves only the specified columns on each row. It is an error
// if any of those columns does not exist.
func (t Table) SelectColumns(columns ...string) Table {
	if len(columns) == 0 {
		panic("No columns specified in SelectColumns()")
	}

	return Table{
		exec: t.ForEach,
		wrap: func(fn RowFunc) RowFunc {
			return func(row Row) (err error) {
				if row, err = row.Select(columns...); err == nil {
					err = fn(row)
				}

				return
			}
		},
	}
}

// Index is a sorted collection of Rows with O(log(n)) complexity of search
// on the indexed columns. Iteration over the Index yields a sequence of Rows sorted on the index.
type Index struct {
	impl indexImpl
}

// ForEach calls the supplied RowFunc once per each Row.
// Rows are sorted by the values of the columns specified when the Index was created.
func (index *Index) ForEach(fn RowFunc) (err error) {
	return TakeRows(index.impl.rows).ForEach(fn)
}

// Find returns a Table of all Rows from the Index that match the specified values
// in the indexed columns, left to the right. The number of specified values may be less than
// the number of the indexed columns.
func (index *Index) Find(values ...string) Table {
	return TakeRows(index.impl.find(values))
}

// SubIndex returns an Index containing only the rows where the values of the
// indexed columns match the supplied values, left to the right. The number of specified values
// must be less than the number of indexed columns.
func (index *Index) SubIndex(values ...string) *Index {
	if len(values) >= len(index.impl.columns) {
		panic("Too many values in SubIndex()")
	}

	return &Index{indexImpl{
		rows:    index.impl.find(values),
		columns: index.impl.columns[len(values):],
	}}
}

// ResolveDuplicates calls the specified function once per each pack of duplicates with the same key.
// The specified function must not modify its parameter and is expected to do one of the following:
//
// - Select and return one row from the input list. The row will be used as the only row with its key;
//
// - Return an empty row. The entire set of rows will be ignored;
//
// - Return an error which will be passed back to the caller of ResolveDuplicates().
func (index *Index) ResolveDuplicates(resolve func(rows []Row) (Row, error)) error {
	return index.impl.dedup(resolve)
}

func createIndex(t Table, columns []string) (*Index, error) {
	switch len(columns) {
	case 0:
		panic("Empty column list in CreateIndex()")
	case 1:
		// do nothing
	default:
		if !allColumnsUnique(columns) {
			panic("Duplicate column name(s) in CreateIndex()")
		}
	}

	index := &Index{indexImpl{columns: columns}}

	// copy Rows with validation
	if err := t.ForEach(func(row Row) error {
		for _, col := range columns {
			if !row.HasColumn(col) {
				return fmt.Errorf(`Missing column "%s" while creating an index`, col)
			}
		}

		index.impl.rows = append(index.impl.rows, row)
		return nil
	}); err != nil {
		return nil, err
	}

	// sort
	sort.Sort(&index.impl)
	return index, nil
}

func createUniqueIndex(t Table, columns []string) (index *Index, err error) {
	// create index
	if index, err = createIndex(t, columns); err != nil || len(index.impl.rows) < 2 {
		return
	}

	// check for duplicates by linear search; not the best idea.
	rows := index.impl.rows

	for i := 1; i < len(rows); i++ {
		if equalRows(columns, rows[i-1], rows[i]) {
			return nil, errors.New("Duplicate value while creating unique index: " + rows[i].SelectExisting(columns...).String())
		}
	}

	return
}

// compare the specified columns from the two rows
func equalRows(columns []string, r1, r2 Row) bool {
	for _, col := range columns {
		if r1[col] != r2[col] {
			return false
		}
	}

	return true
}

// check if all the column names from the specified list are unique
func allColumnsUnique(columns []string) bool {
	set := make(map[string]int, len(columns))

	for _, col := range columns {
		if _, found := set[col]; found {
			return false
		}

		set[col] = 0
	}

	return true
}

// index implementation
type indexImpl struct {
	rows    []Row
	columns []string
}

// functions required by sort.Sort()
func (index *indexImpl) Len() int      { return len(index.rows) }
func (index *indexImpl) Swap(i, j int) { index.rows[i], index.rows[j] = index.rows[j], index.rows[i] }

func (index *indexImpl) Less(i, j int) bool {
	left, right := index.rows[i], index.rows[j]

	for _, col := range index.columns {
		switch strings.Compare(left[col], right[col]) {
		case -1:
			return true
		case 1:
			return false
		}
	}

	return false
}

// deduplication
func (index *indexImpl) dedup(resolve func(rows []Row) (Row, error)) (err error) {
	// find first duplicate
	var lower int

	for lower = 1; lower < len(index.rows); lower++ {
		if equalRows(index.columns, index.rows[lower-1], index.rows[lower]) {
			break
		}
	}

	if lower == len(index.rows) {
		return
	}

	dest := lower - 1

	// loop: find index of the first row with another key, resolve, then find next duplicate
	for lower < len(index.rows) {
		// the duplicate is in [lower-1, upper[ range
		values, _ := index.rows[lower].SelectValues(index.columns...)

		upper := lower + sort.Search(len(index.rows)-lower, func(i int) bool {
			return index.cmp(lower+i, values, false)
		})

		// resolve
		var row Row

		if row, err = resolve(index.rows[lower-1 : upper]); err != nil {
			return
		}

		lower = upper + 1

		// store the chosen row if not 'empty'
		if len(row) >= len(index.columns) {
			index.rows[dest] = row
			dest++
		}

		// find next duplicate
		for lower < len(index.rows) {
			if equalRows(index.columns, index.rows[lower-1], index.rows[lower]) {
				break
			}

			index.rows[dest] = index.rows[lower-1]
			lower++
			dest++
		}
	}

	if err == nil {
		index.rows = index.rows[:dest]
	}

	return
}

// search on the index
func (index *indexImpl) find(values []string) []Row {
	// check boundaries
	if len(values) == 0 {
		return index.rows
	}

	if len(values) > len(index.columns) {
		panic("Too many columns in indexImpl.find()")
	}

	// get bounds
	upper := sort.Search(len(index.rows), func(i int) bool {
		return index.cmp(i, values, false)
	})

	lower := sort.Search(upper, func(i int) bool {
		return index.cmp(i, values, true)
	})

	// done
	return index.rows[lower:upper]
}

func (index *indexImpl) first(values []string) int {
	return sort.Search(len(index.rows), func(i int) bool {
		return index.cmp(i, values, true)
	})
}

func (index *indexImpl) has(values []string) bool {
	// find the lowest index
	i := index.first(values)

	// check if the row at the lowest index matches the values
	return i < len(index.rows) && !index.cmp(i, values, false)
}

func (index *indexImpl) cmp(i int, values []string, eq bool) bool {
	row := index.rows[i]

	for j, val := range values {
		switch strings.Compare(row[index.columns[j]], val) {
		case 1:
			return true
		case -1:
			return false
		}
	}

	return eq
}

// CsvDataSource is an implementation of the DataSource interface that reads its
// data from a .csv file.
type CsvDataSource struct {
	name                         string
	delimiter, comment           rune
	numFields                    int
	lazyQuotes, trimLeadingSpace bool
	header                       map[string]int
	headerFromFile               bool
}

// CsvFileDataSource constructs a new CsvDataSource bound to the specified
// file name and with the default csv.Reader settings.
func CsvFileDataSource(name string) *CsvDataSource {
	return &CsvDataSource{
		name:      name,
		delimiter: ',',
	}
}

// Delimiter sets the symbol to be used as a field delimiter in the input file.
func (s *CsvDataSource) Delimiter(c rune) *CsvDataSource {
	s.delimiter = c
	return s
}

// CommentChar sets the symbol that starts a comment in the input file.
func (s *CsvDataSource) CommentChar(c rune) *CsvDataSource {
	s.comment = c
	return s
}

// LazyQuotes specifies that a quote may appear in an unquoted field and a
// non-doubled quote may appear in a quoted field of the input file.
func (s *CsvDataSource) LazyQuotes() *CsvDataSource {
	s.lazyQuotes = true
	return s
}

// TrimLeadingSpace specifies that the leading white space in a field should be ignored.
func (s *CsvDataSource) TrimLeadingSpace() *CsvDataSource {
	s.trimLeadingSpace = true
	return s
}

// AssumeHeader sets the header for those input files that do not have their column
// names specified on the first line of the file. The header specification is a map
// from assigned column names to their corresponding column indices.
func (s *CsvDataSource) AssumeHeader(spec map[string]int) *CsvDataSource {
	if len(spec) == 0 {
		panic("Empty header spec")
	}

	for name, col := range spec {
		if col < 0 {
			panic("Header spec: Negative index for column " + name)
		}
	}

	s.header = spec
	s.headerFromFile = false
	return s
}

// ExpectHeader sets the header for input files that have their column
// names specified on the first line of the file. The line gets verified
// against this specification each time the input file is opened.
// The header specification is a map from expected column names to their corresponding
// column indices. A negative value for an index means that the real value of the index
// will be found searching the first line of the file for the specified column name.
func (s *CsvDataSource) ExpectHeader(spec map[string]int) *CsvDataSource {
	if len(spec) == 0 {
		panic("Empty header spec")
	}

	s.header = make(map[string]int, len(spec))

	for name, col := range spec {
		s.header[name] = col
	}

	s.headerFromFile = true
	return s
}

// SelectColumns specifies the names of the columns to read from the file.
// The header specification is built by searching the first line of the input file
// for the names specified and recording the indices of those columns. It is an error
// if any of the column names is not found.
func (s *CsvDataSource) SelectColumns(names ...string) *CsvDataSource {
	if len(names) == 0 {
		panic("Empty header spec")
	}

	s.header = make(map[string]int, len(names))

	for _, name := range names {
		if _, found := s.header[name]; found {
			panic("Header spec: Duplicate column name: " + name)
		}

		s.header[name] = -1
	}

	s.headerFromFile = true
	return s
}

// NumFields sets the expected number of fields on each line of the input file.
// It is an error if any line from the input file does not have that exact number of fields.
func (s *CsvDataSource) NumFields(n int) *CsvDataSource {
	s.numFields = n
	return s
}

// NumFieldsAuto specifies that the number of fields on each line must match that of
// the first line of the input file.
func (s *CsvDataSource) NumFieldsAuto() *CsvDataSource {
	return s.NumFields(0)
}

// NumFieldsAny specifies that each line of the input file may have different number
// of fields. Lines shorter than the maximum column index in the header specification will be padded
// with empty fields.
func (s *CsvDataSource) NumFieldsAny() *CsvDataSource {
	return s.NumFields(-1)
}

// ForEach reads the input file line by line, converts each line to a Row and calls
// the supplied RowFunc. ForEach is goroutine-safe and may be called multiple times.
func (s *CsvDataSource) ForEach(fn RowFunc) error {
	var lineNo uint64

	// input file
	file, err := os.Open(s.name)

	if err != nil {
		return s.mapError(err, lineNo)
	}

	defer file.Close()

	// csv.Reader
	reader := csv.NewReader(file)
	reader.Comma = s.delimiter
	reader.Comment = s.comment
	reader.LazyQuotes = s.lazyQuotes
	reader.TrimLeadingSpace = s.trimLeadingSpace
	reader.FieldsPerRecord = s.numFields

	// header
	header := s.header

	if s.headerFromFile {
		if header, err = s.makeHeader(reader); err != nil {
			return s.mapError(err, lineNo)
		}

		lineNo++
	}

	// iteration
	var line []string

loop:
	for line, err = reader.Read(); err == nil; line, err = reader.Read() {
		row := make(map[string]string, len(header))

		for name, index := range header {
			if index < len(line) {
				row[name] = line[index]
			} else if s.numFields < 0 { // padding allowed
				row[name] = ""
			} else {
				err = fmt.Errorf(`Column not found: "%s"(%d)`, name, index)
				break loop
			}
		}

		if err = fn(row); err != nil {
			break
		}

		lineNo++
	}

	if err != io.EOF {
		return s.mapError(err, lineNo)
	}

	return nil
}

// build header spec from the first line of the input file
func (s *CsvDataSource) makeHeader(reader *csv.Reader) (map[string]int, error) {
	line, err := reader.Read()

	if err != nil {
		return nil, err
	}

	header := make(map[string]int, len(s.header))

	// fix column indices
	for i, name := range line {
		if index, found := s.header[name]; found {
			if index == -1 || index == i {
				header[name] = i
			} else {
				return nil, fmt.Errorf(`Misplaced column "%s": expected at pos. %d, but found at pos. %d`, name, index, i)
			}
		}
	}

	// check if all columns are found
	if len(header) < len(s.header) {
		// compose the list of the missing columns
		var list []string

		for name := range s.header {
			if _, found := header[name]; !found {
				list = append(list, name)
			}
		}

		// return error message
		if len(list) > 1 {
			return nil, errors.New("Columns not found: " + strings.Join(list, ", "))
		}

		return nil, errors.New("Column not found: " + list[0])
	}

	// all done
	return header, nil
}

// annotate error with file name and line number
func (s *CsvDataSource) mapError(err error, lineNo uint64) error {
	switch e := err.(type) {
	case *csv.ParseError:
		return &DataSourceError{
			Name: s.name,
			Line: lineNo,
			Err:  e.Err,
		}
	case *os.PathError:
		return &DataSourceError{
			Name: s.name,
			Line: lineNo,
			Err:  errors.New(e.Op + ": " + e.Err.Error()),
		}
	default:
		return &DataSourceError{
			Name: s.name,
			Line: lineNo,
			Err:  err,
		}
	}
}

// DataSourceError is the type of the error returned from CsvDataSource.ForEach method.
type DataSourceError struct {
	Name string
	Line uint64
	Err  error
}

// Error returns a human-readable error message string.
func (e *DataSourceError) Error() string {
	return fmt.Sprintf(`Data source "%s", row %d: %s`, e.Name, e.Line, e.Err)
}

// All is a predicate combinator that takes any number of other predicates and
// produces a new predicate which returns 'true' only if all the specified predicates
// return 'true' for the same input Row.
func All(funcs ...func(Row) bool) func(Row) bool {
	return func(row Row) bool {
		for _, pred := range funcs {
			if !pred(row) {
				return false
			}
		}

		return true
	}
}

// Any is a predicate combinator that takes any number of other predicates and
// produces a new predicate which returns 'true' if any the specified predicates
// returns 'true' for the same input Row.
func Any(funcs ...func(Row) bool) func(Row) bool {
	return func(row Row) bool {
		for _, pred := range funcs {
			if pred(row) {
				return true
			}
		}

		return false
	}
}

// Not produces a new predicate that reverts the return value from the given predicate.
func Not(pred func(Row) bool) func(Row) bool {
	return func(row Row) bool {
		return !pred(row)
	}
}

// Like produces a predicate that returns 'true' if its input Row matches all the corresponding
// values from the specified 'match' Row.
func Like(match Row) func(Row) bool {
	if len(match) == 0 {
		panic("Empty match function in Like() predicate")
	}

	return func(row Row) bool {
		for key, val := range match {
			if v, found := row[key]; !found || v != val {
				return false
			}
		}

		return true
	}
}
