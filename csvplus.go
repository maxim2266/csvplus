/*
Copyright (c) 2016,2017,2018, Maxim Konakov
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
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

/*
Row represents one line from a data source like a .csv file.

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
	buff := append(append(append(append([]byte(`{ "`), header[0]...), `" : "`...), row[header[0]]...), '"')

	for _, col := range header[1:] {
		buff = append(append(append(append(append(buff, `, "`...), col...), `" : "`...), row[col]...), '"')
	}

	buff = append(buff, " }"...)
	return *(*string)(unsafe.Pointer(&buff))
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
			return nil, fmt.Errorf(`Missing column %q`, name)
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
			return nil, fmt.Errorf(`Missing column %q`, name)
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

// ValueAsInt returns the value of the given column converted to integer type, or an error.
// The column must be present on the row.
func (row Row) ValueAsInt(column string) (res int, err error) {
	var val string
	var found bool

	if val, found = row[column]; !found {
		err = fmt.Errorf(`Missing column %q`, column)
		return
	}

	if res, err = strconv.Atoi(val); err != nil {
		if e, ok := err.(*strconv.NumError); ok {
			err = fmt.Errorf(`Column %q: Cannot convert %q to integer: %s`, column, val, e.Err)
		} else {
			err = fmt.Errorf(`Column %q: %s`, column, err)
		}
	}

	return
}

// ValueAsFloat64 returns the value of the given column converted to floating point type, or an error.
// The column must be present on the row.
func (row Row) ValueAsFloat64(column string) (res float64, err error) {
	var val string
	var found bool

	if val, found = row[column]; !found {
		err = fmt.Errorf(`Missing column %q`, column)
		return
	}

	if res, err = strconv.ParseFloat(val, 64); err != nil {
		if e, ok := err.(*strconv.NumError); ok {
			err = fmt.Errorf(`Column %q: Cannot convert %q to float: %s`, column, val, e.Err)
		} else {
			err = fmt.Errorf(`Column %q: %s`, column, err.Error())
		}
	}

	return
}

// RowFunc is the function type used when iterating Rows.
type RowFunc func(Row) error

// DataSource is the iterator type used throughout this library. The iterator
// calls the given RowFunc once per each row. The iteration continues until
// either the data source is exhausted or the supplied RowFunc returns a non-nil error, in
// which case the error is returned back to the caller of the iterator. A special case of io.EOF simply
// stops the iteration and the iterator function returns nil error.
type DataSource func(RowFunc) error

// TakeRows converts a slice of Rows to a DataSource.
func TakeRows(rows []Row) DataSource {
	return func(fn RowFunc) error {
		return iterate(rows, fn)
	}
}

// the core iteration
func iterate(rows []Row, fn RowFunc) (err error) {
	var row Row
	var i int

	for i, row = range rows {
		if err = fn(row.Clone()); err != nil {
			break
		}
	}

	switch err {
	case nil:
		// nothing to do
	case io.EOF:
		err = nil // end of iteration
	default:
		// wrap error
		err = &DataSourceError{
			Line: uint64(i),
			Err:  err,
		}
	}

	return
}

// Take converts anything with Iterate() method to a DataSource.
func Take(src interface {
	Iterate(fn RowFunc) error
}) DataSource {
	return src.Iterate
}

// Transform is the most generic operation on a Row. It takes a function that
// maps a Row to another Row and an error. Any error returned from that function
// stops the iteration, otherwise the returned Row, if not empty, gets passed
// down to the next stage of the processing pipeline.
func (src DataSource) Transform(trans func(Row) (Row, error)) DataSource {
	return func(fn RowFunc) error {
		return src(func(row Row) (err error) {
			if row, err = trans(row); err == nil && len(row) > 0 {
				err = fn(row)
			}

			return
		})
	}
}

// Filter takes a predicate which, when applied to a Row, decides if that Row
// should be passed down for further processing. The predicate should return 'true' to pass the Row.
func (src DataSource) Filter(pred func(Row) bool) DataSource {
	return func(fn RowFunc) error {
		return src(func(row Row) (err error) {
			if pred(row) {
				err = fn(row)
			}

			return
		})
	}
}

// Map takes a function which gets applied to each Row when the source is iterated over. The function
// may return a modified input Row, or an entirely new Row.
func (src DataSource) Map(mf func(Row) Row) DataSource {
	return func(fn RowFunc) error {
		return src(func(row Row) error {
			return fn(mf(row))
		})
	}
}

// Validate takes a function which checks every Row upon iteration and returns an error
// if the validation fails. The iteration stops at the first error encountered.
func (src DataSource) Validate(vf func(Row) error) DataSource {
	return func(fn RowFunc) error {
		return src(func(row Row) (err error) {
			if err = vf(row); err == nil {
				err = fn(row)
			}

			return
		})
	}
}

// Top specifies the number of Rows to pass down the pipeline before stopping the iteration.
func (src DataSource) Top(n uint64) DataSource {
	return func(fn RowFunc) error {
		counter := n

		return src(func(row Row) error {
			if counter == 0 {
				return io.EOF
			}

			counter--
			return fn(row)
		})
	}
}

// Drop specifies the number of Rows to ignore before passing the remaining rows down the pipeline.
func (src DataSource) Drop(n uint64) DataSource {
	return func(fn RowFunc) error {
		counter := n

		return src(func(row Row) error {
			if counter == 0 {
				return fn(row)
			}

			counter--
			return nil
		})
	}
}

// TakeWhile takes a predicate which gets applied to each Row upon iteration.
// The iteration stops when the predicate returns 'false'.
func (src DataSource) TakeWhile(pred func(Row) bool) DataSource {
	return func(fn RowFunc) error {
		var done bool

		return src(func(row Row) error {
			if done = (done || !pred(row)); done {
				return io.EOF
			}

			return fn(row)
		})
	}
}

// DropWhile ignores all the Rows for as long as the specified predicate is true;
// afterwards all the remaining Rows are passed down the pipeline.
func (src DataSource) DropWhile(pred func(Row) bool) DataSource {
	return func(fn RowFunc) error {
		var yield bool

		return src(func(row Row) (err error) {
			if yield = (yield || !pred(row)); yield {
				err = fn(row)
			}

			return
		})
	}
}

// ToCsv iterates the data source and writes the selected columns in .csv format to the given io.Writer.
// The data are written in the "canonical" form with the header on the first line and with all the lines
// having the same number of fields, using default settings for the underlying csv.Writer.
func (src DataSource) ToCsv(out io.Writer, columns ...string) (err error) {
	if len(columns) == 0 {
		panic("Empty column list in ToCsv() function")
	}

	w := csv.NewWriter(out)

	// header
	if err = w.Write(columns); err == nil {
		// rows
		err = src(func(row Row) (e error) {
			var values []string

			if values, e = row.SelectValues(columns...); e == nil {
				e = w.Write(values)
			}

			return
		})
	}

	if err == nil {
		w.Flush()
		err = w.Error()
	}

	return
}

// ToCsvFile iterates the data source and writes the selected columns in .csv format to the given file.
// The data are written in the "canonical" form with the header on the first line and with all the lines
// having the same number of fields, using default settings for the underlying csv.Writer.
func (src DataSource) ToCsvFile(name string, columns ...string) error {
	return withFile(name, func(file io.Writer) error {
		return src.ToCsv(file, columns...)
	})
}

// call the given function with the file stream open for writing
func withFile(name string, fn func(io.Writer) error) (err error) {
	var file *os.File

	if file, err = os.Create(name); err != nil {
		return
	}

	defer func() {
		if p := recover(); p != nil {
			file.Close()
			os.Remove(name)
			panic(p)
		}

		if e := file.Close(); e != nil && err == nil {
			err = e
		}

		if err != nil {
			os.Remove(name)
		}
	}()

	err = fn(file)
	return
}

// ToJSON iterates over the data source and writes all Rows to the given io.Writer in JSON format.
func (src DataSource) ToJSON(out io.Writer) (err error) {
	var buff bytes.Buffer

	buff.WriteByte('[')

	count := uint64(0)
	enc := json.NewEncoder(&buff)

	enc.SetIndent("", "")
	enc.SetEscapeHTML(false)

	err = src(func(row Row) (e error) {
		if count++; count != 1 {
			buff.WriteByte(',')
		}

		if e = enc.Encode(row); e == nil && buff.Len() > 10000 {
			_, e = buff.WriteTo(out)
		}

		return
	})

	if err == nil {
		buff.WriteByte(']')
		_, err = buff.WriteTo(out)
	}

	return
}

// ToJSONFile iterates over the data source and writes all Rows to the given file in JSON format.
func (src DataSource) ToJSONFile(name string) (err error) {
	return withFile(name, src.ToJSON)
}

// ToRows iterates the DataSource storing the result in a slice of Rows.
func (src DataSource) ToRows() (rows []Row, err error) {
	err = src(func(row Row) error {
		rows = append(rows, row)
		return nil
	})

	return
}

// DropColumns removes the specifies columns from each row.
func (src DataSource) DropColumns(columns ...string) DataSource {
	if len(columns) == 0 {
		panic("No columns specified in DropColumns()")
	}

	return func(fn RowFunc) error {
		return src(func(row Row) error {
			for _, col := range columns {
				delete(row, col)
			}

			return fn(row)
		})
	}
}

// SelectColumns leaves only the specified columns on each row. It is an error
// if any such column does not exist.
func (src DataSource) SelectColumns(columns ...string) DataSource {
	if len(columns) == 0 {
		panic("No columns specified in SelectColumns()")
	}

	return func(fn RowFunc) error {
		return src(func(row Row) (err error) {
			if row, err = row.Select(columns...); err == nil {
				err = fn(row)
			}

			return
		})
	}
}

// IndexOn iterates the input source, building index on the specified columns.
// Columns are taken from the specified list from left to the right.
func (src DataSource) IndexOn(columns ...string) (*Index, error) {
	return createIndex(src, columns)
}

// UniqueIndexOn iterates the input source, building unique index on the specified columns.
// Columns are taken from the specified list from left to the right.
func (src DataSource) UniqueIndexOn(columns ...string) (*Index, error) {
	return createUniqueIndex(src, columns)
}

// Join returns a DataSource which is a join between the current DataSource and the specified
// Index. The specified columns are matched against those from the index, in the order of specification.
// Empty 'columns' list yields a join on the columns from the Index (aka "natural join") which all must
// exist in the current DataSource.
// Each row in the resulting table contains all the columns from both the current table and the index.
// This is a lazy operation, the actual join is performed only when the resulting table is iterated over.
func (src DataSource) Join(index *Index, columns ...string) DataSource {
	if len(columns) == 0 {
		columns = index.impl.columns
	} else if len(columns) > len(index.impl.columns) {
		panic("Too many source columns in Join()")
	}

	return func(fn RowFunc) error {
		return src(func(row Row) (err error) {
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
		})
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
func (src DataSource) Except(index *Index, columns ...string) DataSource {
	if len(columns) == 0 {
		columns = index.impl.columns
	} else if len(columns) > len(index.impl.columns) {
		panic("Too many source columns in Except()")
	}

	return func(fn RowFunc) error {
		return src(func(row Row) (err error) {
			var values []string

			if values, err = row.SelectValues(columns...); err == nil {
				if !index.impl.has(values) {
					err = fn(row)
				}
			}

			return
		})
	}
}

// Index is a sorted collection of Rows with O(log(n)) complexity of search
// on the indexed columns. Iteration over the Index yields a sequence of Rows sorted on the index.
type Index struct {
	impl indexImpl
}

// Iterate iterates over all rows of the index. The rows are sorted by the values of the columns
// specified when the Index was created.
func (index *Index) Iterate(fn RowFunc) error {
	return iterate(index.impl.rows, fn)
}

// Find returns a DataSource of all Rows from the Index that match the specified values
// in the indexed columns, left to the right. The number of specified values may be less than
// the number of the indexed columns.
func (index *Index) Find(values ...string) DataSource {
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

// WriteTo writes the index to the specified file.
func (index *Index) WriteTo(fileName string) (err error) {
	var file *os.File

	if file, err = os.Create(fileName); err != nil {
		return
	}

	defer func() {
		if e := file.Close(); e != nil || err != nil {
			os.Remove(fileName)

			if err == nil {
				err = e
			}
		}
	}()

	enc := gob.NewEncoder(file)

	if err = enc.Encode(index.impl.columns); err == nil {
		err = enc.Encode(index.impl.rows)
	}

	return
}

// LoadIndex reads the index from the specified file.
func LoadIndex(fileName string) (*Index, error) {
	var file *os.File
	var err error

	if file, err = os.Open(fileName); err != nil {
		return nil, err
	}

	defer file.Close()

	index := new(Index)
	dec := gob.NewDecoder(file)

	if err = dec.Decode(&index.impl.columns); err != nil {
		return nil, err
	}

	if err = dec.Decode(&index.impl.rows); err != nil {
		return nil, err
	}

	return index, nil
}

func createIndex(src DataSource, columns []string) (*Index, error) {
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
	if err := src(func(row Row) error {
		for _, col := range columns {
			if !row.HasColumn(col) {
				return fmt.Errorf(`Missing column %q while creating an index`, col)
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

func createUniqueIndex(src DataSource, columns []string) (index *Index, err error) {
	// create index
	if index, err = createIndex(src, columns); err != nil || len(index.impl.rows) < 2 {
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
	set := make(map[string]struct{}, len(columns))

	for _, col := range columns {
		if _, found := set[col]; found {
			return false
		}

		set[col] = struct{}{}
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

// Reader is iterable csv reader. The iteration is performed in its Iterate() method, which
// may only be invoked once per each instance of the Reader.
type Reader struct {
	source                       maker
	delimiter, comment           rune
	numFields                    int
	lazyQuotes, trimLeadingSpace bool
	header                       map[string]int
	headerFromFirstRow           bool
}

type maker = func() (io.Reader, func(), error)

// FromReader constructs a new csv reader from the given io.Reader, with default settings.
func FromReader(input io.Reader) *Reader {
	return makeReader(func() (io.Reader, func(), error) {
		return input, func() {}, nil
	})
}

// FromReadCloser constructs a new csv reader from the given io.ReadCloser, with default settings.
func FromReadCloser(input io.ReadCloser) *Reader {
	return makeReader(func() (io.Reader, func(), error) {
		return input, func() { input.Close() }, nil
	})
}

// FromFile constructs a new csv reader bound to the specified file, with default settings.
func FromFile(name string) *Reader {
	return makeReader(func() (io.Reader, func(), error) {
		file, err := os.Open(name)

		if err != nil {
			return nil, nil, err
		}

		return file, func() { file.Close() }, nil
	})
}

func makeReader(fn maker) *Reader {
	return &Reader{
		source:             fn,
		delimiter:          ',',
		headerFromFirstRow: true,
	}
}

// Delimiter sets the symbol to be used as a field delimiter.
func (r *Reader) Delimiter(c rune) *Reader {
	r.delimiter = c
	return r
}

// CommentChar sets the symbol that starts a comment.
func (r *Reader) CommentChar(c rune) *Reader {
	r.comment = c
	return r
}

// LazyQuotes specifies that a quote may appear in an unquoted field and a
// non-doubled quote may appear in a quoted field of the input.
func (r *Reader) LazyQuotes() *Reader {
	r.lazyQuotes = true
	return r
}

// TrimLeadingSpace specifies that the leading white space in a field should be ignored.
func (r *Reader) TrimLeadingSpace() *Reader {
	r.trimLeadingSpace = true
	return r
}

// AssumeHeader sets the header for those input sources that do not have their column
// names specified on the first row. The header specification is a map
// from the assigned column names to their corresponding column indices.
func (r *Reader) AssumeHeader(spec map[string]int) *Reader {
	if len(spec) == 0 {
		panic("Empty header spec")
	}

	for name, col := range spec {
		if col < 0 {
			panic("Header spec: Negative index for column " + name)
		}
	}

	r.header = spec
	r.headerFromFirstRow = false
	return r
}

// ExpectHeader sets the header for input sources that have their column
// names specified on the first row. The row gets verified
// against this specification when the reading starts.
// The header specification is a map from the expected column names to their corresponding
// column indices. A negative value for an index means that the real value of the index
// will be found by searching the first row for the specified column name.
func (r *Reader) ExpectHeader(spec map[string]int) *Reader {
	if len(spec) == 0 {
		panic("Empty header spec")
	}

	r.header = make(map[string]int, len(spec))

	for name, col := range spec {
		r.header[name] = col
	}

	r.headerFromFirstRow = true
	return r
}

// SelectColumns specifies the names of the columns to read from the input source.
// The header specification is built by searching the first row of the input
// for the names specified and recording the indices of those columns. It is an error
// if any column name is not found.
func (r *Reader) SelectColumns(names ...string) *Reader {
	if len(names) == 0 {
		panic("Empty header spec")
	}

	r.header = make(map[string]int, len(names))

	for _, name := range names {
		if _, found := r.header[name]; found {
			panic("Header spec: Duplicate column name: " + name)
		}

		r.header[name] = -1
	}

	r.headerFromFirstRow = true
	return r
}

// NumFields sets the expected number of fields on each row of the input.
// It is an error if any row does not have this exact number of fields.
func (r *Reader) NumFields(n int) *Reader {
	r.numFields = n
	return r
}

// NumFieldsAuto specifies that the number of fields on each row must match that of
// the first row of the input.
func (r *Reader) NumFieldsAuto() *Reader {
	return r.NumFields(0)
}

// NumFieldsAny specifies that each row of the input may have different number
// of fields. Rows shorter than the maximum column index in the header specification will be padded
// with empty fields.
func (r *Reader) NumFieldsAny() *Reader {
	return r.NumFields(-1)
}

// Iterate reads the input row by row, converts each line to the Row type, and calls
// the supplied RowFunc.
func (r *Reader) Iterate(fn RowFunc) error {
	// source
	input, close, err := r.source()

	if err != nil {
		return err
	}

	defer close()

	// csv.Reader
	reader := csv.NewReader(input)
	reader.Comma = r.delimiter
	reader.Comment = r.comment
	reader.LazyQuotes = r.lazyQuotes
	reader.TrimLeadingSpace = r.trimLeadingSpace
	reader.FieldsPerRecord = r.numFields

	// header
	var header map[string]int

	lineNo := uint64(1)

	if r.headerFromFirstRow {
		if header, err = r.makeHeader(reader); err != nil {
			return mapError(err, lineNo)
		}

		lineNo++
	} else {
		header = r.header
	}

	// iteration
	var line []string

	for line, err = reader.Read(); err == nil; line, err = reader.Read() {
		row := make(map[string]string, len(header))

		for name, index := range header {
			if index < len(line) {
				row[name] = line[index]
			} else if r.numFields < 0 { // padding allowed
				row[name] = ""
			} else {
				return &DataSourceError{
					Line: lineNo,
					Err:  fmt.Errorf("Column not found: %q (%d)", name, index),
				}
			}
		}

		if err = fn(row); err != nil {
			break
		}

		lineNo++
	}

	// map error
	if err != io.EOF {
		return mapError(err, lineNo)
	}

	return nil
}

// build header spec from the first row of the input file
func (r *Reader) makeHeader(reader *csv.Reader) (map[string]int, error) {
	line, err := reader.Read()

	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, errors.New("Empty header")
	}

	if len(r.header) == 0 { // the header is not specified - get it from the first line
		header := make(map[string]int, len(line))

		for i, name := range line {
			header[name] = i
		}

		return header, nil
	}

	// check and update the specified header
	header := make(map[string]int, len(r.header))

	// fix column indices
	for i, name := range line {
		if index, found := r.header[name]; found {
			if index == -1 || index == i {
				header[name] = i
			} else {
				return nil, fmt.Errorf(`Misplaced column %q: expected at pos. %d, but found at pos. %d`, name, index, i)
			}
		}
	}

	// check if all columns are found
	if len(header) < len(r.header) {
		// compose the list of the missing columns
		var list []string

		for name := range r.header {
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

// annotate error with line number
func mapError(err error, lineNo uint64) error {
	switch e := err.(type) {
	case *csv.ParseError:
		return &DataSourceError{
			Line: lineNo,
			Err:  e.Err,
		}
	case *os.PathError:
		return &DataSourceError{
			Line: lineNo,
			Err:  errors.New(e.Op + ": " + e.Err.Error()),
		}
	default:
		return &DataSourceError{
			Line: lineNo,
			Err:  err,
		}
	}
}

// DataSourceError is the type of the error returned from Reader.Iterate method.
type DataSourceError struct {
	Line uint64 // counting from 1
	Err  error
}

// Error returns a human-readable error message string.
func (e *DataSourceError) Error() string {
	return fmt.Sprintf(`Row %d: %s`, e.Line, e.Err)
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
