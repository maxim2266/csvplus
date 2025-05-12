/*
Copyright (c) 2016,2017,2018,2019 Maxim Konakov
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

package csvplus

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRow(t *testing.T) {
	row := Row{
		"id":      "12345",
		"Name":    "John",
		"Surname": "Doe",
	}

	if !row.HasColumn("Name") || !row.HasColumn("Surname") || !row.HasColumn("id") {
		t.Error("Failed HasColumn() test")
		return
	}

	hdr := row.Header()

	if len(hdr) != len(row) {
		t.Error("Invalid header length:", len(hdr))
		return
	}

	for _, name := range hdr {
		if !row.HasColumn(name) {
			t.Error("Column not found:", name)
			return
		}
	}

	if row.SafeGetValue("Name", "") != "John" || row.SafeGetValue("xxx", "@") != "@" {
		t.Error("SafeGetValue() test failed")
		return
	}

	if s := row.SelectExisting("Name", "xxx").String(); s != `{ "Name" : "John" }` {
		t.Error("SafeSelect() test failed:", s)
		return
	}

	if _, e := row.Select("xxx", "zzz"); e == nil || e.Error() != `missing column "xxx"` {
		t.Error("Select() test failed:", e)
		return
	}

	if _, e := row.Select("id", "zzz"); e == nil || e.Error() != `missing column "zzz"` {
		t.Error("Select() test failed:", e)
		return
	}

	if r, e := row.Select("id"); e != nil || r.String() != `{ "id" : "12345" }` {
		t.Error("Select() test failed:", e, r.String())
		return
	}

	if _, e := row.SelectValues("id", "xxx"); e == nil || e.Error() != `missing column "xxx"` {
		t.Error("SelectValues() test failed:", e)
		return
	}

	l, e := row.SelectValues("id", "Name")

	if e != nil || len(l) != 2 || l[0] != "12345" || l[1] != "John" {
		t.Error("SelectValues() test failed:", l, e)
		return
	}

	if s := row.String(); s != `{ "Name" : "John", "Surname" : "Doe", "id" : "12345" }` {
		t.Error("String() test failed:", s)
		return
	}
}

func TestSimpleDataSource(t *testing.T) {
	var n int

	hdr := sortedCopy(peopleHeader)
	src := Take(FromFile(tempFiles["people"]).SelectColumns(hdr...)).
		Filter(Any(Like(Row{"name": "Jack"}), Like(Row{"name": "Amelia"})))

	err := src(func(row Row) error {
		if name := row.SafeGetValue("name", ""); name != "Jack" && name != "Amelia" {
			return errors.New("Unexpected name: " + name)
		}

		if len(row) != 4 {
			return fmt.Errorf("Unexpected number of columns: %d", len(row))
		}

		for i, name := range row.Header() {
			if hdr[i] != name {
				return errors.New("Unexpected column name: " + name)
			}
		}

		n++
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	} else if n != len(peopleSurnames)*2 {
		t.Error("Invalid number of rows:", n)
		return
	}
}

func TestFilterMap(t *testing.T) {
	src := Take(FromFile(tempFiles["people"]).SelectColumns("name", "surname", "id")).
		Filter(Like(Row{"name": "Amelia"})).
		Map(func(row Row) Row { row["name"] = "Julia"; return row })

	err := src(func(row Row) error {
		if row["name"] != "Julia" {
			return fmt.Errorf("Unexpected name: %s instead of Julia", row["name"])
		}

		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}
}

func TestWriteFile(t *testing.T) {
	var tmpFileName string
	var file1, file2 []byte

	defer os.Remove(tmpFileName)

	src := Take(FromFile(tempFiles["people"]).SelectColumns(peopleHeader...))

	err := anyFrom(
		func() (e error) { tmpFileName, e = createTempFile(""); return },
		func() (e error) { e = src.ToCsvFile(tmpFileName, peopleHeader...); return },
		func() (e error) { file1, e = os.ReadFile(tmpFileName); return },
		func() (e error) { file2, e = os.ReadFile(tempFiles["people"]); return },
	)

	if err == nil {
		if !bytes.Equal(bytes.TrimSpace(file1), bytes.TrimSpace(file2)) {
			t.Error("Files do not match")
			return
		}
	} else {
		t.Error(err)
		return
	}
}

func TestIndexImpl(t *testing.T) {
	index := indexImpl{
		columns: []string{"x", "y", "z"},
		rows: []Row{
			{"x": "1", "y": "2", "z": "3", "junk": "zzz"},
			{"x": "5", "y": "6", "z": "8", "junk": "nnn"},
			{"x": "0", "y": "5", "z": "3", "junk": "xxx"},
			{"x": "8", "y": "9", "z": "1", "junk": "aaa"},
			{"x": "7", "y": "4", "z": "0", "junk": "bbb"},
			{"x": "5", "y": "6", "z": "9", "junk": "iii"},
			{"x": "2", "y": "6", "z": "7", "junk": "mmm"},
		},
	}

	sort.Sort(&index)

	rows := index.find([]string{"1", "2", "3"})

	if len(rows) != 1 ||
		rows[0].SafeGetValue("x", "") != "1" ||
		rows[0].SafeGetValue("y", "") != "2" ||
		rows[0].SafeGetValue("z", "") != "3" ||
		rows[0].SafeGetValue("junk", "") != "zzz" {
		t.Errorf("Bad rows: %v", rows)
		return
	}

	rows = index.find([]string{"5", "6", "8"})

	if len(rows) != 1 ||
		rows[0].SafeGetValue("x", "") != "5" ||
		rows[0].SafeGetValue("y", "") != "6" ||
		rows[0].SafeGetValue("z", "") != "8" ||
		rows[0].SafeGetValue("junk", "") != "nnn" {
		t.Errorf("Bad rows: %v", rows)
		return
	}

	rows = index.find([]string{"5", "6"})

	if len(rows) != 2 ||
		rows[0].SafeGetValue("x", "") != "5" ||
		rows[0].SafeGetValue("y", "") != "6" ||
		rows[1].SafeGetValue("x", "") != "5" ||
		rows[1].SafeGetValue("y", "") != "6" {
		t.Errorf("Bad rows: %v", rows)
		return
	}
}

func TestLongChain(t *testing.T) {
	var err error
	var products, orders *Index

	orders, err = Take(FromFile(tempFiles["orders"]).SelectColumns("order_id", "cust_id", "prod_id", "qty", "ts")).
		IndexOn("cust_id")

	if err != nil {
		t.Error(err)
		return
	}

	products, err = Take(FromFile(tempFiles["stock"]).SelectColumns("prod_id", "product", "price")).
		UniqueIndexOn("prod_id")

	if err != nil {
		t.Error(err)
		return
	}

	people := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname", "born"))

	var n int

	err = people.Filter(func(row Row) bool {
		year, e := row.ValueAsInt("born")

		if e != nil {
			t.Error(e)
			return false
		}

		return year > 1970
	}).
		SelectColumns("id", "name", "surname").
		Join(orders, "id").
		DropColumns("ts", "order_id", "cust_id").
		Join(products).
		DropColumns("prod_id").
		Map(func(row Row) Row {
			if row["name"] == "Amelia" {
				row["name"] = "Julia"
			}

			return row
		}).
		Filter(Like(Row{"surname": "Smith"})).
		Top(10).
		DropColumns("id")(func(row Row) error {
		if n++; n > 10 {
			return errors.New("Too many rows")
		}

		if row["surname"] != "Smith" {
			return errors.New(`Surname "Smith" not found`)
		}

		if row["name"] == "Amelia" {
			return errors.New(`Name "Amelia" found`)
		}

		if vals := row.SelectExisting("born", "ts", "order_id", "prod_id", "cust_id"); len(vals) != 0 {
			return errors.New("Some deleted fields are still there")
		}

		if len(row) != 5 { // name, surname, qty, product, price
			return fmt.Errorf("Unexpected number of columns: %d instead of 5", len(row))
		}

		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	// check the original orders
	n = 0

	if err = Take(orders)(func(row Row) error {
		n++

		if _, e := row.Select("order_id", "cust_id", "prod_id", "qty", "ts"); e != nil {
			return e
		}

		return nil
	}); err != nil {
		t.Error(err)
		return
	}

	if n != len(ordersData) {
		t.Errorf("Unexpected number of orders: %d instead of %d", n, len(ordersData))
		return
	}

	// check the original products
	n = 0

	if err = Take(products)(func(row Row) error {
		n++

		if _, e := row.Select("prod_id", "product", "price"); e != nil {
			return e
		}

		return nil
	}); err != nil {
		t.Error(err)
		return
	}

	if n != len(stockItems) {
		t.Errorf("Unexpected number of products: %d instead of %d", n, len(stockItems))
		return
	}
}

func TestSimpleUniqueJoin(t *testing.T) {
	people := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname"))
	orders := Take(FromFile(tempFiles["orders"]).SelectColumns("order_id", "cust_id", "qty"))

	idIndex, err := people.UniqueIndexOn("id")

	if err != nil {
		t.Errorf("Cannot create index: %s", err)
		return
	}

	qtyMap := make([]int, len(peopleData))

	err = orders.Join(idIndex, "cust_id")(func(row Row) (e error) {
		var id, orderID, custID, qty int

		if id, e = row.ValueAsInt("id"); e != nil {
			return
		}

		if orderID, e = row.ValueAsInt("order_id"); e != nil {
			return
		}

		if custID, e = row.ValueAsInt("cust_id"); e != nil {
			return
		}

		if qty, e = row.ValueAsInt("qty"); e != nil {
			return
		}

		if id >= len(peopleData) {
			return fmt.Errorf("Invalid id: %d", id)
		}

		if peopleData[id].Name != row.SafeGetValue("name", "") ||
			peopleData[id].Surname != row.SafeGetValue("surname", "") {
			return fmt.Errorf("Invalid parameters associated with id %d", id)
		}

		if id != custID {
			return fmt.Errorf("id = %d, cust_id = %d", id, custID)
		}

		if orderID >= numOrders {
			return fmt.Errorf("Invalid order_id: %d", orderID)
		}

		if ordersData[orderID].custID != custID {
			return fmt.Errorf("cust_id: got %d instead of %d", custID, ordersData[orderID].custID)
		}

		if ordersData[orderID].qty != qty {
			return fmt.Errorf("qty: got %d instead of %d", qty, ordersData[orderID].qty)
		}

		if len(row) != 6 {
			return fmt.Errorf("Invalid number of columns: %d", len(row))
		}

		qtyMap[id] += qty

		return
	})

	if err != nil {
		t.Errorf("Join failed: %s", err)
		return
	}

	// check qty map
	origMap := make([]int, len(peopleData))

	for _, data := range ordersData {
		origMap[data.custID] += data.qty
	}

	for i, qty := range qtyMap {
		if qty != origMap[i] {
			t.Errorf("qty for id %d: %d instead of %d", i, qty, origMap[i])
			return
		}
	}
}

func TestSorted(t *testing.T) {
	people := Take(FromFile(tempFiles["people"]).ExpectHeader(map[string]int{
		"name":    1,
		"surname": 2,
	}))

	// by name, surname
	index, err := people.UniqueIndexOn("name", "surname")

	if err != nil {
		t.Error(err)
		return
	}

	if err = Take(index).Top(uint64(len(peopleSurnames)))(func(row Row) error {
		if name := row.SafeGetValue("name", "???"); name != "Amelia" {
			return errors.New("Unexpected name: " + name)
		}

		return nil
	}); err != nil {
		t.Error(err)
		return
	}

	// second name, DropWhile()
	if err = Take(index).
		DropWhile(Like(Row{"name": "Amelia"})).
		Top(uint64(len(peopleSurnames)))(func(row Row) error {
		if name := row.SafeGetValue("name", "???"); name != "Ava" {
			return errors.New("Unexpected name: " + name)
		}

		return nil
	}); err != nil {
		t.Error(err)
		return
	}

	// by surname, name
	index, err = people.UniqueIndexOn("surname", "name")

	if err != nil {
		t.Error(err)
		return
	}

	// take second surname
	if err = Take(index).
		Drop(uint64(len(peopleNames))).
		Top(uint64(len(peopleNames)))(func(row Row) error {
		if surname := row.SafeGetValue("surname", "???"); surname != "Davies" {
			return errors.New("Unexpected surname: " + surname)
		}

		return nil
	}); err != nil {
		t.Error(err)
		return
	}
}

func TestSimpleTotals(t *testing.T) {
	orders := Take(FromFile(tempFiles["orders"]).SelectColumns("cust_id", "prod_id", "qty"))
	products := Take(FromFile(tempFiles["stock"]).SelectColumns("prod_id", "price"))

	prodIndex, err := products.UniqueIndexOn("prod_id")

	if err != nil {
		t.Error(err)
		return
	}

	totals := make([]float64, len(peopleData))

	if err = orders.Join(prodIndex)(func(row Row) error {
		var id, qty int
		var e error

		if id, e = row.ValueAsInt("cust_id"); e != nil {
			return fmt.Errorf("cust_id: %s", e)
		}

		if id >= len(peopleData) {
			return fmt.Errorf("Invalid id: %d", id)
		}

		if qty, e = row.ValueAsInt("qty"); e != nil {
			return fmt.Errorf("qty: %s", e)
		}

		var price float64

		if price, e = row.ValueAsFloat64("price"); e != nil {
			return fmt.Errorf("price: %s", e)
		}

		totals[id] = price * float64(qty)
		return nil

	}); err != nil {
		t.Error(err)
		return
	}

	origTotals := make([]float64, len(peopleData))

	for _, order := range ordersData {
		origTotals[order.custID] = stockItems[order.prodID].price * float64(order.qty)
	}

	for id, total := range totals {
		if math.Abs((total-origTotals[id])/total) > 1e-6 {
			t.Errorf("total for id %d: %f instead of %f", id, total, origTotals[id])
			return
		}
	}
}

func TestMultiIndex(t *testing.T) {
	source := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname"))
	index, err := source.UniqueIndexOn("name", "surname")

	if err != nil {
		t.Error(err)
		return
	}

	// non-existing name
	if err = index.Find("xxx")(neverCalled); err != nil {
		t.Error(err)
		return
	}

	// test sub-index
	if err = index.Find("Amelia")(func(row Row) (e error) {
		if name := row.SafeGetValue("name", "???"); name != "Amelia" {
			e = fmt.Errorf("name: %s instead of Amelia", name)
		}

		return
	}); err != nil {
		t.Error(err)
		return
	}

	// self-join on existing names
	for _, name := range peopleNames {
		surnames := map[string]int{}
		s := source.Join(index.SubIndex(name))

		if err = s(func(row Row) error {
			surnames[row.SafeGetValue("surname", "???")]++
			return nil
		}); err != nil {
			t.Error(err)
			return
		}

		if len(surnames) != len(peopleSurnames) {
			t.Errorf(`Name "%s": Invalid number of surnames: %d instead of %d`, name, len(surnames), len(peopleSurnames))
			return
		}

		for _, sname := range peopleSurnames {
			if count, found := surnames[sname]; !found || count != len(peopleNames) {
				t.Errorf(`Name "%s": Surname "%s" found %d times`, name, sname, count)
				return
			}
		}
	}

	// find all existing names and surnames
	for _, person := range peopleData {
		var count int

		if err = index.Find(person.Name, person.Surname)(func(Row) error {
			count++
			return nil
		}); err != nil {
			t.Error(err)
			return
		}

		if count != 1 {
			t.Errorf("%s %s found %d times", person.Name, person.Surname, count)
			return
		}
	}

	// try non-existent name and surname
	if err = index.Find("Jack", "xxx")(neverCalled); err != nil {
		t.Error(err)
		return
	}
}

func TestExcept(t *testing.T) {
	const name = "Emily"

	people, err := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname")).
		Filter(Like(Row{"name": name})).
		IndexOn("id")

	if err != nil {
		t.Error(err)
		return
	}

	n := 0

	err = Take(FromFile(tempFiles["orders"]).SelectColumns("cust_id", "prod_id", "qty")).
		Except(people, "cust_id")(func(row Row) error {
		if id, _ := strconv.Atoi(row["cust_id"]); peopleData[id].Name == name {
			return fmt.Errorf("Cust. id %d somehow got through", id)
		}

		n++
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	// calculate the right number of orders
	m := 0

	for _, order := range ordersData {
		if peopleData[order.custID].Name != name {
			m++
		}
	}

	if n != m {
		t.Errorf("Unexpected number of orders: %d instead of %d", n, m)
		return
	}
}

func TestResolver(t *testing.T) {
	source, err := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname")).ToRows()

	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 1000; i++ {
		// copy source
		src := make([]Row, len(source))

		copy(src, source)

		// add random number of duplicates
		dup := src[rand.Intn(len(src))]
		id, name, surname := dup["id"], dup["name"], dup["surname"]
		n := rand.Intn(100) + 1

		for j := 0; j < n; j++ {
			k := rand.Intn(len(src))
			src = append(src, dup)
			src[k], src[len(src)-1] = src[len(src)-1], src[k]
		}

		// index
		index, err := TakeRows(src).IndexOn("name", "surname")

		if err != nil {
			t.Error(err)
			return
		}

		// resolve
		nc := 0

		if err := index.ResolveDuplicates(func(rows []Row) (Row, error) {
			if nc++; nc != 1 {
				return nil, errors.New("Unexpected second call to the resolution function")
			}

			if len(rows) != n+1 {
				return nil, fmt.Errorf("Unexpected number of duplicates: %d instead of %d", len(rows), n+1)
			}

			for _, r := range rows {
				if r["id"] != id || r["name"] != name || r["surname"] != surname {
					return nil, errors.New("Unexpected duplicate: " + r.String())
				}
			}

			return rows[0], nil
		}); err != nil {
			t.Error(err)
			return
		}
	}
}

func TestTransformedSource(t *testing.T) {
	// cust_id, prod_id, amount
	amounts, err := createAmountsTable()

	if err != nil {
		t.Error(err)
		return
	}

	// aggregate by cust_id
	custAmounts := make([]float64, len(peopleData))

	if err = amounts(func(row Row) (e error) {
		var values []string

		if values, e = row.SelectValues("cust_id", "prod_id", "amount"); e != nil {
			return
		}

		var amount float64

		if amount, e = strconv.ParseFloat(values[2], 64); e != nil {
			return
		}

		var cid int

		if cid, e = strconv.Atoi(values[0]); e != nil {
			return
		}

		custAmounts[cid] += amount
		return
	}); err != nil {
		t.Error(err)
		return
	}

	// check custAmounts
	origCustAmounts := make([]float64, len(peopleData))

	for _, order := range ordersData {
		origCustAmounts[order.custID] += float64(order.qty) * stockItems[order.prodID].price
	}

	for i, amount := range origCustAmounts {
		if math.Abs((custAmounts[i]-amount)/amount) > 1e-6 {
			t.Errorf("Amount mismatch for %s %s (%d): %f instead of %f",
				peopleData[i].Name, peopleData[i].Surname, i, custAmounts[i], amount)
			return
		}
	}
}

func TestErrors(t *testing.T) {
	// invalid column name
	err := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "xxx"))(neverCalled)

	if err == nil || !strings.HasSuffix(err.Error(), "row 1: column not found: xxx") {
		t.Error("Unexpected error:", err)
		return
	}

	// duplicate column name
	if err = shouldPanic(func() {
		Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "id"))
	}); err != nil {
		t.Error(err)
		return
	}

	// missing column on index
	source := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname"))

	_, err = source.IndexOn("name", "xxx")

	if err == nil || !strings.HasSuffix(err.Error(), `missing column "xxx" while creating an index`) {
		t.Error("Unexpected error:", err)
		return
	}

	// unique index with duplicate keys
	_, err = source.UniqueIndexOn("name")

	if err == nil || !strings.Contains(err.Error(), "duplicate value while creating unique index:") {
		t.Error(err)
		return
	}

	var index *Index

	if index, err = source.IndexOn("name"); err != nil {
		t.Error(err)
		return
	}

	if err = index.ResolveDuplicates(func(rows []Row) (Row, error) {
		if len(rows) != len(peopleSurnames) {
			return nil, fmt.Errorf("Unexpected number of duplicate rows: %d instead of %d", len(rows), len(peopleSurnames))
		}

		return rows[0], nil
	}); err != nil {
		t.Error(err)
		return
	}

	if len(index.impl.rows) != len(peopleNames) {
		t.Errorf("Unexpected number of rows: %d instead of %d", len(index.impl.rows), len(peopleNames))
	}

	// panics on index
	if err = shouldPanic(func() {
		source.IndexOn() // empty list of columns
	}); err != nil {
		t.Error(err)
		return
	}

	if index, err = source.IndexOn("id"); err != nil {
		t.Error(err)
		return
	}

	if err = shouldPanic(func() {
		index.SubIndex("aaa", "bbb") // too many values
	}); err != nil {
		t.Error(err)
		return
	}

	// invalid header
	people := Take(FromFile(tempFiles["people"]).ExpectHeader(map[string]int{
		"name":    1,
		"surname": 3, // wrong column
	}))

	err = people(neverCalled)

	if err == nil || !strings.HasSuffix(err.Error(), `row 1: misplaced column "surname": expected at pos. 3, but found at pos. 2`) {
		t.Error("Unexpected error:", err)
		return
	}

	people = Take(FromFile(tempFiles["people"]).ExpectHeader(map[string]int{
		"name":    1,
		"surname": 25, // non-existent column
	}))

	err = people(neverCalled)

	if err == nil || !strings.HasSuffix(err.Error(), `row 1: misplaced column "surname": expected at pos. 25, but found at pos. 2`) {
		t.Error("Unexpected error:", err)
		return
	}
}

func TestNumericalConversions(t *testing.T) {
	row := Row{"int": "12345", "float": "3.1415926", "string": "xyz"}

	var intVal int
	var err error

	if intVal, err = row.ValueAsInt("int"); err != nil {
		t.Error("Unexpected error:", err)
		return
	}

	if intVal != 12345 {
		t.Errorf("Unexpected value in integer conversion: %d instead of %s", intVal, row["int"])
		return
	}

	if _, err = row.ValueAsInt("string"); err == nil {
		t.Error("Missed error in integer conversion")
		return
	}

	if err.Error() != `column "string": cannot convert "xyz" to integer: invalid syntax` {
		t.Error("Unexpected error message in integer conversion:", err)
		return
	}

	var floatVal float64

	if floatVal, err = row.ValueAsFloat64("float"); err != nil {
		t.Error("Unexpected error:", err)
		return
	}

	if math.Abs(floatVal-3.1415926)/floatVal > 1e-6 {
		t.Errorf("Unexpected value in float conversion: %f instead of %s", floatVal, row["float"])
		return
	}

	if _, err = row.ValueAsFloat64("string"); err == nil {
		t.Error("Missed error in float conversion")
		return
	}

	if err.Error() != `column "string": cannot convert "xyz" to float: invalid syntax` {
		t.Error("Unexpected error message in float conversion:", err)
		return
	}
}

func TestIndexStore(t *testing.T) {
	const namePrefix = "index"

	// read data and build index
	index, err := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname")).IndexOn("id")

	if err != nil {
		t.Error(err)
		return
	}

	// write index
	if tempFiles[namePrefix], err = createTempFile(namePrefix); err != nil {
		t.Error(err)
		return
	}

	if err = index.WriteTo(tempFiles[namePrefix]); err != nil {
		t.Error(err)
		return
	}

	// read index
	var index2 *Index

	if index2, err = LoadIndex(tempFiles[namePrefix]); err != nil {
		t.Error(err)
		return
	}

	// compare column names
	if len(index.impl.columns) != len(index2.impl.columns) {
		t.Errorf("Column number mismatch: %d instead of %d", len(index2.impl.columns), len(index.impl.columns))
		return
	}

	for i, c := range index.impl.columns {
		if c != index2.impl.columns[i] {
			t.Errorf(`Unexpected column name: "%s" instead of "%s"`, index2.impl.columns[i], c)
			return
		}
	}

	// compare rows
	if len(index.impl.rows) != len(index2.impl.rows) {
		t.Errorf("Rows number mismatch^ %d instead of %d", len(index2.impl.rows), len(index.impl.rows))
		return
	}

	for i, row := range index.impl.rows {
		if row.String() != index2.impl.rows[i].String() {
			t.Errorf(`Mismatching rows at %d:\n\t%s\n\t%s`, i, row.String(), index2.impl.rows[i].String())
		}
	}
}

func TestJSONStruct(t *testing.T) {
	var buff bytes.Buffer

	// read input .csv and convert to JSON
	err := Take(FromFile(tempFiles["people"]).SelectColumns("name", "surname", "born")).ToJSON(&buff)

	if err != nil {
		t.Error(err)
		return
	}

	// de-serialise back from JSON to struct slice
	data, err := peopleFromJSON(&buff)

	if err != nil {
		t.Error(err)
		return
	}

	// validate
	if len(data) != len(peopleData) {
		t.Errorf("Invalid number of records: %d instead of %d", len(data), len(peopleData))
		return
	}

	for i := 0; i < len(data); i++ {
		if data[i].Name != peopleData[i].Name ||
			data[i].Surname != peopleData[i].Surname ||
			data[i].Born != peopleData[i].Born {
			t.Errorf("Data mismatch: %v instead of %v", data[i], peopleData[i])
			return
		}
	}
}

// benchmarks -------------------------------------------------------------------------------------
func BenchmarkCreateSmallSingleIndex(b *testing.B) {
	source, err := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname")).ToRows()

	if err != nil {
		b.Error(err)
		return
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var index *Index

		if index, err = TakeRows(source).UniqueIndexOn("id"); err != nil {
			b.Error(err)
			return
		}

		// just to do something with the index
		if len(index.impl.columns) != 1 || index.impl.columns[0] != "id" || len(index.impl.rows) != len(peopleData) {
			b.Error("Wrong index")
			return
		}
	}
}

func BenchmarkCreateBiggerMultiIndex(b *testing.B) {
	source, err := Take(FromFile(tempFiles["orders"]).SelectColumns("cust_id", "prod_id", "qty")).ToRows()

	if err != nil {
		b.Error(err)
		return
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var index *Index

		if index, err = TakeRows(source).IndexOn("cust_id", "prod_id"); err != nil {
			b.Error(err)
			return
		}

		// just to do something with the index
		if len(index.impl.columns) != 2 || len(index.impl.rows) != len(ordersData) {
			b.Error("Wrong index")
			return
		}
	}
}

func BenchmarkSearchSmallSingleIndex(b *testing.B) {
	index, err := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname")).UniqueIndexOn("id")

	if err != nil {
		b.Error(err)
		return
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		index.Find("0")
	}
}

func BenchmarkSearchBiggerMultiIndex(b *testing.B) {
	index, err := Take(FromFile(tempFiles["orders"]).SelectColumns("cust_id", "prod_id", "qty")).IndexOn("cust_id", "prod_id")

	if err != nil {
		b.Error(err)
		return
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		index.Find("0", "0")
	}
}

func BenchmarkJoinOnSmallSingleIndex(b *testing.B) {
	source, err := Take(FromFile(tempFiles["orders"]).SelectColumns("cust_id", "prod_id", "qty")).ToRows()

	if err != nil {
		b.Error(err)
		return
	}

	var index *Index

	index, err = Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname")).UniqueIndexOn("id")

	if err != nil {
		b.Error(err)
		return
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := TakeRows(source).Join(index, "cust_id")(nop); err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkJoinOnBiggerMultiIndex(b *testing.B) {
	source, err := Take(FromFile(tempFiles["people"]).SelectColumns("id", "name", "surname")).ToRows()

	if err != nil {
		b.Error(err)
		return
	}

	var index *Index

	index, err = Take(FromFile(tempFiles["orders"]).SelectColumns("cust_id", "prod_id", "qty")).IndexOn("cust_id", "prod_id")

	if err != nil {
		b.Error(err)
		return
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := TakeRows(source).Join(index, "id")(nop); err != nil {
			b.Error(err)
			return
		}
	}
}

// generated test data ----------------------------------------------------------------------------
type personData struct {
	Name, Surname string
	Born          int `json:",string"`
}

var peopleData = make([]personData, len(peopleNames)*len(peopleSurnames))

type orderData struct {
	custID, prodID, qty int
	ts                  time.Time
}

const numOrders = 10000

var ordersData [numOrders]orderData

// people.csv -------------------------------------------------------------------------------------
// http://www.ukbabynames.com/
var peopleNames = [...]string{
	"Amelia", "Olivia", "Emily", "Ava", "Isla",
	"Oliver", "Jack", "Harry", "Jacob", "Charlie",
}

// http://surname.sofeminine.co.uk/w/surnames/most-common-surnames-in-great-britain.html
var peopleSurnames = [...]string{
	"Smith", "Jones", "Taylor", "Williams", "Brown", "Davies",
	"Evans", "Wilson", "Thomas", "Roberts", "Johnson", "Lewis",
}

var peopleHeader = []string{"id", "name", "surname", "born"}

func makePersonsCsvFile() error {
	return withTempFileWriter("people", func(out *csv.Writer) error {
		// header
		if err := out.Write(peopleHeader); err != nil {
			return err
		}

		// body
		for i, name := range peopleNames {
			for j, surname := range peopleSurnames {
				id := i*len(peopleSurnames) + j

				peopleData[id] = personData{
					Name:    name,
					Surname: surname,
					Born:    1916 + rand.Intn(90), // at least 10 years old
				}

				person := &peopleData[id]

				if err := out.Write([]string{
					strconv.Itoa(id),
					person.Name,
					person.Surname,
					strconv.Itoa(person.Born),
				}); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func peopleFromJSON(in io.Reader) (pd []personData, err error) {
	err = json.NewDecoder(in).Decode(&pd)
	return
}

// stock.csv ------------------------------------------------------------------------------------
var stockItems = [...]struct {
	name  string
	price float64
}{
	{"banana", 0.01},
	{"apple", 0.02},
	{"orange", 0.03},
	{"pea", 0.04},
	{"tomato", 0.05},
	{"potato", 0.06},
	{"cucumber", 0.07},
	{"iPhone", 0.08},
}

var stockItemsHeader = []string{"prod_id", "product", "price"}

func makeStockCsvFile() error {
	return withTempFileWriter("stock", func(out *csv.Writer) error {
		// header
		if err := out.Write(stockItemsHeader); err != nil {
			return err
		}

		// body
		for i, item := range stockItems {
			price := strconv.FormatFloat(item.price, 'f', 2, 64)

			if err := out.Write([]string{strconv.Itoa(i), item.name, price}); err != nil {
				return err
			}
		}

		return nil
	})
}

// orders.csv -----------------------------------------------------------------------------------
var orderHeader = []string{"order_id", "cust_id", "prod_id", "qty", "ts"}

func makeOrderCsvFile() error {
	return withTempFileWriter("orders", func(out *csv.Writer) error {
		// header
		if err := out.Write(orderHeader); err != nil {
			return err
		}

		// body
		now := time.Now()

		for i := 0; i < numOrders; i++ {
			ordersData[i] = orderData{
				custID: rand.Intn(len(peopleNames) * len(peopleSurnames)),
				prodID: rand.Intn(len(stockItems)),
				qty:    rand.Intn(100) + 1,
				ts:     now.Add(-time.Second * time.Duration(rand.Intn(100000)+1)),
			}

			order := &ordersData[i]

			if err := out.Write([]string{
				strconv.Itoa(i),
				strconv.Itoa(order.custID),
				strconv.Itoa(order.prodID),
				strconv.Itoa(order.qty),
				order.ts.Format(time.RFC3339),
			}); err != nil {
				return err
			}
		}

		return nil
	})
}

// tests set-up -----------------------------------------------------------------------------------
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	defer deleteTemps()

	if err := anyFrom(makePersonsCsvFile, makeOrderCsvFile, makeStockCsvFile); err != nil {
		panic(err)
	}

	saveTemps := flag.Bool("save-temps", false, "Save all generated temporary files")
	flag.Parse()

	if *saveTemps {
		if err := saveTempFiles(); err != nil {
			panic(err)
		}
	}

	return m.Run()
}

// helpers ----------------------------------------------------------------------------------------
func anyFrom(funcs ...func() error) error {
	for _, fn := range funcs {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func sortedCopy(list []string) (r []string) {
	r = make([]string, len(list))

	copy(r, list)
	sort.Strings(r)
	return
}

func createTempFile(prefix string) (name string, err error) {
	var file *os.File

	if file, err = os.CreateTemp("", prefix); err != nil {
		return
	}

	name = file.Name()
	file.Close()
	return
}

// cust_id, prod_id, amount
func createAmountsTable() (amounts DataSource, err error) {
	var prodIndex *Index

	prodIndex, err = Take(FromFile(tempFiles["stock"]).SelectColumns("prod_id", "price")).UniqueIndexOn("prod_id")

	if err == nil {
		amounts = Take(FromFile(tempFiles["orders"]).SelectColumns("cust_id", "prod_id", "qty")).
			Join(prodIndex).
			Transform(func(row Row) (Row, error) {
				var qty int
				var price float64
				var e error

				if qty, e = row.ValueAsInt("qty"); e != nil {
					return nil, e
				}

				if price, e = row.ValueAsFloat64("price"); e != nil {
					return nil, e
				}

				row["amount"] = strconv.FormatFloat(price*float64(qty), 'f', 2, 64)

				delete(row, "price")
				delete(row, "qty")
				return row, nil
			})
	}

	return
}

func neverCalled(Row) error { return errors.New("This must never be called") }

func nop(Row) error { return nil }

func shouldPanic(fn func()) error {
	defer func() {
		_ = recover()
	}()

	fn()
	return errors.New("Panic did not happen")
}

// temporary files --------------------------------------------------------------------------------
var tempFiles = make(map[string]string, 10)

func deleteTemps() {
	for _, name := range tempFiles {
		os.Remove(name)
	}
}

func withTempFileWriter(name string, fn func(*csv.Writer) error) (err error) {
	var file *os.File

	if file, err = os.CreateTemp("", name); err != nil {
		return err
	}

	defer func() {
		if e := file.Close(); e != nil && err == nil {
			err = e
		}
	}()

	tempFiles[name] = file.Name()

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

func saveTempFiles() error {
	for name, fileName := range tempFiles {
		dest := name + ".csv"

		os.Remove(dest) // otherwise os.Link fails

		if err := os.Link(fileName, dest); err != nil {
			return err
		}
	}

	return nil
}
