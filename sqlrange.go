package sqlrange

import (
	"context"
	"iter"
	"reflect"
	"slices"
	"sync/atomic"

	"errors"
	"github.com/jackc/pgx/v5"
)

// Queryable is an interface implemented by types that can send SQL queries,
// such as [sql.DB], [sql.Conn], or [sql.Tx].
type Queryable interface {
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
}

// Query returns the results of the query as a sequence of rows.
//
// The returned function automatically closes the underlying [sql.Rows] value when
// it completes its iteration.
//
// A typical use of Query is:
//
//	for row, err := range sqlrange.Query[RowType](ctx, db, query, args...) {
//	  if err != nil {
//	    ...
//	  }
//	  ...
//	}
//
// The q parameter represents a queryable type, such as [sql.DB], [sql.Conn],
// or [sql.Tx].
//
// See [Scan] for more information about how the rows are mapped to the row type
// parameter Row.
func Query[Row any](ctx context.Context, q Queryable, query string, args ...any) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		if rows, err := q.Query(ctx, query, args...); err != nil {
			var zero Row
			yield(zero, err)
		} else {
			scan[Row](yield, rows)
		}
	}
}

// Scan returns a sequence of rows from a [sql.Rows] value.
//
// The returned function automatically closes the rows passed as argument when
// it completes its iteration.
//
// A typical use of Scan is:
//
//	rows, err := db.Query(ctx, query, args...)
//	if err != nil {
//	  ...
//	}
//	for row, err := range sqlrange.Scan[RowType](rows) {
//	  if err != nil {
//	    ...
//	  }
//	  ...
//	}
//
// Scan uses reflection to map the columns of the rows to the fields of the
// struct passed as argument. The mapping is done by matching the name of the
// columns with the name of the fields. The name of the columns is taken from
// the "sql" tag of the fields. For example:
//
//	type Row struct {
//	  ID   int64  `sql:"id"`
//	  Name string `sql:"name"`
//	}
//
// The fields of the struct that do not have a "sql" tag are ignored.
//
// Ranging over the returned function will panic if the type parameter is not a
// struct.
func Scan[Row any](rows pgx.Rows) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) { scan(yield, rows) }
}

func scan[Row any](yield func(Row, error) bool, rows pgx.Rows) {
	defer rows.Close()
	var zero Row

	fieldDescs := rows.FieldDescriptions()
	if fieldDescs == nil {
		yield(zero, errors.New("missing rows.FieldDescriptions()"))
		return
	}

	columns := make([]string, 0, len(fieldDescs))
	for _, fd := range fieldDescs {
		columns = append(columns, fd.Name)
	}

	scanArgs := make([]any, len(columns))
	row := new(Row)
	val := reflect.ValueOf(row).Elem()

	for columnName, structField := range Fields(val.Type()) {
		if columnIndex := slices.Index(columns, columnName); columnIndex >= 0 {
			scanArgs[columnIndex] = val.FieldByIndex(structField.Index).Addr().Interface()
		}
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			yield(zero, err)
			return
		}
		if !yield(*row, nil) {
			return
		}
		*row = zero
	}

	if err := rows.Err(); err != nil {
		yield(zero, err)
	}
}

// Fields returns a sequence of the fields of a struct type that have a "sql"
// tag.
func Fields(t reflect.Type) iter.Seq2[string, reflect.StructField] {
	return func(yield func(string, reflect.StructField) bool) {
		cache, _ := cachedFields.Load().(map[reflect.Type][]field)

		fields, ok := cache[t]
		if !ok {
			fields = appendFields(nil, t, nil)

			newCache := make(map[reflect.Type][]field, len(cache)+1)
			for k, v := range cache {
				newCache[k] = v
			}
			newCache[t] = fields
			cachedFields.Store(newCache)
		}

		for _, f := range fields {
			if !yield(f.name, f.field) {
				return
			}
		}
	}
}

type field struct {
	name  string
	field reflect.StructField
}

var cachedFields atomic.Value // map[reflect.Type][]field

func appendFields(fields []field, t reflect.Type, index []int) []field {
	for i, n := 0, t.NumField(); i < n; i++ {
		if f := t.Field(i); f.IsExported() {
			if len(index) > 0 {
				f.Index = append(index, f.Index...)
			}
			if f.Anonymous {
				if f.Type.Kind() == reflect.Struct {
					fields = appendFields(fields, f.Type, f.Index)
				}
			} else if s, ok := f.Tag.Lookup("sql"); ok {
				fields = append(fields, field{s, f})
			}
		}
	}
	return fields
}
