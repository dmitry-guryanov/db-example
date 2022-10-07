package dbutils

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"go.uber.org/multierr"
)

func sqlErr(err error, query string, args ...interface{}) error {
	return fmt.Errorf(`run query "%s" with args %+v: %w`, query, args, err)
}

func namedQuery(query string, arg interface{}) (nq string, args []interface{}, err error) {
	nq, args, err = sqlx.Named(query, arg)
	if err != nil {
		return "", nil, sqlErr(err, query, args...)
	}
	return nq, args, nil
}

func Exec(ctx context.Context, db sqlx.ExecerContext, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return res, sqlErr(err, query, args...)
	}

	return res, nil
}

func NamedExec(ctx context.Context, db sqlx.ExtContext, query string, arg interface{}) (sql.Result, error) {
	nq, args, err := namedQuery(query, arg)
	if err != nil {
		return nil, err
	}

	return Exec(ctx, db, db.Rebind(nq), args...)
}

func Select(ctx context.Context, db sqlx.QueryerContext, dest interface{}, query string, args ...interface{}) error {
	if err := sqlx.SelectContext(ctx, db, dest, query, args...); err != nil {
		return sqlErr(err, query, args...)
	}

	return nil
}

func NamedSelect(ctx context.Context, db sqlx.ExtContext, dest interface{}, query string, arg interface{}) error {
	nq, args, err := namedQuery(query, arg)
	if err != nil {
		return err
	}

	return Select(ctx, db, dest, db.Rebind(nq), args...)
}

func Get(ctx context.Context, db sqlx.QueryerContext, dest interface{}, query string, args ...interface{}) error {
	if err := sqlx.GetContext(ctx, db, dest, query, args...); err != nil {
		return sqlErr(err, query, args...)
	}

	return nil
}

func NamedGet(ctx context.Context, db sqlx.ExtContext, dest interface{}, query string, arg interface{}) error {
	nq, args, err := namedQuery(query, arg)
	if err != nil {
		return err
	}

	return Get(ctx, db, dest, db.Rebind(nq), args...)
}

func SelectMaps(ctx context.Context, db sqlx.QueryerContext, query string, args ...interface{}) (ret []map[string]interface{}, err error) {
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, sqlErr(err, query, args...)
	}

	defer func() {
		err = multierr.Combine(err, rows.Close())
	}()

	ret = []map[string]interface{}{}
	numCols := -1
	for rows.Next() {
		var m map[string]interface{}
		if numCols < 0 {
			m = map[string]interface{}{}
		} else {
			m = make(map[string]interface{}, numCols)
		}

		if err = rows.MapScan(m); err != nil {
			return nil, sqlErr(err, query, args...)
		}
		ret = append(ret, m)
		numCols = len(m)
	}

	if err = rows.Err(); err != nil {
		return nil, sqlErr(err, query, args...)
	}

	return ret, nil
}

func NamedSelectMaps(ctx context.Context, db sqlx.ExtContext, query string, arg interface{}) (ret []map[string]interface{}, err error) {
	nq, args, err := namedQuery(query, arg)
	if err != nil {
		return nil, err
	}

	return SelectMaps(ctx, db, db.Rebind(nq), args...)
}

func GetMap(ctx context.Context, db sqlx.QueryerContext, query string, args ...interface{}) (ret map[string]interface{}, err error) {
	row := db.QueryRowxContext(ctx, query, args...)
	if row.Err() != nil {
		return nil, sqlErr(row.Err(), query, args...)
	}

	ret = map[string]interface{}{}
	if err := row.MapScan(ret); err != nil {
		return nil, sqlErr(err, query, args...)
	}

	return ret, nil
}

func NamedGetMap(ctx context.Context, db sqlx.ExtContext, query string, arg interface{}) (ret map[string]interface{}, err error) {
	nq, args, err := namedQuery(query, arg)
	if err != nil {
		return nil, err
	}

	return GetMap(ctx, db, db.Rebind(nq), args...)
}

type TxFunc func(tx *sqlx.Tx) error

type TxRunner interface {
	BeginTxx(context.Context, *sql.TxOptions) (*sqlx.Tx, error)
}

func RunTx(ctx context.Context, db TxRunner, f TxFunc) (err error) {
	var tx *sqlx.Tx

	opts := &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	}

	tx, err = db.BeginTxx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			err = multierr.Combine(err, tx.Rollback())
		} else {
			err = tx.Commit()
		}
	}()

	return f(tx)
}
