package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"

	"db-example/dbutils"
)

var conn = flag.String("conn", "postgres://test:test@localhost/test2700?sslmode=disable", "database connection string")

type pgxLogger struct{}

func (pl *pgxLogger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	var buffer bytes.Buffer
	buffer.WriteString(msg)

	for k, v := range data {
		buffer.WriteString(fmt.Sprintf(" %s=%+v", k, v))
	}

	log.Println(buffer.String())
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("error: %+v", err)
	}
}

// Вспомогательный код для инициалиации коннекта
func run() error {
	ctx := context.Background()

	connConfig, err := pgx.ParseConfig(*conn)
	if err != nil {
		return err
	}
	connConfig.RuntimeParams["application_name"] = "db-example"

	connConfig.Logger = &pgxLogger{}
	connConfig.LogLevel = pgx.LogLevelDebug
	connStr := stdlib.RegisterConnConfig(connConfig)

	dbh, err := sqlx.Connect("pgx", connStr)
	if err != nil {
		return fmt.Errorf("prepare db connection: %w", err)
	}
	defer dbh.Close()

	return example(ctx, dbh)
}

// Примеры
type user struct {
	ID    int64  `db:"id"`
	Login string `db:"login"`
	Name  string `db:"name"`
}

var initScript = `
DROP TABLE IF EXISTS test_users;
CREATE TABLE test_users (
	id BIGSERIAL,
	login TEXT,
	name TEXT
);

INSERT INTO test_users (login, name) VALUES
('ivanov', 'Иванов Иван Иванович'),
('petrov', 'Петров Пётр Петрович'),
('sidorov', 'Сидоров Сидор Сидорович');
`

func example(ctx context.Context, dbh *sqlx.DB) error {
	if _, err := dbutils.Exec(ctx, dbh, initScript); err != nil {
		return err
	}

	var users []user
	q := `SELECT * FROM test_users`
	if err := dbutils.Select(ctx, dbh, &users, q); err != nil {
		return err
	}
	log.Println(users)

	mm, err := dbutils.SelectMaps(ctx, dbh, q)
	if err != nil {
		return err
	}
	log.Println(mm)

	q = `SELECT * FROM test_users WHERE login=$1`
	m, err := dbutils.GetMap(ctx, dbh, q, "ivanov")
	if err != nil {
		return err
	}
	log.Println(m)

	q = `SELECT * FROM test_users WHERE login=:login`
	m, err = dbutils.NamedGetMap(ctx, dbh, q, map[string]interface{}{"login": "petrov"})
	if err != nil {
		return err
	}
	log.Println(m)

	var users2 []user
	q = `SELECT * FROM test_users`
	if err = dbh.SelectContext(ctx, &users2, q); err != nil {
		return err
	}

	q = `SELECT * FROM test_users WHERE login = ANY($1)`
	if err := dbutils.Select(ctx, dbh, &users, q, []string{"ivanov", "petrov"}); err != nil {
		return err
	}
	log.Println(users)

	u, err := updateUser(ctx, dbh, "ivanov", "Сергеев")
	if err != nil {
		return err
	}
	log.Println(u)

	return nil
}

func updateUser(ctx context.Context, dbh *sqlx.DB, login string, newName string) (u user, err error) {
	err = dbutils.RunTx(ctx, dbh, func(tx *sqlx.Tx) error {
		u, err = updateUserTx(ctx, tx, login, newName)
		return err
	})

	return u, err
}

func updateUserTx(ctx context.Context, tx sqlx.ExtContext, login string, newName string) (u user, err error) {
	q := `SELECT * FROM test_users WHERE login = $1`
	if err := dbutils.Get(ctx, tx, &u, q, login); err != nil {
		return user{}, err
	}

	q = `UPDATE test_users
		SET name = $1
		WHERE login = $2
		RETURNING *`
	if err := dbutils.Get(ctx, tx, &u, q, newName, login); err != nil {
		return user{}, err
	}

	return u, nil
}
