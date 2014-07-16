vaquita
=======

Go driver for MariaDB/MySQL server

## Examples
### Registering the driver

    const driverName = "mysql"

    func register() {
            var d mysql.Driver
            sql.Register(driverName, d)
    }


### Opening a connection

        dsn = "mysql://user:pass@localhost:3306/test"

        if db, err = sql.Open(driverName, dsn); err != nil {
                fmt.Println("sql.Open() failed : ", err)
        } else if err = db.Ping(); err != nil { 
                fmt.Println("db.Ping() failed : ", err)
                os.Exit(1)
        } else {
                fmt.Println("connection established successfully")
        }

### Executing queries

        var (
                err             error
                db              *sql.DB
                result          sql.Result
                rows            *sql.Rows
                // ..snip..
        )

        // ..snip..
        if result, err = db.Exec("create table %s (i int, j varchar(20))", "test.t1"); err != nil {
                fmt.Println("Db.Exec() failed : ", err)
        } else {
                fmt.Println("table created successfully")
        }

        // ..snip..
        if result, err = db.Exec("insert into test.t1 values (%d, %s)", 1, "NULL"); err != nil {
                fmt.Println("Db.Exec() failed : ", err)
        } else {
                fmt.Println("record inserted successfully")
        }

        // ..snip..
        if rows, err = db.Query("select * from test.t1"); err != nil {
                fmt.Println("Db.Query() failed : ", err)
        }

        // ..snip..

### Inspecting Exec() result (sql.Result)

        lastInsertId, _ := result.LastInsertId()
        rowsAffected, _ := result.RowsAffected()

### Inspecting Query() rows (sql.Rows)

        for rows.Next() {
                var (
                        i int
                        s sql.NullString
                )
                if err = rows.Scan(&i, &s); err != nil {
                        fmt.Println("Rows.Scan failed : ", err)
                } else {
                        fmt.Println("Row : ", i, s)
                }
        }

### Prepared statements

        var stmt *sql.Stmt

        // ..snip..

        // prepare
        if stmt, err = db.Prepare("select * from test.t1 where i = ?"); err != nil {
                fmt.Println("Db.Prepare() failed : ", err)
                os.Exit(1)
        } else {
                fmt.Println("statement prepared successfully")
        }

        // execute
        if rows, err = stmt.Query(1); err != nil {
                fmt.Println("Db.Exec() failed : ", err)
        } else {
                // process rows
        }

        // close
        if err = stmt.Close(); err != nil {
                fmt.Println("Stmt.Close() failed : ", err)
                os.Exit(1)
        } else {
                fmt.Println("statement closed successfully")
        }

### Handling server errors

        if result, err = db.Exec("create table %s (i int, j varchar(20))", "test.t1"); err != nil {
                fmt.Println("Db.Exec() failed : ", err)
                if e, ok := err.(*mysql.Error); ok {
                        fmt.Printf("| %-4s | %-30s | %-9s | %-40s |\n", "Code", "Message", "SQL State", "When")
                        fmt.Printf("| %-4d | %-30s | %-9s | %-40v |\n", e.Code(), e.Message(), e.SqlState(), e.When())
                }
        } else {
                fmt.Println("table created successfully")
        }

Output:

        Db.Exec() failed :  [2014-07-15 18:37:39.157151115 -0400 EDT] mysqld: (1050) Table 't1' already exists
        | Code | Message                        | SQL State | When                                     |
        | 1050 | Table 't1' already exists      | 42S01     | 2014-07-15 18:37:39.157151115 -0400 EDT  |

