vaquita
=======

Go driver for MariaDB/MySQL server

## Connection URL & Properties

### URL
  mysql://[[user][:password]@][host][:port]/[schema]
    [?propertyName1][=propertyValue1]
    [&propertyName2][=propertyValue2]...

  * user:
      User connecting to the server.
  * password:
      Password to use when connecting to the server.
  * host:
      It is the host name of the machine runing MySQL/MariaDB server.
      (default: 127.0.0.1)
  * port:
      Port number the MySQL/MariaDB server is listening to. (default: 3306)
  * schema:
      Name of the schema to connect to.
  * propertyName=propertyValue
              - It represents an optional, ampersand-separated list of
                properties.

  eg. "mysql://root:pass@localhost:3306/test?Socket=/tmp/mysql.sock"

  Reference : http://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html

### Properties
  * Compress:
      Compress protocol network packets using zlib.
  * LocalInfile:
      Enable 'LOAD DATA LOCAL INFILE' support. (default: false)
  * MaxAllowedPacket:
      Maximum client packet size. (default: 16MB)
  * Socket:
      Unix socket to connect to the server.
  * SSLCA:
      File containing a list of SSL CAs.
  * SSLCert:
      SSL certificate file.
  * SSLKey:
      SSL key file.

  Note : A property name is in Pascal case (or upper Camel case) and case-sensitive.

## Examples
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
        if result, err = db.Exec("insert into test.t1 values (?, ?)", 1, "NULL"); err != nil {
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

        defer rows.Close()
        // ...
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

        Db.Exec() failed :  mysqld: 1050 (42S01): Table 't1' already exists
        | Code | Message                        | SQL State | When                                     |
        | 1050 | Table 't1' already exists      | 42S01     | 2014-07-15 18:37:39.157151115 -0400 EDT  |

