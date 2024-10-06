# This is db file folder

## 1. Install SQLite
First, ensure that SQLite is installed. If not, you can install it using the following commands:

```bash
sudo apt update
sudo apt install sqlite3
```

## 2. Create a SQLite Database File
You can use the `sqlite3` command to create a new SQLite database file. Suppose you want to create a database file named `sqlite.db`. You can do this with the following command:

```bash
sqlite3 sqlite.db
```

This will create a SQLite database file named `sqlite.db` in the current directory and enter the SQLite command-line interface.

## 3. Create a Table
Within the SQLite command-line interface, you can create tables, insert data, query data, etc. For example, to create a simple table:

```sql
CREATE TABLE maps (
    ID int,
    KEY varchar(50) NOT NULL,
    VALUE varchar(50) NOT NULL
);
```

### 4. Exit the SQLite Command-line Interface
After completing your operations, you can exit the SQLite command-line interface using the `.exit` or `.quit` command:

```bash
.exit
```

### 5. Verify the Database File
You can reopen the database file and check if the table was created successfully using the following commands:

```bash
sqlite3 sqlite.db
```
