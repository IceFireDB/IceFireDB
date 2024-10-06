
# SQL Test for IceFireDB-SQLite

## Connecting to IceFireDB-SQLite Server Using MariaDB Client

### Step 1: Install MariaDB Client

First, ensure you have installed the `mariadb-client`:

```bash
sudo apt update
sudo apt install mariadb-client
```

### Step 2: Connect to IceFireDB-SQLite Server

Use the `mariadb` command to connect to the MySQL server running locally on port 23306 with the username `root` and password `123456`:

```bash
mariadb -h 127.0.0.1 -P 23306 -u root -p123456
```

**Explanation**

- `-h 127.0.0.1`: Specifies the connection to the local host.
- `-P 23306`: Specifies the connection to port 23306.
- `-u root`: Specifies the username as `root`.
- `-p123456`: Specifies the password as `123456`. Note that there is no space between `-p` and the password.


## Table Creation

```sql
CREATE TABLE maps (
    ID int,
    KEY varchar(50) NOT NULL,
    VALUE varchar(50) NOT NULL
);
```

## Querying Data

### Select All Records

```sql
SELECT * FROM maps;
```

### Select All Records Ordered by ID Descending

```sql
SELECT * FROM maps ORDER BY id DESC;
```

## Inserting Data

```sql
INSERT INTO `maps` (`id`, `key`, `value`) 
VALUES (1, 'a', 'aa'), (2, 'b', 'bb'), (3, 'c', 'ccccc');
```

## Updating Data

```sql
UPDATE `maps` SET value='bbbbbbb' WHERE id=2;
```

## Deleting Data

```sql
DELETE FROM `maps` WHERE id=3;
```