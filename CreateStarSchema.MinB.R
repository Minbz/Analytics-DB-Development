#title: Practicum II (Summer 2024) / Mine a Database
#author: Baozhang Min
#date: Summer Full 2024

library(RSQLite)
library(DBI)
library(RMySQL)

# Connect with the local SQLite Datebase created in Part 1
conn <- dbConnect(SQLite(), dbname = "practicum2")

# Get table list from SQLite DB
tables <- dbListTables(conn)
# Get the list of tables starting with "Sales_" 
sales.tables <- grep("^Sales_", tables, value = TRUE)
print(sales.tables)

# Helper function to execute a sql command to a list of tables, return the combined result.
l.get.query <- function(conn, sql.start, table.list, sql.end){
  result <- data.frame()
  for (table in table.list){
    this.sql <- paste0(sql.start, table, sql.end)
    result <- rbind(result, dbGetQuery(conn, this.sql))
  }
  return(result)
}

# Helper function to get the corresponding quarter value by the given month
get.quarter <- function(month){
  month <- as.integer(month)
  if(month >= 1 && month <= 3){
    return("1")
  }
  if (month >= 4 && month <= 6){
    return("2")
  }
  if (month >= 7 && month <= 9){
    return("3")
  }
  if (month >= 10 && month <= 12){
    return("4")
  }
  return("-1")
}

# 1-a. Define the granularity for the sales fact table
# As per the given requirement, the smallest granularity the scheme needs includes
#    - each product
#    - total amount
#    - total units
#    - each month
#    - each country
#    - each territory (because Sales records having the same country might include different territory)

# 1-b. Define the sales fact table
# Since there aren't many complex potential roll-up dimensions, I chose to use the One Big Table approach.
create.sales.fact <- "
CREATE TABLE IF NOT EXISTS sales_facts(
  sfID INTEGER PRIMARY KEY,
  product_name TEXT NOT NULL,
  year TEXT NOT NULL,
  quarter TEXT NOT NULL,
  month TEXT NOT NULL,
  country TEXT NOT NULL,
  territory TEXT NOT NULL,
  total_amount_of_month REAL NOT NULL,
  total_units_of_month REAL NOT NULL
);"

# 1-c. Extract necessary data from SQLite DB
sql.start <- "
SELECT p.pName AS product_name, strftime('%Y', s.date) AS year, strftime('%m', s.date) AS month, 
cn.cntryName AS country, t.tName AS territory, 
SUM(p.unitcost * s.quantity) AS total_amount_of_month,
SUM(s.quantity) AS total_units_of_month
FROM "
sql.end <- " AS s
INNER JOIN Products p ON p.pID = s.pID
INNER JOIN Customers cs ON cs.cstmrID = s.cstmrID
INNER JOIN Countries cn ON cs.cntryID = cn.cntryID
INNER JOIN Representatives r ON r.repID = s.repID
INNER JOIN Territories t ON r.tID = t.tID
GROUP BY product_name, year, month, country, territory
;"

# Get query result from all sales tables
sales.df <- l.get.query(conn, sql.start, sales.tables, sql.end)

# Get the quarter values
sales.df$quarter <- sapply(sales.df$month, get.quarter)

# Generate sfID
sales.df$sfID <- seq(1, nrow(sales.df))

# 2-a. Define the granularity for the reps fact table
# As per the given requirement, the smallest granularity the scheme needs includes
#    - each rep
#    - total amount
#    - average amount
#    - each month, quarter, year

# 2-b Define the reps fact table
# Similarly, I chose to use the One Big Table approach.
create.reps.fact <- "
CREATE TABLE IF NOT EXISTS rep_facts(
  rfID INTEGER PRIMARY KEY,
  repID INTEGER NOT NULL,
  surname TEXT NOT NULL,
  firstname TEXT NOT NULL,
  year TEXT NOT NULL,
  quarter TEXT NOT NULL,
  month TEXT NOT NULL,
  total_amount_of_month REAL NOT NULL,
  average_amount_of_month REAL NOT NULL
);"

# 2-c. Extract necessary data from SQLite DB
sql.start <- "
SELECT r.repID, r.surname, r.firstname, 
strftime('%Y', s.date) AS year, strftime('%m', s.date) AS month,
SUM(p.unitcost * s.quantity) AS total_amount_of_month,
AVG(p.unitcost * s.quantity) AS average_amount_of_month
FROM "
sql.end <- " AS s
INNER JOIN Products p ON p.pID = s.pID
INNER JOIN Representatives r ON r.repID = s.repID
GROUP BY r.repID, year, month
;"

# Get query result from all sales tables
reps.df <- l.get.query(conn, sql.start, sales.tables, sql.end)

# Get the quarter values
reps.df$quarter <- sapply(reps.df$month, get.quarter)

# Generate sfID
reps.df$rfID <- seq(1, nrow(reps.df))

# Disconnect the local SQLite DB
dbDisconnect(conn)

# Connect to the cloud DB
db.user <- "admin"            
db.password <- "cs5200practicum2"    
db.name <- "cs5200practicum2"        

db.host <- "cs5200practicum2.cnaq2oqs01ng.ca-central-1.rds.amazonaws.com"      

db.port <- 3306

cloud.con <- dbConnect(
  RMySQL::MySQL(),
  host = db.host,
  port = db.port,
  user = db.user,
  dbname = db.name, 
  password = db.password
)

dbExecute(cloud.con, "DROP TABLE IF EXISTS sales_facts")
dbExecute(cloud.con, "DROP TABLE IF EXISTS rep_facts")

# Create sales_facts table
dbExecute(cloud.con, create.sales.fact)
# Populate sales_facts table
dbWriteTable(cloud.con, "sales_facts", sales.df, overwrite = TRUE, row.names = FALSE)

# Create rep_facts table
dbExecute(cloud.con, create.reps.fact)
# Populate rep_facts table
dbWriteTable(cloud.con, "rep_facts", reps.df, overwrite = TRUE, row.names = FALSE)

# Disconnect the cloud DB
dbDisconnect(cloud.con)

