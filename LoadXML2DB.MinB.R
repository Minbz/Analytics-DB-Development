#title: Practicum II (Summer 2024) / Mine a Database
#author: Baozhang Min
#date: Summer Full 2024

library(RSQLite)
library(XML)
XML.PATH <- "./txn-xml/"

# Part 1 Create a relational schema that contains the required entities.
## Part 1.1-1.3 Create a relational schema that contains the following entities/tables
## Part 1.4 Realize the relational schema in SQLite

conn <- dbConnect(SQLite(), dbname = "practicum2")

# Define and realize necessary tables
create.territory <-
"
CREATE TABLE IF NOT EXISTS Territories(
  tID INTEGER PRIMARY KEY,
  tName TEXT NOT NULL
);
"
dbExecute(conn, create.territory)

create.reps <-
"
CREATE TABLE IF NOT EXISTS Representatives(
  repID INTEGER PRIMARY KEY,
  surname TEXT NOT NULL,
  firstname TEXT NOT NULL,
  phone TEXT NOT NULL,
  hiredate TEXT NOT NULL,
  commission REAL NOT NULL,
  certified INTEGER NOT NULL,
  tID INTEGER NOT NULL,
  FOREIGN KEY (tID)
    REFERENCES Territories (tID)
);
"
dbExecute(conn, create.reps)

create.products <-
"
CREATE TABLE IF NOT EXISTS Products(
  pID INTEGER PRIMARY KEY,
  pName TEXT NOT NULL,
  currency TEXT NOT NULL,
  unitCost REAL NOT NULL
);
"
dbExecute(conn, create.products)

create.country <-
"
CREATE TABLE IF NOT EXISTS Countries(
  cntryID INTEGER PRIMARY KEY,
  cntryName TEXT NOT NULL
);
"
dbExecute(conn, create.country)
  
create.customers <-
"
CREATE TABLE IF NOT EXISTS Customers(
  cstmrID INTEGER PRIMARY KEY,
  cstmrName TEXT NOT NULL,
  cntryID INTEGER NOT NULL,
  FOREIGN KEY (cntryID)
    REFERENCES Countries(cntryID)
);
"
dbExecute(conn, create.customers)

create.sales <-
"
CREATE TABLE IF NOT EXISTS Sales(
  sID INTEGER PRIMARY KEY,
  date TEXT NOT NULL,
  repID INTEGER NOT NULL,
  cstmrID INTEGER NOT NULL,
  pID INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  FOREIGN KEY (repID)
    REFERENCES Representatives (repID),
  FOREIGN KEY (cstmrID)
    REFERENCES Customers (cstmrID),
  FOREIGN KEY (pID)
    REFERENCES Products (pID)
);
"  
dbExecute(conn, create.sales)

## Part 1.5 Load all XML files from the txn-xml folder into R.
## Part 1.6 Extract and transform the data from the XML files and save the data into the tables in the database.

# Helper functions to extract Reps data
# Function to extract data from a single rep node
extract.single.rep <- function(rep.node){
  rID <- xmlAttrs(rep.node)
  rID <- as.integer(sub("r", "", rID))
  demo.items <- xmlChildren(rep.node[["demo"]])
  
  # Check if certified item exists. If yes, set its value to 1 (true). Otherwise, 0(false)
  certified <- rep.node[["certified"]]
  if (is.null(certified)){
    certified <- 0
  } else {
    certified <- 1
  }
  
  rep.df <- data.frame(
    repID = rID,
    surname = xmlValue(demo.items[["sur"]]),
    firstname = xmlValue(demo.items[["first"]]),
    phone = xmlValue(demo.items[["phone"]]),
    hiredate = xmlValue(demo.items[["hiredate"]]),
    commission = xmlValue(rep.node[["commission"]]),
    territory = xmlValue(rep.node[["territory"]]),
    certified = certified,
    row.names = NULL
  )
  return(rep.df)
}

# Function to extract data from xml data consisting multiple rep nodes
extract.reps <- function(xml.dom){
  rep.nodes <- getNodeSet(xml.dom, "//rep")
  reps.df <- do.call(rbind, lapply(rep.nodes, extract.single.rep))
  return(reps.df)
}

# Function to convert the hiredate to ISO 8601 format
convert.reps.date <- function(date.string){
  parts <- strsplit(date.string, " ")[[1]]
  month.str <- parts[1]
  day <- sprintf("%02d", as.numeric(parts[2]))
  year <- parts[3]
  
  # Define a named vector to convert string months to numbers
  months.dic <- c("Jan" = "01", "Feb" = "02", "Mar" = "03", "Apr" = "04",
              "May" = "05", "Jun" = "06", "Jul" = "07", "Aug" = "08",
              "Sep" = "09", "Oct" = "10", "Nov" = "11", "Dec" = "12")
  
  month <- months.dic[month.str]
  
  date.df <- paste(year, month, day, sep = "-")
  
  return(date.df)
}

# Get all the target xml files for Sales records
reps.files <- list.files(path = XML.PATH, pattern = "pharmaReps*")
all.reps <- data.frame()

# Parse each xml file found in the target path into one dataframe
for (rep.file in reps.files){
  rep.path <- paste0(XML.PATH, rep.file)
  rep.dom <- xmlParse(rep.path)
  
  # Extract all sales
  all.reps <- rbind(all.reps, extract.reps(rep.dom))
}

# Extract and make a separate Territories dataframe
unique.territories <- unique(all.reps$territory)
territories.df <- data.frame(tID = seq_along(unique.territories),
                             tName = unique.territories,
                             stringsAsFactors = FALSE)

# Populate Territories SQLite table
dbWriteTable(conn, "Territories", territories.df, overwrite = TRUE, row.names = FALSE)

# Merge the all.reps with territories.df by territory name to get the tID
all.reps <- merge(all.reps, territories.df,
                  by.x = "territory",
                  by.y = "tName",
                  all.x = TRUE)

# Remove the redundant territory column and rename according to the schema
all.reps <- subset(all.reps, select = -territory)

# Format the commission to be numeric for the easy of calculation
all.reps$commission <- as.numeric(sub("%", "", all.reps$commission))/100

# Convert date format
all.reps$hiredate <- unlist(lapply(all.reps$hiredate, convert.reps.date))

# Populate Representatives SQLite table
dbWriteTable(conn, "Representatives", all.reps, overwrite = TRUE, row.names = FALSE)

# Helper functions to extract Sales data
# Function to extract data from a single txn node
extract.single.txn <- function(node){
  attrs <- xmlAttrs(node)
  repID <- attrs[["repID"]]
  repID <- as.integer(repID)
  sale.items <- xmlChildren(node[["sale"]])
  unitcost.item <- sale.items[["unitcost"]]
  currency <- xmlAttrs(unitcost.item)[["currency"]]
  sale.df <- data.frame(
    repID = repID,
    customer = xmlValue(node[["customer"]]),
    country = xmlValue(node[["country"]]),
    date = xmlValue(sale.items[["date"]]),
    product = xmlValue(sale.items[["product"]]),
    currency = currency,
    unitcost = xmlValue(sale.items[["unitcost"]]),
    quantity = xmlValue(sale.items[["qty"]]),
    row.names = NULL
  )
  return(sale.df)
}

# Function to extract data from xml data consisting multiple txn nodes
extract.txns <- function(xml.dom){
  sales.nodes <- getNodeSet(xml.dom, "//txn")
  sales.df <- do.call(rbind, lapply(sales.nodes, extract.single.txn))
  return(sales.df)
}

# Function to convert the sales date to ISO 8601 format
convert.sales.date <- function(date.string){
  parts <- strsplit(date.string, "/")[[1]]
  month <- sprintf("%02d", as.numeric(parts[1]))
  day <- sprintf("%02d", as.numeric(parts[2]))
  year <- parts[3]
  
  date <- paste(year, month, day, sep="-")
  return(date)
}

# Get all the target xml files for Sales records
sales.files <- list.files(path = XML.PATH, pattern = "pharmaSalesTxn*")
all.sales <- data.frame()

# Parse each xml file found in the target path into one dataframe
for (sales.file in sales.files){
  sales.path <- paste0(XML.PATH, sales.file)
  sales.dom <- xmlParse(sales.path)
  
  # Extract all sales
  all.sales <- rbind(all.sales, extract.txns(sales.dom))
}

# Add sID to all.sales
all.sales$sID <- 1:nrow(all.sales)

# Convert date format
all.sales$date <- unlist(lapply(all.sales$date, convert.sales.date))

# Extract and make a separate Customers dataframe
customers.df <- unique(all.sales[, c("customer", "country")])
customers.df$cstmrID <- 1:nrow(customers.df)
names(customers.df)[names(customers.df) == "customer"] <- "cstmrName"

# Extract and make a separate Countries dataframe
unique.countries <- unique(customers.df[,"country"])
countries.df <- data.frame(
  cntryID = seq_along(unique.countries),
  cntryName = unique.countries,
  stringsAsFactors = FALSE
)

# Populate Countries SQLite table
dbWriteTable(conn, "Countries", countries.df, overwrite = TRUE, row.names = FALSE)

# Merge the customers.df with countries.df by country name to get the cntryID
customers.df <- merge(customers.df, countries.df, 
                      by.x = "country",
                      by.y = "cntryName",
                      all.x = TRUE)
customers.df <- subset(customers.df, select = -country)

# Populate Customers SQLite table
dbWriteTable(conn, "Customers", customers.df, overwrite = TRUE, row.names = FALSE)

# Merge the all.sales with customers.df by customer name to get the cstmrID
all.sales <- merge(all.sales, customers.df[,c("cstmrID", "cstmrName")], 
                  by.x = "customer",
                  by.y = "cstmrName",
                  all.x = TRUE)

# Remove the customer and country columns
all.sales <- subset(all.sales, select = -customer)
all.sales <- subset(all.sales, select = -country)

# Extract and make a separate Products dataframe
products.df <- unique(all.sales[, c("product", "currency", "unitcost")])
products.df$pID <- 1:nrow(products.df)
names(products.df)[names(products.df) == "product"] <- "pName"

# Populate Products SQLite table
dbWriteTable(conn, "Products", products.df, overwrite = TRUE, row.names = FALSE)

# Merge the all.sales with products.df by product name to get the pID
all.sales <- merge(all.sales, products.df[,c("pID", "pName")], 
                  by.x = "product",
                  by.y = "pName",
                  all.x = TRUE)

# Remove the customer and country columns
all.sales <- subset(all.sales, select = -product)
all.sales <- subset(all.sales, select = -currency)
all.sales <- subset(all.sales, select = -unitcost)

# Populate Sales SQLite table
dbWriteTable(conn, "Sales", all.sales, overwrite = TRUE, row.names = FALSE)


## Part 1.7 Split the large "sales" table into several smaller ones, one for each year.
sql <-
"
SELECT DISTINCT strftime('%Y', date) AS year
FROM Sales
ORDER BY year;
"
years.df <- dbGetQuery(conn, sql)

# Helper function to create small sales tables by year
create.sales.by.year <- function(conn, year){
  table.name <- paste0("Sales_", year)
  target.sql <- paste0("WHERE strftime('%Y', date) = '", year, "';")
  sql <- paste0("
                CREATE TABLE ", table.name, " AS
                SELECT *
                FROM Sales
                ", target.sql)
  dbExecute(conn, sql)
}

# Create small sales tables for each year
for (year in years.df$year){
  create.sales.by.year(conn, year)
}

# Drop the big Sales table
dbExecute(conn, "DROP TABLE Sales;")

# Disconnect before quitting
dbDisconnect(conn)