# Title: Create Analytics Database
# Subtitle: CS5200 / Practicum II
# Name: Zi Ju
# Date: 2024 Summer 2 Semester

# Load libraries
library(RSQLite)
library(lubridate)

# Connect to SQLite database
conn <- dbConnect(SQLite(), dbname = "pharma_sales.db")


# Create relational schema
# Drop table if exists
dbExecute(conn, "DROP TABLE IF EXISTS Sales")
dbExecute(conn, "DROP TABLE IF EXISTS Reps")
dbExecute(conn, "DROP TABLE IF EXISTS Products")
dbExecute(conn, "DROP TABLE IF EXISTS Customers")

# Create tables
query_create_products <- "
  CREATE TABLE Products (
    prodID INTEGER PRIMARY KEY AUTOINCREMENT,
    prodName TEXT NOT NULL,
    unitCost REAL CHECK(unitCost >= 0)
)"
dbExecute(conn, query_create_products)

query_create_reps <- "
  CREATE TABLE Reps (
    repID INTEGER PRIMARY KEY,
    repFN TEXT,
    repLN TEXT,
    repTR TEXT,
    repPh TEXT,
    repCm REAL,
    repHireDate DATE
)"
dbExecute(conn, query_create_reps)

query_create_customers <- "
  CREATE TABLE Customers (
    custID INTEGER PRIMARY KEY AUTOINCREMENT,
    custName TEXT NOT NULL,
    country TEXT
)"
dbExecute(conn, query_create_customers)

query_create_sales <- "
  CREATE TABLE Sales (
    saleID INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE,
    custID INTEGER,
    prodID INTEGER,
    qty INTEGER CHECK(qty >= 0),
    repID INTEGER,
    FOREIGN KEY (custID) REFERENCES Customers(custID),
    FOREIGN KEY (prodID) REFERENCES Products(prodID),
    FOREIGN KEY (repID) REFERENCES Reps(repID)
)"
dbExecute(conn, query_create_sales)


# Load data
# Helper function to get csv files based on patterns
get_csv_files <- function(pattern) {
  list.files(path = "csv-data", pattern = pattern, full.names = TRUE)
}

# Get all csv files for pharmaReps
reps_files <- get_csv_files("pharmaReps-.*\\.csv")

# Helper function to parse repHireDate
convert_rep_date <- function(rep_date_str) {
  parsed_rep_date <- parse_date_time(rep_date_str, orders = "b d Y")
  formatted_rep_date <- format(as.Date(parsed_rep_date), "%Y-%m-%d")
  return(formatted_rep_date)
}

# Load reps data
reps_data <- data.frame()
for (file in reps_files) {
  temp_reps_data <- read.csv(file, stringsAsFactors = FALSE)
  reps_data <- rbind(reps_data, temp_reps_data)
}
# parse date
reps_data$repHireDate <- sapply(reps_data$repHireDate, convert_rep_date)
# Insert into Reps table
dbWriteTable(conn, name = "Reps", value = reps_data, append = TRUE, row.names = FALSE)

  
# Get all csv files for pharmaSalesTxn
txn_files <- get_csv_files("pharmaSalesTxn-.*\\.csv")

# Helper function to parse sales date 
convert_sale_date <- function(sale_date_str) {
  parsed_sale_date <- parse_date_time(sale_date_str, orders = "m/d/Y")
  formatted_sale_date <- format(as.Date(parsed_sale_date), "%Y-%m-%d")
  return(formatted_sale_date)
}

# Load sales data
txn_data <- data.frame()
for (file in txn_files) {
  temp_txn_data <- read.csv(file, stringsAsFactors = FALSE)
  txn_data <- rbind(txn_data, temp_txn_data)
}
# parse date 
txn_data$date <- sapply(txn_data$date, convert_sale_date)

# Extract unique products and insert into Products table
products_data <- unique(txn_data[, c("prod", "unitcost")])
colnames(products_data) <- c("prodName", "unitCost")
dbWriteTable(conn, name = "Products", value = products_data, append = TRUE, row.names = FALSE)

# Extract unique customers and insert into Customers table
customers_data <- unique(txn_data[, c("cust", "country")])
colnames(customers_data) <- c("custName", "country")
dbWriteTable(conn, name = "Customers", value = customers_data, append = TRUE, row.names = FALSE)

# Map product and customer names to IDs
product_ids <- dbGetQuery(conn, "SELECT rowid as prodID, prodName FROM Products")
customer_ids <- dbGetQuery(conn, "SELECT rowid as custID, custName FROM Customers")

# Merge to get foreign keys
txn_data <- merge(txn_data, customer_ids, by.x = "cust", by.y = "custName")
txn_data <- merge(txn_data, product_ids, by.x = "prod", by.y = "prodName")

# Prepare sales data with foreign keys
sales_data <- txn_data[, c("date", "custID", "prodID", "qty", "repID")]

# Insert into Sales table
dbWriteTable(conn, name = "Sales", value = sales_data, append = TRUE, row.names = FALSE)



# Split Sales table into smaller ones based on years
# Extract unique years from the Sales table
query <- "SELECT DISTINCT strftime('%Y', date) as year FROM Sales"
years <- dbGetQuery(conn, query)
years_list <- years$year

# Drop table if exists
for (year in years_list) {
  query_drop_sales_year <- paste0(
    "DROP TABLE IF EXISTS Sales_", year
  )
  dbExecute(conn, query_drop_sales_year)
}

# Create Sales_year tables and transfer data
for (year in years_list) {
  create_table_query <- paste0(
    "CREATE TABLE IF NOT EXISTS Sales_", year, " AS ",
    "SELECT * FROM Sales WHERE strftime('%Y', date) = '", year, "';"
  )
  dbExecute(conn, create_table_query)
}

# Drop the original Sales table
dbExecute(conn, "DROP TABLE IF EXISTS Sales")


# Disconnect from the database
dbDisconnect(conn)
