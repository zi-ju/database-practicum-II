# Title: Create Star/Snowflake Schema
# Subtitle: CS5200 / Practicum II
# Name: Zi Ju
# Date: 2024 Summer 2 Semester


# Load Libraries
library(RMySQL)
library(DBI)

# Connect to MySQL database
conn <- dbConnect(RMySQL::MySQL(),
                user = 'ziju5200',
                password = '12345678',
                dbname = 'ziju5200db',
                host = 'db4free.net')

# Connect to SQLite database
conn_local <- dbConnect(SQLite(), dbname = "pharma_sales.db")


# Create fact tables
# Drop table if exists
dbExecute(conn, "DROP TABLE IF EXISTS product_facts")
dbExecute(conn, "DROP TABLE IF EXISTS rep_facts")

# Create product_facts fact table
query_create_product_facts <- "
  CREATE TABLE product_facts (
    pfid INTEGER PRIMARY KEY,
    prodName TEXT,
    country TEXT,
    year INTEGER,
    month INTEGER,
    total_amount REAL,
    total_units INTEGER
)"
dbExecute(conn, query_create_product_facts)

# Create rep_facts fact table
query_create_rep_facts <- "
  CREATE TABLE rep_facts (
    rfid INTEGER PRIMARY KEY,
    repName TEXT,
    year INTEGER,
    month INTEGER,
    total_amount REAL,
    average_amount REAL
)"
dbExecute(conn, query_create_rep_facts)

# Initialize data frames for insert data into fact tables
product_facts_df <- data.frame(prodName = character(),
                               country = character(),
                               year = integer(),
                               month = integer(),
                               total_amount = numeric(),
                               total_units = integer(),
                               stringsAsFactors = FALSE)

rep_facts_df <- data.frame(repName = character(),
                        year = integer(),
                        month = integer(),
                        total_amount = numeric(),
                        average_amount = numeric(),
                        stringsAsFactors = FALSE)

# Get all years
# Get all table names
query_table_names <- "SELECT name FROM sqlite_master 
                      WHERE type='table' AND name LIKE 'Sales_%'"
sales_tables <- dbGetQuery(conn_local, query_table_names)$name
# Extract years
extract_year <- function(table_name) {
  sub("Sales_", "", table_name)
}
years <- unique(sapply(sales_tables, extract_year))
years <- as.integer(years)


# Populate dataframe
for (year in years) {
  # Product facts dataframe
  query_product_facts <- sprintf("
        SELECT Products.prodName, Customers.country, strftime('%%Y', s.date) AS year, strftime('%%m', s.date) AS month,
               SUM(s.qty * Products.unitCost) AS total_amount, SUM(s.qty) AS total_units
        FROM Sales_%d s
        JOIN Products ON s.prodID = Products.prodID
        JOIN Customers ON s.custID = Customers.custID
        GROUP BY Products.prodName, Customers.country, year, month", year)
  product_facts_data <- dbGetQuery(conn_local, query_product_facts)
  product_facts_df <- rbind(product_facts_df, product_facts_data)
  
  # Rep facts dataframe
  query_rep_facts <- sprintf("
        SELECT Reps.repFN || ' ' || Reps.repLN AS repName, strftime('%%Y', s.date) AS year, strftime('%%m', s.date) AS month,
               SUM(s.qty * Products.unitCost) AS total_amount,
               AVG(s.qty * Products.unitCost) AS average_amount
        FROM Sales_%d s
        JOIN Products ON s.prodID = Products.prodID
        JOIN Reps ON s.repID = Reps.repID
        GROUP BY repName, year, month", year)
  data_rep_facts <- dbGetQuery(conn_local, query_rep_facts)
  rep_facts_df <- rbind(rep_facts_df, data_rep_facts)
}

# Add pfid for each row
product_facts_df$pfid <- seq_len(nrow(product_facts_df))
# Add rfid for each row
rep_facts_df$rfid <- seq_len(nrow(rep_facts_df))

# Batch insert
batch_size <- 1000
# Batch insert into product_facts fact table
total_rows <- nrow(product_facts_df)
for (start in seq(1, total_rows, by = batch_size)) {
  end <- min(start + batch_size - 1, total_rows)
  batch <- product_facts_df[start:end, ]
  
  values_single <- apply(batch, 1, function(row) {
    paste0("(", 
           row['pfid'], ", '", 
           row['prodName'], "', '", 
           row['country'], "', ", 
           row['year'], ", ", 
           row['month'], ", ", 
           row['total_amount'], ", ", 
           row['total_units'], ")")
  })
  values_str <- paste(values_single, collapse = ", ")
  sql_insert_product_facts <- paste0("INSERT INTO product_facts (pfid, prodName, country, year, month, total_amount, total_units) VALUES ", values_str)
  dbExecute(conn, sql_insert_product_facts)
}

# Batch insert into rep_facts fact table
total_rows <- nrow(rep_facts_df)
for (start in seq(1, total_rows, by = batch_size)) {
  end <- min(start + batch_size - 1, total_rows)
  batch <- rep_facts_df[start:end, ]
  
  values_single <- apply(batch, 1, function(row) {
    paste0("(", 
           row['rfid'], ", '", 
           row['repName'], "', ", 
           row['year'], ", ", 
           row['month'], ", ", 
           row['total_amount'], ", ", 
           row['average_amount'], ")")
  })
  values_str <- paste(values_single, collapse = ", ")
  sql_insert_rep_facts <- paste0("INSERT INTO rep_facts (rfid, repName, year, month, total_amount, average_amount) VALUES ", values_str)
  dbExecute(conn, sql_insert_rep_facts)
}


# Disconnect from the database
dbDisconnect(conn)
dbDisconnect(conn_local)
