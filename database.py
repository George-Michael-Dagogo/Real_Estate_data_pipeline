import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy


# Replace the placeholders with your PostgreSQL database connection details
#db_connection = "postgresql://testtech:George1234@testtech.postgres.database.azure.com:5432/postgres"
#engine = create_engine(db_connection)

# Define data types for each column
dtype_mapping = {
    'title': sqlalchemy.types.VARCHAR(),
    'categories': sqlalchemy.types.VARCHAR(),
    'address': sqlalchemy.types.VARCHAR(),
    'agent': sqlalchemy.types.VARCHAR(),
    'PIDs': sqlalchemy.types.VARCHAR(),
    'newly_built': sqlalchemy.types.BOOLEAN(),
    'serviced': sqlalchemy.types.BOOLEAN(),
    'furnished': sqlalchemy.types.BOOLEAN(),
    'beds': sqlalchemy.types.INTEGER(),
    'baths': sqlalchemy.types.INTEGER(),
    'toilets': sqlalchemy.types.INTEGER(),
    'price_₦': sqlalchemy.types.NUMERIC(precision=15),
    'date_posted': sqlalchemy.types.DATE(),
    'date_updated': sqlalchemy.types.DATE(),
    'type': sqlalchemy.types.TEXT(),
    'state': sqlalchemy.types.VARCHAR(length=20),
    'price': sqlalchemy.types.NUMERIC(precision=15 ),
    'price_per_day_₦': sqlalchemy.types.NUMERIC(precision=15),
    'price_per_month_₦': sqlalchemy.types.NUMERIC(precision=15)
}

# Read the CSV file into a Pandas DataFrame
#df = pd.read_csv('../Real_Estate_data_pipeline_NG/property_csv/propertypro_merged.csv')

# Use the to_sql method to insert the data into the PostgreSQL database
#df.to_sql('propertypro_merged', engine, if_exists='replace', index=False )

#engine.dispose()





# Database connection parameters
db_params = {
    'host': 'testtech.postgres.database.azure.com',
    'database': 'postgres',
    'user': 'testtech',
    'password': 'George1234',
    'port': '5432',  # 5432 is the default port for PostgreSQL
}


# Establish a connection to the database
connection = psycopg2.connect(**db_params)

# Create a cursor object to interact with the database
cursor = connection.cursor()

create_tables = '''
 CREATE SEQUENCE PropertyIDSeq
    START 1000
    INCREMENT 1;

 -- Create the Property Dimension Table
CREATE TABLE PropertyDimension (
    PropertyID INT DEFAULT nextval('PropertyIDSeq') PRIMARY KEY,
    NewlyBuilt BOOLEAN,
    Serviced BOOLEAN,
    Furnished BOOLEAN,
    Beds INT,
    Baths INT,
    Toilets INT,
    Type TEXT
);
 CREATE SEQUENCE AgentIDSeq
    START 20
    INCREMENT 3;

-- Create the Agent Dimension Table
CREATE TABLE AgentDimension (
    AgentID INT DEFAULT nextval('AgentIDSeq') PRIMARY KEY,
    AgentName VARCHAR
);

 CREATE SEQUENCE CategoryIDSeq
    START 534
    INCREMENT 3;

-- Create the Category Dimension Table
CREATE TABLE CategoryDimension (
    CategoryID INT DEFAULT nextval('CategoryIDSeq') PRIMARY KEY,
    Title VARCHAR,
    Categories VARCHAR
);

CREATE SEQUENCE DateIDSeq
    START 233
    INCREMENT 2;
-- Create the Time Dimension Table
CREATE TABLE TimeDimension (
    DateID INT DEFAULT nextval('DateIDSeq') PRIMARY KEY,
    DatePosted DATE,
    DateUpdated DATE
);

CREATE SEQUENCE LocationIDSeq
    START 47
    INCREMENT 2;

-- Create the Location Dimension Table
CREATE TABLE LocationDimension (
    LocationID INT DEFAULT nextval('LocationIDSeq') PRIMARY KEY,
    Address VARCHAR,
    State VARCHAR

);

CREATE SEQUENCE PropertyListingIDSeq
    START 10
    INCREMENT 1;

-- Create the Property Listing Fact Table
CREATE TABLE PropertyListingFact (
    PropertyListingID INT DEFAULT nextval('PropertyListingIDSeq') PRIMARY KEY,
    PropertyID INT,
    AgentID INT,
    CategoryID INT,
    DateID INT,
    LocationID INT,
    Price_₦ NUMERIC(15, 2),
    Price_Per VARCHAR, -- Adjust the precision and scale as needed
    PricePerDay NUMERIC(15, 2),
    PricePerMonth NUMERIC(15, 2)
);

-- Define foreign key constraints for the fact table
ALTER TABLE PropertyListingFact
ADD CONSTRAINT fk_property FOREIGN KEY (PropertyID) REFERENCES PropertyDimension(PropertyID),
ADD CONSTRAINT fk_agent FOREIGN KEY (AgentID) REFERENCES AgentDimension(AgentID),
ADD CONSTRAINT fk_category FOREIGN KEY (CategoryID) REFERENCES CategoryDimension(CategoryID),
ADD CONSTRAINT fk_date FOREIGN KEY (DateID) REFERENCES TimeDimension(DateID),
ADD CONSTRAINT fk_location FOREIGN KEY (LocationID) REFERENCES LocationDimension(LocationID);
'''


insert_data = '''-- Insert data into the Property Dimension Table from 'propertypro_merged'
INSERT INTO PropertyDimension (NewlyBuilt, Serviced, Furnished, Beds, Baths, Toilets, Type)
SELECT
    newly_built, serviced, furnished, beds, baths, toilets, type
FROM propertypro_merged;

-- Insert data into the Agent Dimension Table from 'propertypro_merged'
INSERT INTO AgentDimension (AgentName)
SELECT DISTINCT agent
FROM propertypro_merged;

-- Insert data into the Category Dimension Table from 'propertypro_merged'
INSERT INTO CategoryDimension (Title, Categories)
SELECT Title, categories
FROM propertypro_merged;

-- Insert data into the Time Dimension Table from 'propertypro_merged'
INSERT INTO TimeDimension (DatePosted, DateUpdated)
SELECT date_posted::DATE, date_updated::DATE
FROM propertypro_merged;

-- Insert data into the Location Dimension Table from 'propertypro_merged'
INSERT INTO LocationDimension (Address, State)
SELECT address, state
FROM propertypro_merged;

-- Insert data into the Property Listing Fact Table from 'propertypro_merged'
INSERT INTO PropertyListingFact (PropertyID, AgentID, CategoryID, DateID, LocationID, Price_₦, Price_Per, PricePerDay, PricePerMonth)
SELECT
    pd.PropertyID, ad.AgentID, cd.CategoryID, td.DateID, ld.LocationID, pm.price_₦, price, pm.price_per_day_₦, pm.price_per_month_₦
FROM propertypro_merged AS pm
INNER JOIN PropertyDimension AS pd ON pm.newly_built = pd.NewlyBuilt
INNER JOIN AgentDimension AS ad ON pm.agent = ad.AgentName
INNER JOIN CategoryDimension AS cd ON pm.categories = cd.Categories
INNER JOIN TimeDimension AS td ON pm.date_posted::DATE = td.DatePosted AND pm.date_updated::DATE = td.DateUpdated
INNER JOIN LocationDimension AS ld ON pm.address = ld.Address;
'''

# Execute a simple query
cursor.execute(insert_data)

# Fetch and print the query result
result = cursor.fetchall()
for row in result:
   print(row)

# Close the cursor and the connection
cursor.close()
connection.commit()
connection.close()


