---
title: "Snowflake Case Study Python"
date: 2023-01-01T15:34:30-04:00
categories:
  - Case Study
tags:
  - Snowflake Case Study
  - Case Study
---
<h5>Yujin Kim, MS in Business Analytics at UCSD - Class of 2023</h5>
<p><h5>1. Introduction</h5></p>
This project's goal is to be familiar with loading data from multiple sources into Snowflake with data cleaning and manipulation. I worked with the purchase order, supplier, invoice, and weather data that should be loaded into Snowflake and then created queries that calculate differences between invoice and purchase order amounts. And lastly, I joined the external weather data based on the zip code added from third-party data.
<p> </p>
<p>
<p><h5>2. The data are stored in five different formats/sources:</h5></p>
<p>· csv (comma delimited) – 41 files with monthly purchase order data (at the line item level)</p>
<p>· XML – one file with supplier invoice</p>
<p>· postgres – one table with supplier information</p>
<p>· Snowflake Marketplace – weather data from Environment Data Atlas</p>
<p>· txt - one file with geographic information</p>
</p>
<p><h5>3. The tools and languages used:</h5></p>
<p>· Python</p>
<p>· SQL</p>
<p>· Cloud computing Basics - SnowFlake</p>

Part 1. Introduction to Snowflake
```html
#!pip install --upgrade snowflake-connector-python
import snowflake.connector

# conn is the object that connects you to your Snowflake account.
# cs is a database cursor object for execute and fetch operations. You can use that to execute SQL commands with the following format: cs.execute("YOUR SQL COMMAND")

conn = snowflake.connector.connect(
    user='',
    password='',
    account='' #snowflake worksheets url middle name
    )
cs = conn.cursor()
## Creat Snowflake Objects 
# First, Create a virtual warehouse. 
# Virtual warehouses contain the compute resources that are required to perform queries and DML operations with Snowflake:

cs.execute("CREATE WAREHOUSE IF NOT EXISTS Kim_warehouse")
```
::: {.output .execute_result execution_count="4"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cca628ac0>
```html
# We will next create a database. Databases contain your schemas, which contain your database objects. 
# We can create one in a similar manner to creating a warehouse:

cs.execute("CREATE DATABASE IF NOT EXISTS Kim_db")
```
::: {.output .execute_result execution_count="5"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cca628ac0>
```html
# Next up are schemas. Schemas are grouping of database objects, including tables, data within them, and views. Schemas are found within databases:
cs.execute("CREATE SCHEMA IF NOT EXISTS Kim_schema")
```
::: {.output .execute_result execution_count="6"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cca628ac0>
```html
cs.execute("USE WAREHOUSE Kim_warehouse")
cs.execute("USE DATABASE Kim_db")
cs.execute("USE SCHEMA Kim_schema")
```
::: {.output .execute_result execution_count="129"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>
```html
# We will next create a table and insert data:

cs.execute(
"CREATE OR REPLACE TABLE "    
"test_table(col1 integer, col2 string)")
cs.execute(
    	"INSERT INTO test_table(col1, col2)"
    	"VALUES (123, 'test string1'),(456, 'test string2')")
```
::: {.output .execute_result execution_count="18"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc6071f00>
```html
# Finally, to run a query and view the result set use:

cs.execute('SELECT * FROM test_table')
print(cs.fetchmany(2))
```
::: {.output .stream .stdout}
[(123, 'test string1'), (456, 'test string2')]

<h5>Part 2. Python and Snowflake {#part2-python-and-snowflake}</h5>

<p>1) Extract and load the 41 comma delimited purchases data files and
form a single table of purchases data:</p>

```html
a. Created the destination table.
b. Then used the PUT command to copy the local files into the Snowflake staging area for the table.
c. Finally, used the COPY command to copy data from the data source into the Snowflake table
```
```html
cs.execute("CREATE DATABASE IF NOT EXISTS MonthlyPurchaseData")
cs.execute("CREATE SCHEMA IF NOT EXISTS MPD_Schema")
cs.execute('CREATE STAGE IF NOT EXISTS PurchaseDataStage')
```
::: {.output .execute_result execution_count="27"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5c09f00>
```html
cs.execute("USE WAREHOUSE Kim_warehouse")
cs.execute("USE DATABASE MonthlyPurchaseData")
cs.execute("USE SCHEMA MPD_Schema")
```
::: {.output .execute_result execution_count="130"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>
```html
# Create table
cs.execute("CREATE OR REPLACE TABLE MPD (PurchaseOrderID varchar, SupplierID integer, OrderDate date, DeliveryMethodID integer, ContactPersonID integer, ExpectedDeliveryDate date, SupplierReference varchar, IsOrderFinalized integer,Comments varchar, InternalComments varchar, LastEditedBy integer, LastEditedWhen date, PurchaseOrderLineID integer, StockItemID integer, OrderedOuters integer, Description varchar, ReceivedOuters integer, PackageTypeID integer, ExpectedUnitPricePerOuter number, LastReceiptDate date, IsOrderLineFinalized integer, Right_LastEditedBy integer, Right_LastEditedWhen date)")
```
::: {.output .execute_result execution_count="26"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5c09f00>
```html
# Load the data nto the Snowflake staging area for the table
cs.execute("PUT file:///home/jovyan/Documents/0.Rady/1.Summer/MGTA495_SQL/MonthlyPOData/*.csv @PurchaseDataStage")
```
::: {.output .execute_result execution_count="28"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5c09f00>
```html
# Copy data from the data source into the Snowflake table
cs.execute("COPY INTO MPD FROM @PurchaseDataStage on_error=continue")
```
::: {.output .execute_result execution_count="29"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5c09f00>
```html
# Data verification
cs.execute('SELECT DISTINCT EXTRACT(year FROM orderdate) FROM MPD')
print(cs.fetchmany(4))

cs.execute('SELECT count(DISTINCT PurchaseOrderID) FROM MPD')
print(cs.fetchmany(4))
```
::: {.output .stream .stdout}
[(2013,), (2014,), (2015,), (2016,)]
[(1918,)]

<h5>2) Create a calculated field that shows purchase order totals:</h5>

```html
# Deleting the columns from Purchase_Data table that only have NULL values: Comments, InternalComments
cs.execute('ALTER TABLE MPD DROP COLUMN Comments, InternalComments')
```

```html
# Create a calculated View
cs.execute('CREATE OR REPLACE VIEW POAmount AS SELECT DISTINCT purchaseorderid,\
            sum(ReceivedOuters * ExpectedUnitPricePerOuter) as POAmount FROM MPD \
            GROUP BY purchaseorderid \
            ORDER BY purchaseorderid')
print(cs.fetchmany(1918))
```
::: {.output .stream .stdout}
[('View POAMOUNT successfully created.',)]

<h5>3) Extract and load the supplier invoice XML data:</h5>

```html
# Create invoice XML table
cs.execute("CREATE OR REPLACE TABLE invoiceXML (xmldoc VARIANT)")
cs.execute("PUT 'file:///home/jovyan/Documents/0.Rady/1.Summer/MGTA495_SQL/Module 5 ETL/Case Data/SupplierTransactionsXML.xml' @PurchaseDataStage")
```
::: {.output .execute_result execution_count="131"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>
```html
cs.execute("COPY INTO invoiceXML FROM @PurchaseDataStage file_format = (type=XML, strip_outer_element=true) on_error=continue")
```
::: {.output .execute_result execution_count="132"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>
```html
# Create Invoice View
cs.execute("CREATE OR REPLACE VIEW Invoice AS (SELECT XMLGET(XMLDOC, 'SupplierTransactionID'):\"$\"::NUMBER AS SupplierTransactionID,\
    XMLGET(XMLDOC, 'SupplierID'):\"$\"::FLOAT AS SupplierID,\
    XMLGET(XMLDOC, 'TransactionTypeID'):\"$\"::FLOAT AS TransactionTypeID,\
    XMLGET(XMLDOC, 'PurchaseOrderID'):\"$\"::STRING AS PurchaseOrderID,\
    XMLGET(XMLDOC, 'SupplierInvoiceNumber'):\"$\"::VARCHAR AS SupplierInvoiceNumber,\
    XMLGET(XMLDOC, 'TransactionDate'):\"$\"::DATE AS TransactionDate,\
    XMLGET(XMLDOC, 'AmountExcludingTax'):\"$\"::FLOAT AS AmountExcludingTax,\
    XMLGET(XMLDOC, 'TaxAmount'):\"$\"::FLOAT AS TaxAmount,\
    XMLGET(XMLDOC, 'TransactionAmount'):\"$\"::FLOAT AS TransactionAmount,\
    XMLGET(XMLDOC, 'OutstandingBalance'):\"$\"::FLOAT AS OutstandingBalance,\
    XMLGET(XMLDOC, 'FinalizationDate'):\"$\"::STRING AS FinalizationDate,\
    XMLGET(XMLDOC, 'IsFinalized'):\"$\"::NUMBER AS IsFinalized,\
    XMLGET(XMLDOC, 'LastEditedBy'):\"$\"::NUMBER AS LastEditedBy,\
    XMLGET(XMLDOC, 'LastEditedWhen'):\"$\"::STRING AS LastEditedWhen\
    FROM invoiceXML)")
```
::: {.output .execute_result execution_count="133"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>
```html
# Verify Data
cs.execute('SELECT * FROM Invoice LIMIT 5')
print(cs.fetchmany(1)
```
::: {.output .stream .stdout}
[(134, 2.0, 5.0, '1', '7290', datetime.date(2013, 1, 2), 313.5, 47.03, 360.53, 0.0, '2013-01-07', 1, 4, '2013-01-07 09:00:00.0000000')]

<h5>4) Join the purchase data from step 2 and the supplier invoices data
from step 3:</h5>

```html
cs.execute("SELECT a.*, b.POAmount FROM Invoice a LEFT JOIN POAmount b USING (purchaseorderid)")
print(cs.fetchmany(1))
```
::: {.output .stream .stdout}
[('1', 134, 2.0, 5.0, '7290', datetime.date(2013, 1, 2), 313.5, 47.03, 360.53, 0.0, '2013-01-07', 1, 4, '2013-01-07 09:00:00.0000000', 342)]

<h5>5) Create a calculated field that shows the difference between
AmountExcludingTax and POAmount:</h5>

```html
cs.execute('CREATE OR REPLACE TABLE purchase_orders_and_invoices AS SELECT A.* , B.POAmount, (A.AmountExcludingTax - B.POAmount) AS invoiced_vs_quoted FROM Invoice A\
     LEFT JOIN POAmount B ON A.PurchaseOrderID = B.PurchaseOrderID')
print(cs.fetchmany(1))
```
::: {.output .stream .stdout}
[('Table PURCHASE_ORDERS_AND_INVOICES successfully created.',)]

<h5>6) Extract the supplier_case data from postgres:</h5>

```html
cs.execute('CREATE DATABASE IF NOT EXISTS Supplier_Case')
cs.execute('USE DATABASE Supplier_Case')
cs.execute('CREATE SCHEMA IF NOT EXISTS Supplier_Case_Schema')
cs.execute('USE Supplier_Case.Supplier_Case_Schema')
cs.execute('CREATE STAGE IF NOT EXISTS Supplier_Case_Stage')
```
::: {.output .execute_result execution_count="70"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5f93160>

```html
# Create Supplier_case table

cs.execute("DROP TABLE IF EXISTS Supplier_Case")

cs.execute("CREATE OR REPLACE TABLE Supplier_Case(SupplierID INTEGER,SupplierName VARCHAR, SupplierCategoryID INTEGER, PrimaryContactPersonID INTEGER, AlternateContactPersonID INTEGER,\
            DeliveryMethodID INTEGER, PostalCityID INTEGER, SupplierReference VARCHAR, BankAccountName VARCHAR, BankAccountBranch VARCHAR, BankAccountCode INTEGER, BankAccountNumber NUMERIC, \
            BankInternationalCode INTEGER, PaymentDays INTEGER, InternalComments VARCHAR, PhoneNumber VARCHAR, FaxNumber VARCHAR, WebsiteURL VARCHAR, DeliveryAddressLine1 VARCHAR, \
            DeliveryAddressLine2 VARCHAR, DeliveryPostalCode INTEGER, DeliveryLocation VARCHAR,PostalAddressLine1 VARCHAR,PostalAddressLine2 VARCHAR,PostalPostalCode INTEGER, \
            LastEditedBy INTEGER,ValidFrom VARCHAR ,ValidTo VARCHAR)")
```
::: {.output .execute_result execution_count="138"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>
```html
# Insert values

cs.execute("INSERT INTO supplier_case VALUES (1,'A Datum Corporation',2,21,22,7,22202,'AA20384','A Datum Corporation','Woodgrove Bank Zionsville',356981,8575824136,25986,14,NULL,'(847) 555-0100','(847) 555-0101','http://www.adatum.com', 'Suite 10','183838 Southwest Boulevard',22202,'0xE6100000010CDE115F37B6F9434031276893C39055C0','PO Box 1039','Arlington',22202,1,'05:00.0','##################')")
cs.execute("INSERT INTO supplier_case VALUES (2,'Contoso, Ltd.',2,23,24,9,80125,'B2084020','Contoso Ltd','Woodgrove Bank Greenbank',358698,4587965215,25868,7,NULL,'(360) 555-0100','(360) 555-0101','http://www.contoso.com','Unit 2','2934 Night Road',80125,'0xE6100000010CDA4B6430900C4840C04EFBF7AAA45EC0','PO Box 1012','Highlands Ranch',80125,1,'05:00.0','##################')")
cs.execute("INSERT INTO supplier_case VALUES (3,'Consolidated Messenger',6,25,26,NULL,60523,'209340283','Consolidated Messenger','Woodgrove Bank San Francisco',354269,3254872158,45698,30,NULL,'(415) 555-0100','(415) 555-0101','http://www.consolidatedmessenger.com',NULL,'894 Market Day Street',60523,'0xE6100000010C529ACDE330E34240DFFB1BB4D79A5EC0','PO Box 1014','Westmont',60523,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (4,'Fabrikam, Inc.',4,27,28,7,95642,'293092','Fabrikam Inc','Woodgrove Bank Lakeview Heights',789568,4125863879,12546,30,NULL,'(203) 555-0104','(203) 555-0108','http://www.fabrikam.com','Level 2','393999 Woodberg Road',95642,'0xE6100000010C86E7A5626313434023C9625147E054C0','PO Box 301','Jackson',95642,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (5,'Graphic Design Institute',2,29,30,10,80125,'8803922','Graphic Design Institute','Woodgrove Bank Lanagan',563215,1025869354,32587,14,NULL,'(406) 555-0105','(406) 555-0106','http://www.graphicdesigninstitute.com',NULL,'45th Street',80125,'0xE6100000010C15E46723D74D424081F8AF62A79C57C0','PO Box 393','Highlands Ranch',80125,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (6,'Humongous Insurance',9,31,32,NULL,42437,'82420938','Humongous Insurance','Woodgrove Bank Lancing',325001,2569874521,32569,14,NULL,'(423) 555-0105','(423) 555-0100','http://www.humongousinsurance.com',NULL,'9893 Mount Norris Road',42437,'0xE6100000010CCCF2D0D2700F424085C7235DD82955C0','PO Box 94829','Boxville',42437,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (7,'Litware, Inc.',5,33,34,2,95642,'BC0280982','Litware Inc','Woodgrove Bank Mokelumne Hill',358769,3256896325,21445,30,NULL,'(209) 555-0108','(209) 555-0104','http://www.litwareinc.com','Level 3','19 Le Church Street',95642,'0xE6100000010C297398D475264340B63CC560342D5EC0','PO Box 20290','Jackson',95642,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (8,'Lucerne Publishing',2,35,36,10,60523,'JQ082304802','Lucerne Publishing','Woodgrove Bank Jonesborough',654789,3254123658,21569,30,NULL,'(423) 555-0103','(423) 555-0105','http://www.lucernepublishing.com','Suite 34','949482 Miller Boulevard',60523,'0xE6100000010C9D8F21B6AA25424091F69A794D9E54C0','PO Box 8747','Westmont',60523,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (9,'Nod Publishers',2,37,38,10,29625,'GL08029802','Nod Publishers','Woodgrove Bank Elizabeth City',365985,2021545878,48758,7,'Marcos is not in on Mondays','(252) 555-0100','(252) 555-0101','http://www.nodpublishers.com','Level 1','389 King Street',29625,'0xE6100000010C0EB0A07AB52542407452A923111053C0','PO Box 3390','Anderson',29625,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (10,'Northwind Electric Cars',3,39,40,8,22202,'ML0300202','Northwind Electric Cars','Woodgrove Bank Crandon Lakes',325447,3258786987,36214,30,NULL,'(201) 555-0105','(201) 555-0104','http://www.northwindelectriccars.com',NULL,'440 New Road',22202,'0xE6100000010C6C4E14D7E78F4440C74ED3C2C0B552C0','PO Box 30920','Arlington',22202,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (11,'Trey Research',8,41,42,NULL,34269,'82304822','Trey Research','Woodgrove Bank Kadoka',658968,1254785321,56958,7,NULL,'(605) 555-0103','(605) 555-0101','http://www.treyresearch.net','Level 43','9401 Polar Avenue',34269,'0xE6100000010C8E5C37A5BCEA454054162AA4A16059C0','PO  Box 595','North Port',34269,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (12,'The Phone Company',2,43,44,7,91942,'237408032','The Phone Company','Woodgrove Bank Karlstad',214568,7896236589,25478,30,NULL,'(218) 555-0105','(218) 555-0105','http://www.thephone-company.com','Level 83','339 Toorak Road',80125,'0xE6100000010C1D1B26BFEA494840BF993D75512158C0','PO Box 3837','Highlands Ranch',80125,1,'05:00.0','##################');")
cs.execute("INSERT INTO supplier_case VALUES (13,'Woodgrove Bank',7,45,46,NULL,6331,'28034202','Woodgrove Bank','Woodgrove Bank San Francisco',325698,2147825698,65893,7,'Only speak to Donald if Hubert really is not available','(415) 555-0103','(415) 555-0107','http://www.woodgrovebank.com','Level 3','8488 Vienna Boulevard',6331,'0xE6100000010C529ACDE330E34240DFFB1BB4D79A5EC0','PO Box 2390','Canterbury',6331,1,'05:00.0','##################');")
```
::: {.output .execute_result execution_count="139"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>

<h5>7) Connect manually inside Snowflake Marketplace Environment
data(NOAACD2019R) and extract weather data for each unique zip code in
the Supplier_Case table. Load ZCTA data to find weather stations closest
to each zip code:</h5>

```html
cs.execute('CREATE DATABASE IF NOT EXISTS Gazetteer2021')
cs.execute('USE DATABASE Gazetteer2021')
cs.execute('CREATE SCHEMA IF NOT EXISTS Gazetteer_Schema')
cs.execute('USE Gazetteer2021.Gazetteer_Schema')
cs.execute('CREATE STAGE IF NOT EXISTS Gazetteer_Stage')
```
::: {.output .execute_result execution_count="80"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc64d8ac0>
```html
# Extract / Load Gazetteer 2021 Data(ZCTA)
cs.execute("CREATE OR REPLACE TABLE Gazetteer2021(GEOID NUMBER, ALAND NUMBER, AWATER NUMBER, ALAND_SQMI NUMBER, AWATER_SQMI NUMBER, INTPTLAT FLOAT, INTPTLONG FLOAT)")
cs.execute("PUT 'file:///home/jovyan/Documents/0.Rady/1.Summer/MGTA495_SQL/Module 5 ETL/Case Data/2021_Gaz_zcta_national.txt' @Gazetteer_Stage")
```
::: {.output .execute_result execution_count="112"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc64d8ac0>
```html
cs.execute("COPY INTO Gazetteer2021 FROM @Gazetteer_Stage on_error=continue")
```
::: {.output .execute_result execution_count="114"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc64d8ac0>
```html
cs.execute("CREATE OR REPLACE FILE FORMAT Gazetteer2021 TYPE = CSV FIELD_DELIMITER = '\t'")
```
::: {.output .execute_result execution_count="115"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc64d8ac0>
```html
# Connect Gazetteer2021 data(ZCTA) with Environment data(NOAACD2019R)to get the zip code
cs.execute('CREATE OR REPLACE VIEW zctaNOA AS SELECT A.*, B.geoid \
            FROM ENVIRONMENT_DATA_ATLAS.ENVIRONMENT.NOAACD2019R A \
            INNER JOIN Gazetteer2021.Gazetteer_Schema.Gazetteer2021 B \
            ON ROUND(A."Stations Latitude") = ROUND(B.INTPTLAT) AND ROUND(A."Stations Longitude") = ROUND(B.INTPTLONG)')
```
::: {.output .execute_result execution_count="140"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>

<h5>8) Create a VIEW that contains zip codes from the supplier data, date,
and daily high temperatures:</h5>

```html
cs.execute('CREATE OR REPLACE VIEW supplier_zip_code_weather AS SELECT a.geoid AS zip_code, a."Date", MAX(a."Value") AS High_temp \
            FROM Gazetteer2021.Gazetteer_Schema.zctaNOA a \
            WHERE a."Indicator Name" = \'Maximum temperature (Fahrenheit)\'\
            group by a.geoid, a."Date"')
```
::: {.output .execute_result execution_count="141"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>

<h5>9) Join purchase_orders_and_invoices / supplier_case /
supplier_zip_code_weather based on zip codes:</h5>

```html
cs.execute('SELECT * FROM MonthlyPurchaseData.MPD_Schema.purchase_orders_and_invoices a \
            JOIN Supplier_Case.Supplier_Case_Schema.Supplier_case b USING (supplierid) \
            JOIN Gazetteer2021.Gazetteer_Schema.supplier_zip_code_weather c \
            ON b.POSTALPOSTALCODE = c.zip_code')
```
::: {.output .execute_result execution_count="145"}
<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>
