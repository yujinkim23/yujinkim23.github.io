Listed Below is the Sample File
<p>
{
 "cells": [
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# **ETL/ELT Case Study - Snowflake, Python, SQL**\n",
    "\n",
    "### Yujin Kim, MS in Business Analytics at UCSD - Class of 2023"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**ETL Tech Case Study Introduction**\n",
    "\n",
    "I worked with purchase order, supplier, invoice, and weather data that should be loaded into Snowflake. I then created queries that calculate differences between invoice and purchase order amounts and joined the external weather data based on zip code that added from third party data.  \n"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "\n",
    "\n",
    "**The data are stored in five different formats/sources:**\n",
    "\n",
    "· csv (comma delimited) – 41 files with monthly purchase order data (at the line item level)\\\n",
    "· XML – one file with supplier invoice\\\n",
    "· postgres – one table with supplier information\\\n",
    "· Snowflake Marketplace – weather data from Environment Data Atlas\\\n",
    "· txt - one file with geographic information\n",
    "\n"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "\n",
    "\n",
    "**The tools and languages used:**\n",
    "\n",
    "· Python\\\n",
    "· SQL\\\n",
    "· Cloud computing Basics - SnowFlake\n",
    "\n",
    "\n",
    "\n"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "##### **Part1. Introduction to Snowflake**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "metadata": {},
   "outputs": [],
   "source": [
    "# !pip install --upgrade snowflake-connector-python\n",
    "import snowflake.connector"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 128,
   "metadata": {},
   "outputs": [],
   "source": [
    "# conn is the object that connects you to your Snowflake account.\n",
    "# cs is a database cursor object for execute and fetch operations. You can use that to execute SQL commands with the following format: cs.execute(\"YOUR SQL COMMAND\")\n",
    "\n",
    "conn = snowflake.connector.connect(\n",
    "    user='yuk004',\n",
    "    password='Mauimaumau6844**',\n",
    "    account='mvb56748' #snowflake worksheets url middle name\n",
    "    )\n",
    "cs = conn.cursor()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cca628ac0>"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "## Creat Snowflake Objects \n",
    "# First, Create a virtual warehouse. \n",
    "# Virtual warehouses contain the compute resources that are required to perform queries and DML operations with Snowflake:\n",
    "\n",
    "cs.execute(\"CREATE WAREHOUSE IF NOT EXISTS Kim_warehouse\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cca628ac0>"
      ]
     },
     "execution_count": 5,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# We will next create a database. Databases contain your schemas, which contain your database objects. \n",
    "# We can create one in a similar manner to creating a warehouse:\n",
    "\n",
    "cs.execute(\"CREATE DATABASE IF NOT EXISTS Kim_db\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cca628ac0>"
      ]
     },
     "execution_count": 6,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Next up are schemas. Schemas are grouping of database objects, including tables, data within them, and views. Schemas are found within databases:\n",
    "cs.execute(\"CREATE SCHEMA IF NOT EXISTS Kim_schema\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 129,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 129,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute(\"USE WAREHOUSE Kim_warehouse\")\n",
    "cs.execute(\"USE DATABASE Kim_db\")\n",
    "cs.execute(\"USE SCHEMA Kim_schema\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc6071f00>"
      ]
     },
     "execution_count": 18,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# We will next create a table and insert data:\n",
    "\n",
    "cs.execute(\n",
    "\"CREATE OR REPLACE TABLE \"    \n",
    "\"test_table(col1 integer, col2 string)\")\n",
    "cs.execute(\n",
    "    \t\"INSERT INTO test_table(col1, col2)\"\n",
    "    \t\"VALUES (123, 'test string1'),(456, 'test string2')\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "[(123, 'test string1'), (456, 'test string2')]\n"
     ]
    }
   ],
   "source": [
    "# Finally, to run a query and view the result set use:\n",
    "\n",
    "cs.execute('SELECT * FROM test_table')\n",
    "print(cs.fetchmany(2))"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "#### **Part2. Python and Snowflake** "
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**1) Extract and load the 41 comma delimited purchases data files and form a single table of purchases data:**\n",
    "\n",
    "    a. Created the destination table.\n",
    "    b. Then used the PUT command to copy the local files into the Snowflake staging area for the table.\n",
    "    c. Finally, used the COPY command to copy data from the data source into the Snowflake table"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5c09f00>"
      ]
     },
     "execution_count": 27,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute(\"CREATE DATABASE IF NOT EXISTS MonthlyPurchaseData\")\n",
    "cs.execute(\"CREATE SCHEMA IF NOT EXISTS MPD_Schema\")\n",
    "cs.execute('CREATE STAGE IF NOT EXISTS PurchaseDataStage')\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 130,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 130,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute(\"USE WAREHOUSE Kim_warehouse\")\n",
    "cs.execute(\"USE DATABASE MonthlyPurchaseData\")\n",
    "cs.execute(\"USE SCHEMA MPD_Schema\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5c09f00>"
      ]
     },
     "execution_count": 26,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Create table\n",
    "cs.execute(\"CREATE OR REPLACE TABLE MPD (PurchaseOrderID varchar, SupplierID integer, OrderDate date, DeliveryMethodID integer, ContactPersonID integer, ExpectedDeliveryDate date, SupplierReference varchar, IsOrderFinalized integer,Comments varchar, InternalComments varchar, LastEditedBy integer, LastEditedWhen date, PurchaseOrderLineID integer, StockItemID integer, OrderedOuters integer, Description varchar, ReceivedOuters integer, PackageTypeID integer, ExpectedUnitPricePerOuter number, LastReceiptDate date, IsOrderLineFinalized integer, Right_LastEditedBy integer, Right_LastEditedWhen date)\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5c09f00>"
      ]
     },
     "execution_count": 28,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Load the data nto the Snowflake staging area for the table\n",
    "cs.execute(\"PUT file:///home/jovyan/Documents/0.Rady/1.Summer/MGTA495_SQL/MonthlyPOData/*.csv @PurchaseDataStage\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5c09f00>"
      ]
     },
     "execution_count": 29,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Copy data from the data source into the Snowflake table\n",
    "cs.execute(\"COPY INTO MPD FROM @PurchaseDataStage on_error=continue\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "[(2013,), (2014,), (2015,), (2016,)]\n",
      "[(1918,)]\n"
     ]
    }
   ],
   "source": [
    "# Data verification\n",
    "cs.execute('SELECT DISTINCT EXTRACT(year FROM orderdate) FROM MPD')\n",
    "print(cs.fetchmany(4))\n",
    "\n",
    "cs.execute('SELECT count(DISTINCT PurchaseOrderID) FROM MPD')\n",
    "print(cs.fetchmany(4))"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**2) Create a calculated field that shows purchase order totals:**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Deleting the columns from Purchase_Data table that only have NULL values: Comments, InternalComments\n",
    "cs.execute('ALTER TABLE MPD DROP COLUMN Comments, InternalComments')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 48,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "[('View POAMOUNT successfully created.',)]\n"
     ]
    }
   ],
   "source": [
    "# Create a calculated View\n",
    "cs.execute('CREATE OR REPLACE VIEW POAmount AS SELECT DISTINCT purchaseorderid,\\\n",
    "            sum(ReceivedOuters * ExpectedUnitPricePerOuter) as POAmount FROM MPD \\\n",
    "            GROUP BY purchaseorderid \\\n",
    "            ORDER BY purchaseorderid')\n",
    "print(cs.fetchmany(1918))"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**3) Extract and load the supplier invoice XML data:**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 131,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 131,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Create invoice XML table\n",
    "cs.execute(\"CREATE OR REPLACE TABLE invoiceXML (xmldoc VARIANT)\")\n",
    "cs.execute(\"PUT 'file:///home/jovyan/Documents/0.Rady/1.Summer/MGTA495_SQL/Module 5 ETL/Case Data/SupplierTransactionsXML.xml' @PurchaseDataStage\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 132,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 132,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute(\"COPY INTO invoiceXML FROM @PurchaseDataStage file_format = (type=XML, strip_outer_element=true) on_error=continue\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 133,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 133,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Create Invoice View\n",
    "cs.execute(\"CREATE OR REPLACE VIEW Invoice AS (SELECT XMLGET(XMLDOC, 'SupplierTransactionID'):\\\"$\\\"::NUMBER AS SupplierTransactionID,\\\n",
    "    XMLGET(XMLDOC, 'SupplierID'):\\\"$\\\"::FLOAT AS SupplierID,\\\n",
    "    XMLGET(XMLDOC, 'TransactionTypeID'):\\\"$\\\"::FLOAT AS TransactionTypeID,\\\n",
    "    XMLGET(XMLDOC, 'PurchaseOrderID'):\\\"$\\\"::STRING AS PurchaseOrderID,\\\n",
    "    XMLGET(XMLDOC, 'SupplierInvoiceNumber'):\\\"$\\\"::VARCHAR AS SupplierInvoiceNumber,\\\n",
    "    XMLGET(XMLDOC, 'TransactionDate'):\\\"$\\\"::DATE AS TransactionDate,\\\n",
    "    XMLGET(XMLDOC, 'AmountExcludingTax'):\\\"$\\\"::FLOAT AS AmountExcludingTax,\\\n",
    "    XMLGET(XMLDOC, 'TaxAmount'):\\\"$\\\"::FLOAT AS TaxAmount,\\\n",
    "    XMLGET(XMLDOC, 'TransactionAmount'):\\\"$\\\"::FLOAT AS TransactionAmount,\\\n",
    "    XMLGET(XMLDOC, 'OutstandingBalance'):\\\"$\\\"::FLOAT AS OutstandingBalance,\\\n",
    "    XMLGET(XMLDOC, 'FinalizationDate'):\\\"$\\\"::STRING AS FinalizationDate,\\\n",
    "    XMLGET(XMLDOC, 'IsFinalized'):\\\"$\\\"::NUMBER AS IsFinalized,\\\n",
    "    XMLGET(XMLDOC, 'LastEditedBy'):\\\"$\\\"::NUMBER AS LastEditedBy,\\\n",
    "    XMLGET(XMLDOC, 'LastEditedWhen'):\\\"$\\\"::STRING AS LastEditedWhen\\\n",
    "    FROM invoiceXML)\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 134,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "[(134, 2.0, 5.0, '1', '7290', datetime.date(2013, 1, 2), 313.5, 47.03, 360.53, 0.0, '2013-01-07', 1, 4, '2013-01-07 09:00:00.0000000')]\n"
     ]
    }
   ],
   "source": [
    "# Verify Data\n",
    "cs.execute('SELECT * FROM Invoice LIMIT 5')\n",
    "print(cs.fetchmany(1))"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**4) Join the purchase data from step 2 and the supplier invoices data from step 3:**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 136,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "[('1', 134, 2.0, 5.0, '7290', datetime.date(2013, 1, 2), 313.5, 47.03, 360.53, 0.0, '2013-01-07', 1, 4, '2013-01-07 09:00:00.0000000', 342)]\n"
     ]
    }
   ],
   "source": [
    "cs.execute(\"SELECT a.*, b.POAmount FROM Invoice a LEFT JOIN POAmount b USING (purchaseorderid)\")\n",
    "print(cs.fetchmany(1))"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**5) Create a calculated field that shows the difference between AmountExcludingTax and POAmount:**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 137,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "[('Table PURCHASE_ORDERS_AND_INVOICES successfully created.',)]\n"
     ]
    }
   ],
   "source": [
    "cs.execute('CREATE OR REPLACE TABLE purchase_orders_and_invoices AS SELECT A.* , B.POAmount, (A.AmountExcludingTax - B.POAmount) AS invoiced_vs_quoted FROM Invoice A\\\n",
    "     LEFT JOIN POAmount B ON A.PurchaseOrderID = B.PurchaseOrderID')\n",
    "print(cs.fetchmany(1))"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**6) Extract the supplier_case data from postgres:**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 70,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc5f93160>"
      ]
     },
     "execution_count": 70,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute('CREATE DATABASE IF NOT EXISTS Supplier_Case')\n",
    "cs.execute('USE DATABASE Supplier_Case')\n",
    "cs.execute('CREATE SCHEMA IF NOT EXISTS Supplier_Case_Schema')\n",
    "cs.execute('USE Supplier_Case.Supplier_Case_Schema')\n",
    "cs.execute('CREATE STAGE IF NOT EXISTS Supplier_Case_Stage')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 138,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 138,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Create Supplier_case table\n",
    "\n",
    "cs.execute(\"DROP TABLE IF EXISTS Supplier_Case\")\n",
    "\n",
    "cs.execute(\"CREATE OR REPLACE TABLE Supplier_Case(SupplierID INTEGER,SupplierName VARCHAR, SupplierCategoryID INTEGER, PrimaryContactPersonID INTEGER, AlternateContactPersonID INTEGER,\\\n",
    "            DeliveryMethodID INTEGER, PostalCityID INTEGER, SupplierReference VARCHAR, BankAccountName VARCHAR, BankAccountBranch VARCHAR, BankAccountCode INTEGER, BankAccountNumber NUMERIC, \\\n",
    "            BankInternationalCode INTEGER, PaymentDays INTEGER, InternalComments VARCHAR, PhoneNumber VARCHAR, FaxNumber VARCHAR, WebsiteURL VARCHAR, DeliveryAddressLine1 VARCHAR, \\\n",
    "            DeliveryAddressLine2 VARCHAR, DeliveryPostalCode INTEGER, DeliveryLocation VARCHAR,PostalAddressLine1 VARCHAR,PostalAddressLine2 VARCHAR,PostalPostalCode INTEGER, \\\n",
    "            LastEditedBy INTEGER,ValidFrom VARCHAR ,ValidTo VARCHAR)\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 139,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 139,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Insert values\n",
    "\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (1,'A Datum Corporation',2,21,22,7,22202,'AA20384','A Datum Corporation','Woodgrove Bank Zionsville',356981,8575824136,25986,14,NULL,'(847) 555-0100','(847) 555-0101','http://www.adatum.com', 'Suite 10','183838 Southwest Boulevard',22202,'0xE6100000010CDE115F37B6F9434031276893C39055C0','PO Box 1039','Arlington',22202,1,'05:00.0','##################')\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (2,'Contoso, Ltd.',2,23,24,9,80125,'B2084020','Contoso Ltd','Woodgrove Bank Greenbank',358698,4587965215,25868,7,NULL,'(360) 555-0100','(360) 555-0101','http://www.contoso.com','Unit 2','2934 Night Road',80125,'0xE6100000010CDA4B6430900C4840C04EFBF7AAA45EC0','PO Box 1012','Highlands Ranch',80125,1,'05:00.0','##################')\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (3,'Consolidated Messenger',6,25,26,NULL,60523,'209340283','Consolidated Messenger','Woodgrove Bank San Francisco',354269,3254872158,45698,30,NULL,'(415) 555-0100','(415) 555-0101','http://www.consolidatedmessenger.com',NULL,'894 Market Day Street',60523,'0xE6100000010C529ACDE330E34240DFFB1BB4D79A5EC0','PO Box 1014','Westmont',60523,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (4,'Fabrikam, Inc.',4,27,28,7,95642,'293092','Fabrikam Inc','Woodgrove Bank Lakeview Heights',789568,4125863879,12546,30,NULL,'(203) 555-0104','(203) 555-0108','http://www.fabrikam.com','Level 2','393999 Woodberg Road',95642,'0xE6100000010C86E7A5626313434023C9625147E054C0','PO Box 301','Jackson',95642,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (5,'Graphic Design Institute',2,29,30,10,80125,'8803922','Graphic Design Institute','Woodgrove Bank Lanagan',563215,1025869354,32587,14,NULL,'(406) 555-0105','(406) 555-0106','http://www.graphicdesigninstitute.com',NULL,'45th Street',80125,'0xE6100000010C15E46723D74D424081F8AF62A79C57C0','PO Box 393','Highlands Ranch',80125,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (6,'Humongous Insurance',9,31,32,NULL,42437,'82420938','Humongous Insurance','Woodgrove Bank Lancing',325001,2569874521,32569,14,NULL,'(423) 555-0105','(423) 555-0100','http://www.humongousinsurance.com',NULL,'9893 Mount Norris Road',42437,'0xE6100000010CCCF2D0D2700F424085C7235DD82955C0','PO Box 94829','Boxville',42437,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (7,'Litware, Inc.',5,33,34,2,95642,'BC0280982','Litware Inc','Woodgrove Bank Mokelumne Hill',358769,3256896325,21445,30,NULL,'(209) 555-0108','(209) 555-0104','http://www.litwareinc.com','Level 3','19 Le Church Street',95642,'0xE6100000010C297398D475264340B63CC560342D5EC0','PO Box 20290','Jackson',95642,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (8,'Lucerne Publishing',2,35,36,10,60523,'JQ082304802','Lucerne Publishing','Woodgrove Bank Jonesborough',654789,3254123658,21569,30,NULL,'(423) 555-0103','(423) 555-0105','http://www.lucernepublishing.com','Suite 34','949482 Miller Boulevard',60523,'0xE6100000010C9D8F21B6AA25424091F69A794D9E54C0','PO Box 8747','Westmont',60523,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (9,'Nod Publishers',2,37,38,10,29625,'GL08029802','Nod Publishers','Woodgrove Bank Elizabeth City',365985,2021545878,48758,7,'Marcos is not in on Mondays','(252) 555-0100','(252) 555-0101','http://www.nodpublishers.com','Level 1','389 King Street',29625,'0xE6100000010C0EB0A07AB52542407452A923111053C0','PO Box 3390','Anderson',29625,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (10,'Northwind Electric Cars',3,39,40,8,22202,'ML0300202','Northwind Electric Cars','Woodgrove Bank Crandon Lakes',325447,3258786987,36214,30,NULL,'(201) 555-0105','(201) 555-0104','http://www.northwindelectriccars.com',NULL,'440 New Road',22202,'0xE6100000010C6C4E14D7E78F4440C74ED3C2C0B552C0','PO Box 30920','Arlington',22202,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (11,'Trey Research',8,41,42,NULL,34269,'82304822','Trey Research','Woodgrove Bank Kadoka',658968,1254785321,56958,7,NULL,'(605) 555-0103','(605) 555-0101','http://www.treyresearch.net','Level 43','9401 Polar Avenue',34269,'0xE6100000010C8E5C37A5BCEA454054162AA4A16059C0','PO  Box 595','North Port',34269,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (12,'The Phone Company',2,43,44,7,91942,'237408032','The Phone Company','Woodgrove Bank Karlstad',214568,7896236589,25478,30,NULL,'(218) 555-0105','(218) 555-0105','http://www.thephone-company.com','Level 83','339 Toorak Road',80125,'0xE6100000010C1D1B26BFEA494840BF993D75512158C0','PO Box 3837','Highlands Ranch',80125,1,'05:00.0','##################');\")\n",
    "cs.execute(\"INSERT INTO supplier_case VALUES (13,'Woodgrove Bank',7,45,46,NULL,6331,'28034202','Woodgrove Bank','Woodgrove Bank San Francisco',325698,2147825698,65893,7,'Only speak to Donald if Hubert really is not available','(415) 555-0103','(415) 555-0107','http://www.woodgrovebank.com','Level 3','8488 Vienna Boulevard',6331,'0xE6100000010C529ACDE330E34240DFFB1BB4D79A5EC0','PO Box 2390','Canterbury',6331,1,'05:00.0','##################');\")"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**7) Connect manually inside Snowflake Marketplace Environment data(NOAACD2019R) and extract weather data for each unique zip code in the Supplier_Case table. Load ZCTA data to find weather stations closest to each zip code:**\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 80,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc64d8ac0>"
      ]
     },
     "execution_count": 80,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute('CREATE DATABASE IF NOT EXISTS Gazetteer2021')\n",
    "cs.execute('USE DATABASE Gazetteer2021')\n",
    "cs.execute('CREATE SCHEMA IF NOT EXISTS Gazetteer_Schema')\n",
    "cs.execute('USE Gazetteer2021.Gazetteer_Schema')\n",
    "cs.execute('CREATE STAGE IF NOT EXISTS Gazetteer_Stage')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 112,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc64d8ac0>"
      ]
     },
     "execution_count": 112,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Extract / Load Gazetteer 2021 Data(ZCTA)\n",
    "cs.execute(\"CREATE OR REPLACE TABLE Gazetteer2021(GEOID NUMBER, ALAND NUMBER, AWATER NUMBER, ALAND_SQMI NUMBER, AWATER_SQMI NUMBER, INTPTLAT FLOAT, INTPTLONG FLOAT)\")\n",
    "cs.execute(\"PUT 'file:///home/jovyan/Documents/0.Rady/1.Summer/MGTA495_SQL/Module 5 ETL/Case Data/2021_Gaz_zcta_national.txt' @Gazetteer_Stage\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 114,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc64d8ac0>"
      ]
     },
     "execution_count": 114,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute(\"COPY INTO Gazetteer2021 FROM @Gazetteer_Stage on_error=continue\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 115,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc64d8ac0>"
      ]
     },
     "execution_count": 115,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute(\"CREATE OR REPLACE FILE FORMAT Gazetteer2021 TYPE = CSV FIELD_DELIMITER = '\\t'\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 140,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 140,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Connect Gazetteer2021 data(ZCTA) with Environment data(NOAACD2019R)to get the zip code\n",
    "cs.execute('CREATE OR REPLACE VIEW zctaNOA AS SELECT A.*, B.geoid \\\n",
    "            FROM ENVIRONMENT_DATA_ATLAS.ENVIRONMENT.NOAACD2019R A \\\n",
    "            INNER JOIN Gazetteer2021.Gazetteer_Schema.Gazetteer2021 B \\\n",
    "            ON ROUND(A.\"Stations Latitude\") = ROUND(B.INTPTLAT) AND ROUND(A.\"Stations Longitude\") = ROUND(B.INTPTLONG)')"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**8) Creat a VIEW that contains zip codes from the supplier data, date, and daily high temperatures:**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 141,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 141,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute('CREATE OR REPLACE VIEW supplier_zip_code_weather AS SELECT a.geoid AS zip_code, a.\"Date\", MAX(a.\"Value\") AS High_temp \\\n",
    "            FROM Gazetteer2021.Gazetteer_Schema.zctaNOA a \\\n",
    "            WHERE a.\"Indicator Name\" = \\'Maximum temperature (Fahrenheit)\\'\\\n",
    "            group by a.geoid, a.\"Date\"')"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**9) Join purchase_orders_and_invoices / supplier_case / supplier_zip_code_weather based on zip codes:** "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 145,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<snowflake.connector.cursor.SnowflakeCursor at 0x7f0cc45c75b0>"
      ]
     },
     "execution_count": 145,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cs.execute('SELECT * FROM MonthlyPurchaseData.MPD_Schema.purchase_orders_and_invoices a \\\n",
    "            JOIN Supplier_Case.Supplier_Case_Schema.Supplier_case b USING (supplierid) \\\n",
    "            JOIN Gazetteer2021.Gazetteer_Schema.supplier_zip_code_weather c \\\n",
    "            ON b.POSTALPOSTALCODE = c.zip_code')"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "base",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.6"
  },
  "orig_nbformat": 4,
  "vscode": {
   "interpreter": {
    "hash": "d4d1e4263499bec80672ea0156c357c1ee493ec2b1c70f0acce89fc37c4a6abe"
   }
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
</p>
