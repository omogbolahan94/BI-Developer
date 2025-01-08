-- create the database
CREATE DATABASE DWHDB;
CREATE DATABASE StagingDB;

-- use database
DROP DATABASE DWHDB;
USE DWHDB;
USE StagingDB;


/************************** SET STAGGING LAYER *********************/ 
--Just data transformation in the this database. No Primary or Secondary key identifier

-- DROP TABLES
DROP TABLE Customer;
DROP TABLE ProductInfo;
DROP TABLE Transactions;

TRUNCATE TABLE Transactions;
TRUNCATE TABLE Customer;
TRUNCATE TABLE ProductInfo;


CREATE TABLE Customer (
    Customer_ID INT,
    Date_of_Birth DATE,
	Age INT,
    Gender VARCHAR(50),
    City_Code VARCHAR(50)
);

CREATE TABLE ProductInfo (
	Category_Subcategory_Code VARCHAR(50),
    Category_Code VARCHAR(50),
    Category VARCHAR(50),
    Sub_Category_Code VARCHAR(50),
    Sub_Category VARCHAR(50)
);

CREATE TABLE Transactions ( 
    Transaction_ID VARCHAR(50),
    Customer_ID VARCHAR(50),
    Transaction_Date DATE,
    Prod_Sub_Category_Code VARCHAR(50),
    Prod_Category_Code VARCHAR(50),
	Category_Subcategory_Code VARCHAR(50),
    Quantity INT,
    Rate INT,
    Tax DECIMAL(10,2),
    Total_Amount DECIMAL(10,4),
    Store_Type VARCHAR(50)
);

SELECT * FROM Customer;
SELECT * FROM ProductInfo;
SELECT * FROM Transactions;


/************************** SET UP DATA WAREHOUSE ******************/
-- TRUNCATE TABLES
TRUNCATE TABLE DimCustomer;
TRUNCATE TABLE DimProductInfo;
TRUNCATE TABLE FactTransactions;

-- DROP TABLES
DROP TABLE FactTransactions;
DROP TABLE DimCustomer;
DROP TABLE DimProductInfo;

-- CREATE TABLE
CREATE TABLE DimCustomer (
    Customer_ID VARCHAR(50) PRIMARY KEY,
    Date_of_Birth DATE,
	Age INT,
    Gender VARCHAR(50),
    City_Code VARCHAR(50)
);

CREATE TABLE DimProductInfo (
	Category_Subcategory_Code VARCHAR(50) PRIMARY KEY,
    Category_Code VARCHAR(50),
    Category VARCHAR(50),
    Sub_Category_Code VARCHAR(50),
    Sub_Category VARCHAR(50)
);

CREATE TABLE FactTransactions (
    Transaction_ID VARCHAR(50),
    Customer_ID VARCHAR(50),
    Transaction_Date DATE,
    Prod_Sub_Category_Code VARCHAR(50),
    Prod_Category_Code VARCHAR(50),
	Category_Subcategory_Code VARCHAR(50),
    Quantity INT,
    Rate INT,
    Tax DECIMAL(10,2),
    Total_Amount DECIMAL(10,4),
    Store_Type VARCHAR(50)
    FOREIGN KEY (Customer_ID) REFERENCES DimCustomer(Customer_ID),
    FOREIGN KEY (Category_Subcategory_Code) REFERENCES DimProductInfo(Category_Subcategory_Code)
);

SELECT  * FROM DimCustomer;
SELECT * FROM DimProductInfo;
SELECT * FROM FactTransactions;

--**************************** SET STOPRE PROCEDURE FOR UPDATING DWH ******************
GO
CREATE PROCEDURE MergeCustomerData
AS
BEGIN
    -- Ensure the BEGIN and END are properly formatted
    SET NOCOUNT ON;

    -- MERGE statement to synchronize DimCustomer with StagingDB.dbo.Customer
    MERGE INTO DimCustomer AS Target
    USING (SELECT Customer_ID, Date_of_Birth, Age, Gender, City_Code 
           FROM StagingDB.dbo.Customer) AS Source
    ON Target.Customer_ID = Source.Customer_ID
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (Customer_ID, Date_of_Birth, Age, Gender, City_Code)
        VALUES (Source.Customer_ID, Source.Date_of_Birth, Source.Age, Source.Gender, Source.City_Code);

END;
GO

EXEC MergeCustomerData;

-- CREATE STORE PROCEDURE FOR PRODUCT INFO
GO
CREATE PROCEDURE MergeProductData
AS
BEGIN
    -- Ensure the BEGIN and END are properly formatted
    SET NOCOUNT ON;

    -- MERGE statement to synchronize DimCustomer with StagingDB.dbo.Customer
    MERGE INTO DimProductInfo AS Target
    USING (SELECT Category_Subcategory_Code, Category_Code, Category, Sub_Category_Code, Sub_Category 
           FROM StagingDB.dbo.ProductInfo) AS Source
    ON Target.Category_Subcategory_Code = Source.Category_Subcategory_Code
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (Category_Subcategory_Code, Category_Code, Category, Sub_Category_Code, Sub_Category)
        VALUES (Source.Category_Subcategory_Code, Source.Category_Code, Source.Category, Source.Sub_Category_Code, Source.Sub_Category);

END;

EXEC MergeProductData;
GO


GO
CREATE PROCEDURE MergeTransactionData
AS
BEGIN
    SET NOCOUNT ON;

    -- Merge transactions from StagingDB.dbo.Transaction into DWHDB.dbo.TransactionDim
    MERGE INTO FactTransactions AS Target
    USING (
        -- Select distinct transactions from the staging table
        SELECT DISTINCT 
            Transaction_ID, 
            Customer_ID, 
            Transaction_Date, 
            Prod_Sub_Category_Code, 
            Prod_Category_Code, 
            Category_Subcategory_Code, 
            Quantity, 
            Rate, 
            Tax, 
            Total_Amount, 
            Store_Type
        FROM StagingDB.dbo.Transactions
    ) AS Source
    ON Target.Transaction_ID = Source.Transaction_ID
       AND Target.Transaction_Date = Source.Transaction_Date 
    WHEN NOT MATCHED BY TARGET THEN
        -- Insert new records from the source
        INSERT (Transaction_ID, Customer_ID, Transaction_Date, Prod_Sub_Category_Code, Prod_Category_Code, 
                Category_Subcategory_Code, Quantity, Rate, Tax, Total_Amount, Store_Type)
        VALUES (Source.Transaction_ID, Source.Customer_ID, Source.Transaction_Date, Source.Prod_Sub_Category_Code, 
                Source.Prod_Category_Code, Source.Category_Subcategory_Code, Source.Quantity, Source.Rate, 
                Source.Tax, Source.Total_Amount, Source.Store_Type);

    -- Optionally, handle transactions that are no longer in the source but exist in the target
    -- Uncomment the following if such rows should be deleted from the target:
    -- WHEN NOT MATCHED BY SOURCE THEN
    --     DELETE;

END;
GO

EXEC MergeTransactionData;
