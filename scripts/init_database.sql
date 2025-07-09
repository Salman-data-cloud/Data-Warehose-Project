/* This script creates a new database named 'DataWarehouse' after checking if it already exists or not.
If it is already there, it drops the database and then again recreate it. Then it creates three schemas
inside the database named bronze, silver and gold.
*/

USE master;
GO
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO 

-- CREATE DATABASE DataWarehouse
create database DataWarehouse;
Go
Use DataWarehouse;
Go

-- CREATE SCHEMAS

create schema bronze;
Go
create schema silver;
Go
create schema gold;
