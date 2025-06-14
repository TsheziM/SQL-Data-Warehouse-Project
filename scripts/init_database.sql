/*
=================================================================================================================
CREATE DATABASE AND SCHEMAS
=================================================================================================================

Script Objective:
  This script creates a new database by the name of DataWarehouse after checking for it's availability.
  If it exists, it will be dropped and recreated.
  This script also sets up the three schemas within the database namely: bronze, silver & gold.

NB://
  Running this script will drop the entire DataWarehouse database if it already exists.
  Do backups if you have any important in this database.

  */
Use Master
GO

--Dropping and Recreating the DataWarehouse database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse';
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABSE DataWarehouse;
END
GO


--Create the DataWarehouse database

CREATE DATABASE DataWarehouse;
GO

Use DataWarehouse;
GO

--Create layers schemas

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

