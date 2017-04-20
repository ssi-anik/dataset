-- DELETE EXISTING dataset
DROP DATABASE IF EXISTS dataset;

-- CREATE DATABASE
CREATE DATABASE dataset;

-- CREATE companies TABLE SQL
CREATE TABLE `dataset`.`companies` ( `id` INT NOT NULL AUTO_INCREMENT , `name` VARCHAR(255) NOT NULL , `url` VARCHAR(255) NOT NULL , `slug` VARCHAR(100) NOT NULL , `created_at` DATETIME NULL , `updated_at` DATETIME NULL , PRIMARY KEY (`id`)) ENGINE = InnoDB;
-- companies CREATE TABLE QUERY ENDS

-- CREATE categories TABLE SQL
CREATE TABLE `dataset`.`categories` ( `id` INT NOT NULL AUTO_INCREMENT , `name` VARCHAR(255) NOT NULL , `description` TEXT NOT NULL , `created_at` DATETIME NOT NULL , `updated_at` DATETIME NOT NULL , PRIMARY KEY (`id`)) ENGINE = InnoDB;
-- categories CREATE TABLE QUERY ENDS

-- CREATE employees TABLE SQL
CREATE TABLE `dataset`.`employees` ( `employeeID` INT NOT NULL AUTO_INCREMENT , `lastName` VARCHAR(255) NOT NULL , `firstName` VARCHAR(255) NOT NULL , `title` VARCHAR(255) NOT NULL , `titleOfCourtesy` VARCHAR(255) NOT NULL , `birthDate` DATETIME NOT NULL , `hireDate` DATETIME NOT NULL , `address` VARCHAR(255) NOT NULL , `city` VARCHAR(30) NOT NULL , `region` VARCHAR(30) NULL , `postalCode` VARCHAR(30) NOT NULL , `country` VARCHAR(30) NOT NULL , `homePhone` VARCHAR(30) NOT NULL , `extension` INT NOT NULL , `notes` TEXT NOT NULL , `reportsTo` INT NULL , `photoPath` VARCHAR(255) NOT NULL , `created_at` DATETIME NOT NULL , `updated_at` DATETIME NOT NULL , PRIMARY KEY (`employeeID`)) ENGINE = InnoDB;
-- employees CREATE TABLE QUERY ENDS

-- CREATE products TABLE SQL
CREATE TABLE `dataset`.`products` ( `id` INT NOT NULL AUTO_INCREMENT , `productName` VARCHAR(255) NOT NULL , `supplierID` INT NOT NULL , `categoryID` INT DEFAULT NULL , `quantityPerUnit` VARCHAR(255) NOT NULL , `unitPrice` DECIMAL NOT NULL , `unitsInStock` INT NOT NULL , `unitsOnOrder` INT NOT NULL , `reorderLevel` INT NOT NULL , `discontinued` TINYINT NOT NULL , `created_at` DATETIME NOT NULL , `updated_at` DATETIME NOT NULL , PRIMARY KEY (`id`)) ENGINE = InnoDB;
-- products CREATE TABLE QUERY ENDS
