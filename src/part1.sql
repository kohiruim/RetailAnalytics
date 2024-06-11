SET DATESTYLE TO iso, DMY;

DROP PROCEDURE IF EXISTS import(), export() CASCADE;
DROP TABLE IF EXISTS Personal_data, Cards, Groups_SKU, SKU, Transactions,
    Stores, Checks, Date_analysis_formation CASCADE;

CREATE TABLE Personal_data
(
    Customer_ID            INT     NOT NULL PRIMARY KEY,
    Customer_Name          VARCHAR NOT NULL CHECK (Customer_Name SIMILAR TO '[A-ZА-Я][a-zа-я\- ]*'),
    Customer_Surname       VARCHAR NOT NULL CHECK (Customer_Surname SIMILAR TO '[A-ZА-Я][a-zа-я\- ]*'),
    Customer_Primary_Email VARCHAR NOT NULL CHECK (Customer_Primary_Email SIMILAR TO
                                                   '[A-Za-z0-9\-_.]*[@][A-Za-z0-9\-_.]*[.][A-Za-z]*'),
    Customer_Primary_Phone VARCHAR NOT NULL CHECK (Customer_Primary_Phone SIMILAR TO '[+][7][0-9]{10}')
);

CREATE TABLE Cards
(
    Customer_Card_ID INT NOT NULL PRIMARY KEY,
    Customer_ID      INT NOT NULL,
    FOREIGN KEY (Customer_ID) REFERENCES Personal_data (Customer_ID)
);

-- группы товаров
CREATE TABLE Groups_SKU
(
    Group_ID   INT     NOT NULL PRIMARY KEY,
    Group_Name VARCHAR NOT NULL CHECK (Group_Name SIMILAR TO '[0-9A-za-zА-яа-я%, \-\[\]\/\\\{\}\(\)\*\+\?\.\^\$\|]+')
);

-- товарная матрица
CREATE TABLE SKU
(
    SKU_ID   INT     NOT NULL PRIMARY KEY,
    SKU_Name VARCHAR NOT NULL CHECK (SKU_Name SIMILAR TO '[0-9A-za-zА-яа-я%, \-\[\]\/\\\{\}\(\)\*\+\?\.\^\$\|]+'),
    Group_ID INT     NOT NULL,
    FOREIGN KEY (Group_ID) REFERENCES Groups_SKU (Group_ID)
);

CREATE TABLE Transactions
(
    Transaction_ID       INT       NOT NULL PRIMARY KEY,
    Customer_Card_ID     INT       NOT NULL,
    Transaction_Sum      NUMERIC   NOT NULL CHECK (Transaction_Sum > 0),
    Transaction_DateTime timestamp NOT NULL,
    Transaction_Store_ID INT       NOT NULL,
    FOREIGN KEY (Customer_Card_ID) REFERENCES Cards (Customer_Card_ID)
);

CREATE TABLE Stores
(
    Transaction_Store_ID INT     NOT NULL,
    SKU_ID               INT     NOT NULL,
    SKU_Purchase_Price   NUMERIC NOT NULL CHECK (SKU_Purchase_Price >= 0),
    SKU_Retail_Price     NUMERIC NOT NULL CHECK (SKU_Purchase_Price >= 0),
    CHECK (SKU_Purchase_Price < SKU_Retail_Price),
    FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID)
);

CREATE TABLE Checks
(
    Transaction_ID INT     NOT NULL,
    SKU_ID         INT     NOT NULL,
    SKU_Amount     NUMERIC NOT NULL CHECK (SKU_Amount >= 0),
    SKU_Sum        NUMERIC NOT NULL CHECK (SKU_Sum >= 0),
    SKU_Sum_Paid   NUMERIC NOT NULL CHECK (SKU_Sum_Paid >= 0),
    SKU_Discount   NUMERIC NOT NULL CHECK (SKU_Discount >= 0),
    FOREIGN KEY (Transaction_ID) REFERENCES Transactions (Transaction_ID),
    FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID)
);

CREATE TABLE Date_analysis_formation
(
    Analysis_Formation timestamp NOT NULL
);

CREATE OR REPLACE PROCEDURE import(IN tablename varchar, IN path text, IN separator char) AS
$$
BEGIN
    EXECUTE FORMAT('COPY %s FROM %L DELIMITER %L CSV', tablename, path, separator);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE export(IN tablename varchar, IN path text, IN separator char) AS
$$
BEGIN
    EXECUTE FORMAT('COPY %s TO %L DELIMITER %L CSV HEADER', tablename, path, separator);
END;
$$ LANGUAGE plpgsql;


CALL import('Personal_data', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/datasets/Personal_Data_Mini.tsv', E'\t');
CALL import('Cards', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/datasets/Cards_Mini.tsv', E'\t');
CALL import('Groups_SKU', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/datasets/Groups_SKU_Mini.tsv', E'\t');
CALL import('SKU', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/datasets/SKU_Mini.tsv', E'\t');
CALL import('Transactions', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/datasets/Transactions_Mini.tsv', E'\t');
CALL import('Stores', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/datasets/Stores_Mini.tsv', E'\t');
CALL import('Checks', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/datasets/Checks_Mini.tsv', E'\t');
CALL import('Date_analysis_formation',
            '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/datasets/Date_Of_Analysis_Formation.tsv', E'\t');

SELECT *
FROM Personal_data;
SELECT *
FROM cards;
SELECT *
FROM groups_sku;
SELECT *
FROM sku;
SELECT *
FROM Transactions;
SELECT *
FROM checks;
SELECT *
FROM stores;
SELECT *
FROM Date_analysis_formation;

CALL export('Personal_data', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/src/export/Personal_Data_Mini.tsv', E'\t');
CALL export('Cards', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/src/export/Cards_Mini.tsv', E'\t');
CALL export('Groups_SKU', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/src/export/Groups_SKU_Mini.tsv', E'\t');
CALL export('SKU', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/src/export/SKU_Mini.tsv', E'\t');
CALL export('Transactions', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/src/export/Transactions_Mini.tsv', E'\t');
CALL export('Stores', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/src/export/Stores_Mini.tsv', E'\t');
CALL export('Checks', '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/src/export/Checks_Mini.tsv', E'\t');
CALL export('Date_analysis_formation',
            '/Users/kohiruim/SQL3_RetailAnalitycs_v1.0-2/src/export/Date_Of_Analysis_Formation.tsv', E'\t');
