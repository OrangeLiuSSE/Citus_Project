-- Author: Liu Zhicheng

-- Create Warehouse Database
CREATE TABLE WAREHOUSE
(
    W_ID INT PRIMARY KEY,
    W_NAME VARCHAR(10),
    W_STREET_1 VARCHAR(20),
    W_STREET_2 VARCHAR(20),
    W_CITY VARCHAR(20),
    W_STATE CHAR(2),
    W_ZIP CHAR(9),
    W_TAX DECIMAL(4,4),
    W_YTD DECIMAL(12,2)
);


-- CREATE DISTRICT TABLE
CREATE TABLE DISTRICT
(
    W_ID INT,   --  REFERENCES WAREHOUSE (W_ID),
    D_ID INT,
    D_NAME VARCHAR(10),
    D_STREET_1 VARCHAR(20),
    D_STREET_2 VARCHAR(20),
    D_CITY VARCHAR(20),
    D_STATE CHAR(2),
    D_ZIP CHAR(9),
    D_TAX DECIMAL(4,4),
    D_YTD DECIMAL(12,2),
    D_NEXT_O_ID INT,
    PRIMARY KEY (W_ID, D_ID)
);


-- CREATE CUSTOMER TABLE
CREATE TABLE CUSTOMER
(
    W_ID INT,
    D_ID INT,
    C_ID INT,
    C_FIRST VARCHAR(16),
    C_MIDDLE CHAR(2),
    C_LAST VARCHAR(16),
    C_STREET_1 VARCHAR(20),
    C_STREET_2 VARCHAR(20),
    C_CITY VARCHAR(20),
    C_STATE CHAR(2),
    C_ZIP CHAR(9),
    C_PHONE CHAR(16),
    C_SINCE TIMESTAMP,
    C_CREDIT CHAR(2),
    C_CREDIT_LIM DECIMAL(12,2),
    C_DISCOUNT DECIMAL(5,4),
    C_BALANCE DECIMAL(12,2),
    C_YTD_PAYMENT FLOAT,
    C_PAYMENT_CNT INT,
    C_DELIVERY_CNT INT,
    C_DATA VARCHAR(500),
    -- FOREIGN KEY (W_ID, D_ID) REFERENCES DISTRICT (W_ID, D_ID),
	PRIMARY KEY (W_ID, D_ID, C_ID)
);

CREATE TABLE CUSTOMER_ORDER
(
    W_ID INT,
    D_ID INT,
    C_ID INT,
    O_INFO TEXT, 
	PRIMARY KEY (W_ID, D_ID, C_ID)
);


-- CREATE ORDER TABLE
CREATE TABLE "order"
(
    W_ID INT,
    D_ID INT,
    O_ID INT,
    C_ID INT,
    O_CARRIER_ID INT, -- must be 1 to 10
    O_OL_CNT DECIMAL(2,0),
    O_ALL_LOCAL DECIMAL(1,0),
    O_ENTRY_D TIMESTAMP,
    O_AMOUNT DECIMAL(12,2),
    -- FOREIGN KEY (W_ID, D_ID, C_ID) REFERENCES CUSTOMER (W_ID, D_ID, C_ID),
    PRIMARY KEY (W_ID, D_ID, O_ID, C_ID)
);

-- CREATE TABLE ITEM
CREATE TABLE ITEM
(
    I_ID INT PRIMARY KEY,
    I_NAME VARCHAR(24),
    I_PRICE DECIMAL(5,2),
    I_IM_ID INT,
    I_DATA VARCHAR(50)
);

-- CREATE TABLE ORDER LINE
CREATE TABLE ORDER_LINE
(
    W_ID INT,
    D_ID INT,
    O_ID INT,
    OL_NUMBER INT,
    I_ID INT,
    OL_DELIVERY_D TIMESTAMP,
    OL_AMOUNT DECIMAL(7,2),
    OL_SUPPLY_W_ID INT,
    OL_QUANTITY DECIMAL(2,0),
    OL_DIST_INFO CHAR(24),
    -- FOREIGN KEY (W_ID, D_ID, O_ID) REFERENCES "order" (W_ID, D_ID, O_ID),
    -- FOREIGN KEY (I_ID) REFERENCES ITEM (I_ID),
    PRIMARY KEY (W_ID, D_ID, O_ID, I_ID, OL_NUMBER)
);

-- CREATE TABLE STOCK
CREATE TABLE STOCK
(
    W_ID INT,
    I_ID INT, -- REFERENCES ITEM (I_ID),
    S_QUANTITY DECIMAL(4,0),
    S_YTD DECIMAL(8,2),
    S_ORDER_CNT INT,
    S_REMOTE_CNT INT,
    S_DIST_01 CHAR(24),
    S_DIST_02 CHAR(24),
    S_DIST_03 CHAR(24),
    S_DIST_04 CHAR(24),
    S_DIST_05 CHAR(24),
    S_DIST_06 CHAR(24),
    S_DIST_07 CHAR(24),
    S_DIST_08 CHAR(24),
    S_DIST_09 CHAR(24),
    S_DIST_10 CHAR(24),
    S_DATA VARCHAR(50),
    PRIMARY KEY (W_ID, I_ID)
);


CREATE INDEX c_w_id_index ON CUSTOMER(W_ID);
CREATE INDEX c_d_id_index ON CUSTOMER(D_ID);
CREATE INDEX c_id_index ON CUSTOMER(C_ID);
CREATE INDEX o_w_id_index ON public.order(W_ID);
CREATE INDEX o_d_id_index ON public.order(D_ID);
CREATE INDEX o_c_id_index ON public.order(C_ID);
CREATE INDEX o_id_index ON public.order(O_ID);
-- use CITUS UDF to partition the data

--distribute ITEM, STOCK, ORDER_LINE to a same colocation
--create reference table: warehouse & district
SELECT create_reference_table('WAREHOUSE');
SELECT create_reference_table('DISTRICT');
SELECT create_reference_table('ORDER');
SELECT create_distributed_table('CUSTOMER', 'c_id');
SELECT create_distributed_table('ITEM', 'i_id');
SELECT create_distributed_table('STOCK', 'i_id');
SELECT create_distributed_table('ORDER_LINE', 'i_id');

-- distribute CUSTOMER, ORDER to a same colocation



-- use copy to insert data
-- example: \COPY github_events FROM 'github_events-2015-01-01-0.csv' WITH (format CSV)
\COPY WAREHOUSE FROM '/home/stuproj/cs4224f/proj_data/project_files/data_files/warehouse.csv' WITH (format CSV);
\COPY DISTRICT FROM '/home/stuproj/cs4224f/proj_data/project_files/data_files/district.csv' WITH (format CSV);
\COPY CUSTOMER FROM '/home/stuproj/cs4224f/proj_data/project_files/data_files/customer.csv' WITH (format CSV);
\COPY "order" FROM '/home/stuproj/cs4224f/proj_data/project_files/data_files/add_amount_order.csv' WITH (format CSV);
\COPY ITEM FROM '/home/stuproj/cs4224f/proj_data/project_files/data_files/item.csv' WITH (format CSV);
\COPY ORDER_LINE FROM '/home/stuproj/cs4224f/proj_data/project_files/data_files/new_order-line.csv' WITH (format CSV);
\COPY STOCK FROM '/home/stuproj/cs4224f/proj_data/project_files/data_files/stock.csv' WITH (format CSV);
\COPY CUSTOMER_ORDER FROM '/home/stuproj/cs4224f/proj_data/project_files/data_files/customer_order.csv' WITH (format CSV);
