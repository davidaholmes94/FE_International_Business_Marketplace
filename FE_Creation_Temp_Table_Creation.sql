CREATE TABLE IF NOT EXISTS fe_creation_temp (
    ID int AUTO_INCREMENT,
    listing_id int NOT NULL,
    title text,
    revenue text,
    profit text,
    asking_price text,
    status text,
    listing_date date,
    url text,
    UNIQUE(listing_id),
    PRIMARY KEY(ID))