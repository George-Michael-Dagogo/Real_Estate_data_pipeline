CREATE TABLE IF NOT EXISTS categories (
  category_id SERIAL,
  category VARCHAR(45) NOT NULL,
  description VARCHAR(255) NOT NULL,
	-- title = decription
  PRIMARY KEY (category_id)
);
ALTER SEQUENCE categories_category_id_seq RESTART WITH 1000 INCREMENT BY 10;

CREATE TABLE iF NOT EXISTS address (
  address_id SERIAL,
  address VARCHAR(500) NOT NULL,
  state VARCHAR(10),
  PRIMARY KEY (address_id)
);
ALTER SEQUENCE address_address_id_seq RESTART WITH 50 INCREMENT BY 3;

CREATE TABLE agent (
  agent_id SERIAL,
  name VARCHAR(255) NOT NULL,
  location VARCHAR(255) NOT NULL,
  registered_on VARCHAR(255) NOT NULL,
  PRIMARY KEY (agent_id)
);
ALTER SEQUENCE agent_agent_id_seq RESTART WITH 2654 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS listings (
  listings_id SERIAL,
  category_id INT NOT NULL,
  address_id INT NOT NULL,
  agent_id INT NOT NULL,
  --PIDs VARCHAR(12) NOT NULL,
  newly_built BOOLEAN NOT NULL,
  serviced BOOLEAN NOT NULL,
  furnished BOOLEAN NOT NULL,
  beds INT NOT NULL,
  baths INT NOT NULL,
  toilets INT NOT NULL,
  price_₦ INT NOT NULL,
  price VARCHAR,
	--price_per_time = price
	--price_₦ = all prices per year
  date_posted DATE,
  date_updated DATE,
  type VARCHAR(12) NOT NULL,
  PRIMARY KEY (listings_id),
  FOREIGN KEY (category_id) REFERENCES categories(category_id),
  FOREIGN KEY (address_id) REFERENCES address(address_id),
  FOREIGN KEY (agent_id) REFERENCES agent(agent_id),
);

ALTER SEQUENCE listings_listings_id_seq RESTART WITH 10 INCREMENT BY 1;

INSERT INTO agent ("name", "location", registered_on)
SELECT "name", located, registered_on
FROM agents_staging;

ALTER TABLE "propertypro_merged_2023-09-30"
RENAME TO propertypro_merged_staging; 
INSERT INTO categories (category, description)
SELECT categories, title
FROM propertypro_merged_staging;

INSERT INTO address (address, "state")
SELECT address, "state"
FROM propertypro_merged_staging;


INSERT INTO agent (name, location, registered_on)
  SELECT DISTINCT name,located, registered_on
  FROM agents_staging;

  INSERT INTO listings (category_id, address_id, agent_id, newly_built, serviced, furnished, beds, baths, toilets, price_₦, price, date_posted, date_updated, type)
SELECT
    c.category_id,
    a.address_id,
    ag.agent_id,
    pm.newly_built,
    pm.serviced,
    pm.furnished,
    pm.beds,
    pm.baths,
    pm.toilets,
    pm.price_₦,
    pm.price,
    pm.date_posted::date, -- Cast the text to a date
    pm.date_updated::date, -- Cast the text to a date
    pm.type
FROM propertypro_merged_staging pm
JOIN categories c ON pm.categories = c.category
JOIN address a ON pm.address = a.address
JOIN agent ag ON pm.agent = ag.name;