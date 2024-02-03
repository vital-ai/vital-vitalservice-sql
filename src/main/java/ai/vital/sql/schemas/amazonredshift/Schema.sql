CREATE TABLE "${NAME}" (

  id bigint identity(0, 1),
  -- id SERIAL,
  uri varchar(191) NOT NULL, 
  name varchar(191) NOT NULL,
  vitaltype varchar(191) NOT NULL,
  tstamp BIGINT,
  channeluri varchar(191),
  external BOOLEAN NOT NULL,
  
  value_boolean BOOLEAN,
  value_boolean_multivalue BOOLEAN,
  
  value_date BIGINT,
  value_date_multivalue BIGINT,
  
  value_double DOUBLE PRECISION,
  value_double_multivalue DOUBLE PRECISION, 

  value_float FLOAT, 
  value_float_multivalue FLOAT,

  value_geolocation VARCHAR(64),
  value_geolocation_multivalue VARCHAR(64),

  value_integer INT,
  value_integer_multivalue INT,

  value_long BIGINT,
  value_long_multivalue BIGINT,

  value_other VARCHAR(2000),
  value_other_multivalue VARCHAR(2000),
  
  value_string VARCHAR(5000),
  value_string_multivalue VARCHAR(5000),
  
  value_truth SMALLINT,
  value_truth_multivalue SMALLINT,
  
  value_full_text VARCHAR(65535),
  -- TEXT is VAR(256) according to the docs, we use max allowed VARCHAR(65535)
  
  value_uri VARCHAR(191),
  value_uri_multivalue VARCHAR(191),
  
  PRIMARY KEY (id)
  
  
  -- indices NOT AVAILABLE
    
)
