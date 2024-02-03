CREATE TABLE "${NAME}" (

  --id bigint identity(0, 1),
  id SERIAL,
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
  
  value_full_text TEXT,
  
  value_uri VARCHAR(191),
  value_uri_multivalue VARCHAR(191),
  
  PRIMARY KEY (id)
  
);

-- indices
CREATE INDEX "${NAME}_uri_index" ON "${NAME}" ( uri );
CREATE INDEX "${NAME}_name_index" ON "${NAME}" ( name );
CREATE INDEX "${NAME}_vitaltype_index" ON "${NAME}" ( vitaltype );
CREATE INDEX "${NAME}_tstamp_index" ON "${NAME}" ( tstamp );
CREATE INDEX "${NAME}_channeluri_index" ON "${NAME}" ( channeluri );
  
CREATE INDEX "${NAME}_value_boolean_index" ON "${NAME}" ( value_boolean );
CREATE INDEX "${NAME}_value_boolean_multivalue_index" ON "${NAME}" ( value_boolean_multivalue );
  
CREATE INDEX "${NAME}_value_date_index" ON "${NAME}" ( value_date );
CREATE INDEX "${NAME}_value_date_multivalue_index" ON "${NAME}" ( value_date_multivalue );
  
CREATE INDEX "${NAME}_value_double_index" ON "${NAME}" ( value_double );
CREATE INDEX "${NAME}_value_double_multivalue_index" ON "${NAME}" ( value_double_multivalue );
  
CREATE INDEX "${NAME}_value_float_index" ON "${NAME}" ( value_float );
CREATE INDEX "${NAME}_value_float_multivalue_index" ON "${NAME}" ( value_float_multivalue );
  
  -- CREATE INDEX "${NAME}_value_geolocation_index" ON "${NAME}" ( value_geolocation );
  -- CREATE INDEX "${NAME}_value_geolocation_multivalue_index" ON "${NAME}" ( value_geolocation_multivalue );
  
CREATE INDEX "${NAME}_value_integer_index" ON "${NAME}" ( value_integer );
CREATE INDEX "${NAME}_value_integer_multivalue_index" ON "${NAME}" ( value_integer_multivalue );
  
CREATE INDEX "${NAME}_value_long" ON "${NAME}" ( value_long );
CREATE INDEX "${NAME}_value_long_multivalue_index" ON "${NAME}" ( value_long_multivalue );
  
  -- CREATE INDEX "${NAME}_value_other_index" ON "${NAME}" ( value_other );
  -- CREATE INDEX "${NAME}_value_long_multivalue_index" ON "${NAME}" ( value_other_multivalue );
  
CREATE INDEX "${NAME}_value_string_index" ON "${NAME}" ( value_string );
CREATE INDEX "${NAME}_value_string_multivalue_index" ON "${NAME}" ( value_string_multivalue );

CREATE INDEX "${NAME}_value_truth_index" ON "${NAME}" ( value_truth );
CREATE INDEX "${NAME}_value_truth_multivalue_index" ON "${NAME}" ( value_truth_multivalue );
  
CREATE INDEX "${NAME}_value_uri_index" ON "${NAME}" ( value_uri );
CREATE INDEX "${NAME}_value_uri_multivalue" ON "${NAME}" ( value_uri_multivalue );
