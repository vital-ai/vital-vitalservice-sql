CREATE TABLE `${NAME}` (

  id BIGINT NOT NULL AUTO_INCREMENT,
  uri varchar(191) NOT NULL, 
  name varchar(191) NOT NULL,
  vitaltype varchar(191) NOT NULL,
  tstamp BIGINT,
  channeluri varchar(191),
  external BIT NOT NULL,
    
  value_boolean BIT,
  value_boolean_multivalue BIT,
  
  value_date BIGINT,
  value_date_multivalue BIGINT,
  
  value_double DOUBLE,
  value_double_multivalue DOUBLE, 

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
  
  value_truth TINYINT,
  value_truth_multivalue TINYINT,
  
  value_full_text MEDIUMTEXT,
  
  value_uri VARCHAR(191),
  value_uri_multivalue VARCHAR(191),
  
  PRIMARY KEY (id),
  
  -- indices
  INDEX uri_index( uri(191) ),
  INDEX name_index( name(191) ),
  INDEX vitaltype_index( vitaltype(191) ),
  INDEX tstamp_index( tstamp ),
  INDEX channeluri_index( channeluri(191) ),
  
  INDEX value_boolean_index( value_boolean ),
  INDEX value_boolean_multivalue_index( value_boolean_multivalue ),
  
  INDEX value_date_index( value_date ),
  INDEX value_date_multivalue_index( value_date_multivalue ),
  
  INDEX value_double_index( value_double ),
  INDEX value_double_multivalue_index( value_double_multivalue ),

  INDEX value_float_index( value_float ),
  INDEX value_float_multivalue_index( value_float_multivalue ),
  
  -- INDEX value_geolocation_index( value_geolocation(64) ),
  -- INDEX value_geolocation_multivalue_index( value_geolocation_multivalue(64) ),
  
  INDEX value_integer_index( value_integer ),
  INDEX value_integer_multivalue_index( value_integer_multivalue ),
  
  INDEX value_long( value_long ),
  INDEX value_long_multivalue_index( value_long_multivalue ),
  
  -- INDEX value_other_index( value_other(191) ),
  -- INDEX value_long_multivalue_index( value_other_multivalue(191) ),
  
  INDEX value_string_index( value_string(191) ),
  INDEX value_string_multivalue_index( value_string_multivalue(191) ),

  INDEX value_truth_index( value_truth ),
  INDEX value_truth_multivalue_index( value_truth_multivalue ),
  
  INDEX value_uri_index( value_uri (191) ),
  INDEX value_uri_multivalue( value_uri_multivalue(191) )
  
) ENGINE = InnoDB 
  CHARACTER SET utf8mb4 
--  COLLATE utf8_general_ci
  COLLATE utf8mb4_bin