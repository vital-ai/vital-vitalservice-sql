package ai.vital.sql.config

import ai.vital.sql.config.VitalSqlConfig.SqlDBType;

class VitalSqlConfig {

	public static enum SqlDBType {
		MySQL,
		MySQLMemSQL,
		MySQLAurora,
		MariaDB,
		AmazonRedshift,
		PostgreSQL
	}
	
	String tablesPrefix
	
	String endpointURL
	
	String username
	
	String password
	
	SqlDBType dbType

	Integer poolInitialSize = 1
	
	Integer poolMaxTotal = 5
	
}
