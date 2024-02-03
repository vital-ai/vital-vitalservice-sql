package ai.vital.sql.schemas.postgresql;

import ai.vital.sql.schemas.amazonredshift.AmazonRedshiftSqlDialect;

public class PostgreSQLDialect extends AmazonRedshiftSqlDialect {


	@Override
	public String regexp(String needle, String stack) {
		return stack + " ~ " + needle;
	}
	
}
