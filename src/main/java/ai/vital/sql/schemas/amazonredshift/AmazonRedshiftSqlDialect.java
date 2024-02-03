package ai.vital.sql.schemas.amazonredshift;

import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_EXTERNAL;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_NAME;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_URI;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_BOOLEAN;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_BOOLEAN_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_DATE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_DATE_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_DOUBLE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_DOUBLE_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_FLOAT;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_FLOAT_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_FULL_TEXT;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_GEOLOCATION;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_GEOLOCATION_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_INTEGER;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_INTEGER_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_LONG;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_LONG_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_OTHER;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_OTHER_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_STRING;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_STRING_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_TRUTH;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_TRUTH_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_URI;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_URI_MULTIVALUE;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import ai.vital.sql.schemas.SqlDialect;

public class AmazonRedshiftSqlDialect implements SqlDialect {

	@Override
	public String getShowTables() {
		return "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
	}

	static Map<String, Integer> column2Type = new HashMap<String, Integer>();
	static {
		column2Type.put(COLUMN_URI, Types.VARCHAR);
		column2Type.put(COLUMN_NAME, Types.VARCHAR);
		column2Type.put(COLUMN_EXTERNAL, Types.BOOLEAN);
		
		column2Type.put(COLUMN_VALUE_BOOLEAN, Types.BOOLEAN);
		column2Type.put(COLUMN_VALUE_BOOLEAN_MULTIVALUE, Types.BOOLEAN);
		
		column2Type.put(COLUMN_VALUE_DATE, Types.BIGINT);
		column2Type.put(COLUMN_VALUE_DATE_MULTIVALUE, Types.BIGINT);
		
		column2Type.put(COLUMN_VALUE_DOUBLE, Types.DOUBLE);
		column2Type.put(COLUMN_VALUE_DOUBLE_MULTIVALUE, Types.DOUBLE);
		
		column2Type.put(COLUMN_VALUE_FLOAT, Types.FLOAT);
		column2Type.put(COLUMN_VALUE_FLOAT_MULTIVALUE, Types.FLOAT);
		
		column2Type.put(COLUMN_VALUE_GEOLOCATION, Types.VARCHAR);
		column2Type.put(COLUMN_VALUE_GEOLOCATION_MULTIVALUE, Types.VARCHAR);
		
		column2Type.put(COLUMN_VALUE_INTEGER, Types.INTEGER);
		column2Type.put(COLUMN_VALUE_INTEGER_MULTIVALUE, Types.INTEGER);
		
		column2Type.put(COLUMN_VALUE_LONG, Types.BIGINT);
		column2Type.put(COLUMN_VALUE_LONG_MULTIVALUE, Types.BIGINT);
		
		column2Type.put(COLUMN_VALUE_LONG, Types.BIGINT);
		column2Type.put(COLUMN_VALUE_LONG_MULTIVALUE, Types.BIGINT);
		  
		column2Type.put(COLUMN_VALUE_OTHER, Types.VARCHAR);
		column2Type.put(COLUMN_VALUE_OTHER_MULTIVALUE, Types.VARCHAR);
		
		column2Type.put(COLUMN_VALUE_STRING, Types.VARCHAR);
		column2Type.put(COLUMN_VALUE_STRING_MULTIVALUE, Types.VARCHAR);
		
		column2Type.put(COLUMN_VALUE_TRUTH, Types.SMALLINT);
		column2Type.put(COLUMN_VALUE_TRUTH_MULTIVALUE, Types.SMALLINT);
		  
		column2Type.put(COLUMN_VALUE_FULL_TEXT, Types.VARCHAR);
		
		column2Type.put(COLUMN_VALUE_URI, Types.VARCHAR);
		column2Type.put(COLUMN_VALUE_URI_MULTIVALUE, Types.VARCHAR);
		  
	}
	
	
	@Override
	public int getColumnType(String string) {
		Integer integer = column2Type.get(string);
		if(integer == null) throw new RuntimeException("No sql type for column: " + string);
		return integer.intValue();
	}


	@Override
	public String locate(String needle, String stack) {
		return "strpos(" +stack + ", " + needle + ")";
	}


	@Override
	public String regexp(String needle, String stack) {
		return stack + " ~ " + needle;
	}

}
