package ai.vital.sql.schemas.mysql;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import ai.vital.sql.schemas.SqlDialect;
import static ai.vital.sql.model.VitalSignsToSqlBridge.*;

public class MySQLDialect implements SqlDialect {

	@Override
	public String getShowTables() {
		return "show tables";
	}

	static Map<String, Integer> column2Type = new HashMap<String, Integer>();
	static {
		column2Type.put(COLUMN_URI, Types.VARCHAR);
		column2Type.put(COLUMN_NAME, Types.VARCHAR);
		column2Type.put(COLUMN_EXTERNAL, Types.BIT);
		
		column2Type.put(COLUMN_VALUE_BOOLEAN, Types.BIT);
		column2Type.put(COLUMN_VALUE_BOOLEAN_MULTIVALUE, Types.BIT);
		
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
		
		column2Type.put(COLUMN_VALUE_TRUTH, Types.TINYINT);
		column2Type.put(COLUMN_VALUE_TRUTH_MULTIVALUE, Types.TINYINT);
		  
		column2Type.put(COLUMN_VALUE_FULL_TEXT, Types.CLOB);
		
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
		return "LOCATE(" + needle + ", " + stack + ")";
	}


	@Override
	public String regexp(String needle, String stack) {
		return stack + " REGEXP " + needle;
	}

}
