package ai.vital.sql.schemas;

public interface SqlDialect {

	public String getShowTables();

	//returns java.sql.Types constant for given column
	public int getColumnType(String string);

	public String locate(String needle, String stack);
	
	public String regexp(String needle, String stack);
	
}
