package ai.vital.sql.services;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import ai.vital.sql.model.VitalSignsToSqlBridge;
import ai.vital.sql.model.VitalSignsToSqlBridge.OutputType;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.sql.ToSQLRowsProvider;

public class ToSQLRowsProviderImpl implements ToSQLRowsProvider {

	@Override
	public List<String> getColumns() {
		return VitalSignsToSqlBridge.columns;
	}

	@Override
	public List<String> toSQLRows(GraphObject arg0) {
		try {
			return VitalSignsToSqlBridge.batchInsertGraphObjects(null, null, null, Arrays.asList(arg0), OutputType.SQLRows);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
