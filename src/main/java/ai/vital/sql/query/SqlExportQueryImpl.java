package ai.vital.sql.query;

import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_URI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import ai.vital.sql.dao.CoreOperations;
import ai.vital.sql.model.SegmentTable;
import ai.vital.sql.model.VitalSignsToSqlBridge;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery;
import ai.vital.vitalsigns.model.GraphObject;

public class SqlExportQueryImpl {

	public static ResultList export(Connection connection,
			SegmentTable segmentTable, VitalExportQuery sq) throws Exception {
		return export(connection, segmentTable, sq, null);
	}
	
	public static ResultList export(Connection connection,
			SegmentTable segmentTable, VitalExportQuery sq, QueryStats stats) throws Exception {

		long total = System.currentTimeMillis();
		
		int offset = sq.getOffset();
		int limit = sq.getLimit();
		
		if(offset < 0) throw new RuntimeException("offset must be >= 0");
		if(limit <= 0) throw new RuntimeException("limit must be > 0");
		
		
		PreparedStatement stmt = null;
		
		ResultSet rs = null;
		
		String tname = SQLUtils.escapeID(connection, segmentTable.tableName) ;
		
		ResultList r = new ResultList();
		r.setLimit(limit);
		r.setOffset(offset);
		r.setQueryStats(stats);
		
		try {
			
			r.setTotalResults(CoreOperations.getSegmentSize(connection, segmentTable, stats));
					
			stmt = connection.prepareStatement(
			"SELECT * FROM " + tname + " AS t1 INNER JOIN (" + 
				"SELECT DISTINCT " + COLUMN_URI + " FROM " + tname + " ORDER BY " + COLUMN_URI + " ASC LIMIT " + limit + " OFFSET "+ offset +
			") AS t2 ON t1." + COLUMN_URI + " = t2." + COLUMN_URI + " ORDER BY t1." + COLUMN_URI);
			
			
//			SELECT v.VID, v.thumb
//			FROM video AS v
//			INNER JOIN
//			     (SELECT VID
//			     FROM video
//			     WHERE title LIKE "%'.$Channel['name'].'%"
//			     ORDER BY viewtime DESC
//			     LIMIT 5) as v2
//			  ON v.VID = v2.VID
//			ORDER BY RAND()
//			LIMIT 1
			
			long start = System.currentTimeMillis();
			rs = stmt.executeQuery();
			
			if(stats != null) {
				long time = stats.addDatabaseTimeFrom(start);
				if(stats.getQueriesTimes() != null) stats.getQueriesTimes().add(new QueryTime("exportQuery", stmt.toString(), time));
			}
			
			List<GraphObject> fromSql = VitalSignsToSqlBridge.fromSql(segmentTable, rs);
			
			for(GraphObject g : fromSql) {
				r.getResults().add(new ResultElement(g, 1D));
			}
			
		} finally {
			SQLUtils.closeQuietly(stmt, rs);
		}
		
		if(stats != null) {
			stats.setQueryTimeMS(System.currentTimeMillis() - total);
		}
		
		return r;
		
	}

}
