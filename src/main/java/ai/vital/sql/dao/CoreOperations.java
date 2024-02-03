package ai.vital.sql.dao;

import static ai.vital.sql.model.VitalSignsToSqlBridge.*;
import static ai.vital.sql.utils.SQLUtils.escapeID;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.sql.VitalSqlImplementation.ScanHandler;
import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.model.SegmentTable;
import ai.vital.sql.model.VitalSignsToSqlBridge;
import ai.vital.sql.model.VitalSignsToSqlBridge.GraphObjectsStreamHandler;
import ai.vital.sql.query.SQLGraphObjectResolver;
import ai.vital.sql.query.URIResultElement;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;
import ai.vital.vitalsigns.model.GraphObject;

public class CoreOperations {

	private final static Logger log = LoggerFactory.getLogger(CoreOperations.class);

	public static GraphObject getGraphObject(Connection connection, SegmentTable segment, String uri) throws SQLException {
		
		ResultSet rs = null;
		
		PreparedStatement stmt = null;
		
		try {

			String tableName = escapeID(connection, segment.tableName);
			
			String q = "SELECT * FROM " + tableName + " WHERE " + COLUMN_URI + " = ? ";
			
			if(log.isDebugEnabled()) {
				log.debug("Get object URI {}, query: {}", uri, q);
			}
			
			stmt = connection.prepareStatement(q);
			stmt.setString(1, uri);
			
			long start = System.currentTimeMillis();
			
			rs = stmt.executeQuery();
			
			if(log.isDebugEnabled()) {
				log.debug("Get object URI {} time: {}ms", uri, System.currentTimeMillis() - start);
			}
			
			start = System.currentTimeMillis();
			List<GraphObject> fromSql = fromSql(segment, rs);
			if(log.isDebugEnabled()) {
				log.debug("Get object from sql iteration time: {}ms", System.currentTimeMillis() - start);
			}
			
			if(fromSql.size() > 1) throw new RuntimeException("More than 1 graph object found: " + fromSql.size());
			
			if( fromSql.size() > 0 ) {
				
				return fromSql.get(0);
				
			} else {
				
				return null;
				
			}
			
			
		} finally {
			SQLUtils.closeQuietly(stmt, rs);
		}
		
	}
	
	private static class URIsListComparator implements Comparator<GraphObject> {

		private List<String> uris;
		
		public URIsListComparator(List<String> uris) {
			super();
			this.uris = uris;
		}

		@Override
		public int compare(GraphObject o1, GraphObject o2) {

			Integer i1 = uris.indexOf(o1.getURI());
			Integer i2 = uris.indexOf(o2.getURI());
			
			return i1.compareTo(i2);
		}
		
	}
	public static List<GraphObject> getGraphObjectsBatch(Connection connection, SegmentTable segment, List<String> uris, SQLGraphObjectResolver resolver, QueryStats queryStats) throws SQLException {
		
		ResultSet rs = null;
		
		PreparedStatement stmt = null;
		
		List<GraphObject> objects = new ArrayList<GraphObject>();
		
		try {

			String tableName = escapeID(connection, segment.tableName);
			
			String columnsList = "";
			if(resolver == null) {
				columnsList = "*";
			} else {
				
				List<String> l = new ArrayList<String>(VitalSignsToSqlBridge.columns);
				l.remove(COLUMN_VALUE_FULL_TEXT);
				
				for(int i = 0 ; i < l.size(); i++) {
					if(i > 0) columnsList += ", ";
					columnsList += SQLUtils.escapeID(connection, l.get(i));
				}
				
			}
			
			StringBuilder q = new StringBuilder("SELECT " + columnsList + " FROM ").append(tableName).append(" WHERE ").append(COLUMN_URI).append(" IN (");
			
			for(int i = 1; i <= uris.size(); i ++) {
				if(i > 1) q.append(", ");
				q.append('?');
			}
			q.append(") ORDER BY ").append( COLUMN_URI ).append(" ASC ");
			
			if(log.isDebugEnabled()) {
				log.debug("Get objects URI [{}], query: {}", uris.size(), q.length() > 1000 ? q.subSequence(0,  997) + "..." : q);
			}
			
			
			stmt = connection.prepareStatement(q.toString());
			
			for(int i = 1; i <= uris.size(); i++) {
				stmt.setString(i, uris.get(i-1));
			}
			
			long start = System.currentTimeMillis();
			
			rs = stmt.executeQuery();
			
			if(queryStats != null) {
				long time = queryStats.addObjectsBatchGetTimeFrom(start);
				if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime("GraphObjects batch get " + uris.size(), stmt.toString(), time));
			}
			
			if(log.isDebugEnabled()) {
				log.debug("GET objects URI [{}], time: {}", uris.size(), System.currentTimeMillis() - start);
			}
			
			objects = fromSql(segment, rs, null, null, resolver);
			
			//sort objects
			Collections.sort(objects, new URIsListComparator(uris));
			
			return objects;
			
		} finally {
			SQLUtils.closeQuietly(stmt, rs);
		}
		
	}
	
	/**
	 * Delete a graph object from given segment table
	 * @param connection
	 * @param systemSegmentTable
	 * @param string
	 * @return <code> true if removed, <code>false</code> otherwise
	 * @throws SQLException 
	 */
	public static boolean deleteGraphObject(Connection connection,
			SegmentTable segment, String uri) throws SQLException {

		PreparedStatement stmt = null;
		
		try {
			
			String tableName = escapeID(connection, segment.tableName);
			
			stmt = connection.prepareStatement("DELETE FROM " + tableName + " WHERE " + COLUMN_URI + " = ? ");
			stmt.setString(1, uri);
			
			if(log.isDebugEnabled()) {
				log.debug("DELETE graph object statement: {}", stmt);
			}

			return stmt.executeUpdate() > 0;
				
		} finally {
			SQLUtils.closeQuietly(stmt);
		}

	}

	public static void insertGraphObject(VitalSqlDataSource dataSource, Connection connection,
			SegmentTable segment, GraphObject g) throws SQLException {
		insertGraphObjects(dataSource, connection, segment, Arrays.asList(g));
	}
	
	public static void insertGraphObjects(VitalSqlDataSource dataSource, Connection connection,
			SegmentTable segment, Collection<GraphObject> gs) throws SQLException {

		boolean initialAutoCommit = connection.getAutoCommit();
		
		if(dataSource.isSparkSQL()) {
			//overridden
			initialAutoCommit = false;
			
		}
		
		long start = System.currentTimeMillis();
		log.debug("Inserting {} objects", gs.size());
		
		boolean transactionStarted = false;
		
		try {
			
			
			if(initialAutoCommit) {
				connection.setAutoCommit(false);
				transactionStarted = true;
			}
			
			if(dataSource.isSparkSQL()) {
				
				// SparkSQLCustomImplementation.insertGraphObjects(dataSource, connection, segment, gs);
				
				
			} else {
				
				VitalSignsToSqlBridge.batchInsertGraphObjects(dataSource, connection, segment, gs);//toSql(connection, segment, g);
				
			}
			
//
			if(initialAutoCommit) {
				connection.commit();
			}
			
		} catch(SQLException e) {
			log.error(e.getLocalizedMessage(), e);
			if(transactionStarted) connection.rollback();
			throw e;
		} finally {
			
			if(transactionStarted) {
				connection.setAutoCommit(initialAutoCommit);
			}
			if(log.isDebugEnabled()) {
				log.debug("Insert {} objects time: {}ms", gs.size(), System.currentTimeMillis() - start);
			}
			
		}
		
//			for(PreparedStatement stmt : statements) {
//			
//				stmt.executeUpdate();
//				
//			}
//			
//		} finally {
//			
//			SQLUtils.closeQuietly(statements);
//			
//		}
		
	}

	public static int clearSegmentTable(Connection connection,
			SegmentTable segmentTable) throws SQLException {

		PreparedStatement stmt = null;
		
		try {
			stmt = connection.prepareStatement("DELETE FROM " + SQLUtils.escapeID(connection, segmentTable.tableName));
			return stmt.executeUpdate();
		} finally {
			
		}
		
	}
	
	public static int truncateTable(Connection connection, SegmentTable segmentTable) throws SQLException {
		
		PreparedStatement stmt = null;
		
		try {
			stmt = connection.prepareStatement("TRUNCATE TABLE " + SQLUtils.escapeID(connection, segmentTable.tableName));
			return stmt.executeUpdate();
		} finally {
			
		}
		
	}

	public static Set<String> containsURIs(Connection connection,
			SegmentTable segmentTable, Set<String> urisSet) throws SQLException {

		Set<String> output = new HashSet<String>();
		
		StringBuilder sb = new StringBuilder("SELECT DISTINCT ").append(COLUMN_URI).append(" FROM ").append(escapeID(connection, segmentTable.tableName));
		sb.append(" WHERE ").append(COLUMN_URI).append(" IN (");		
		
		for(int i = 1; i <= urisSet.size(); i++) {
			if(i > 1) {
				sb.append(", ?");
			} else {
				sb.append("?");
			}
		}
		
		sb.append(')');
		
		PreparedStatement stmt = null;
		
		ResultSet rs = null;
		
		long start = System.currentTimeMillis();
		
		log.debug("Segment contains URIs check, set size: {}", urisSet.size());
		
		try {
			
			stmt = connection.prepareStatement(sb.toString());
			
			int i = 1;
			
			for(String u : urisSet) {
				
				stmt.setString(i, u);
				
				i++;
				
			}
		
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				
				output.add(rs.getString(1));
				
			}
			
		} finally {
			
			log.debug("Segment constains URIs check time: {}ms" , System.currentTimeMillis() - start);
			
			SQLUtils.closeQuietly(stmt, rs);
		}
		
		
		return output;
	}

	public static int getSegmentSize(Connection connection,
			SegmentTable segmentTable) throws SQLException {
		return getSegmentSize(connection, segmentTable, null);
	}
	
	public static int getSegmentSize(Connection connection,
			SegmentTable segmentTable, QueryStats stats) throws SQLException {

		PreparedStatement stmt = null; 
		ResultSet rs = null;
		
		try {
			
			stmt = connection.prepareStatement("SELECT COUNT( DISTINCT " + escapeID(connection, COLUMN_URI)+ ") FROM " + escapeID(connection, segmentTable.tableName));
			
			long start = System.currentTimeMillis();
			
			rs = stmt.executeQuery();

			rs.next();
			
			if(stats != null) {
				long time = stats.addDatabaseTimeFrom(start);
				if(stats.getQueriesTimes() != null) stats.getQueriesTimes().add(new QueryTime("getSegmentSize", stmt.toString(), time));
			}
			
			return rs.getInt(1);
			
		} finally {
			SQLUtils.closeQuietly(stmt, rs);
		}
		
	}

	public static void ping(VitalSqlDataSource dataSource, Connection connection) throws SQLException {

		PreparedStatement stmt = null;
		
		ResultSet rs = null;
				
		
		try {
			
			stmt = connection.prepareStatement(dataSource.getDialect().getShowTables());
			
			//list all tables
			for( rs = stmt.executeQuery(); rs.next(); )  {
				rs.getString(1);
			}
			
		} finally {
			SQLUtils.closeQuietly(stmt, rs);
		}
		
	}

	//if necessary this can be optimized and use 
	public static boolean containsURI(Connection connection,
			SegmentTable segmentTable, String uri) throws SQLException {

		Set<String> s = new HashSet<String>();
		s.add(uri);
		
		Set<String> res = containsURIs(connection, segmentTable, s);
		
		return res.contains(uri);
		
	}


	public static void deleteBatch(VitalSqlDataSource dataSource, Connection connection,
			SegmentTable segmentTable, Set<String> urisSet) throws SQLException {

		if(dataSource.isSparkSQL()) {
			// SparkSQLCustomImplementation.deleteGraphObjects(dataSource, connection, segmentTable, urisSet);
			return;
		}
		
		long start = System.currentTimeMillis();
		
		log.debug("Delete batch, table: {}, uris count: {}", segmentTable.tableName, urisSet.size());
		
		PreparedStatement stmt = null;
		
		StringBuilder sb = new StringBuilder("DELETE FROM ").append(escapeID(connection, segmentTable.tableName)).append(" WHERE ");
		sb.append(COLUMN_URI).append(" IN (");

		for(int i = 1; i <= urisSet.size(); i++) {
			if(i > 1) {
				sb.append(", ?");
			} else {
				sb.append('?');
			}
		}
		
		sb.append(")");
		try {
			
			stmt = connection.prepareStatement(sb.toString());
			
			int i = 1;
			for(String u : urisSet) {
				stmt.setString(i, u);
				i++;
			}
			
			stmt.executeUpdate();
			
			
		} finally {
			log.debug("Delete batch, table: {}, uris count: {}, time: {}ms", segmentTable.tableName, urisSet.size(), System.currentTimeMillis() - start);
			SQLUtils.closeQuietly(stmt);
		}
		
	}
	
	public static void batchSaveObjects(VitalSqlDataSource dataSource, Connection connection,
			SegmentTable segmentTable, List<GraphObject> asList) throws SQLException {

		log.debug("Batch save {} objects (delete+insert) ...", asList.size()); 
		long start = System.currentTimeMillis();

		Set<String> urisSet = new HashSet<String>(asList.size());
		
		for(GraphObject g : asList) {
			urisSet.add(g.getURI());
		}
		
		deleteBatch(dataSource, connection, segmentTable, urisSet);

		insertGraphObjects(dataSource, connection, segmentTable, asList);
		
		log.debug("Batch save (delete+insert) time: {}ms", System.currentTimeMillis() - start);
		
	}

	public static void scanSegment(VitalSqlDataSource dataSource, Connection connection,
			SegmentTable segmentTable, final int limit, final ScanHandler scanHandler) throws SQLException {

		Statement stmt = null;

		ResultSet rs = null;
		
		try {
			
			String tname = segmentTable.tableName;
			
			String order = null;
			
			if(dataSource.isSparkSQL()) {
				order = "";
			} else {
				order = " ORDER BY " + SQLUtils.escapeID(connection, VitalSignsToSqlBridge.COLUMN_URI) + " ASC";
			}
			
//			stmt = connection.prepareStatement("SELECT * FROM " + SQLUtils.escapeID(connection, tname) + order);
			stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			
			rs = stmt.executeQuery("SELECT * FROM " + SQLUtils.escapeID(connection, tname) + order);
			
			final List<GraphObject> buffer = new ArrayList<GraphObject>();
			
			VitalSignsToSqlBridge.fromSql(segmentTable, rs, new GraphObjectsStreamHandler() {
				
				@Override
				public void onNoMoreObjects() {
					
					if(buffer.size() > 0) {
						scanHandler.onResultsPage(buffer);
					}
					
					scanHandler.onComplete();
				}
				
				@Override
				public void onGraphObject(GraphObject g) {
					
					buffer.add(g);
					
					if(buffer.size() >= limit) {
						
						scanHandler.onResultsPage(buffer);
						buffer.clear();
						
					}
					
				}
				
			});
				
			
		} finally {
			SQLUtils.closeQuietly(stmt, rs);
		}
		
	}

	public static Map<String, URIResultElement> getDataAttributes(Connection connection, SegmentTable segmentTable,
			List<String> urisList, List<String> propsToGet, QueryStats queryStats) throws SQLException {

		//split matching
		String tableName = SQLUtils.escapeID(connection, segmentTable.tableName);

		Map<String, URIResultElement> m = new HashMap<String, URIResultElement>();
		
		for(String u : urisList) {
			
			URIResultElement el = new URIResultElement();
			el.segment = segmentTable;
			el.URI = u;
			
			m.put(u, el);
			
		}
		
		if(urisList.size() == 0 || propsToGet.size() == 0) return m;
		
		int pageSize = 1000;
		
		int x = 0;
		
		for(int i = 0; i < urisList.size(); i += pageSize) {
			
			x++;
			
			List<String> page = urisList.subList(i, Math.min(i + pageSize, urisList.size()));
			
			PreparedStatement stmt = null;
			
			ResultSet rs = null;
			
			try {
				
				StringBuilder sb = new StringBuilder("SELECT * FROM " + tableName + " WHERE " + VitalSignsToSqlBridge.COLUMN_NAME);
				
				if(propsToGet.size() == 1) {
					
					sb.append(" = ? ");
					
				} else {
					
					sb.append(" IN ( ");
					
					for( int j = 0 ; j < propsToGet.size(); j ++ ) {
						
						if(j > 0) {
							sb.append(", ");
						}
						
						sb.append('?');
						
					}
					sb.append(" ) ");
					
				}
				
				
				sb.append(" AND " + COLUMN_URI);
				
				if(page.size() == 1 ) {
					
					sb.append(" = ? ");
					
				} else {

					sb.append(" IN ( ");
					
					for( int j = 0; j < page.size(); j++ ) {
						
						if(j > 0) {
							sb.append(", ");
						}
						
						sb.append('?');
						
					}
					
					sb.append(" ) ");
					
				}
				
				stmt = connection.prepareStatement(sb.toString());
				
				int z = 1;
				
				for(String p : propsToGet) {
				
					stmt.setString(z++, p);
					
				}
				
				for(String u : page) {
					
					stmt.setString(z++, u);
					
				}
				
				long start = System.currentTimeMillis();
				
				rs = stmt.executeQuery();
				
				if(queryStats != null) {
					long time = queryStats.addAttrDataGetTimeFrom(start);
					if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime("batch attr get, page: " + x, stmt.toString(), time));
				}
				
				ResultSetMetaData meta = rs.getMetaData();
				int columnCount = meta.getColumnCount();

				int uriCol = -1;
				int idCol = -1;
				int nameCol = -1;
				int extCol = -1;
				int vitaltypeCol = -1;
				int tstampCol = -1;
				int channelURICol = -1;
				for(int c = 1 ; c <= columnCount; c++) {
					String columnLabel = meta.getColumnLabel(c);
					if(COLUMN_URI.equals(columnLabel)) {
						uriCol = c;
					} else if(COLUMN_NAME.equals(columnLabel)) {
						nameCol = c;
					} else if(COLUMN_ID.equals(columnLabel)) {
						idCol = c;
					} else if(COLUMN_EXTERNAL.equals(columnLabel)) {
						extCol = c;
					} else if(COLUMN_VITALTYPE.equals(columnLabel)) {
						vitaltypeCol = c;
					} else if(COLUMN_TSTAMP.equals(columnLabel)) {
						tstampCol = c;
					} else if(COLUMN_CHANNEL_URI.equals(columnLabel)) {
						channelURICol = c;
					}
				}
				
				while(rs.next()) {
				
					String uri = rs.getString(uriCol);
					String name = rs.getString(nameCol);
					
					URIResultElement el = m.get(uri);
					
				    for (int column = 1; column <= columnCount; column++) {
				    	if(column == uriCol || column == idCol || column == nameCol || column == extCol || column == vitaltypeCol || column == tstampCol || column == channelURICol) {
				    		
				    	} else {
				    		
				    		Object v = rs.getObject(column);
				    		
				    		if(v != null) {
				    			
				    			if( el.attributes == null ) el.attributes = new HashMap<String, Object>();
				    			el.attributes.put(name, v);
				    			
				    		}
				    		
				    	}
				    }
					
				}
				
			} finally {
				
				SQLUtils.closeQuietly(stmt, rs);
				
			}
			
		}
		
		
		return m;
		
	}

	public static Set<String> getAllURIs(Connection connection, SegmentTable segmentTable, QueryStats queryStats) throws SQLException {

		PreparedStatement stmt = null;
		
		ResultSet rs = null;
		
		String tableName = SQLUtils.escapeID(connection, segmentTable.tableName); 
		
		Set<String> l = new LinkedHashSet<String>();
		
		try {
			
			String query = "SELECT DISTINCT " + COLUMN_URI + " FROM " + tableName;
			
			stmt = connection.prepareStatement(query);
			
			long start = System.currentTimeMillis(); 
			
			rs = stmt.executeQuery();
			
			if(queryStats != null) {
				long time = queryStats.addDatabaseTimeFrom(start);
				if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime("getAllSegmentURIs query", stmt.toString(), time));
			}
			
			while(rs.next()) {

				l.add(rs.getString(1));
				
			}
			
		} finally {
			
			SQLUtils.closeQuietly(stmt, rs);
			
		}
		
		return l;
		
	}


	public static Map<String, Map<String, String>> resolveProperties(Connection connection,
			Map<SegmentTable, Map<String, Set<String>>> segmentTable2uri2PropsMap, QueryStats queryStats) throws SQLException {

		int pageSize = 1000;
		
		Map<String, Map<String, String>> output = new HashMap<String, Map<String, String>>();
		
		for(Entry<SegmentTable, Map<String, Set<String>>> entry : segmentTable2uri2PropsMap.entrySet() ) {
			
			SegmentTable table = entry.getKey();
			
			String tableName = SQLUtils.escapeID(connection, table.tableName);
			
			Map<String, Set<String>> value = entry.getValue();
			
			//get thousand at a time
			List<Entry<String, Set<String>>> l = new ArrayList<Entry<String, Set<String>>>(value.entrySet());
			
			for(int i = 0 ; i < l.size(); i += pageSize) {
				
				List<Entry<String, Set<String>>> subList = l.subList(i, Math.min(i + pageSize, l.size()));

				StringBuilder sb = new StringBuilder("SELECT " + COLUMN_URI + ", " + COLUMN_NAME + ", " + COLUMN_VALUE_FULL_TEXT);
				sb.append(" FROM ").append(tableName).append(" WHERE ").append(COLUMN_URI).append(l.size() > 1 ? " IN ( " : " = ");
				
				
				for(int j = 0 ; j < subList.size(); j++) {

					
					if(j == 0) {
						sb.append("?");
 					} else {
 						sb.append(", ?");
 					}
					
				}
				
				if(subList.size() > 1) sb.append(" ) ");
				
				sb.append(" AND " + COLUMN_VALUE_FULL_TEXT + " IS NOT NULL");
			

				
				PreparedStatement stmt = null;
				ResultSet rs = null;
				
				try {
					
					stmt = connection.prepareStatement(sb.toString());
					
					for(int j = 1; j <= subList.size(); j++) {
						stmt.setString(j, subList.get(j-1).getKey());
					}
					
					long start = System.currentTimeMillis();
					
					rs = stmt.executeQuery();
					
					if(queryStats != null) {
						long time = queryStats.addObjectsResolvingTimeFrom(start);
						if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime("long properties resolving", stmt.toString(), time));
					}
				
					
					while(rs.next()) {
						
						String uri = rs.getString(1);
						String pname = rs.getString(2);
						String fullText = rs.getString(3);
						
						Map<String, String> map = output.get(uri);
						if(map == null) {
							map = new HashMap<String, String>();
							output.put(uri, map);
						}
						
						map.put(pname, fullText);
						
					}
					
				} finally {
					SQLUtils.closeQuietly(stmt, rs);
				}
				
			}
			
		}
		
		return output;
	}

}
