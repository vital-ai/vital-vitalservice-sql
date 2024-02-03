package ai.vital.sql.schemas;

import static ai.vital.sql.utils.SQLUtils.closeQuietly;
import static ai.vital.sql.utils.SQLUtils.escapeID;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.model.SegmentTable;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;
import ai.vital.vitalsigns.model.VitalSegment;

public class SchemasUtils {

	private final static Logger log = LoggerFactory.getLogger(SchemasUtils.class);
	
	private static String getSegmentSchema(VitalSqlDataSource dataSource, SegmentTable segmentTable) throws IOException {
		
		InputStream is = null;
		try {
			is = SchemasUtils.class.getResourceAsStream(dataSource.getConfig().getDbType().name().toLowerCase() + "/Schema.sql");
			String s = IOUtils.toString(is);
			return segmentNameFilter(dataSource, segmentTable,  s);
		} finally {
			IOUtils.closeQuietly(is);
		}
	}
	
	private static String segmentNameFilter(VitalSqlDataSource dataSource, SegmentTable segmentTable, String s) {
		return s.replace("${NAME}", segmentTable.tableName);
	}

	public static boolean deleteSegmentTables(VitalSqlDataSource dataSource) throws SQLException {
		
		Pattern segmentTablePattern = Pattern.compile(dataSource.getConfig().getTablesPrefix() + ".+");
		
		Connection connection = null;
		
		ResultSet rs = null;
		
		PreparedStatement stmt1 = null;
		
		PreparedStatement stmt = null;
		
		boolean r = false;
		
		try {
			connection = dataSource.getConnection();
		
			
			String showTablesCommand = dataSource.getDialect().getShowTables();
			
			stmt1 = connection.prepareStatement(showTablesCommand);
			
			for( rs = stmt1.executeQuery(); rs.next(); )  {
				
				String tableName = rs.getString(1);

				boolean drop = false;
				
				if(segmentTablePattern.matcher(tableName).matches()) {
					if(tableName.endsWith("__")) {
						log.warn("Index table won't be dropped manually: " + tableName);
					} else {
						log.warn("Dropping segment table: " + tableName);
						drop = true;
					}
				}
				
				if(drop) {
					r = true;
					log.warn("Dropping segment table: " + tableName);;
					stmt = connection.prepareStatement("DROP TABLE " + escapeID(connection, tableName) );
					stmt.executeUpdate();
					stmt.close();
				}
				
			}

		} finally {
			closeQuietly(stmt1);
			closeQuietly(stmt);
			closeQuietly(connection, rs);
		}
		
		return r;
		
	}
	
	public static SegmentTable getSegmentTable(VitalSqlDataSource dataSource, VitalSegment segment) {
		
//		String tableName = dataSource.getConfig().getTablesPrefix() + BASE32.encodeToString(segment.getURI().getBytes(StandardCharsets.UTF_8)).toLowerCase();
		
		
		
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		
		byte[] digest = md5.digest(segment.getURI().getBytes(StandardCharsets.UTF_8));

		String t = dataSource.getConfig().getTablesPrefix() + bytesToHex(digest);
		
		return new SegmentTable(segment, t);
	}

	public static void deleteSegmentTable(VitalSqlDataSource dataSource, Connection connection, VitalSegment segment) throws SQLException {

		PreparedStatement stmt = null;
		
		try {
			
			SegmentTable segmentTable = getSegmentTable(dataSource, segment);
			
			stmt = connection.prepareStatement("DROP TABLE " + escapeID(connection, segmentTable.tableName));
			stmt.executeUpdate();
			
		} finally {
			
			closeQuietly(stmt);
			
		}
		
	}

	public static boolean createSegmentTable(VitalSqlDataSource dataSource,
			Connection connection, VitalSegment added) throws Exception {

		ResultSet rs = null;
		
		boolean tableFound = false;
		
		SegmentTable segmentTable = getSegmentTable(dataSource, added);
		
		PreparedStatement stmt = null;
		
		try {
			
			String showTablesCommand = dataSource.getDialect().getShowTables();
			
			stmt = connection.prepareStatement(showTablesCommand);
			
			for( rs = stmt.executeQuery(); rs.next(); )  {
				
				String tableName = rs.getString(1);
				
				if(segmentTable.tableName.equals(tableName)) {
					tableFound = true;
				}
				
			}
			
			rs.close();
			stmt.close();
			
			if(tableFound) { 
				
				log.debug("Segment table already exists: " + segmentTable.tableName);

				return false;
				
			} else {
				
				String schema = getSegmentSchema(dataSource, segmentTable);
				
				log.debug("Creating system table: {}",  schema);
				
				if( dataSource.singleStatementAtATime() ) {
					
					String[] split = schema.split(";");
					
					
					for(String s : split) {
					
						s = s.trim();
						if(s.isEmpty()) continue;
						
						log.debug("Single statement at a time: {}", s);
						
						stmt = connection.prepareStatement(s);
						
						stmt.executeUpdate();
						
						stmt.close();
						
					}
					
					
				} else {
					
					stmt = connection.prepareStatement(schema);
					
					stmt.executeUpdate();
					
					stmt.close();
					
				}
				
				
				return true;
				
			}
				
			
		} finally {
			closeQuietly(stmt, rs);
		}
		
	}

	public static List<String> listAllTables(Connection connection,
			VitalSqlDataSource dataSource) throws SQLException {
		return listAllTables(connection, dataSource, null);
	}
	
	public static List<String> listAllTables(Connection connection,
			VitalSqlDataSource dataSource, QueryStats queryStats) throws SQLException {
		
		String showTablesCommand = dataSource.getDialect().getShowTables();
		
		PreparedStatement stmt = null;
		
		ResultSet rs = null;
		
		List<String> res = new ArrayList<String>();
		
		String pref = dataSource.getConfig().getTablesPrefix();
		
		try {
			
			stmt = connection.prepareStatement(showTablesCommand);
			
			long start = System.currentTimeMillis();
			rs = stmt.executeQuery();
			
			if(queryStats != null) {
				
				long diff = queryStats.addDatabaseTimeFrom(start);
				if(queryStats.getQueriesTimes() != null) {
					queryStats.getQueriesTimes().add(new QueryTime("list all segment tables", stmt.toString(), diff));
				}
				
			}
			
			while(rs.next()) {
				String s = rs.getString(1);
				if(s.startsWith(pref)) {
					res.add(s);
				}
			}
			
		} finally {
			closeQuietly(stmt, rs);
		}
		
		return res;
		
	}
	
	/*
	public static List<String> listSegments(Connection connection,
			VitalSqlDataSource dataSource) throws SQLException {

		
		String showTablesCommand = dataSource.getDialect().getShowTables();
		
		PreparedStatement stmt = null;
		
		ResultSet rs = null;
		
		List<String> segments = new ArrayList<String>();
		
		String pref = dataSource.getConfig().getTablesPrefix();
		
		try {
			stmt = connection.prepareStatement(showTablesCommand);
			
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				String s = rs.getString(1);
				if(s.startsWith(pref)) {
					String x = s.substring(pref.length());
					x = x.toUpperCase();
					x = new String( BASE32.decode(x), StandardCharsets.UTF_8 );
					segments.add(x);
				}
			}
			
		} finally {
			closeQuietly(stmt, rs);
		}
		
		return segments;
		
	}
	*/

	public static boolean tableExists(Connection connection, VitalSqlDataSource dataSource, String tname) throws SQLException {

		PreparedStatement stmt = null; 
		
		ResultSet rs = null;
		
		long start = System.currentTimeMillis();
		
		try {
			
			
			String showTablesCommand = dataSource.getDialect().getShowTables();
			log.debug("Checking segment table: {}", tname);
			
			stmt = connection.prepareStatement(showTablesCommand);
			
			for( rs = stmt.executeQuery(); rs.next(); )  {
				
				String tableName = rs.getString(1);
				
				if(tableName.equals(tname)) {
					return true;
				}
				
			}
			
		} finally {
			
			SQLUtils.closeQuietly(stmt, rs);
			log.debug("Segment table check time: {}ms", System.currentTimeMillis() - start);
			
		}
		
		return false;
	}
	
	

	final protected static char[] hexArray = "0123456789abcdef".toCharArray();
	public static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
}
