package ai.vital.sql.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SQLUtils {

	public static String escapeID(Connection connection, String id) throws SQLException {
		
		String q = connection.getMetaData().getIdentifierQuoteString();
		
		if(q.equals(" ")) {
			q = "`";
		}
		
		return q + id + q;
	}
	
	public static void closeQuietly(ResultSet rs) {
		if(rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {}
		}
	}
	
	public static void closeQuietly(Connection c) {
		if(c != null) {
			try {
				c.close();
			} catch (SQLException e) {}
		}
	}
	
	public static void closeQuietly(Statement s) {
		if(s != null) {
			try {
				s.close();
			} catch(SQLException e) {}
		}
	}
	
	public static void closeQuietly(Connection c, ResultSet rs) {
		closeQuietly(rs);
		closeQuietly(c);
	}
	
	public static void closeQuietly(Statement s, ResultSet rs) {
		closeQuietly(rs);
		closeQuietly(s);
	}

	public static void closeQuietly(List<PreparedStatement> statements) {

		if(statements == null) return;
		for(PreparedStatement stmt : statements) {
			closeQuietly(stmt);
		}
		
	}

}
