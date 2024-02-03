package ai.vital.sql.connector;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.sql.config.VitalSqlConfig;
import ai.vital.sql.config.VitalSqlConfig.SqlDBType;
import ai.vital.sql.model.SegmentTable;
import ai.vital.sql.model.VitalSignsToSqlBridge;
import ai.vital.sql.schemas.SqlDialect;
import ai.vital.sql.schemas.amazonredshift.AmazonRedshiftSqlDialect;
import ai.vital.sql.schemas.mysql.MySQLDialect;
import ai.vital.sql.schemas.postgresql.PostgreSQLDialect;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;

//have a another pool of connections for transactions?
public class VitalSqlDataSource extends BasicDataSource {

	private VitalSqlConfig config;

	private SqlDialect dialect;
	
	private static final Map<SqlDBType, String> driversMap = new HashMap<SqlDBType, String>();
	static {
		driversMap.put(SqlDBType.MySQL, "com.mysql.jdbc.Driver");
		driversMap.put(SqlDBType.MySQLAurora, "com.mysql.jdbc.Driver");
		driversMap.put(SqlDBType.MySQLMemSQL, "com.mysql.jdbc.Driver");
		driversMap.put(SqlDBType.MariaDB, "com.mysql.jdbc.Driver");
		driversMap.put(SqlDBType.AmazonRedshift, "com.amazon.redshift.jdbc41.Driver");
		driversMap.put(SqlDBType.PostgreSQL, "org.postgresql.Driver");

	}
	
	private static final Map<SqlDBType, Class<? extends SqlDialect>> dialectsMap = new HashMap<SqlDBType, Class<? extends SqlDialect>>();
	static {
		dialectsMap.put(SqlDBType.MySQL, MySQLDialect.class);
		dialectsMap.put(SqlDBType.MySQLAurora, MySQLDialect.class);
		dialectsMap.put(SqlDBType.MySQLMemSQL, MySQLDialect.class);
		dialectsMap.put(SqlDBType.MariaDB, MySQLDialect.class);
		dialectsMap.put(SqlDBType.AmazonRedshift, AmazonRedshiftSqlDialect.class);
		dialectsMap.put(SqlDBType.PostgreSQL, PostgreSQLDialect.class);

		
	}
	
	private final static Logger log = LoggerFactory.getLogger(VitalSqlDataSource.class);
	
	private SegmentTable systemSegmentTable = null;

	private Driver hiveDriver;
	
	@Override
	public Connection getConnection() throws SQLException {
		return getConnection(null);
	}

	private String getDBNameFromConnectionString(String endpointURL) {
		
		String u = endpointURL;
		
		if(u.indexOf('?') >= 0) {
			u = u.substring(0, u.indexOf('?'));
		}
		
		if(u.endsWith("/")) u = u.substring(0, u.length() - 1);
		
		int lastSlash = u.lastIndexOf('/');
		if(lastSlash >= 0) {
			u = u.substring(lastSlash + 1); 
		}
		
		if(u.isEmpty()) throw new RuntimeException("Couldn't determine database from URL: " + endpointURL );
		
		//database is taken from the URI part
		String dbName = u;
		if(dbName.startsWith("/")) dbName = dbName.substring(1);
		
		return dbName;
		
	}
	
	@SuppressWarnings("unchecked")
	private synchronized void initHiveDriver() {

		if(hiveDriver == null) {
			
			try {
				Class<? extends Driver> driverClass = (Class<? extends Driver>) Class.forName("org.apache.hive.jdbc.HiveDriver");
				hiveDriver = driverClass.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
		}
		
		
	}
	
	public Connection getConnection(QueryStats queryStats) throws SQLException {


		
		if(log.isDebugEnabled()) {
			log.debug("Obtaining connection, idle: " + getNumIdle() + " active: " + getNumActive() + " max open statements" + this.getMaxOpenPreparedStatements());
		}
		long start = System.currentTimeMillis();
		Connection c = super.getConnection();
		if(queryStats != null ) {
			long diff = queryStats.addDatabaseTimeFrom(start);
			if(queryStats.getQueriesTimes() != null) {
				queryStats.getQueriesTimes().add(new QueryTime("getConnection", "getConnection", diff));
			}
		}
		if(log.isDebugEnabled()) {
			log.debug("Connection obtained, isolation " + c.getTransactionIsolation() + " auto commit: " + c.getAutoCommit());
		}
		return c;
		
	}
	
	public VitalSqlDataSource(VitalSqlConfig config) {
		super();
		this.config = config;
		
		SqlDBType dbType = config.getDbType();
		if(dbType == null) throw new NullPointerException("No dbType");
		String driverClassName = driversMap.get(dbType);
		if(driverClassName == null) throw new RuntimeException("Unsupported dbType: " + dbType.name());
		
		Class<? extends SqlDialect> dialectClass = dialectsMap.get(dbType);
		if(dialectClass == null) throw new RuntimeException("No dialect for " + dbType.name());
		try {
			dialect = dialectClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		this.setDriverClassName(driverClassName);
		
		String endpointURL = config.getEndpointURL();
		if(endpointURL == null) throw new NullPointerException("No endpointURL");
        this.setUrl(endpointURL);
//
        String username = config.getUsername();
        if(username == null) throw new NullPointerException("No username");
        this.setUsername(username);

        String password = config.getPassword();
        if(password == null) throw new NullPointerException("No password");
        this.setPassword(password);;

                
        Integer poolInitialSize = config.getPoolInitialSize();
        if(poolInitialSize == null) throw new NullPointerException("No poolInitialSize");
        this.setInitialSize(poolInitialSize);

        Integer poolMaxTotal = config.getPoolMaxTotal();
        if(poolMaxTotal == null) throw new NullPointerException("No poolMaxTotal");
        this.setMaxTotal(poolMaxTotal);
        
		setDefaultAutoCommit(true);
		log.debug("Default transaction isolation: " + getDefaultTransactionIsolation());
		

		setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

		
		
//		setTestOnBorrow(true);
		
//        this.setMaxOpenPreparedStatements(180);
		
	}
	
	public VitalSqlConfig getConfig() {
		return config;
	}
	

	public SegmentTable getSystemSegmentTable() {
		return systemSegmentTable;
	}

	public SqlDialect getDialect() {
		return dialect;
	}

	public boolean singleStatementAtATime() {
		return false;
	}

	public boolean isSparkSQL() {
		return false;
	}
	
	public String getInsertCommandTemplate(Connection connection, String tableName) throws SQLException {

		String template = initInsertCommandTemplate(connection);
		
		return template.replace("${TABLENAME}", SQLUtils.escapeID(connection, tableName));
		
	}
		
	String insertCommandTemplate = null;
		
	private String initInsertCommandTemplate(Connection connection) throws SQLException {
		
		if(insertCommandTemplate != null) return insertCommandTemplate;

		List<String> columns = VitalSignsToSqlBridge.columns;
		
		StringBuilder command = new StringBuilder("INSERT INTO ")
		.append("${TABLENAME}").append(" ( ");
		
		for(int i = 1; i <= columns.size(); i++) {
			if(i > 1) {
				command.append(", ");
			}
			command.append(SQLUtils.escapeID(connection, columns.get(i-1)));
		}
		
		command.append(" ) VALUES ( ");
		
		for(int i = 1; i <= columns.size() ; i ++) {
			if(i > 1) {
				command.append(", ?");
			} else {
				command.append("?");
			}
		}
		
		command.append(")");
		
		insertCommandTemplate = command.toString();

		return insertCommandTemplate;
	}

}
