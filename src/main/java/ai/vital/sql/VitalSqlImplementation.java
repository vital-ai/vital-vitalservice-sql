package ai.vital.sql;

import static ai.vital.sql.utils.SQLUtils.closeQuietly;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;

import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.dao.BulkOperationsImpl;
import ai.vital.sql.dao.CoreOperations;
import ai.vital.sql.model.SegmentTable;
import ai.vital.sql.query.SQLGraphObjectResolver;
import ai.vital.sql.query.SQLSingleSegmentQueryHandler;
import ai.vital.sql.query.SqlExportQueryImpl;
import ai.vital.sql.query.SqlResultsProvider;
import ai.vital.sql.schemas.SchemasUtils;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalservice.VitalServiceConstants;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.query.CollectStats;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;
import ai.vital.vitalsigns.model.properties.Property_hasTransactionID;
import ai.vital.vitalsigns.model.property.StringProperty;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.query.graph.GraphQueryImplementation;
import ai.vital.vitalsigns.query.graph.GraphQueryImplementation.ResultsProvider;
import ai.vital.vitalsigns.utils.StringUtils;

public class VitalSqlImplementation {

	VitalSqlDataSource dataSource;
	
	private static class ActiveTransaction {
		
		Connection connection;
		
		String transactionID;
		
		public ActiveTransaction(Connection connection2, String txID) {
			this.connection = connection2;
			this.transactionID = txID;
		}
		public void rollback() {
			try {
				connection.rollback();
				connection.setAutoCommit(true);
				connection.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			
		}
		public void commit() {
			try {
				connection.commit();
				connection.setAutoCommit(true);
				connection.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
//	private ActiveTransaction currentTransaction;
	
	private Map<String, ActiveTransaction> activeTransactions = new HashMap<String, ActiveTransaction>();
	
	private int maxActiveTransactions;
	
	public VitalSqlImplementation(VitalSqlDataSource dataSource) {
		this.dataSource = dataSource;
		this.maxActiveTransactions = (int) Math.floor( dataSource.getConfig().getPoolMaxTotal() * 4 / 5 );
	}

//	
//	//return the list of all segments
//	public List<String> getAllSegments() throws Exception {
//
//		Connection connection = null;
//		
//		try {
//			connection = dataSource.getConnection();
//			return SchemasUtils.listSegments(connection, dataSource);
//		} finally {
//			SQLUtils.closeQuietly(connection);
//		}
//		 
//		
//	}
	
	
	public boolean segmentExists(VitalSegment segment) throws Exception {
		
		SegmentTable table = SchemasUtils.getSegmentTable(dataSource, segment);
		
		Connection connection = null;
		
		try {
		
			connection = dataSource.getConnection();
			
			return SchemasUtils.tableExists(connection, dataSource, table.tableName);
			
		} finally {
			SQLUtils.closeQuietly(connection);
		}
		
	}
	
	
	public VitalSegment addSegment(VitalSegment dbSegment, boolean createIfNotExists) throws Exception {

		Connection connection = null;
		
		
		try {
			
			connection = dataSource.getConnection();
		
			SegmentTable segmentTable = SchemasUtils.getSegmentTable(dataSource, dbSegment);
			
			if(SchemasUtils.tableExists(connection, dataSource, segmentTable.tableName)) {
				
				throw new Exception("Segment table with URI: " + dbSegment.getURI() + " already exists, id: " + dbSegment.getRaw(Property_hasSegmentID.class));
				
			}

			SchemasUtils.createSegmentTable(dataSource, connection, dbSegment);
			
			return dbSegment;
			
		} finally {
			SQLUtils.closeQuietly(connection);
		}
	}
	
	public void deleteSegment(VitalSegment s, boolean deleteData) throws Exception {
		
		Connection connection = null;
		
		String segmentID = (String) s.getRaw(Property_hasSegmentID.class);
		
		try {
			
			connection = dataSource.getConnection();
			
			SegmentTable segmentTable = SchemasUtils.getSegmentTable(dataSource, s);
			if( ! SchemasUtils.tableExists(connection, dataSource, segmentTable.tableName)) {
				throw new Exception("Segment with URI: " + s.getURI() + ", id:" + segmentID + " not found");
			}
			
			if(deleteData) {
				
				SchemasUtils.deleteSegmentTable(dataSource, connection, s);
				
			}
			
			
		} finally {
			closeQuietly(connection);
		}
			
	}
	
	
	public VitalStatus bulkExport(VitalSegment segment, OutputStream outputStream, String datasetURI) throws Exception {
		
		
		Connection connection = null;
		try {
		
			connection = dataSource.getConnection();

			segment = checkSegment(connection, segment);
			
			return BulkOperationsImpl.bulkExport(dataSource, connection, SchemasUtils.getSegmentTable(dataSource, segment), outputStream, datasetURI);
			
		} finally {
			
			closeQuietly(connection);
			
		}
	}
	
	private VitalSegment checkSegment(Connection connection,
			VitalSegment segment) throws Exception {

		String segmentID = (String) segment.getRaw(Property_hasSegmentID.class);
		SegmentTable segmentTable = SchemasUtils.getSegmentTable(dataSource, segment);
		if( ! SchemasUtils.tableExists(connection, dataSource, segmentTable.tableName)) {
			throw new Exception("Segment not found, URI: " + segment.getURI() + " id: " + segmentID);
		}
		
		return segment;
	}


	public VitalStatus bulkImport(VitalSegment segment, InputStream inputStream, String datasetURI) throws Exception {
		
		check_noSparkSQL("bulkImport");
		
		Connection connection = null;
		try {
			
			connection = dataSource.getConnection();

			segment = checkSegment(connection, segment);
			
			return BulkOperationsImpl.bulkImport(dataSource, connection, SchemasUtils.getSegmentTable(dataSource, segment), inputStream, datasetURI);
			
		} finally {
			closeQuietly(connection);
		}
		
		
	}
	
	private void check_noSparkSQL(String m) {

		if(dataSource.isSparkSQL()) throw new RuntimeException("'" + m + "' method unavailable in sparkSQL endpoint");
		
	}

	public void close() throws SQLException {
		
		synchronized (activeTransactions) {
			
			for(ActiveTransaction at : activeTransactions.values() ) {
			
				try {
					at.rollback();
				} catch(Exception e) {}
				
			}
			
			activeTransactions.clear();
			
		}
		
//		if(currentTransaction != null) {
//			try {
//				currentTransaction.rollback();
//			} catch(Exception e) {}
//		}
		
		dataSource.close();
	}
	
	public void commitTransaction(String transactionID) {
		
		check_noSparkSQL("commitTransaction");
		
		ActiveTransaction currentTransaction = null;
		
		synchronized(activeTransactions) {
			currentTransaction = activeTransactions.get(transactionID);
		}
		
		if(currentTransaction == null) throw new RuntimeException("Transaction with ID: " + transactionID + " not found");
		
//		if(!currentTransaction.transactionID.equals(transactionID)) throw new RuntimeException("Current transaction is different than provided: " + currentTransaction.transactionID + " vs " + transactionID);

		currentTransaction.commit();
		
		synchronized(activeTransactions) {
			activeTransactions.remove(transactionID);
		}
		
	}
	
	public String createTransaction() {

		check_noSparkSQL("createTransaction");
		
		synchronized (activeTransactions) {
			if( activeTransactions.size() >= maxActiveTransactions ) {
				throw new TooManyActiveTransactionsException("The limit of active transactions has been exceeded", this.maxActiveTransactions);
			}
		}
		
		Connection connection = null;
		
		try {
			
			connection = dataSource.getConnection();
			
			connection.setAutoCommit(false);
			String txID = null;
			
			synchronized (activeTransactions) {
				
				while(txID == null) {
					txID = RandomStringUtils.randomAlphanumeric(32);
					if(activeTransactions.containsKey(txID)) {
						txID = null;
					}
				}

				ActiveTransaction currentTransaction = new ActiveTransaction(connection, txID);
				
				activeTransactions.put(txID, currentTransaction);

				return currentTransaction.transactionID;
				
			}
			
		} catch(Exception e) {
			closeQuietly(connection);
			throw new RuntimeException(e);
		}
		
	}
	
	public VitalStatus delete(VitalTransaction transaction, List<VitalSegment> segmentsPool, List<URIProperty> uris) throws Exception {
	
		check_sparkSQLSegmentsPool("delete", segmentsPool);
		
		Connection connection = null;
		
		try {
			
			connection = transactionAwareConnection(transaction);
			
			Set<String> urisSet = new HashSet<String>();
			
			for(URIProperty u : uris) {
				urisSet.add(u.get());
			}
			
			for(VitalSegment segment : segmentsPool) {
				
				CoreOperations.deleteBatch(dataSource, connection, SchemasUtils.getSegmentTable(dataSource, segment), urisSet);
				
			}
			
			return VitalStatus.withOK();
			
		} finally {
			closeIfNotTransaction(transaction, connection);
		}
		
	}
	
	private void check_sparkSQLSegmentsPool(String method, List<VitalSegment> segmentsPool) {

		if(!dataSource.isSparkSQL()) return;
		
		/*
		boolean valid = true;
		if(segmentsPool.size() != 1) {
			valid = false;
		}

		if(valid) {
			
			VitalSegment vitalSegment = segmentsPool.get(0);
			
			if(!SparkSQLCustomImplementation.SYSTEM_SEGMENT_URI.equals(vitalSegment.getURI())) {
				valid = false;
			}
			
		}
		
		if(!valid) throw new RuntimeException("'" + method + "' only available for system segment in sparkSQL mode");
		*/

	}

	public VitalStatus deleteAll(VitalSegment segment) throws Exception {
	
		check_noSparkSQL("deleteAll");
		
		Connection connection = null;
		
		try {
			
			connection = dataSource.getConnection();//transactionAwareConnection();
			
			segment = checkSegment(connection, segment);
			
			int updated = CoreOperations.clearSegmentTable(connection, SchemasUtils.getSegmentTable(dataSource, segment));
			
			VitalStatus status = VitalStatus.withOKMessage("All segment " + segment.getURI() + " id: " + segment.getRaw(Property_hasSegmentID.class) + " objects deleted: " + updated);
			status.setSuccesses(updated);
			return status;
			
		} finally {
			SQLUtils.closeQuietly(connection);
		}
		

		
	}
	
	
	public VitalStatus delete(VitalTransaction transaction, List<VitalSegment> segmentsPool, URIProperty uri) throws Exception {

		check_sparkSQLSegmentsPool("delete", segmentsPool);
		
		Connection connection = null;
		try {
			
			if(uri.get().startsWith(URIProperty.MATCH_ALL_PREFIX)) {
				
				throw new RuntimeException("Should be handled by the upper layer");
				
			}
			
			connection = transactionAwareConnection(transaction);
			
			
			List<VitalSegment> segments  = segmentsPool;
			
			//delete until first hit ?
			boolean deleted = false;
			
			VitalSegment target = null;
			
			for(VitalSegment segment : segments) {
				
				deleted = CoreOperations.deleteGraphObject(connection, SchemasUtils.getSegmentTable(dataSource, segment), uri.get());
				if(deleted) {
					target = segment;
					break;
				}
				
			}
			
			if(!deleted) return VitalStatus.withError("Object with URI: " + uri + " not found.");
			
			String segmentID = (String) target.getRaw(Property_hasSegmentID.class);
			
			return VitalStatus.withOKMessage("Object found and deleted from segment: " + segmentID);
			
		} finally {
			closeIfNotTransaction(transaction, connection);
		}
		
	}
	
	public GraphObject get(List<VitalSegment> segmentsPool, URIProperty uri) throws Exception {
		
		Connection connection = null;
		
		try {
			
			connection = dataSource.getConnection();
			
			GraphObject g = null;
			
			for(VitalSegment segment : segmentsPool) {
				
				g = CoreOperations.getGraphObject(connection, SchemasUtils.getSegmentTable(dataSource, segment), uri.get());
				
				if(g != null) {
					return g;
				}
				
			}
			
			return g;
			
		} finally {
			closeQuietly(connection);
		}
		
	}
	
	


	public List<GraphObject> getBatch(List<VitalSegment> segmentsPool, Collection<String> uris) throws Exception {
		
		Connection connection = null;
		
		try {
			
			connection = dataSource.getConnection();
			
			//clone it
			List<String> urisList = new ArrayList<String>(uris);
			
			List<GraphObject> rs = new ArrayList<GraphObject>(urisList.size());
			
			for(VitalSegment segment : segmentsPool) {
				
				List<GraphObject> objects = CoreOperations.getGraphObjectsBatch(connection, SchemasUtils.getSegmentTable(dataSource, segment), urisList, null, null);
				
				if(objects != null)  {
					rs.addAll(objects);
					for(GraphObject g : objects) {
						urisList.remove(g.getURI());
					}
				}
				
				//done
				if(urisList.size() < 1) break;
				
			}
			
			return rs;
			
		} finally {
			closeQuietly(connection);
		}
		
	}
	
	public VITAL_GraphContainerObject getExistingObjects(List<VitalSegment> segmentsPool, List<String> uris) throws Exception {

		Connection connection = null;
		
		try {
			
			connection = dataSource.getConnection();
			
			VITAL_GraphContainerObject c = new VITAL_GraphContainerObject();
			c.setURI("urn:x");
			
			//clone the list
			Set<String> urisSet = new HashSet<String>(uris);
			
			for( VitalSegment segment : segmentsPool ) {
				
				for(String u : CoreOperations.containsURIs(connection, SchemasUtils.getSegmentTable(dataSource, segment), urisSet) ) {
					
					urisSet.remove(u);
					
					String segmentID = (String) segment.getRaw(Property_hasSegmentID.class);
					
					c.setProperty(u, new StringProperty(segmentID));
					
				}
				
				if(urisSet.size() < 1) break;
				
			}
			
			return c;
			
		} finally {
			closeQuietly(connection);
		}
		
	}

	
	public int getSegmentSize(VitalSegment segment) throws Exception {
		
		Connection connection = null;
		
		try {
			
			connection = dataSource.getConnection();

			segment = checkSegment(connection, segment);
			
			return CoreOperations.getSegmentSize(connection, SchemasUtils.getSegmentTable(dataSource, segment));
			
		} finally {
			closeQuietly(connection);
		}
		
	}

	public void open() {
		//nop
	}
	
	public void ping() throws SQLException {
		
		Connection connection = null;
		
		try {
			connection = dataSource.getConnection();
			CoreOperations.ping(dataSource, connection);
		} finally {
			closeQuietly(connection);
		}
		
	}
	
	public void rollbackTransaction(String transactionID) {

		check_noSparkSQL("rollbackTransaction");
		
		ActiveTransaction currentTransaction = null;
		
		synchronized(activeTransactions) {
			currentTransaction = activeTransactions.get(transactionID);
		}
				
		
		if(currentTransaction == null) throw new RuntimeException("Transaction with ID: " + transactionID + " not found");
//		
//		if(!this.currentTransaction.transactionID.equals(transactionID)) throw new RuntimeException("Current transaction is different than provided: " + this.currentTransaction.transactionID + " vs " + transactionID);
//		
		currentTransaction.rollback();
		
		synchronized(activeTransactions) {
			activeTransactions.remove(transactionID);
		}
//		
//		this.currentTransaction = null;
		
	}
	

	/**
	 * updates or inserts an object
	 * @param transaction
	 * @param targetSegment
	 * @param graphObject
	 * @param segmentsPool
	 * @return
	 * @throws Exception
	 */
	public GraphObject save(VitalTransaction transaction, VitalSegment targetSegment, GraphObject graphObject, List<VitalSegment> segmentsPool) throws Exception {
		
//		check_sparkSQLSegmentsPool("save", segmentsPool);
		
		String segmentID = (String) targetSegment.getRaw(Property_hasSegmentID.class);
		
		Connection connection = null;
		
		try {
			
			connection = transactionAwareConnection(transaction);
			
			targetSegment = checkSegment(connection, targetSegment);
			
			if(StringUtils.isEmpty(graphObject.getURI())) throw new NullPointerException("graph object's URI cannot be null or empty");
			if(targetSegment == null) throw new NullPointerException("target segment cannot be null");
			if(StringUtils.isEmpty(segmentID)) throw new NullPointerException("target segment id cannot be null or empty");
		
			for(VitalSegment s : segmentsPool) {
				
				if(s.getURI().equals(targetSegment.getURI())) {
					continue;
				}

				String sid = (String) s.getRaw(Property_hasSegmentID.class);

				if( CoreOperations.containsURI(connection, SchemasUtils.getSegmentTable(dataSource, s), graphObject.getURI()) ) throw new Exception("Object with URI: " + graphObject.getURI() + " already found in another segment: " + sid); 
					
			}
					
			//this would throw a transaction stage specific exception
			CoreOperations.batchSaveObjects(dataSource, connection, SchemasUtils.getSegmentTable(dataSource, targetSegment), Arrays.asList(graphObject));
			
			return graphObject;
			
		} finally {
			closeIfNotTransaction(transaction, connection);
		}
		
	}
	
	private Connection transactionAwareConnection(VitalTransaction transaction) throws SQLException {

		if(transaction != null && transaction != VitalServiceConstants.NO_TRANSACTION) {

			if(dataSource.isSparkSQL()) throw new RuntimeException("SparkSQL endpoint does not support transactions");
			
			String id = (String) transaction.getRaw(Property_hasTransactionID.class);
			if(id == null) throw new SQLException("No transactionID in a transaction object");
			
			ActiveTransaction activeTransaction = null;
			synchronized (activeTransactions) {
				activeTransaction = activeTransactions.get(id);
			}
			
			if(activeTransaction == null) throw new SQLException("Transaction not found: " + id);
			
			return activeTransaction.connection;
			
		}
		
//		if(currentTransaction != null) return currentTransaction.connection;
		
		return dataSource.getConnection();
		
	}

	public ResultList save(VitalTransaction transaction, VitalSegment targetSegment, List<GraphObject> graphObjectsList, List<VitalSegment> segmentsPool) throws Exception {

//		check_sparkSQLSegmentsPool("save", segmentsPool);
		
		String segmentID = (String) targetSegment.getRaw(Property_hasSegmentID.class);
		
		Connection connection = null;
		
		try {
		
			connection = transactionAwareConnection(transaction);
			
			targetSegment = checkSegment(connection, targetSegment);
			
			if(targetSegment == null) throw new NullPointerException("target segment cannot be null");
			if(StringUtils.isEmpty(segmentID)) throw new NullPointerException("target segment id cannot be null or empty");
			
			Set<String> uris = new HashSet<String>();
					
			for(int i = 0 ; i < graphObjectsList.size(); i++) {
				GraphObject g = graphObjectsList.get(i);
				if(g == null) throw new NullPointerException("one of graph object is null, index: " + i);
				
				if(StringUtils.isEmpty(g.getURI())) throw new NullPointerException("one of graph objects\'s URI is null or empty, index: " + i);
				
				uris.add(g.getURI());
			}
			
			
			for(VitalSegment s : segmentsPool) {
				
				String sid = (String) s.getRaw(Property_hasSegmentID.class);
				
				if(s.getURI().equals(targetSegment.getURI())) {
					continue;
				}

				Set<String> containsURIs = CoreOperations.containsURIs(connection, SchemasUtils.getSegmentTable(dataSource, s), uris);
				if(containsURIs.size() > 0) {
					throw new Exception("Object with URIs [" +containsURIs.size() + "]:  already found in another segment: " + sid + " " + containsURIs);
				}
							
			}
			
			CoreOperations.batchSaveObjects(dataSource, connection, SchemasUtils.getSegmentTable(dataSource, targetSegment), graphObjectsList);
			
			ResultList l = new ResultList();
			for(GraphObject g : graphObjectsList) {
				l.getResults().add(new ResultElement(g, 1d));
			}
			
			l.setTotalResults(graphObjectsList.size());
			
			return l;
			
		} finally {
			closeIfNotTransaction(transaction, connection);
		}
		
	}

	public static interface ScanHandler {
		
		public void onResultsPage(List<GraphObject> objects);
		
		public void onComplete();
		
	}
	
	public void scan(VitalSegment segment, int limit, ScanHandler scanHandler) throws Exception {
		
		Connection connection = null;
		
		try {
			
			connection = dataSource.getConnection();
			
			segment = checkSegment(connection, segment);
			
			CoreOperations.scanSegment(dataSource, connection, SchemasUtils.getSegmentTable(dataSource, segment), limit, scanHandler);
			
		} finally {
			closeQuietly(connection);
		}
	
		
	}

	private void closeIfNotTransaction(VitalTransaction transaction, Connection connection) {

//		if(currentTransaction != null && currentTransaction.connection == connection) {
		if(transaction != null && transaction != VitalServiceConstants.NO_TRANSACTION) {
		} else {
			closeQuietly(connection);
		}
		
	}

	private QueryStats initStatsObject(VitalQuery sq) {
		
		QueryStats queryStats = null;
		if(sq.getCollectStats() == CollectStats.normal || sq.getCollectStats() == CollectStats.detailed) {
			queryStats = new QueryStats();
		}
		if(sq.getCollectStats() == CollectStats.detailed) {
			queryStats.setQueriesTimes(new ArrayList<QueryTime>());
		} else {
			if(queryStats != null) {
				queryStats.setQueriesTimes(null);
			}
		}
		
		return queryStats;
	}
	
	public ResultList selectQuery(VitalSelectQuery sq) throws Exception {
		
		Connection connection = null;
		
		QueryStats queryStats = initStatsObject(sq);
		
		try {
			
			if( sq.getSegments() == null || sq.getSegments().size() < 1 ) throw new NullPointerException("select query segments list cannot be null or empty");
		
			//main select query implementation
			connection = dataSource.getConnection();
			
			List<SegmentTable> segments = new ArrayList<SegmentTable>();
			
			List<String> allTables = SchemasUtils.listAllTables(connection, dataSource);
			
			for(VitalSegment segment : sq.getSegments()) {
				
				String tableName = SchemasUtils.getSegmentTable(dataSource, segment).tableName;
				
				String segmentID = (String) segment.getRaw(Property_hasSegmentID.class);
				
				VitalSegment found = null;
				
				for(String c : allTables) {
					
					if( c.equals( tableName ) ) {
						
						found = segment;
						
						break;
						
					}
					
				}
				
				if(found == null) throw new Exception("Segment for querying: " + segmentID + " not found");
				
				segments.add( SchemasUtils.getSegmentTable(dataSource, found));
				
			}
			
			if(sq instanceof VitalExportQuery) {
				
				if( sq.getSegments().size() != 1 ) throw new NullPointerException("export query requires exactly 1 segment");
				
				//the segment does not have to be indexed
				
				return SqlExportQueryImpl.export(connection, segments.get(0), (VitalExportQuery)sq, queryStats);
				
			}

//			return new SQLSelectQueryHandler(dataSource, connection, sq, segments).execute();
			return new SQLSingleSegmentQueryHandler(dataSource, connection, sq, segments, null, queryStats).execute();
				
		} finally {
			closeQuietly(connection);
		}
		
	}
	
	public Connection getConnection() throws SQLException {
		return dataSource.getConnection();
	}

	public SegmentTable getSegmentTable(VitalSegment segment) {
		return SchemasUtils.getSegmentTable(dataSource, segment);
	}

	public VitalSqlDataSource getDataSource() {
		return dataSource;
	}

	public ResultList graphQuery(VitalGraphQuery query) throws Exception {
		
		long total = System.currentTimeMillis();
		
		QueryStats queryStats = initStatsObject(query);
		
		Connection connection = null;
		
		try {
		
			connection = dataSource.getConnection();
			
			List<SegmentTable> segments = new ArrayList<SegmentTable>();
			
			List<VitalSegment> inputSegments = query.getSegments();
			
			if(inputSegments == null || inputSegments.isEmpty()) {
				throw new RuntimeException("Segments list must not be empty");
			}
			
			
			List<String> listTables = SchemasUtils.listAllTables(connection, dataSource);
			
			for(VitalSegment s : inputSegments) {
				
				String tableName = SchemasUtils.getSegmentTable(dataSource, s).tableName;
				
				String segmentID = (String) s.getRaw(Property_hasSegmentID.class);
				
				VitalSegment dbvs = null;
				
				for(String x : listTables) {
					
					if( x.equals( tableName ) ) {
						
						dbvs = s;
						break;
					}
					
				}
				
				if(dbvs == null) throw new VitalServiceException("Segment not found: " + segmentID);
				
				segments.add(SchemasUtils.getSegmentTable(dataSource, dbvs));
				
				
			}
			
			VitalGraphQuery gq = (VitalGraphQuery) query;
			
			SQLGraphObjectResolver resolver = new SQLGraphObjectResolver(connection, !gq.getPayloads(), queryStats);
			
			ResultsProvider provider = new SqlResultsProvider(dataSource, connection, segments, resolver, queryStats);
			
			GraphQueryImplementation impl = new GraphQueryImplementation(provider, gq, resolver);
			ResultList rl = impl.execute();
			
			rl.setQueryStats(queryStats);
			
			if(queryStats != null) {
				queryStats.setQueryTimeMS(System.currentTimeMillis() - total);
			}
			
			return rl;
		} finally {
			closeQuietly(connection);
		}
	}

	public int getMaxActiveTransactions() {
		return maxActiveTransactions;
	}

	public int getActiveTransactionsCount() {
		synchronized (activeTransactions) {
			return activeTransactions.size();
		}
	}

}
