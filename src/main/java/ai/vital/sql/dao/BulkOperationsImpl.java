package ai.vital.sql.dao;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.model.SegmentTable;
import ai.vital.sql.model.VitalSignsToSqlBridge;
import ai.vital.sql.model.VitalSignsToSqlBridge.GraphObjectsStreamHandler;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.properties.Property_hasProvenance;

public class BulkOperationsImpl {

	private final static Logger log = LoggerFactory.getLogger(BulkOperationsImpl.class);
	
	public static boolean printProgress = true;
	
	public static VitalStatus bulkExport(VitalSqlDataSource dataSource, Connection connection, SegmentTable segmentTable, OutputStream outputStream, final String datasetURI) throws Exception {

		log.info("BulkExport, table: {}", segmentTable.tableName);
		
		long start = System.currentTimeMillis();
		
		final AtomicInteger skipped = new AtomicInteger(0);
		
		final AtomicInteger c = new AtomicInteger(0);
		
		final AtomicLong step = new AtomicLong(System.currentTimeMillis());
		
		Statement stmt = null;

		ResultSet rs = null;
		
		try {
			
			String tname = segmentTable.tableName;
			
			String order =  null;
			
//			if(dataSource.isSparkSQL() /*&& !dataSource.getConfig().isVanillaSparkSQL()*/) {
//				order = "";
//			} else {
				order = " ORDER BY " + SQLUtils.escapeID(connection, VitalSignsToSqlBridge.COLUMN_URI) + " ASC";
//			}
			
			String sql = "SELECT * FROM " + SQLUtils.escapeID(connection, tname) + order;
			
			if(log.isDebugEnabled()) log.debug("Bulk export sql: {}", sql);
			
//			stmt = connection.prepareStatement(sql)
			
			//https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html
			stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			rs = stmt.executeQuery(sql);
			
			final OutputStreamWriter local = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
			
			final BlockCompactStringSerializer serializer = new BlockCompactStringSerializer(local);
			
			VitalSignsToSqlBridge.fromSql(segmentTable, rs, new GraphObjectsStreamHandler() {
				
				@Override
				public void onNoMoreObjects() {
					try {
						serializer.flush();
						local.flush();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				
				@Override
				public void onGraphObject(GraphObject g) {
					
	                 if(datasetURI != null) {
                        String thisDataset = (String) g.getRaw(Property_hasProvenance.class);
                        if(thisDataset == null || !datasetURI.equals(thisDataset)) {
                            //skip the object
                            skipped.incrementAndGet();
                            return;
                        }
                    }
						
					
					try {
						serializer.startBlock();
						serializer.writeGraphObject(g);
						serializer.endBlock();
						int val = c.incrementAndGet();
						
						if(printProgress && val % 1000 == 0) {
							log.info("Exported so far {}, {}ms", val, System.currentTimeMillis() - step.get());
							step.set(System.currentTimeMillis());
						}
						
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			});
				
			
		} finally {
			SQLUtils.closeQuietly(stmt, rs);
			
		}
		
		log.info("BulkExport, table: {}, time: {}ms", segmentTable.tableName, System.currentTimeMillis() - start);
		
		VitalStatus vs = VitalStatus.withOKMessage("Exported " + c.get() + " object(s)" + (datasetURI != null ? (", filtered out " + skipped + " object(s)") : ""));
		
		vs.setSuccesses(c.get());
		
		return vs;
		
	}

	public static VitalStatus bulkImport(VitalSqlDataSource dataSource,
			Connection connection, SegmentTable segmentTable, InputStream inputStream, String datasetURI) throws Exception {
		
		log.info("BulkImport, table: {}", segmentTable.tableName);
		
		long start = System.currentTimeMillis();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		
		BlockIterator blocksIterator = BlockCompactStringSerializer.getBlocksIterator(reader, false);
			
		int c = 0;
		
		List<GraphObject> buffer = new ArrayList<GraphObject>();
		
		long step = System.currentTimeMillis();
		
		while(blocksIterator.hasNext()) {
			
			VitalBlock _next = blocksIterator.next();
			
			List<GraphObject> list = _next.toList();
			
			for(GraphObject g : list) {
				if(! "".equals(datasetURI)) {
					g.set(Property_hasProvenance.class, datasetURI);
				}
			}

			buffer.addAll(list);
			
			c += list.size();
			
			if(buffer.size() >= 1000) {
				CoreOperations.insertGraphObjects(dataSource, connection, segmentTable, buffer);
				buffer.clear();
				
				if(printProgress) {
					log.info("Imported so far {}, {}ms", c, System.currentTimeMillis() - step);
					step = System.currentTimeMillis();
				}
				
				
			}
			
			
			
		}
		
		if(buffer.size() > 0) {
			CoreOperations.insertGraphObjects(dataSource, connection, segmentTable, buffer);
		}
		
		log.info("BulkImport, table: {}, time: {}ms", segmentTable.tableName, System.currentTimeMillis() - start);
		
		VitalStatus vs = VitalStatus.withOKMessage("Imported " + c + " object(s)");
		vs.setSuccesses(c);
		return vs;
	} 
	
}
