package ai.vital.sql.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.model.SegmentTable;
import ai.vital.sql.query.SQLSelectQueryHandler.CoreSelectQuery;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryElement;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion.Comparator;

/**
 * An optimization that skips segments for AND containers
 * when there's not a chance that there will be any results.
 * It does not involve filter expressions and should be very fast
 *  
 *
 */
public class ANDConstraintProbing {

	private final static Logger log = LoggerFactory.getLogger(ANDConstraintProbing.class);
	
	/**
	 * Returns true if the container should be processed. If false, there's no need to process it as
	 * 0 results are expected
	 * @param container
	 * @return
	 * @throws SQLException 
	 */
	public static boolean probeContainer(VitalSqlDataSource dataSource, SQLSelectQueryHandler handler, SegmentTable segmentTable, VitalGraphCriteriaContainer container, QueryStats queryStats) throws SQLException {
		
		if(container.getType() != QueryContainerType.and) {
			return true;
		}
		
		List<VitalGraphQueryPropertyCriterion> propConstraints = new ArrayList<VitalGraphQueryPropertyCriterion>();
		
		int allConstraints = 0;
		
		Set<String> pURIs = new HashSet<String>();
		
		for( VitalGraphQueryElement el : container ) {
			
			if(el instanceof VitalGraphQueryPropertyCriterion) {
				
				VitalGraphQueryPropertyCriterion pc = (VitalGraphQueryPropertyCriterion) el;
	
				
				if(pc.isNegative()) {
					//cannot check negative constraints
					continue;
				}
				
				if(pc.getClass().equals(VitalGraphQueryPropertyCriterion.class)) {
					
					Comparator c = pc.getComparator();
					
					if(c == Comparator.NE || c == Comparator.NONE_OF || c == Comparator.NOT_CONTAINS || c == Comparator.NOT_EXISTS ) {
						//cannot check negative constraints
						continue;
					}
					
				}

				String pURI = pc.getPropertyURI();
				if(pURI != null) {
					if(!pURIs.add(pURI)) {
						continue;
					}
				}
				
				allConstraints++;
				
				propConstraints.add(pc);
				
			}
			
		}
		
		
		//don't probe if one constraint
		if(allConstraints < 2) {
			return true;
		}
		
		
		String tableName = SQLUtils.escapeID(handler.connection, segmentTable.tableName);
		
		int c = 0;
		
		//TODO consolidate range into BETWEEN
		for(VitalGraphQueryPropertyCriterion pc : propConstraints ) {
			
			c++;
			
			CoreSelectQuery x = new CoreSelectQuery();
			x.tableName = tableName;
			handler.processCriterion(x, null, pc, "", true, true, null, true);
			//OFFSET 0
			x.queryTemplate.append(" LIMIT 1 " + ( !dataSource.isSparkSQL() ? " OFFSET 0" : ""));
			
			
			PreparedStatement stmt = null;
			
			ResultSet rs = null;
			
			
			try {
				
				
				stmt = handler.connection.prepareStatement(x.queryTemplate.toString());
				
				for(int i = 0; i < x.substitutes.size(); i++) {
					stmt.setObject(i+1, x.substitutes.get(i));
				}
				
				
				if(log.isDebugEnabled()) {
					log.debug("Probe test {}, sql: {}", c, stmt);
				}
				
				long start = System.currentTimeMillis();
				
				rs = stmt.executeQuery();
				
				if(queryStats != null) {
					long time = queryStats.addProbingTimeFrom(start);
					if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime("PROBE: " + pc.toString(), stmt.toString(), time));
				}
				
				if(log.isDebugEnabled()) {
					log.debug("Probe test time: {}ms", System.currentTimeMillis() - start);
				}
				
				if(!rs.next()) {

					if(log.isDebugEnabled()) {
						log.debug("Probe test {} returned 0 results, breaking", c);
					}
					
					return false;
				}
				
			} finally {
				SQLUtils.closeQuietly(stmt, rs);
			}
			
		}
		
		return true;
	}
	
}
