package ai.vital.sql.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.query.SQLGraphObjectResolver;
import ai.vital.sql.services.MysqlStringEscape;
import ai.vital.sql.services.ToCSVProviderImpl;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.classes.ClassMetadata;
import ai.vital.vitalsigns.conf.VitalSignsConfig.VersionEnforcement;
import ai.vital.vitalsigns.datatype.Truth;
import ai.vital.vitalsigns.domains.DifferentDomainVersionLoader.VersionedDomain;
import ai.vital.vitalsigns.domains.DifferentDomainVersionLoader.VersionedURI;
import ai.vital.vitalsigns.model.DomainOntology;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.properties.Property_hasOntologyIRI;
import ai.vital.vitalsigns.model.properties.Property_hasTimestamp;
import ai.vital.vitalsigns.model.properties.Property_hasVersionIRI;
import ai.vital.vitalsigns.model.properties.Property_types;
import ai.vital.vitalsigns.model.properties.Property_vitaltype;
import ai.vital.vitalsigns.model.property.BooleanProperty;
import ai.vital.vitalsigns.model.property.DateProperty;
import ai.vital.vitalsigns.model.property.DoubleProperty;
import ai.vital.vitalsigns.model.property.FloatProperty;
import ai.vital.vitalsigns.model.property.GeoLocationProperty;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.IntegerProperty;
import ai.vital.vitalsigns.model.property.LongProperty;
import ai.vital.vitalsigns.model.property.MultiValueProperty;
import ai.vital.vitalsigns.model.property.OtherProperty;
import ai.vital.vitalsigns.model.property.StringProperty;
import ai.vital.vitalsigns.model.property.TruthProperty;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import ai.vital.vitalsigns.properties.PropertyMetadata;

public class VitalSignsToSqlBridge {

	private final static Logger log = LoggerFactory.getLogger(VitalSignsToSqlBridge.class);

	private final static Set<String> processedProperties = new HashSet<String>(Arrays.asList(
		VitalCoreOntology.vitaltype.getURI(),
		VitalCoreOntology.URIProp.getURI(),
		VitalCoreOntology.types.getURI(),
		VitalCoreOntology.hasOntologyIRI.getURI(),
		VitalCoreOntology.hasVersionIRI.getURI()
	));
	
	/*
CREATE TABLE ${NAME} (

  id BIGINT NOT NULL AUTO_INCREMENT,
  uri varchar(255) NOT NULL, 
  name varchar(255) NOT NULL,
  
  external BIT NOT NULL,
  
  value_boolean BIT,
  value_boolean_multivalue BIT,
  
  value_date TIMESTAMP,
  value_date_multivalue TIMESTAMP,
  
  -- all numeric types and dates
  value_number DOUBLE,
  value_number_multivalue DOUBLE,
  
  
  -- also include geo
  -- value_geolocation varchar(1024),
  -- value_other
  value_string varchar(65535),
  value_string_multivalue varchar(65535),
  
  value_uri varchar(255),
  value_uri_multivalue varchar(255),
  
  PRIMARY KEY (id)
)
	 */
	
	public final static int MAX_STRING_PROPERTY_LENGTH = 5000;
	
	public final static String FULL_STRING_LABEL = ":::TEXT:::";
	
	public final static String COLUMN_ID = "id";
	public final static String COLUMN_URI = "uri";
	public final static String COLUMN_NAME = "name";
	public final static String COLUMN_EXTERNAL = "external";
	public final static String COLUMN_VITALTYPE = "vitaltype";
	public final static String COLUMN_TSTAMP= "tstamp";
	public final static String COLUMN_CHANNEL_URI = "channeluri";
	
	public final static String COLUMN_VALUE_BOOLEAN = "value_boolean";
	public final static String COLUMN_VALUE_BOOLEAN_MULTIVALUE = "value_boolean_multivalue";
	
	public final static String COLUMN_VALUE_DATE = "value_date";
	public final static String COLUMN_VALUE_DATE_MULTIVALUE = "value_date_multivalue";
	
	public final static String COLUMN_VALUE_DOUBLE = "value_double";
	public final static String COLUMN_VALUE_DOUBLE_MULTIVALUE = "value_double_multivalue";
	
	public final static String COLUMN_VALUE_FLOAT = "value_float";
	public final static String COLUMN_VALUE_FLOAT_MULTIVALUE = "value_float_multivalue";
	
	public final static String COLUMN_VALUE_GEOLOCATION = "value_geolocation";
	public final static String COLUMN_VALUE_GEOLOCATION_MULTIVALUE = "value_geolocation_multivalue";

	public final static String COLUMN_VALUE_INTEGER = "value_integer";
	public final static String COLUMN_VALUE_INTEGER_MULTIVALUE = "value_integer_multivalue";
	
	public final static String COLUMN_VALUE_LONG = "value_long";
	public final static String COLUMN_VALUE_LONG_MULTIVALUE = "value_long_multivalue";
	
	public final static String COLUMN_VALUE_OTHER = "value_other";
	public final static String COLUMN_VALUE_OTHER_MULTIVALUE = "value_other_multivalue";
	
	public final static String COLUMN_VALUE_STRING = "value_string";
	public final static String COLUMN_VALUE_STRING_MULTIVALUE = "value_string_multivalue";
	
	public final static String COLUMN_VALUE_TRUTH = "value_truth";
	public final static String COLUMN_VALUE_TRUTH_MULTIVALUE = "value_truth_multivalue";
	
	public final static String COLUMN_VALUE_URI = "value_uri";
	public final static String COLUMN_VALUE_URI_MULTIVALUE = "value_uri_multivalue";

	//special
	public final static String COLUMN_VALUE_FULL_TEXT = "value_full_text";
	
	public final static URIProperty aimpMessageClassURI = URIProperty.withString("http://vital.ai/ontology/vital-aimp#AIMPMessage");
	
	
	private static class SimpleEntry implements Entry<String, IProperty> {

		String k;
		IProperty v;
		
		public SimpleEntry(String k, IProperty v) {
			super();
			this.k = k;
			this.v = v;
		}

		@Override
		public String getKey() {
			return k;
		}

		@Override
		public IProperty getValue() {
			return v;
		}

		@Override
		public IProperty setValue(IProperty value) {
			IProperty prev = v;
			v = value;
			return prev;
		}
		
	}
	
	
	public static List<String> columns = new ArrayList<String>();
	//hashmap lookup is much faster than list.indexOf
	static HashMap<String, Integer> column2Index = new HashMap<String, Integer>();
	
	static int fullTextIndex = -1;
	
	static int uriIndex = -1;
	static int nameIndex = -1;
	static int vitaltypeIndex = -1;
	static int externalIndex = -1;
	static int tstampIndex = -1;
	static int channelURIIndex = -1;
	
	static {
		columns = Arrays.asList(
			COLUMN_URI,
			COLUMN_NAME,
			COLUMN_VITALTYPE,
			COLUMN_EXTERNAL,
			COLUMN_TSTAMP,
			COLUMN_CHANNEL_URI,
			COLUMN_VALUE_BOOLEAN,
			COLUMN_VALUE_BOOLEAN_MULTIVALUE,
			COLUMN_VALUE_DATE,
			COLUMN_VALUE_DATE_MULTIVALUE,
			COLUMN_VALUE_DOUBLE,
			COLUMN_VALUE_DOUBLE_MULTIVALUE,
			COLUMN_VALUE_FLOAT,
			COLUMN_VALUE_FLOAT_MULTIVALUE, 
			COLUMN_VALUE_GEOLOCATION,
			COLUMN_VALUE_GEOLOCATION_MULTIVALUE,
			COLUMN_VALUE_INTEGER,
			COLUMN_VALUE_INTEGER_MULTIVALUE,
			COLUMN_VALUE_LONG,
			COLUMN_VALUE_LONG_MULTIVALUE,
			COLUMN_VALUE_OTHER,
			COLUMN_VALUE_OTHER_MULTIVALUE,
			COLUMN_VALUE_STRING,
			COLUMN_VALUE_STRING_MULTIVALUE,
			COLUMN_VALUE_TRUTH,
			COLUMN_VALUE_TRUTH_MULTIVALUE,
			COLUMN_VALUE_FULL_TEXT,
			COLUMN_VALUE_URI,
			COLUMN_VALUE_URI_MULTIVALUE
		);
		
		for(int i = 1; i <= columns.size(); i++) {
			String c = columns.get(i-1);
			if(COLUMN_URI.equals(c)) {
				uriIndex = i;
			} else if(COLUMN_NAME.equals(c)) {
				nameIndex = i;
			} else if(COLUMN_VITALTYPE.equals(c)) {
				vitaltypeIndex = i;
			} else if(COLUMN_EXTERNAL.equals(c)) {
				externalIndex = i;
			} else if(COLUMN_VALUE_FULL_TEXT.equals(c)) {
				fullTextIndex = i;
			} else if(COLUMN_TSTAMP.equals(c)) {
				tstampIndex = i;
			} else if(COLUMN_CHANNEL_URI.equals(c)) {
				channelURIIndex = i;
			}
			column2Index.put(c, new Integer(i));
		}
		
		
	}
	
	
	public static enum OutputType {
		
		CSV,
		SQLRows,
		JDBC
		
	}
	
	public static List<String> batchInsertGraphObjects(VitalSqlDataSource dataSource, Connection connection, SegmentTable segmentTable, Collection<GraphObject> gs) throws SQLException {
		return batchInsertGraphObjects(dataSource, connection, segmentTable, gs, OutputType.JDBC);
	}
	@SuppressWarnings("unchecked")
	public static List<String> batchInsertGraphObjects(VitalSqlDataSource dataSource, Connection connection, SegmentTable segmentTable, Collection<GraphObject> gs, OutputType outputType) throws SQLException {
		
		Class<? extends GraphObject> aimpClass = VitalSigns.get().getClass(aimpMessageClassURI);
		
		if(outputType == OutputType.JDBC && dataSource.isSparkSQL()) throw new RuntimeException("Cannot use JDBC prep statement batch insert in SparkSQL mode");
		
		PreparedStatement stmt = null;
		
		List<String> output = outputType != OutputType.JDBC ? new ArrayList<String>() : null;
		
		try {
			
			int batchSize = 100;
			
			int c = 0;
			
			
			if(outputType == OutputType.JDBC) {
				String command = dataSource.getInsertCommandTemplate(connection, segmentTable.tableName);
				stmt = connection.prepareStatement(command);
			}
			
			for(GraphObject g : gs) {
				
				String vitaltypeURI = (String) g.getRaw(Property_vitaltype.class);
				if(vitaltypeURI == null) throw new RuntimeException("No vitaltype property in a graph object: " + g.getClass().getCanonicalName());
				
				Long timestamp = (Long) g.getRaw(Property_hasTimestamp.class);
				
				String channelURI = null;
				
				if(aimpClass != null && aimpClass.isInstance(g)) {
					IProperty channelURIValue = (IProperty) g.getProperty("channelURI");
					channelURI = channelURIValue != null ? (String) channelURIValue.rawValue() : null;
				}
						
				
				String uri = g.getURI();
				
				List<Entry<String, IProperty>> entries = new ArrayList<Entry<String, IProperty>>(g.getPropertiesMap().entrySet());
				
				//set reverse transient properties !
				DomainOntology _do = VitalSigns.get().getClassDomainOntology(g.getClass());
				if(_do == null) throw new RuntimeException("Domain ontology for class: " + g.getClass().getCanonicalName() + " not found");
				
				entries.add(new SimpleEntry(VitalSigns.get().getPropertiesRegistry().getPropertyURI(Property_hasOntologyIRI.class), URIProperty.withString(_do.getUri())));
				entries.add(new SimpleEntry(VitalSigns.get().getPropertiesRegistry().getPropertyURI(Property_hasVersionIRI.class), new StringProperty(_do.toVersionString())));
				
				for( Entry<String, IProperty> entry : entries ) {
					
					
					String k = entry.getKey();
					
					IProperty v = entry.getValue();
					
					if(v == null) continue;
					
					v = v.unwrapped();
					
					//store all properties as they are

					PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(k);
					
					boolean external = false;
					
					Collection<IProperty> collectionValue = null;
					
					IProperty singleValue = null;
					
					if(pm == null) {
						
						external = true;
						
						if(v instanceof MultiValueProperty) {
							
							collectionValue = (Collection<IProperty>) v;
							
						} else {
							
							singleValue = v;
							
//							throw new RuntimeException("Expected an instanceof of multivalue property");
						}
						
					} else {
						
						if( pm.isMultipleValues() ) {
						
							collectionValue = (Collection<IProperty>) v;
							
						} else {
							
							singleValue = v;
							
						}
						
					}
					
					String propURI = oldVersionFilter(k);
					
					if(collectionValue != null) {
						
						for(IProperty pv : collectionValue) {
							
							String o = propertyToRowBatch(dataSource, stmt, segmentTable, vitaltypeURI, uri, timestamp, channelURI, propURI, pv, external, true, outputType);
							if(output != null) output.add(o);
							c++;
							
							if(stmt != null && c % batchSize == 0 ) {
								stmt.executeBatch();
								
							}
							
							
						}
						
					} else {
						
						String o = propertyToRowBatch(dataSource, stmt, segmentTable, vitaltypeURI, uri, timestamp, channelURI, propURI, singleValue, external, false, outputType);
						if(output != null) output.add(o);
						c++;
						
						if(stmt != null && c % batchSize == 0) {
							stmt.executeBatch();
						}				
						
					}
					
				}
				
			}
			
			//only if there are any pending statements to insert
			if(stmt != null && c % batchSize > 0) {
				stmt.executeBatch();
			}	
			
			
		} finally {
			SQLUtils.closeQuietly(stmt);
		}
		
		return output;
		
		
	}
	
	//target segment
	@SuppressWarnings("unchecked")
	@Deprecated
	public static List<PreparedStatement> toSqlX(Connection connection, SegmentTable segmentTable, GraphObject g) throws SQLException {
		
		Class<? extends GraphObject> aimpClass = VitalSigns.get().getClass(aimpMessageClassURI);
		
		String vitaltypeURI = (String) g.getRaw(Property_vitaltype.class);
		if(vitaltypeURI == null) throw new RuntimeException("No vitaltype property in a graph object: " + g.getClass().getCanonicalName());
		
		Long timestamp = (Long) g.getRaw(Property_hasTimestamp.class);
		
		String channelURI = null;
		if(aimpClass != null && aimpClass.isInstance(g)) {
			IProperty channelURIValue = (IProperty) g.getProperty("channelURI"); 
			channelURI = channelURIValue != null ? (String) channelURIValue.rawValue() : null;
		}
		
		List<PreparedStatement> insertCommands = new ArrayList<PreparedStatement>();
		
		String uri = g.getURI();
		
		List<Entry<String, IProperty>> entries = new ArrayList<Entry<String, IProperty>>(g.getPropertiesMap().entrySet());
		
		//set reverse transient properties !
		DomainOntology _do = VitalSigns.get().getClassDomainOntology(g.getClass());
		if(_do == null) throw new RuntimeException("Domain ontology for class: " + g.getClass().getCanonicalName() + " not found");
		
		entries.add(new SimpleEntry(VitalSigns.get().getPropertiesRegistry().getPropertyURI(Property_hasOntologyIRI.class), URIProperty.withString(_do.getUri())));
		entries.add(new SimpleEntry(VitalSigns.get().getPropertiesRegistry().getPropertyURI(Property_hasVersionIRI.class), new StringProperty(_do.toVersionString())));
		
		for( Entry<String, IProperty> entry : entries ) {
			
			
			String k = entry.getKey();
			
			IProperty v = entry.getValue();
			
			if(v == null) continue;
			
			v = v.unwrapped();
			
			//store all properties as they are

			PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(k);
			
			boolean external = false;
			
			Collection<IProperty> collectionValue = null;
			
			IProperty singleValue = null;
			
			if(pm == null) {
				
				external = true;
				
				if(v instanceof MultiValueProperty) {
					
					collectionValue = (Collection<IProperty>) v;
					
				} else {
					
					singleValue = v;
					
//					throw new RuntimeException("Expected an instanceof of multivalue property");
				}
				
			} else {
				
				if( pm.isMultipleValues() ) {
				
					collectionValue = (Collection<IProperty>) v;
					
				} else {
					
					singleValue = v;
					
				}
				
			}
			
			String propURI = oldVersionFilter(k);
			
			if(collectionValue != null) {
				
				for(IProperty pv : collectionValue) {
					
					PreparedStatement command = propertyToRow(connection, segmentTable, vitaltypeURI, uri, timestamp, channelURI, propURI, pv, external, true);
					
					insertCommands.add(command);
					
				}
				
			} else {
				
				PreparedStatement command = propertyToRow(connection, segmentTable, vitaltypeURI, uri, timestamp, channelURI, propURI, singleValue, external, false);
				
				insertCommands.add(command);
				
			}
			
		}
		
		return insertCommands;
		
	}
	
	
	private static String propertyToRowBatch(VitalSqlDataSource dataSource, PreparedStatement stmt, SegmentTable segmentTable, String vitaltypeURI, String URI, Long timestamp, String channelURI, String propertyURI, IProperty pv, boolean external, boolean multipleValue, OutputType outputType) throws SQLException {
		
		String cn = null;

		String fullTextValue = null;
		
		Object v = null;
		
		String out = null;
		
		if(pv instanceof BooleanProperty) {
			
			BooleanProperty bp = (BooleanProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_BOOLEAN_MULTIVALUE : COLUMN_VALUE_BOOLEAN;
			v = bp.booleanValue();
			
		} else if(pv instanceof DateProperty) {
			
			DateProperty dp = (DateProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_DATE_MULTIVALUE : COLUMN_VALUE_DATE;
			v = dp.getTime();
			
		} else if(pv instanceof DoubleProperty) {
			
			DoubleProperty np = (DoubleProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_DOUBLE_MULTIVALUE : COLUMN_VALUE_DOUBLE;
			v = np.doubleValue();
			
		} else if(pv instanceof FloatProperty) {
			
			FloatProperty fp = (FloatProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_FLOAT_MULTIVALUE : COLUMN_VALUE_FLOAT;
			v = fp.floatValue();
			
		} else if(pv instanceof GeoLocationProperty) {
			
			GeoLocationProperty gp = (GeoLocationProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_GEOLOCATION_MULTIVALUE : COLUMN_VALUE_GEOLOCATION;
			v = gp.toRDFValue();
			
		} else if(pv instanceof IntegerProperty) {
			
			IntegerProperty ip = (IntegerProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_INTEGER_MULTIVALUE : COLUMN_VALUE_INTEGER;
			v = ip.intValue();
			
		} else if(pv instanceof LongProperty) {
			
			LongProperty lp = (LongProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_LONG_MULTIVALUE : COLUMN_VALUE_LONG;
			v = lp.longValue();
			
		} else if(pv instanceof OtherProperty) {
			
			OtherProperty op = (OtherProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_OTHER_MULTIVALUE : COLUMN_VALUE_OTHER;
			
			String rdfString = op.toRDFString();
			if(rdfString.length() > MAX_STRING_PROPERTY_LENGTH) {
				v = FULL_STRING_LABEL;
				fullTextValue = rdfString;
			} else {
				v = rdfString;
			}
			
		} else if(pv instanceof StringProperty) {
			
			StringProperty sp = (StringProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_STRING_MULTIVALUE : COLUMN_VALUE_STRING;
			String s = sp.toString();
			if(s.length() > MAX_STRING_PROPERTY_LENGTH) {
				v = FULL_STRING_LABEL;
				fullTextValue = s;
			} else {
				v = s;
			}
			
		} else if(pv instanceof TruthProperty) {
			
			TruthProperty tp = (TruthProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_TRUTH_MULTIVALUE : COLUMN_VALUE_TRUTH;
			v = tp.asTruth();
			
		} else if(pv instanceof URIProperty) {
			
			URIProperty up = (URIProperty) pv;
			String uri = oldVersionFilter(up.get());
			
			cn = multipleValue ? COLUMN_VALUE_URI_MULTIVALUE : COLUMN_VALUE_URI;
			v = uri;
			
		} else {
			throw new RuntimeException("Unhandled property type: " + pv.getClass().getCanonicalName());
		}
		
		Integer valIndex = column2Index.get(cn);
		
		if(valIndex == null) throw new RuntimeException("column index not found: " + cn);
		
		List<Object> record = stmt == null ? new ArrayList<Object>() : null; 
		
		for(int i = 1 ; i <= columns.size(); i++) {
			
			Object raw = null;
			
			if(i == uriIndex) {
				
				if(stmt != null) {
					stmt.setString(uriIndex, URI);
				} else {
					raw = URI;
				}
				
			} else if( i == nameIndex) {
				
				if(stmt != null) {
					stmt.setString(nameIndex, propertyURI);
				} else {
					raw = propertyURI;
				}
				
			} else if( i == vitaltypeIndex ) {
				
				if(stmt != null) {
					stmt.setString(vitaltypeIndex, vitaltypeURI);
				} else {
					raw = vitaltypeURI;
				}
				
			} else if( i == externalIndex) {
				
				if(stmt != null) {
					stmt.setBoolean(externalIndex, external);
				} else {
					raw = external;
				}
				
			} else if( i == tstampIndex ) {
				
				if(stmt != null) {
					if(timestamp != null) {
						stmt.setLong(tstampIndex, timestamp);
					} else {
						stmt.setNull(tstampIndex, Types.BIGINT);
					}
				} else {
					raw = timestamp;
				}
				
			} else if( i == channelURIIndex ) {
				
				if(stmt != null) {
					if(channelURI != null) {
						stmt.setString(channelURIIndex, channelURI);
					} else {
						stmt.setNull(channelURIIndex, Types.VARCHAR);
					}
				}
				
			} else if( i == valIndex.intValue() ) {
				
				if(stmt != null) {
					stmt.setObject(valIndex.intValue(), v);
				} else {
					raw = v;
				}
				
			} else if(fullTextValue != null && i == fullTextIndex) {
				
				if(stmt != null) {
					stmt.setString(fullTextIndex, fullTextValue);
				} else {
					raw = fullTextValue;
				}
				
			} else {
				
				String c = columns.get(i-1);
				
				if(stmt != null) {
					stmt.setNull(i, dataSource.getDialect().getColumnType(c));
				} else {
					raw = null;
				}
				
			}
			
			if(stmt == null) {
			
				if(raw instanceof Date) {
					raw = ((Date)raw).getTime();
//				} else if(raw instanceof Boolean /* && outputType != OutputType.CSV*/) {
//					raw = ((Boolean)raw).booleanValue() ? 1 : 0;
				}
				record.add(raw); 
				
			}
			
		}

		if(stmt != null) {
			stmt.addBatch();
		} else if(outputType == OutputType.CSV){
			//ID
			record.add(0, null);
//			for(int i = 0 ; i < record.size(); i++) {
//				Object object = record.get(i);
//				if(object == null) record.set(i, "NULL");
//			}
			out = ToCSVProviderImpl.format.format(record.toArray());
		} else if(outputType == OutputType.SQLRows) {
			StringBuilder sb = new StringBuilder("( ");
			boolean first = true;
			for(Object r : record) {
				if(first) {
					first = false;
				} else {
					sb.append(", ");
				}
				
				if(r == null) {
					sb.append("NULL");
				} else if(r instanceof Boolean) {
					boolean b = (boolean) r;
					sb.append(b);
				} else if(r instanceof Number) {
					sb.append((Number)r);
				} else if(r instanceof String) {
					sb.append('\'').append(MysqlStringEscape.escape((String) r)).append('\'');
//				} else if(r instanceof Date) {
//					sb.append(((Date)r).getTime());
				} else {
					throw new RuntimeException("Unsupported value type: " + r);
				}
				
			}
			sb.append(" )");
			out = sb.toString();
//			out = "( " + CSVFormat.RFC4180.format(record) + " )";
		}
		
		return out;
		
		
	}

	private static PreparedStatement propertyToRow(Connection connection, SegmentTable segmentTable, String vitaltypeURI, String URI, Long timestamp, String channelURI, String propertyURI, IProperty pv, boolean external, boolean multipleValue) throws SQLException {

		String cn = null;

		String fullTextValue = null;
		
		
		Object v = null;
		
		if(pv instanceof BooleanProperty) {
			
			BooleanProperty bp = (BooleanProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_BOOLEAN_MULTIVALUE : COLUMN_VALUE_BOOLEAN;
			v = bp.booleanValue();
			
		} else if(pv instanceof DateProperty) {
			
			DateProperty dp = (DateProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_DATE_MULTIVALUE : COLUMN_VALUE_DATE;
			v = dp.getTime();
			
		} else if(pv instanceof DoubleProperty) {
			
			DoubleProperty np = (DoubleProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_DOUBLE_MULTIVALUE : COLUMN_VALUE_DOUBLE;
			v = np.doubleValue();
			
		} else if(pv instanceof FloatProperty) {
			
			FloatProperty fp = (FloatProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_FLOAT_MULTIVALUE : COLUMN_VALUE_FLOAT;
			v = fp.floatValue();
			
		} else if(pv instanceof GeoLocationProperty) {
			
			GeoLocationProperty gp = (GeoLocationProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_GEOLOCATION_MULTIVALUE : COLUMN_VALUE_GEOLOCATION_MULTIVALUE;
			v = gp.toRDFValue();
			
		} else if(pv instanceof IntegerProperty) {
			
			IntegerProperty ip = (IntegerProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_INTEGER_MULTIVALUE : COLUMN_VALUE_INTEGER;
			v = ip.intValue();
			
		} else if(pv instanceof LongProperty) {
			
			LongProperty lp = (LongProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_LONG_MULTIVALUE : COLUMN_VALUE_LONG;
			v = lp.longValue();
			
		} else if(pv instanceof OtherProperty) {
			
			OtherProperty op = (OtherProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_OTHER_MULTIVALUE : COLUMN_VALUE_OTHER;
			
			String rdfString = op.toRDFString();
			if(rdfString.length() > MAX_STRING_PROPERTY_LENGTH) {
				v = FULL_STRING_LABEL;
				fullTextValue = rdfString;
			} else {
				v = rdfString;
			}
			
		} else if(pv instanceof StringProperty) {
			
			StringProperty sp = (StringProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_STRING_MULTIVALUE : COLUMN_VALUE_STRING;
			String s = sp.toString();
			if(s.length() > MAX_STRING_PROPERTY_LENGTH) {
				v = FULL_STRING_LABEL;
				fullTextValue = s;
			} else {
				v = s;
			}
			
		} else if(pv instanceof TruthProperty) {
			
			TruthProperty tp = (TruthProperty) pv;
			cn = multipleValue ? COLUMN_VALUE_TRUTH_MULTIVALUE : COLUMN_VALUE_TRUTH;
			v = tp.asTruth();
			
		} else if(pv instanceof URIProperty) {
			
			URIProperty up = (URIProperty) pv;
			String uri = oldVersionFilter(up.get());
			
			cn = multipleValue ? COLUMN_VALUE_URI_MULTIVALUE : COLUMN_VALUE_URI;
			v = uri;
			
		} else {
			throw new RuntimeException("Unhandled property type: " + pv.getClass().getCanonicalName());
		}
		
		
		StringBuilder command = new StringBuilder("INSERT INTO ")
		.append(SQLUtils.escapeID(connection, segmentTable.tableName)).append(" ( ")
		.append(COLUMN_URI).append(", ")
		.append(COLUMN_NAME).append(", ")
		.append(COLUMN_VITALTYPE).append(", ")
		.append(COLUMN_EXTERNAL).append(", ")
		.append(COLUMN_TSTAMP).append(", ")
		.append(COLUMN_CHANNEL_URI).append(", ");
		
		if(fullTextValue != null) {
			command.append(COLUMN_VALUE_FULL_TEXT).append(", ");
		}
		
		command.append(cn).append(") VALUES ( ?, ?, ?, ?, ?, ?, ?");
		if(fullTextValue != null) {
			command.append(", ?");
		}
		command.append(" )");
		
		
		log.debug("Row insert sql: {}", command.toString());
		
		int vi = fullTextValue != null ? 8 : 7;
		
		PreparedStatement stmt = connection.prepareStatement(command.toString());
		
		stmt.setString(1, URI);
		stmt.setString(2, propertyURI);
		stmt.setString(3, vitaltypeURI);
		stmt.setBoolean(4, external);
		if(timestamp != null) {
			stmt.setLong(5, timestamp);
		} else {
			stmt.setNull(5, Types.BIGINT);
		}
		if(channelURI != null) {
			stmt.setString(6, channelURI);
		} else {
			stmt.setNull(6, Types.VARCHAR);
		}

		if(fullTextValue != null) {
			stmt.setString(7, fullTextValue);
		}
		
		stmt.setObject(vi, v);
		
		
		return stmt;
		
	}

	private static class WrappedProperty {
		IProperty property;
		boolean external;
		public WrappedProperty(IProperty property, boolean external) {
			super();
			this.property = property;
			this.external = external;
		}
	}
	
	public static List<GraphObject> fromSql(SegmentTable segmentTable, ResultSet rs) throws SQLException {
		return fromSql(segmentTable, rs, null, null, null);
	}
	
	public static interface GraphObjectsStreamHandler {
		public void onGraphObject(GraphObject g);
		public void onNoMoreObjects();
	}
	
	public static List<GraphObject> fromSql(SegmentTable segmentTable, ResultSet rs, GraphObjectsStreamHandler handler) throws SQLException {
		return fromSql(segmentTable, rs, null, handler, null);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	/**
	 * Overloaded method to read from either a result set or a single object version from column->value map
	 * @param segmentTable
	 * @param rs
	 * @param handler
	 * @param resolver
	 * @return
	 * @throws SQLException
	 */
	public static List<GraphObject> fromSql(SegmentTable segmentTable, ResultSet rs, List<Map<String, Object>> objectsRows, GraphObjectsStreamHandler handler, SQLGraphObjectResolver resolver) throws SQLException {

		if(rs != null && objectsRows != null) throw new RuntimeException("Cannot use both ResultSet and objectsRows object simultaneously");
		
		boolean resolveFullText = resolver == null;
		
		Map<String, Map<String, WrappedProperty>> m = handler == null ? new HashMap<String, Map<String, WrappedProperty>>() : null;
		
		//limit, especially important huge dump
		int processedURIsLimit = 50000;
		Set<String> processedURIs = handler != null ? new LinkedHashSet<String>(processedURIsLimit) : null;
		Map<String, WrappedProperty> currentObject = handler != null ? new HashMap<String, WrappedProperty>() : null;
		
		String previousURI = null;
		
		int i = 0;
		
		while ( (objectsRows != null && i < objectsRows.size()) || ( rs != null && rs.next() ) ) {
			
			Map<String, Object> row = null;
			if(objectsRows != null) {
				row = objectsRows.get(i);
			}
			
			i++;
				
			String uri = getString(rs, row, COLUMN_URI);
			
			if(handler != null) {
				if(previousURI != null && !uri.equals(previousURI)) {
					
					handler.onGraphObject(propsToObject(previousURI, currentObject, true));
					
					if(processedURIs.size() == processedURIsLimit) {
						Iterator<String> iterator = processedURIs.iterator();
						iterator.next();
						iterator.remove();
					}
					processedURIs.add(previousURI);
					if(processedURIs.contains(uri)) {
						throw new RuntimeException("The stream version query was not sorted by column " + COLUMN_URI + " !");
					}
					
					//keep the processedURIs cache 
					
					currentObject.clear();
				}
				
				
			}
			
			boolean external = getBoolean(rs, row, COLUMN_EXTERNAL);
			
			String propertyName = getString(rs, row, COLUMN_NAME);
			
			Map<String, WrappedProperty> obj = handler != null ? currentObject : m.get(uri);
			
			if(obj == null) {
				obj = new HashMap<String, WrappedProperty>();
				m.put(uri, obj);
			}
			
//			
			Boolean singleBoolean = null;
			Boolean multiBoolean = null;
			Object singleBooleanVal = getObject(rs, row, COLUMN_VALUE_BOOLEAN);
			Object multiBooleanVal = getObject(rs, row, COLUMN_VALUE_BOOLEAN_MULTIVALUE);

			if(singleBooleanVal != null) {
				if(singleBooleanVal instanceof Boolean) {
					singleBoolean = (Boolean) singleBooleanVal;
				} else if(singleBooleanVal instanceof Integer) {
					singleBoolean = ((Integer)singleBooleanVal).intValue() > 0;
				} else {
					throw new RuntimeException("Unexpected single boolean column value: " + singleBooleanVal.getClass());
				}
			} else if(multiBooleanVal != null) {
				if(multiBooleanVal instanceof Boolean) {
					multiBoolean = (Boolean) multiBooleanVal;
				} else if(multiBooleanVal instanceof Integer) {
					multiBoolean = ((Integer)multiBooleanVal).intValue() > 0;
				} else {
					throw new RuntimeException("Unexpected multi boolean column value: " + multiBooleanVal.getClass());
				}
			}
			
			Long singleDate = getLong(rs, row, COLUMN_VALUE_DATE);
			Long multiDate = getLong(rs, row, COLUMN_VALUE_DATE_MULTIVALUE);
			
			Double singleDouble = getDouble(rs, row, COLUMN_VALUE_DOUBLE);
			Double multiDouble = getDouble(rs, row, COLUMN_VALUE_DOUBLE_MULTIVALUE);
			
			Float singleFloat = getFloat(rs, row, COLUMN_VALUE_FLOAT);
			Float multiFloat = getFloat(rs, row, COLUMN_VALUE_FLOAT_MULTIVALUE);
			
			String singleGeo = getString(rs, row, COLUMN_VALUE_GEOLOCATION);
			String multiGeo = getString(rs, row, COLUMN_VALUE_GEOLOCATION_MULTIVALUE);
			
			Integer singleInteger = getInteger(rs, row, COLUMN_VALUE_INTEGER);
			Integer multiInteger = getInteger(rs, row, COLUMN_VALUE_INTEGER_MULTIVALUE);
			
			Long singleLong = getLong(rs, row, COLUMN_VALUE_LONG);
			Long multiLong= getLong(rs, row, COLUMN_VALUE_LONG_MULTIVALUE);
			
			String singleOther = getString(rs, row, COLUMN_VALUE_OTHER);
			String multiOther = getString(rs, row, COLUMN_VALUE_OTHER_MULTIVALUE);
			
			String singleString = getString(rs, row, COLUMN_VALUE_STRING);
			String multiString = getString(rs, row, COLUMN_VALUE_STRING_MULTIVALUE);
			
			Integer singleTruth = getInteger(rs, row, COLUMN_VALUE_TRUTH);
			Integer multiTruth = getInteger(rs, row, COLUMN_VALUE_TRUTH_MULTIVALUE);
			
			String singleURI = getString(rs, row, COLUMN_VALUE_URI);
			String multiURI = getString(rs, row, COLUMN_VALUE_URI_MULTIVALUE);
			
			IProperty v = null;
			
			boolean multiValue = false;
			
			if(singleBoolean != null) {
				
				v = new BooleanProperty(singleBoolean.booleanValue());
				
			} else if(multiBoolean != null) {
				
				multiValue = true;
				v = new BooleanProperty(multiBoolean.booleanValue());
				
			} else if(singleDate != null) {
				
				v = new DateProperty(singleDate);
				
			} else if(multiDate != null) {
				
				multiValue = true;
				v = new DateProperty(multiDate);
				
			} else if(singleDouble != null) {
				
				v = new DoubleProperty(singleDouble);
				
			} else if(multiDouble != null) {
				
				multiValue = true;
				v = new DoubleProperty(multiDouble);
			
			} else if(singleFloat != null) {
				
				v = new FloatProperty(singleFloat);
				
			} else if(multiFloat != null) {
				
				multiValue = true;
				v = new FloatProperty(multiFloat);
				
			} else if(singleGeo != null) {
				
				v = GeoLocationProperty.fromRDFString(singleGeo);
				
			} else if(multiGeo != null) {
				
				multiValue = true;
				v = GeoLocationProperty.fromRDFString(multiGeo);
				
			} else if(singleInteger != null) {
				
				v = new IntegerProperty(singleInteger);
				
			} else if(multiInteger != null) {
				
				multiValue = true;
				v = new IntegerProperty(multiInteger);
				
			} else if(singleLong != null) {
				
				v = new LongProperty(singleLong);
				
			} else if(multiLong != null) {
				
				multiValue = true;
				v = new LongProperty(multiLong);
				
			} else if(singleOther != null) {
				
				if(FULL_STRING_LABEL.equals(singleOther)) {
					
					if(resolveFullText) {
						
						singleOther = getString(rs, row, COLUMN_VALUE_FULL_TEXT);
						
						v = OtherProperty.fromRDFString(singleOther);
						
					} else {
						
						resolver.putGraphObjectPropertyToGet(segmentTable, uri, propertyName);
						
					}
					
				} else {
					
					v = OtherProperty.fromRDFString(singleOther);
					
				}
				

			} else if(multiOther != null) {
				
				if(FULL_STRING_LABEL.equals(multiOther)) {
					
					if(resolveFullText) {
						
						multiOther = getString(rs, row, COLUMN_VALUE_FULL_TEXT);
						
						multiValue = true;
						v = OtherProperty.fromRDFString(multiOther);
						
					} else {

						resolver.putGraphObjectPropertyToGet(segmentTable, uri, propertyName);
						
					}
					
				} else {
					
					multiValue = true;
					v = OtherProperty.fromRDFString(multiOther);
					
				}
				
				
			} else if(singleString != null) {
				
				if(FULL_STRING_LABEL.equals(singleString)) {
					
					if(resolveFullText) {
						
						singleString = getString(rs, row, COLUMN_VALUE_FULL_TEXT);
						
						v = new StringProperty(singleString);
						
					} else {
						
						resolver.putGraphObjectPropertyToGet(segmentTable, uri, propertyName);
						
					}
					
				} else {
					
					v = new StringProperty(singleString);
					
				}
				
				
			} else if(multiString != null) {

				if(FULL_STRING_LABEL.equals(multiString)) {
					
					if(resolveFullText) {
						
						multiString= getString(rs, row, COLUMN_VALUE_FULL_TEXT);
						
						multiValue = true;
						
						v = new StringProperty(multiString);
						
					} else {
						
						resolver.putGraphObjectPropertyToGet(segmentTable, uri, propertyName);
						
					}
					
				} else {
					
					multiValue = true;
					
					v = new StringProperty(multiString);
					
				}
				
			} else if(singleTruth != null) {
				
				v = new TruthProperty(Truth.fromInteger(singleTruth));
				
			} else if(multiTruth != null) {
				
				multiValue = true;
				v = new TruthProperty(Truth.fromInteger(multiTruth));
				
			} else if(singleURI != null) {
				
				v = new URIProperty(singleURI);
				
			} else if(multiURI != null) {
				
				multiValue = true;
				v = new URIProperty(multiURI);
				
			} else {
				
				throw new RuntimeException("No value in sql row");
				
			}
			
			if(v == null) continue;
			
			WrappedProperty wp = obj.get(propertyName);
			
			if(wp != null) {
				
				if(!(wp.property instanceof MultiValueProperty)) {
					log.warn("More than 1 property for object with: " + uri + " property " + propertyName + "  value: " + wp.property);
//					throw new RuntimeException("Property value shoud be multivalue: " + propertyName + " " + wp.property);
				} else {
					
					MultiValueProperty mvp = (MultiValueProperty) wp.property;
					List<IProperty> props = new ArrayList<IProperty>(mvp);
					props.add(v);
					
					obj.put(propertyName, new WrappedProperty(new MultiValueProperty(props), external));
					
				}
				
			} else {
				
				if(multiValue) {
					MultiValueProperty mvp = new MultiValueProperty(Arrays.asList(v));
					obj.put(propertyName, new WrappedProperty(mvp, external));
				} else {
					obj.put(propertyName, new WrappedProperty(v, external));
				}
				
				
			}
			
			previousURI = uri;
			
		}
		
		
		if(handler != null) {
			
			if(currentObject.size() > 0) {
				if(processedURIs.contains(previousURI)) throw new RuntimeException("The stream version query was not sorted by column " + COLUMN_URI + " !");
				
				handler.onGraphObject(propsToObject(previousURI, currentObject, true));
				
			}
			handler.onNoMoreObjects();
			
			return null;
			
		} else {
			
			List<GraphObject> res = new ArrayList<GraphObject>(m.size());
			
			for(Entry<String, Map<String, WrappedProperty>> entry : m.entrySet()) {
				
				String URI = entry.getKey();
				
				Map<String, WrappedProperty> props = entry.getValue();
				
				res.add(propsToObject(URI, props, false));
				
			}
			
			return res;
		}
		
		
	}
	
	private static Integer getInteger(ResultSet rs, Map<String, Object> row, String column) throws SQLException {
		if(rs != null) {
			Number n = (Number) rs.getObject(column);
			if(n != null) return n.intValue();
			return null;
		} else {
			Object v = row.get(column);
			if(v == null) return null;
			if(v instanceof Number) {
				return ((Number)v).intValue();
			} else {
				throw new RuntimeException("Couldn't convert column `" + column + "` value into integer: " + v);
			}			
		}
	}
	private static Float getFloat(ResultSet rs, Map<String, Object> row, String column) throws SQLException {

		if(rs != null) {
			Number n = (Number) rs.getObject(column);
			if(n != null) return n.floatValue();
			return null;
		} else {
			Object v = row.get(column);
			if(v == null) return null;
			if(v instanceof Number) {
				return ((Number)v).floatValue();
			} else {
				throw new RuntimeException("Couldn't convert column `" + column + "` value into float: " + v);
			}
		}
		
	}
	
	private static Double getDouble(ResultSet rs, Map<String, Object> row, String column) throws SQLException {
		if(rs != null) {
			Number n = (Number) rs.getObject(column);
			if(n != null) return n.doubleValue();
			return null;
		} else {
			
			Object v = row.get(column);
			if(v == null) return null;
			if(v instanceof Number) {
				return ((Number)v).doubleValue();
			} else {
				throw new RuntimeException("Couldn't convert column `" + column + "` value into double: " + v);
			}
			
		}
	}
	private static Long getLong(ResultSet rs, Map<String, Object> row, String column) throws SQLException {
		if(rs != null) {
			Number n = (Number) rs.getObject(column);
			if(n != null) return n.longValue();
			return null;
		} else {
			
			Object v = row.get(column);
			if(v == null) return null;
			if(v instanceof Number) {
				return ((Number)v).longValue();
			} else {
				throw new RuntimeException("Couldn't convert column `" + column + "` value into long: " + v);
			}
			
		}
	}
	private static Object getObject(ResultSet rs, Map<String, Object> row, String column) throws SQLException {

		if(rs != null) {
			return rs.getObject(column);
		} else {
			return row.get(column);
		}
		
	}
	private static boolean getBoolean(ResultSet rs, Map<String, Object> row, String column) throws SQLException {
		if(rs != null) {
			return rs.getBoolean(column);
		} else {
			Object obj = row.get(column);
			if(obj == null) return false;
			if(!(obj instanceof Boolean)) throw new RuntimeException("Column `" + column + "` value couldn't be mapped to boolean: " + obj);
			return ((Boolean)obj).booleanValue();
		}
	}
	private static String getString(ResultSet rs, Map<String, Object> row, String column) throws SQLException {
		if(rs != null) {
			return rs.getString(column);
		} else {
			return (String) row.get(column);
		}
	}
	@SuppressWarnings("rawtypes")
	private static GraphObject propsToObject(String URI, Map<String, WrappedProperty> props, boolean ignoreBrokenDataErrors) {
		
		WrappedProperty typeProp = props.get(VitalCoreOntology.vitaltype.getURI());
		
		if(typeProp == null) throw new RuntimeException("No VitalCoreOntology.vitaltype.getURI() property, URI: " + URI);
		
		String vitalRDFType = ((URIProperty)typeProp.property).get();
		
		WrappedProperty ontologyIRIProp = props.get(VitalCoreOntology.hasOntologyIRI.getURI());
		WrappedProperty versionIRIProp = props.get(VitalCoreOntology.hasVersionIRI.getURI());
		String ontologyIRI = ontologyIRIProp != null ? ((URIProperty)ontologyIRIProp.property).get() : null;
		String versionIRI = versionIRIProp != null ? ((StringProperty)versionIRIProp.property).toString() : null;

		
		Map<String, String> oldVersions = null;
        
		
		if(ontologyIRI != null && versionIRI != null) {

		    DomainOntology _do = new DomainOntology(ontologyIRI, versionIRI);
		    
		    DomainOntology _cdo = null;
		    
		    
		    //first check if it's not a temporary loaded older version
		    for(DomainOntology d : VitalSigns.get().getDomainList()) {
		        
		        VersionedDomain vns = VersionedDomain.analyze(d.getUri());
		        
		        if(vns.versionPart != null) {
		            String ontURI = vns.domainURI;
                    if( ontURI.equals(ontologyIRI) ) {
		                if(_do.compareTo(d) == 0) {
		                    _cdo = d;
		                    
		                    oldVersions = new HashMap<String, String>();
		                    oldVersions.put(ontURI, d.getUri());
		                    //collect imports tree
		                    List<String> imports = VitalSigns.get().getOntologyURI2ImportsTree().get(d.getUri());
		                    
		                    if(imports != null) {
		                        for(String i : imports) {
		                            VersionedDomain ivd = VersionedDomain.analyze(i);
		                            if(ivd.versionPart != null) {
		                                oldVersions.put(ivd.domainURI, i);
		                            }
		                        }
		                    }
		                    
		                    break;
		                }
		            }
	            }
		    }

		    if(_cdo == null) {
                _cdo = VitalSigns.get().getDomainOntology(ontologyIRI);
                
                if(_cdo != null) {
                    
                    int comp = _do.compareTo(_cdo);
                    
                    if( comp != 0 ) {
                        
                        if( VitalSigns.get().getConfig().versionEnforcement == VersionEnforcement.strict ) {
                            
                            
                            boolean backwardCompatible = false;
                            
                            String backwardMsg = "";
                            
                            //give it a try
                            if(comp < 1 && _cdo.getBackwardCompatibleVersion() != null) {
                                
                                comp = _do.compareTo( _cdo.getBackwardCompatibleVersion());
                                    
                                if(comp >= 0) {
                                    
                                    backwardCompatible = true;
                                    
                                } else {
                                    
                                    backwardMsg = " nor its backward compatible version: " + _cdo.getBackwardCompatibleVersion().toVersionString();
                                    
                                }
                                
                            }
                            
                            if(!backwardCompatible) 
                                throw new RuntimeException("Strict version mode - persisted object domain " + ontologyIRI + " version " + _do.toVersionString() + " does not match currently loaded: " + _cdo.toVersionString() + backwardMsg);
                            
                        }
                        
                    }
                    
                }
                

            }
		    
			
			if(_cdo != null) {
				
			} else {
			
				log.error("Domain ontology with IRI not found: " + ontologyIRI + ", object class (RDF): " + vitalRDFType + ", uri: " + URI);
				
			}
			
		}
		
		vitalRDFType = toOldVersion(vitalRDFType, oldVersions);
		
		Set<String> rdfTypes = new HashSet<String>();
		WrappedProperty typesVal = props.get(VitalCoreOntology.types.getURI());
		
		if(typesVal == null) throw new RuntimeException("No " + VitalCoreOntology.types.getURI() + " properties found, vitaltype: " + vitalRDFType + " uri: " + URI);
		
		for( Object p : ((MultiValueProperty)typesVal.property) ) {
			rdfTypes.add(((URIProperty)p).get());
		}
		
        //deserialize in the same manner as in rdf
        List<String> types = null;
        
        //do the inferencing part, types hierarchy is cached, should it be
        
        for(String rdfType : rdfTypes) {

            rdfType = toOldVersion(rdfType, oldVersions);
            
            if(rdfType.equals(vitalRDFType)) continue;
            
            if(types == null) {
                types = new ArrayList<String>();
                types.add(vitalRDFType);
            }
            
            types.add(rdfType);
            
        }
        
        
        
        ClassMetadata cm = VitalSigns.get().getClassesRegistry().getClass(vitalRDFType);
        if(cm == null) {
        	if(!ignoreBrokenDataErrors) throw new RuntimeException("Class not found in VitalSigns: " + vitalRDFType);
        	log.error("Class not found in VitalSigns: " + vitalRDFType);
        	return null;
        }
		
		
//			Class cls = VitalSigns.get().getGroovyClass(rdfClazz)
//			if(cls == null) throw new IOException("No groovy class for URI found: ${rdfClazz}");
				
		GraphObject object = null;
		try {
			object = cm.getClazz().newInstance();
		} catch (Exception e) {
			if(!ignoreBrokenDataErrors) throw new RuntimeException(e);
			log.error(e.getLocalizedMessage());;
			return null;
		}
		
		object.setURI(URI);
		if(types != null) {
			object.set(Property_types.class, types);
		}

		
		for(Entry<String, WrappedProperty> e : props.entrySet() ) {
			
			String n = e.getKey();
			
			if(processedProperties.contains(n)) continue;
			

			WrappedProperty value2 = e.getValue();
			
			if(value2.external) {
					
			    if( !(object instanceof VITAL_GraphContainerObject) && ! VitalSigns.get().getConfig().externalProperties) {
			        throw new RuntimeException("Cannot deserialize an object with external properties - they are disabled, property: " + n);
			    }

			    if(ignoreBrokenDataErrors) {
				    try {
				    	object.setProperty(n, value2.property);
	 				} catch(Exception ex) {
	 					log.error(ex.getLocalizedMessage());
	 				}
			    } else {
			    	object.setProperty(n, value2.property);
			    }
			    
			} else {

//			    String originalFieldName = n;
			    
			    n = toOldVersion(n, oldVersions);
				
				PropertyMetadata prop = VitalSigns.get().getPropertiesRegistry().getProperty(n);
				
				if(prop == null) {
				    
                    if( VitalSigns.get().getConfig().versionEnforcement != VersionEnforcement.lenient ) {
                        
                    	if(!ignoreBrokenDataErrors) {
                    		throw new RuntimeException("Property not found : " + n + " " + object.getClass().getCanonicalName());
                    	} else {
                    		log.error("Property not found : " + n + " " + object.getClass().getCanonicalName());
                    		continue;
                    	}
                    	
                        
                    } else {
                        
                        log.warn("Property not found: " + n + " " + object.getClass().getCanonicalName());
                        
                    }
                    
					//ignore such errors - assumed external properties
//						throw new IOException("Property with URI not found: " + fn);
					continue;
					
				}
				
				if(ignoreBrokenDataErrors) {
					try {
						object.setProperty(prop.getShortName(), value2.property);
					} catch(Exception ex) {
						log.error(ex.getLocalizedMessage());
					}
				} else {
					object.setProperty(prop.getShortName(), value2.property);
				}
				
			}
			
			
		}
		
		return object;
			
		
	}

	public static String toOldVersion(String input,
            Map<String, String> oldVersions) {
        
	    if(input == null) return null;
	    
	    if(oldVersions == null) return input;
	    
	    for(Entry<String, String> e : oldVersions.entrySet()) {
	        
	        input = input.replace(e.getKey() + '#', e.getValue() + '#');
	        
	    }
	    
        return input;
    }

	public static String oldVersionFilter(String input) {

		if (input == null)
			return null;

		VersionedURI vu = VersionedURI.analyze(input);
		return vu.versionlessURI;

	}
}
