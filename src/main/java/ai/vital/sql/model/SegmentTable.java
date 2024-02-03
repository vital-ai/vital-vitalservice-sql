package ai.vital.sql.model;

import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;

public class SegmentTable {

	public VitalSegment segment;
	
	public String tableName;

	private String segmentID;

	public SegmentTable(VitalSegment segment, String tableName) {
		super();
		this.segment = segment;
		this.segmentID = segment != null ? (String) segment.getRaw(Property_hasSegmentID.class) : null; 
		this.tableName = tableName;
	}

	public String getID() {
		return segmentID;
	}
	
	
	
}
