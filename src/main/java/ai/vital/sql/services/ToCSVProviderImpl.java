package ai.vital.sql.services;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import ai.vital.sql.model.VitalSignsToSqlBridge;
import ai.vital.sql.model.VitalSignsToSqlBridge.OutputType;
import ai.vital.vitalsigns.csv.ToCSVProvider;
import ai.vital.vitalsigns.model.GraphObject;

public class ToCSVProviderImpl implements ToCSVProvider {

	String headersCached = null;
	
	public static CSVFormat format = CSVFormat.DEFAULT;
	
	@Override
	public String getHeaders() {

		if(headersCached == null) {
			
			List<String> l = new ArrayList<String>(VitalSignsToSqlBridge.columns);
			l.add(0, VitalSignsToSqlBridge.COLUMN_ID);
			
			headersCached = format.format(l.toArray());
			
//			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//			CsvWriter writer = new CsvWriter(byteArrayOutputStream, ',', StandardCharsets.UTF_8);
//			
//			try {
//				writer.writeRecord(VitalSignsToSqlBridge.columns.toArray(new String[0]));
//				headersCached = byteArrayOutputStream.toString(StandardCharsets.UTF_8.name()).trim();
//			} catch (IOException e) {
//				throw new RuntimeException(e);
//			}
			
		}
		
		return headersCached;
	}

	@Override
	public void toCSV(GraphObject arg0, List<String> arg1) {

		List<String> processed = null;
		try {
			processed = VitalSignsToSqlBridge.batchInsertGraphObjects(null, null, null, Arrays.asList(arg0), OutputType.CSV);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		arg1.addAll(processed);

	}

}
