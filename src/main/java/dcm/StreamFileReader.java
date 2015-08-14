package dcm;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;


public class StreamFileReader {

	Iterator<CSVRecord> iterator=null;
	private String inputFilePath;
	private Reader csvData;
	int currentTime;
	List<Double[]> pointsPrimitive = new ArrayList<Double[]>();
	Double[] hangingVector = null;
	private long clusteringTime=0;
	private long startTime=0;
	private CSVParser records;
	public StreamFileReader() {
	}
	public StreamFileReader(String inputFilePath) throws FileNotFoundException {
		File file = new File(inputFilePath);
		int count=0;
		if(!file.exists()){
			throw new FileNotFoundException(inputFilePath);
		}
		csvData = null;
		try {
			csvData = new FileReader(file);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		try {
			records = CSVFormat.RFC4180.withHeader("oid","t","lat","long").parse(csvData);
			iterator = records.iterator();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		currentTime=-1;
	}

	public List<Double[]> getNextPointsAsDoubleArray(long t){
		startTime = System.currentTimeMillis();
		pointsPrimitive=new ArrayList<Double[]>();
		if(hangingVector!=null){
			pointsPrimitive.add(hangingVector);
		}

		if(iterator.hasNext()==false){
			return null;	//null shows that no more data
		}
		if(currentTime>t){
			return null;
		}

        CSVRecord record = null;

		while(iterator.hasNext()) {
			record = iterator.next();
			currentTime = Double.valueOf(record.get("t")).intValue();
			if(currentTime<t){
				continue;
			}
			else if(currentTime==t){
				Double [] vector = new Double[4];
				vector[0]=Double.parseDouble(record.get("long"));
				vector[1]=Double.parseDouble(record.get("lat"));
				vector[2]=Double.parseDouble(record.get("oid"));
				vector[3]=Double.parseDouble(record.get("t"));
				pointsPrimitive.add(vector);
			}
			else if(currentTime>t){
				hangingVector = new Double[4];
				hangingVector[0]=Double.parseDouble(record.get("long"));
				hangingVector[1]=Double.parseDouble(record.get("lat"));
				hangingVector[2]=Double.parseDouble(record.get("oid"));
				hangingVector[3]=Double.parseDouble(record.get("t"));
				break;
			}
		}
		return pointsPrimitive;
	}
}
