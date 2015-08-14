package dcm;


import graph.model.Edge;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;


public class StreamFileReader {

	private Iterator<CSVRecord> iterator=null;

	public StreamFileReader(String inputFilePath) throws FileNotFoundException {
		File file = new File(inputFilePath);
		if(!file.exists()){
			throw new FileNotFoundException(inputFilePath);
		}
		Reader csvData = null;
		try {
			csvData = new FileReader(file);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		try {
			CSVParser records = CSVFormat.RFC4180.withHeader("source", "target", "property").parse(csvData);
			iterator = records.iterator();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public Edge getNextEdge(){
		if(!iterator.hasNext()){
			return null;	//null shows that no more data
		}
		CSVRecord record;
		record = iterator.next();
		Long source = Long.valueOf(record.get("source"));
		Long target = Long.valueOf(record.get("target"));
		Long property = Long.valueOf(record.get("property"));
		return new Edge(source,target,property);
	}
}
