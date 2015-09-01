package pig.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.pigscript.parser.ParseException;

public class Xml extends LoadFunc {
	
	private RecordReader reader;
	private TupleFactory tupleFactory;
	
	public Xml() {
		tupleFactory = TupleFactory.getInstance();
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return new TextInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		Tuple tuple = null;
		List<Object> values = new ArrayList<Object>();
		try {
			boolean notDone = reader.nextKeyValue();
			if (!notDone) {
				return null;
			}
			Text value = (Text) reader.getCurrentValue();
			if (value != null) {
				try {
					InputStream is = new ByteArrayInputStream(value.toString()
							.getBytes("UTF-8"));
					XMLInputFactory inFactory = XMLInputFactory.newInstance();
					XMLStreamReader xmlReader = inFactory
							.createXMLStreamReader(is);
					try {
						while (xmlReader.hasNext()) {
							if (xmlReader.next() == XMLStreamConstants.START_ELEMENT) {
								if (xmlReader.getLocalName().equals("row")) {
									parseListItem(values, xmlReader);
								}
							}
						}
					} finally {
						xmlReader.close();
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				tuple = tupleFactory.newTuple(values);
			}

		} catch (InterruptedException e) {
			// add more information to the runtime exception condition.
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}

		return tuple;
	}
	
	public void parseListItem(List<Object> values, XMLStreamReader xmlReader) throws ParseException {
		
		Object id = xmlReader.getAttributeValue(null, "Id");
		Object parentId = xmlReader.getAttributeValue(null, "ParentId");
		Object postTypeId = xmlReader.getAttributeValue(null, "PostTypeId");
		Object acceptedAnswerId = xmlReader.getAttributeValue(null, "AcceptedAnswerId");
		Object creationDate = xmlReader.getAttributeValue(null, "CreationDate");
		Object score = xmlReader.getAttributeValue(null, "Score");
		Object viewCount = xmlReader.getAttributeValue(null, "ViewCount");
		Object body = xmlReader.getAttributeValue(null, "Body");
		Object ownerUserId = xmlReader.getAttributeValue(null, "OwnerUserId");
		Object lastEditorUserId = xmlReader.getAttributeValue(null, "LastEditorUserId");
		Object lastEditorDisplayName = xmlReader.getAttributeValue(null, "LastEditorDisplayName");
		Object lastEditDate = xmlReader.getAttributeValue(null, "LastEditDate");
		Object lastActivityDate = xmlReader.getAttributeValue(null, "LastActivityDate");
		Object title = xmlReader.getAttributeValue(null, "Title");
		DataByteArray tags = new DataByteArray(xmlReader.getAttributeValue(null, "Tags"));
		//byte[] tags = xmlReader.getAttributeValue(null, "Tags").getBytes(Charset.forName("UTF-8"));
		Object answerCount = xmlReader.getAttributeValue(null, "AnswerCount");
		Object commentCount = xmlReader.getAttributeValue(null, "CommentCount");
		Object favoriteCount = xmlReader.getAttributeValue(null, "FavoriteCount");
        
        values.add(id);
        values.add(parentId);
        values.add(postTypeId);
        values.add(acceptedAnswerId);
        values.add(creationDate);
        values.add(score);
        values.add(viewCount);
        values.add(body);
        values.add(ownerUserId);
        values.add(lastEditorUserId);
        values.add(lastEditorDisplayName);
        values.add(lastEditDate);
        values.add(lastActivityDate);
        values.add(title);
        values.add(tags);
        values.add(answerCount);
        values.add(commentCount);
        values.add(favoriteCount);
        
    }
	
	@Override
	public void prepareToRead(RecordReader arg0, PigSplit arg1)
			throws IOException {
		// TODO Auto-generated method stub
		this.reader = arg0;
	}

	@Override
	public void setLocation(String arg0, Job arg1) throws IOException {
		// TODO Auto-generated method stub
		FileInputFormat.setInputPaths(arg1, arg0);
	}

}
