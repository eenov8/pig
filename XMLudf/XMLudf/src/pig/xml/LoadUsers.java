package pig.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.pigscript.parser.ParseException;

public class LoadUsers extends LoadFunc {
	
	private RecordReader reader;
	private TupleFactory tupleFactory;
	
	public LoadUsers() {
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
		List<String> values = new ArrayList<String>();
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
	//<row Id="1" Reputation="3669" CreationDate="2009-04-30T07:08:27.067" DisplayName="Jeff Atwood" 
	//EmailHash="51d623f33f8b83095db84ff35e15dbe8" LastAccessDate="2010-11-07T08:16:06.823" 
	//WebsiteUrl="http://www.codinghorror.com/blog/" Location="El Cerrito, CA" Age="39" 
	//AboutMe="&lt;p&gt;&lt;img src=&quot;http://img377.imageshack.us/img377/4074/wargames1xr6.jpg&quot; 
		//width=&quot;250&quot;&gt;&lt;/p&gt;&#xA;&#xA;&lt;p&gt;&lt;
		//a href=&quot;http://www.codinghorror.com/blog/archives/001169.html&quot; rel=&quot;nofollow
		//&quot;&gt;Stack Overflow Valued Associate #00001&lt;/a&gt;&lt;/p&gt;&#xA;&#xA;&lt;p&gt;
		//Wondering how our software development process works? &lt;a href=&quot;http://www.youtube.com/watch?v=08xQLGWTSag&quot; 
		//rel=&quot;nofollow&quot;&gt;Take a look!&lt;/a&gt;&lt;/p&gt;&#xA;" 
	//Views="3107" UpVotes="1745" DownVotes="29" />
	public void parseListItem(List<String> values, XMLStreamReader xmlReader) throws ParseException {
		
		String id = xmlReader.getAttributeValue(null, "Id");
        String parentId = xmlReader.getAttributeValue(null, "ParentId");
        String postTypeId = xmlReader.getAttributeValue(null, "PostTypeId");
        String acceptedAnswerId = xmlReader.getAttributeValue(null, "AcceptedAnswerId");
        String creationDate = xmlReader.getAttributeValue(null, "CreationDate");
        String score = xmlReader.getAttributeValue(null, "Score");
        String viewCount = xmlReader.getAttributeValue(null, "ViewCount");
        String body = xmlReader.getAttributeValue(null, "Body");
        String ownerUserId = xmlReader.getAttributeValue(null, "OwnerUserId");
        String lastEditorUserId = xmlReader.getAttributeValue(null, "LastEditorUserId");
        String lastEditorDisplayName = xmlReader.getAttributeValue(null, "LastEditorDisplayName");
        String lastEditDate = xmlReader.getAttributeValue(null, "LastEditDate");
        String lastActivityDate = xmlReader.getAttributeValue(null, "LastActivityDate");
        String title = xmlReader.getAttributeValue(null, "Title");
        String tags = xmlReader.getAttributeValue(null, "Tags");
        String answerCount = xmlReader.getAttributeValue(null, "AnswerCount");
        String commentCount = xmlReader.getAttributeValue(null, "CommentCount");
        String favoriteCount = xmlReader.getAttributeValue(null, "FavoriteCount");
        
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
