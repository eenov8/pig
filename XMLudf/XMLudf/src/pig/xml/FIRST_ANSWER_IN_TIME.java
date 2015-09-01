package pig.xml;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class FIRST_ANSWER_IN_TIME extends EvalFunc<Tuple> {
	
	private TupleFactory tupleFactory;

    public FIRST_ANSWER_IN_TIME(){
        tupleFactory = TupleFactory.getInstance();
    }
	@Override
	public Tuple exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		if (input == null || input.size() == 0)
            return null;
        try {
            List values = new ArrayList();
            String startDateStr = null;
            String firstAnswerDateStr = null;

            String questionId = (String)input.get(0);
            DataBag questionCreationDate = (DataBag)input.get(1);
            DataBag answersCreationDates = (DataBag)input.get(2);

            Tuple qCreationDate = questionCreationDate.iterator().next();

            if(qCreationDate != null){
                startDateStr = (String)qCreationDate.get(0);
            }

            firstAnswerDateStr = getFirstAnswerDate(answersCreationDates);
            long timeInSeconds = getTimeInSeconds(startDateStr,firstAnswerDateStr);
            values.add(questionId);
            values.add(timeInSeconds);
            return tupleFactory.newTuple(values);
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row ", e);
        }
	}
	
	private String getFirstAnswerDate(DataBag answersCreationDates) throws ExecException {
        List<String> dates = new ArrayList<String>();
        if(answersCreationDates != null){
            Iterator iterator = answersCreationDates.iterator();
            while(iterator.hasNext()){
                Tuple answerDate = (Tuple) iterator.next();
                if(answerDate != null){
                    dates.add((String)answerDate.get(0));
                }
            }
            Collections.sort(dates);
        }
        if(dates.size() > 0){
            return dates.get(0);
        }
        return null;
    }
    private long getTimeInSeconds(final String startDateStr, final String endDateStr) throws ParseException {
        if(startDateStr == null || endDateStr == null){
            return 0L;
        }
        Date dateStart = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.ENGLISH).parse(startDateStr);
        Date dateEnd = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.ENGLISH).parse(endDateStr);
        long timeStart = dateStart.getTime();
        long timeEnd = dateEnd.getTime();
        long seconds = (timeEnd - timeStart)/1000;
        return seconds;
    }

}
