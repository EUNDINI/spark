import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.*;
import java.text.*;

public final class UBERStudent20180955 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: UBER <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20180955")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
	
	FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
		public Iterator<String> call(String s) {
			StringTokenizer tokenizer = new StringTokenizer(s, ",");
			int i = 0;
			String result = "";
			
                        while(tokenizer.hasMoreTokens()) {
                                if (i == 0) {
					result += tokenizer.nextToken() + ",";
				}
				else if (i == 1) {
					String[] week = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
					SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
					Calendar cal = Calendar.getInstance();
					Date getDate;
					
					try {
						getDate = format.parse(tokenizer.nextToken());
						cal.setTime(getDate);
						int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
						result += week[w] + ",";
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				else if (i == 2) {
					result += tokenizer.nextToken() + ",";
				}
				else {
					result += tokenizer.nextToken();
				}
				i++;
                        }
			
			return Arrays.asList(result).iterator();
		}
	};
	JavaRDD<String> words = lines.flatMap(fmf);
	
	PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
		public Tuple2<String, String> call(String s) {
			String[] data = s.split(",");
			String key = data[0] + "," + data[1];
			String value = data[3] + "," + data[2];
			return new Tuple2(key, value);
		}
	};
	JavaPairRDD<String, String> ones = words.mapToPair(pf);

	Function2<String, String, String> f2 = new Function2<String, String, String>() {
		public String call(String x, String y) {
			String[] x_data = x.split(",");
			String[] y_data = y.split(",");
			
			int[] result = new int[2];
			result[0] = Integer.parseInt(x_data[0]) + Integer.parseInt(y_data[0]);
			result[1] = Integer.parseInt(x_data[1]) + Integer.parseInt(y_data[1]);
			
			String output = Integer.toString(result[0]) + "," + Integer.toString(result[1]); 
			return output;
		}
	};
	JavaPairRDD<String, String> counts = ones.reduceByKey(f2);
	
	counts.saveAsTextFile(args[1]);
	
        spark.stop();
    }
}
