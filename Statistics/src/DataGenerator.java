import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataGenerator {

	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException,
			URISyntaxException {
		final int COUNT = 1000000;
		
		Path pt = new Path(
				"hdfs://bivm.ibm.com:9000/user/biadmin/Statistics_Input/test.txt");
		FileSystem fs = FileSystem.get(new Configuration());

		BufferedWriter br = null;
		
		if (fs.exists(pt)) {
			br = new BufferedWriter(new OutputStreamWriter(
					fs.append(pt)));
		} else {
			br = new BufferedWriter(new OutputStreamWriter(
					fs.create(pt, true)));
		}
		
		// TO append data to a file, use fs.append(Path f)
		int flag = 1;
		while (flag <= COUNT) {
			int number = (int) (Math.random() * 20);
//			System.out.println(flag + " -- " + number);
			br.append(""+number+"\n");
			flag++;
		}
		br.close();
		System.out.println("Done");
	}
}
