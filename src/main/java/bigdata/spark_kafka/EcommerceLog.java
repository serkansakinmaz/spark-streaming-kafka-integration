package bigdata.spark_kafka;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EcommerceLog {
	
	private String time;
	private String productName;

	public EcommerceLog(String product) {
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		parseString(product, timeStamp);
	}
	
	
	private void parseString(String productName,String time) {
		this.time = time;
		this.productName = productName;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

}
