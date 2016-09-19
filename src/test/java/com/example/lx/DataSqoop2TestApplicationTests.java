package com.example.lx;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DataSqoop2TestApplicationTests {

	@Test
	public void contextLoads() {


		System.setProperty("hadoop.home.dir","F:\\git\\hadoop-common-2.2.0-bin");
		System.setProperty("user.name","hadoop");

		String url = "http://192.168.0.201:12000/sqoop/";

//		url = "http://123.207.151.200:5200/sqoop/";

		SqoopClient client = new SqoopClient(url);


		try{
			client.deleteAllLinksAndJobs();
			client.clearCache();
		}catch(Exception e){

		}

		Map<String, String> map = System.getenv();
		for(Iterator<String> itr = map.keySet().iterator(); itr.hasNext();){
			String key = itr.next();
			System.out.println(key + "=" + map.get(key));
		}

		Properties props = System.getProperties();
		props.list(System.out);


		//RDBMS link
		MLink mysqlLink = client.createLink("generic-jdbc-connector");
		mysqlLink.setName("FromMysqlLink");
		mysqlLink.setCreationUser("hadoop");
		mysqlLink.setCreationDate(new Date());


		MLinkConfig linkConfig = mysqlLink.getConnectorLinkConfig();
		linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://ip:3306/hadoopguide?autoReconnect=true&useSSL=false");
		linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");
		linkConfig.getStringInput("linkConfig.username").setValue("");
		linkConfig.getStringInput("linkConfig.password").setValue("");


		Status mysqlLinkStatus = client.saveLink(mysqlLink);

		if(mysqlLinkStatus.canProceed()){
			System.out.println("create link with name:"+mysqlLink.getName());
		}else{
			System.out.println("something went wrong creating the link");
		}

		//hdfs link
		MLink hdfsLink = client.createLink("hdfs-connector");
		hdfsLink.setName("ToHdfsLink");
		hdfsLink.setCreationUser("hadoop");
		hdfsLink.setCreationDate(new Date());

		MConfigList mConfigList = hdfsLink.getConnectorLinkConfig();
		mConfigList.getStringInput("linkConfig.confDir").setValue("/home/hadoop/hadoop-2.6.2/etc/hadoop");
//		mConfigList.getStringInput("linkConfig.confDir").setValue("/opt/hadoop-2.7.3/etc/hadoop");

		Status hdfsLinkStatus = client.saveLink(hdfsLink);
		if(hdfsLinkStatus.canProceed()){
			System.out.println("create link with name:"+hdfsLink.getName());
		}else{
			System.out.println("something went wrong creating the link");
		}


		//job
		MJob job = client.createJob("FromMysqlLink","ToHdfsLink");
		job.setName("MysqlToHdfsJob");
		job.setCreationUser("hadoop");
		job.setCreationDate(new Date());

		//from config
		MFromConfig fromJobConfig = job.getFromJobConfig();
//		fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("hadoopguide");
//		fromJobConfig.getStringInput("fromJobConfig.tableName").setValue("widgets");
		fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue("id");
		fromJobConfig.getStringInput("fromJobConfig.boundaryQuery").setValue("select min(id), max(id) from widgets");
//		fromJobConfig.getListInput("fromJobConfig.columnList").setValue(Arrays.asList("id","widget_name","price","design_date"));
		fromJobConfig.getStringInput("fromJobConfig.sql").setValue("select * from widgets where ${CONDITIONS}");

		//to config
		MToConfig toJobConfig = job.getToJobConfig();
		toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue("/home/hadoop/hadoop-2.6.2/tmp/fromRDBMS");
//		toJobConfig.getEnumInput("toJobConfig.outputFormat").setValue(ToFormat.TEXT_FILE);

		//set the driver config values
		MDriverConfig driverConfig = job.getDriverConfig();
		driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);


		Status status = client.saveJob(job);
		if(status.canProceed()) {
			System.out.println("Created Job with Job Name: "+ job.getName());
		} else {
			System.out.println("Something went wrong creating the job");
		}

		//execute job
		MSubmission submission = null;
		try {
			submission = client.startJob("MysqlToHdfsJob", new SubmissionCallback() {
                @Override
                public void submitted(MSubmission mSubmission) {

                }

                @Override
                public void updated(MSubmission mSubmission) {

                }

                @Override
                public void finished(MSubmission mSubmission) {

                }
            },3000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Job Submission Status : " + submission.getStatus());
		if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
			System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
		System.out.println("Hadoop job id :" + submission.getExternalJobId());
		System.out.println("Job link : " + submission.getExternalLink());
		Counters counters = submission.getCounters();
		if(counters != null) {
			System.out.println("Counters:");
			for(CounterGroup group : counters) {
				System.out.print("\t");
				System.out.println(group.getName());

				for(Counter counter : group) {
					System.out.print("\t\t");
					System.out.print(counter.getName());
					System.out.print(": ");
					System.out.println(counter.getValue());
				}
			}
		}
		if(submission.getError() != null) {
			System.out.println("Exception info : " +submission.getError());
		}


		//Check job status for a running job
//		MSubmission submission = client.getJobStatus("jobName");
//		if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
//			System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
//		}

		//Stop a running job
//		submission.stopJob("jobName");
	}

}
