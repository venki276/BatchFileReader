package com.batch.filereader;

import java.util.Properties;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class SpringBatchBoot {
     
    public static void main(String[] args) throws Exception {
    	if(args.length == 2){
    		Properties property = new Properties();
            property.put("filePath", args[0]);
            property.put("numberOfThreads", args[1]);
            ConfigurableApplicationContext context = SpringApplication.run(SpringBatchBoot.class, args);
            JobLauncher jobLauncher = context.getBean("jobLauncher", JobLauncher.class);
    		JobParameters jobParameters = new DefaultJobParametersConverter().getJobParameters(property);
            Job job = (Job)context.getBean("filreReaderJob", Job.class);
            jobLauncher.run(job, jobParameters);
    	} else {
    		throw new Exception ("Please pass two arguments");
    	}
    }

}
