package com.batch.filereader;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
@EnableAutoConfiguration
public class BatchConfig extends DefaultBatchConfigurer {   

	@Autowired
    private JobBuilderFactory jobBuilderFactory;
 
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    
    @Bean
    public Step firstStep(){
        return stepBuilderFactory.get("firstStep")
                .tasklet(new BatchFileReader())
                .build();
    }
    
    @Bean
    public Job filreReaderJob(){
        return jobBuilderFactory.get("filreReaderJob")
                .incrementer(new RunIdIncrementer())
                .start(firstStep())
                .build();
    }
    
    @Override
    public void setDataSource(DataSource dataSource) {
        //This BatchConfigurer ignores any DataSource
    }
	

}
