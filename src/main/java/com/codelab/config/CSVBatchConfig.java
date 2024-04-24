package com.codelab.config;

import com.codelab.model.Customer;
import com.codelab.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@AllArgsConstructor
public class CSVBatchConfig {


    private  final CustomerRepository customerRepository;

    // Create ItemReader
    @Bean
    public FlatFileItemReader<Customer> customerReader(){

        FlatFileItemReader<Customer> itemReader=new FlatFileItemReader<>();

        itemReader.setResource(new FileSystemResource("D://Springboot-Workbench/customers.csv"));
        itemReader.setName("csv-reader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());

        return itemReader;

    }

    private LineMapper<Customer> lineMapper() {

        DefaultLineMapper<Customer> lineMapper=new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(true);
        lineTokenizer.setNames("id","firstName","lastName","email","gender","contactNo","country","dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper=new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }


    // Create ItemProcessor
    @Bean
    public CustomerProcessor customerProcessor(){

        return new CustomerProcessor();
    }

    // Create ItemWriter
    @Bean
    public RepositoryItemWriter<Customer> customerWriter(){

        RepositoryItemWriter<Customer> repositoryWriter=new RepositoryItemWriter<>();
        repositoryWriter.setRepository(customerRepository);
        repositoryWriter.setMethodName("save");

        return repositoryWriter;
    }

    // Create Step

    @Bean
    public Step step(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){

        return new StepBuilder("csv-step", jobRepository)
                .<Customer,Customer>chunk(10, platformTransactionManager)
                .allowStartIfComplete(true)
                .reader(customerReader())
                .processor(customerProcessor())
                .writer(customerWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    private TaskExecutor taskExecutor() {

        SimpleAsyncTaskExecutor asyncTaskExecutor=new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }

    // Create Job

    @Bean
    public Job runJob(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){

        return new JobBuilder("writeInToCustomerDb", jobRepository)
                .flow(step(jobRepository, platformTransactionManager))
                .end()
                .build();
    }


}
