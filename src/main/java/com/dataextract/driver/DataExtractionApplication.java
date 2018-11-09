package com.dataextract.driver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan("com.dataextract.*")
@SpringBootApplication
public class DataExtractionApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataExtractionApplication.class, args);
	}
}
