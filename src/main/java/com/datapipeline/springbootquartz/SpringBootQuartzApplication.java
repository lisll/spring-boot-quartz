package com.datapipeline.springbootquartz;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

//@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@SpringBootApplication
public class SpringBootQuartzApplication {

  public static void main(String[] args) {
    SpringApplication.run(SpringBootQuartzApplication.class, args);
  }

}
