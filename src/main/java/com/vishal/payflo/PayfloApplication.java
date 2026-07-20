package com.vishal.payflo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class PayfloApplication {

    public static void main(String[] args) {
        SpringApplication.run(PayfloApplication.class, args);
    }

}
