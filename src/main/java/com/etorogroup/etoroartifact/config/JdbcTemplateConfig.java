package com.etorogroup.etoroartifact.config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
@ConfigurationProperties(prefix = "spring.datasource")
public class JdbcTemplateConfig {

    private String url;
    private String username;
    private String password;
    private String driverClassName;

    // Getter and setter methods for the properties

    @Bean
    public JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(dataSource());
    }


    @Autowired
    private ApplicationContext applicationContext;

    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(applicationContext.getEnvironment().getProperty("spring.datasource.url"));
        dataSource.setUsername(applicationContext.getEnvironment().getProperty("spring.datasource.username"));
        dataSource.setPassword(applicationContext.getEnvironment().getProperty("spring.datasource.password"));
        dataSource.setDriverClassName(applicationContext.getEnvironment().getProperty("spring.datasource.driver-class-name"));

        System.out.println("aitorrr2 Loaded properties:");
        System.out.println("  url: " + applicationContext.getEnvironment().getProperty("spring.datasource.url"));
        System.out.println("  username: " + applicationContext.getEnvironment().getProperty("spring.datasource.username"));
        System.out.println("  password: " + applicationContext.getEnvironment().getProperty("spring.datasource.password"));
        System.out.println("  driverClassName: " + applicationContext.getEnvironment().getProperty("spring.datasource.driver-class-name"));

        return dataSource;
    }
}
