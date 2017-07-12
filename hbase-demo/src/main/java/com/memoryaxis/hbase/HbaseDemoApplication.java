package com.memoryaxis.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
@EnableAutoConfiguration
@EnableSwagger2
@Controller
public class HbaseDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(HbaseDemoApplication.class, args);
    }

    @RequestMapping("/")
    public String index() {
        return "redirect:/swagger-ui.html";
    }

}
