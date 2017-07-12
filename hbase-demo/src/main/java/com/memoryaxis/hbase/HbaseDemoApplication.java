package com.memoryaxis.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

//@EnableSwagger2
@SpringBootApplication
@EnableAutoConfiguration
// @EnableWebMvc
@Controller
public class HbaseDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(HbaseDemoApplication.class, args);
    }

    @RequestMapping("/")
    public String index() {
        return "redirect:/doc.html";
    }

}
