package delta_service.config;

import delta_service.web.RootController;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

@Configuration
@EnableWebMvc
@ComponentScan(basePackageClasses = RootController.class)
public class WebConfiguration extends WebMvcConfigurationSupport {


}
