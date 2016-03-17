package com.objectivity.thingspan.examples.flights.spring;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class JspController {
	@RequestMapping("/jsp")
    public String test(ModelAndView modelAndView) {
        
        return "graph";
  }
}
