package com.test.mmm.kafka.consumer.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by Marian_Mykhalchuk on 8/19/2016.
 */
@Controller
@RequestMapping("/")
public class HomeController {

	private static final Logger LOGGER = Logger.getLogger(HomeController.class);
	private final SimpMessagingTemplate template;

	@Autowired
	public HomeController(SimpMessagingTemplate template) {
		this.template = template;
	}

	@RequestMapping(method = RequestMethod.GET)
	public String home() {
		return "home";
	}

	@RequestMapping(path = "/stomp", method = RequestMethod.GET)
	public String stomp() {
		return "stomp";
	}

	@RequestMapping(path="/stomp", method = RequestMethod.POST)
	public @ResponseBody String postData(@RequestBody String data) {
		LOGGER.info("POST DATA -> " + data);
		template.convertAndSend("/topic/data", data);
		return "OK";
	}

	@MessageMapping("/subscribe-ws-test")
	@SendTo("/topic/data")
	public String wsData(String data) {
		LOGGER.info("WS DATA -> " + data);
		return "OK -> " + data;
	}
}
