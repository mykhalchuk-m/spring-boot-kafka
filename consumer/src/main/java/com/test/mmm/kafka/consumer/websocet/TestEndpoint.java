package com.test.mmm.kafka.consumer.websocet;

import org.apache.log4j.Logger;

import javax.websocket.OnMessage;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;

/**
 * Created by Marian_Mykhalchuk on 8/23/2016.
 */
@ServerEndpoint("/ws-test")
public class TestEndpoint {

	private static final Logger LOGGER = Logger.getLogger(TestEndpoint.class);

	@OnMessage
	public void onMessage(Session session, String message) throws IOException {
		LOGGER.info("Message has come: " + message);
		session.getBasicRemote().sendText("We've got your message");
	}
}
