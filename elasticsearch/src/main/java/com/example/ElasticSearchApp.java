package com.example;

import java.io.IOException;
import java.net.InetAddress;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class ElasticSearchApp {

	public static void run(String[] args) throws IOException {

		Settings settings = Settings.settingsBuilder().build();
		TransportClient client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		System.out.println(client.connectedNodes());

		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("name", "Saravana").field("age", 27).field("country", "India")
				.field("citizen", "Indian").endObject();

		IndexResponse indexResponse = client.prepareIndex("twitter", "tweet", "1").setSource(builder).get();
		System.out.println(indexResponse);

		GetResponse getResponse = client.prepareGet("twitter", "tweet", "1").setOperationThreaded(false).get();
		System.out.println(getResponse.getSource());

		client.close();
	}

}
