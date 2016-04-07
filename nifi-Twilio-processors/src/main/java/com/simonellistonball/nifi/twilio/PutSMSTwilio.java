/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.simonellistonball.nifi.twilio;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.twilio.sdk.TwilioRestClient;
import com.twilio.sdk.TwilioRestException;
import com.twilio.sdk.resource.factory.MessageFactory;
import com.twilio.sdk.resource.instance.Message;

@Tags({ "sms", "twilio", "notification" })
@CapabilityDescription("Sends messages to the twilio service. Currently supports simple SMS")
@SeeAlso({})
@ReadsAttributes({
		@ReadsAttribute(attribute = "sms.to", description = "Phone number to send to, with +CountryCode"),
		@ReadsAttribute(attribute = "sms.body", description = "Message body to send") })
@WritesAttributes({
		@WritesAttribute(attribute = "sms.sid", description = "Unique Message Identifier from Twilio"),
		@WritesAttribute(attribute = "sms.price", description = "The cost of sending the message") })
public class PutSMSTwilio extends AbstractProcessor {

	public static final PropertyDescriptor ACCOUNT_ID = new PropertyDescriptor.Builder()
			.name("Account Id").description("Twilio Account Id").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor AUTH_TOKEN = new PropertyDescriptor.Builder()
			.name("Auth token").description("Twilio Auth token").required(true)
			.sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor FROM_NUMBER = new PropertyDescriptor.Builder()
			.name("From").description("Twilio sending number").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success").description("Message sent successfully").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure").description("Message failed to send").build();

	public static final List<PropertyDescriptor> properties = Collections
			.unmodifiableList(Arrays
					.asList(ACCOUNT_ID, AUTH_TOKEN, FROM_NUMBER));

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	public static final Set<Relationship> relationships = Collections
			.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS,
					REL_FAILURE)));

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	public void onTrigger(final ProcessContext context,
			final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		String accountSid = context.getProperty(ACCOUNT_ID).getValue();
		String authToken = context.getProperty(AUTH_TOKEN).getValue();
		String from = context.getProperty(FROM_NUMBER).getValue();
		String to = flowFile.getAttribute("sms.to");
		String msg = flowFile.getAttribute("sms.body");

		TwilioRestClient client = new TwilioRestClient(accountSid, authToken);
		MessageFactory messageFactory = client.getAccount().getMessageFactory();

		List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("Body", msg));
		params.add(new BasicNameValuePair("To", to));
		params.add(new BasicNameValuePair("From", from));

		try {
			Message message = messageFactory.create(params);

			Map<String, String> attributes = new HashMap<String, String>();
			attributes.put("sms.sid", message.getSid());
			attributes.put("sms.price", message.getPrice());

			flowFile = session.putAllAttributes(flowFile, attributes);
			session.transfer(flowFile, REL_SUCCESS);
		} catch (TwilioRestException e) {
			flowFile = session.penalize(flowFile);
			session.transfer(flowFile, REL_FAILURE);
		}
	}
}
