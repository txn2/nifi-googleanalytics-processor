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
package com.txn2.nifi.processors.googleanalytics;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting;
import com.google.api.services.analyticsreporting.v4.AnalyticsReportingScopes;
import com.google.api.services.analyticsreporting.v4.model.*;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.*;

@Tags({"google","analytics","report"})
@CapabilityDescription("Get a Google Analytics report by view id, dimensions, metrics and date range")
@InputRequirement(Requirement.INPUT_ALLOWED)
@WritesAttributes({
        @WritesAttribute(attribute="application_name", description="Application name from configuration parameter."),
        @WritesAttribute(attribute="start_date", description="Start date from configuration parameter."),
        @WritesAttribute(attribute="end_date", description="End date from configuration parameter."),
        @WritesAttribute(attribute="view_id", description="View ID from configuration parameter."),
        @WritesAttribute(attribute="dimensions", description="Dimensions CSV from configuration parameter."),
        @WritesAttribute(attribute="metrics", description="Metrics CSV from configuration parameter."),
        @WritesAttribute(attribute="page_size", description="Pagination parameter."),
        @WritesAttribute(attribute="page_token", description="Pagination token."),
        @WritesAttribute(attribute="order_by_dsc", description="Pagination token."),
        @WritesAttribute(attribute="order_by_asc", description="Pagination token.")
})
public class GetGoogleAnalyticsReport extends AbstractProcessor {

    public static final String APPLICATION_JSON = "application/json";

    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    public static final PropertyDescriptor KEY_JSON = new PropertyDescriptor
            .Builder().name("google_key_json")
            .displayName("Google Key JSON")
            .description("Google service account API key in JSON format.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor APP_NAME = new PropertyDescriptor
            .Builder().name("application_name")
            .displayName("Application Name")
            .description("Application name used for communicating with the Google API.")
            .required(true)
            .defaultValue("NiFi")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor START_DATE = new PropertyDescriptor
            .Builder().name("start_date")
            .displayName("Start Date")
            .description("The inclusive start date for the query in the format YYYY-MM-DD. "
                    + "Cannot be after End Date. The format NdaysAgo, yesterday, or today is "
                    + "also accepted, and in that case, the date is inferred based on the property's "
                    + "reporting time zone.")
            .required(true)
            .defaultValue("7DaysAgo")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor END_DATE = new PropertyDescriptor
            .Builder().name("end_date")
            .displayName("End Date")
            .description("The inclusive end date for the query in the format YYYY-MM-DD. "
                    + "Cannot be before Start Date. The format NdaysAgo, yesterday, or "
                    + "today is also accepted, and in that case, the date is inferred based "
                    + "on the property's reporting time zone")
            .required(true)
            .defaultValue("today")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VIEW_ID = new PropertyDescriptor
            .Builder().name("view_id")
            .displayName("View ID")
            .description("A view is your access point for reports; a defined view of data from a property. "
                    + "You give users access to a view so they can see the reports based on that view's data. "
                    + "A property can contain one or more views.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DIMENSIONS = new PropertyDescriptor
            .Builder().name("dimensions")
            .displayName("Dimensions")
            .description("Comma seperated list of dimensions.")
            .required(true)
            .defaultValue("ga:pageTitle")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor METRICS = new PropertyDescriptor
            .Builder().name("metrics")
            .displayName("Metrics")
            .description("Comma seperated list of metrics.")
            .required(true)
            .defaultValue("ga:sessions")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor
            .Builder().name("page_size")
            .displayName("Page Size")
            .description("A query returns the default of 1,000 rows. The Analytics Core Reporting API returns a maximum of 100,000 rows per request.")
            .required(true)
            .defaultValue("1000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PAGE_TOKEN = new PropertyDescriptor
            .Builder().name("page_token")
            .displayName("Page Token")
            .description("A continuation token to get the next page of the results. Adding this to the request will return the rows after the pageToken. The pageToken should be the value returned in the nextPageToken parameter.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ORDER_BY_DSC = new PropertyDescriptor
            .Builder().name("order_by_dsc")
            .displayName("Order Descending")
            .description("Comma seperated list of dimensions to sort descending by value.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ORDER_BY_ASC = new PropertyDescriptor
            .Builder().name("order_by_asc")
            .displayName("Order Ascending")
            .description("Comma seperated list of dimensions ascending by value.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("success relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = new ArrayList<>();
        this.descriptors.add(KEY_JSON);
        this.descriptors.add(APP_NAME);
        this.descriptors.add(START_DATE);
        this.descriptors.add(END_DATE);
        this.descriptors.add(VIEW_ID);
        this.descriptors.add(DIMENSIONS);
        this.descriptors.add(PAGE_SIZE);
        this.descriptors.add(PAGE_TOKEN);
        this.descriptors.add(METRICS);
        this.descriptors.add(ORDER_BY_DSC);
        this.descriptors.add(ORDER_BY_ASC);

        this.relationships = new HashSet<>();
        this.relationships.add(SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            flowFile = session.create();
        }

        String jsonKeyString = context.getProperty(KEY_JSON.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        InputStream jsonKeyInputStream = new ByteArrayInputStream(jsonKeyString.getBytes(StandardCharsets.UTF_8));

        // implement state for credential expiration
        GoogleCredential credential;
        try {
            credential = GoogleCredential
                    .fromStream(jsonKeyInputStream)
                    .createScoped(AnalyticsReportingScopes.all());
        } catch (IOException e) {
            throw new ProcessException(e.getMessage());
        }

        HttpTransport httpTransport;
        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        } catch (GeneralSecurityException | IOException e) {
            throw new ProcessException(e.getMessage());
        }

        String applicationName = context.getProperty(APP_NAME.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        AnalyticsReporting analyticsService = new AnalyticsReporting.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(applicationName).build();

        String startDate = context.getProperty(START_DATE.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        String endDate = context.getProperty(END_DATE.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        DateRange dateRange = new DateRange();
        dateRange.setStartDate(startDate);
        dateRange.setEndDate(endDate);

        String metricsCSV = context.getProperty(METRICS.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        String dimensionsCSV = context.getProperty(DIMENSIONS.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        String[] metricNames = metricsCSV.split(",[ ]*");
        String[] dimensionNames = dimensionsCSV.split(",[ ]*");

        List<Metric> metrics = new ArrayList<>(Collections.emptyList());
        List<Dimension> dimensions = new ArrayList<>(Collections.emptyList());

        for (String metricName : metricNames) {
            metrics.add(new Metric().setExpression(metricName));
        }

        for (String dimensionName : dimensionNames) {
            dimensions.add(new Dimension().setName(dimensionName));
        }

        String viewID = context.getProperty(VIEW_ID.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        Integer pageSize = context.getProperty(PAGE_SIZE.getName())
                .evaluateAttributeExpressions(flowFile)
                .asInteger();

        // Create the ReportRequest object.
        ReportRequest request = new ReportRequest()
                .setPageSize(pageSize)
                .setViewId(viewID)
                .setDateRanges(Collections.singletonList(dateRange))
                .setMetrics(metrics)
                .setDimensions(dimensions);

        // check for page token
        String pageToken = context.getProperty(PAGE_TOKEN.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        if(pageToken != null && !pageToken.equals("")) {
            request.setPageToken(pageToken);
        }

        String orderByAscCSV = context.getProperty(ORDER_BY_ASC.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        if (orderByAscCSV != null && !orderByAscCSV.equals("")) {
            String[] orderByAscDims = orderByAscCSV.split(",[ ]*");
            ArrayList<OrderBy> ascOrderBys = new ArrayList<>();
            for (String orderByAscDim : orderByAscDims) {
                ascOrderBys.add(new OrderBy()
                        .setSortOrder("ASCENDING")
                        .setOrderType("VALUE")
                        .setFieldName(orderByAscDim));
            }

            if (!ascOrderBys.isEmpty()) {
                request.setOrderBys(ascOrderBys);
            }
        }

        String orderByDscCSV = context.getProperty(ORDER_BY_DSC.getName())
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        if (orderByDscCSV != null && !orderByDscCSV.equals("")) {
            String[] orderByDscDims = orderByDscCSV.split(",[ ]*");
            ArrayList<OrderBy> dscOrderBys = new ArrayList<>();
            for (String orderByDscDim : orderByDscDims) {
                dscOrderBys.add(new OrderBy()
                        .setSortOrder("DESCENDING")
                        .setOrderType("VALUE")
                        .setFieldName(orderByDscDim));
            }

            if (!dscOrderBys.isEmpty()) {
                request.setOrderBys(dscOrderBys);
            }
        }

        ArrayList<ReportRequest> requests = new ArrayList<>();
        requests.add(request);

        // Create the GetReportsRequest object.
        GetReportsRequest getReport = new GetReportsRequest()
                .setReportRequests(requests);

        final String jsonStringResponse;

        // Call the batchGet method.
        try {
            GetReportsResponse response = analyticsService.reports().batchGet(getReport).execute();
            jsonStringResponse = response.toPrettyString();
        } catch (IOException e) {
            throw new ProcessException(e.getMessage());
        }

        if (jsonStringResponse != null) {
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(jsonStringResponse.getBytes(StandardCharsets.UTF_8));
                }
            });

            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
            flowFile = session.putAttribute(flowFile, APP_NAME.getName(), applicationName);
            flowFile = session.putAttribute(flowFile, VIEW_ID.getName(), viewID);
            flowFile = session.putAttribute(flowFile, METRICS.getName(), metricsCSV);
            flowFile = session.putAttribute(flowFile, DIMENSIONS.getName(), dimensionsCSV);
            flowFile = session.putAttribute(flowFile, START_DATE.getName(), startDate);
            flowFile = session.putAttribute(flowFile, END_DATE.getName(), endDate);
            flowFile = session.putAttribute(flowFile, PAGE_SIZE.getName(), String.valueOf(pageSize));
            flowFile = session.putAttribute(flowFile, PAGE_TOKEN.getName(), pageToken);
            flowFile = session.putAttribute(flowFile, ORDER_BY_DSC.getName(), orderByDscCSV);
            flowFile = session.putAttribute(flowFile, ORDER_BY_ASC.getName(), orderByAscCSV);

            session.transfer(flowFile, SUCCESS);
        }
    }
}
