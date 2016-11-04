/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.atlas.reporting;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.atlas.model.NiFiDataModelGenerator;
import org.apache.nifi.atlas.model.NiFiDataTypes;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.web.ProcessorInfo;
import org.apache.nifi.web.ProcessorInfo.Builder;

@Tags({"reporting", "atlas", "lineage", "governance"})
@CapabilityDescription("Publishes flow changes and metadata to Apache Atlas")
public class AtlasFlowReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor ATLAS_URL = new PropertyDescriptor.Builder()
            .name("Atlas URL")
            .description("The URL of the Atlas Server")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("http://localhost:21000")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    static final PropertyDescriptor NIFI_URL = new PropertyDescriptor.Builder()
            .name("Nifi URL")
            .description("The URL of the Nifi UI")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("http://localhost:9090")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    static final PropertyDescriptor ACTION_PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("Action Page Size")
            .description("The size of each page to use when paging through the NiFi actions list.")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    
    private int lastId = 0; // TODO store on disk, this is for demo only
    private int timesTriggered = 0;
    private AtlasClient atlasClient;
    private AtomicReference<Referenceable> flowControllerRef = new AtomicReference<>();
    
    private Double atlasVersion = 0.0;
    private String encoding = "YWRtaW46YWRtaW4=";
    private String DEFAULT_ADMIN_USER = "admin";
    private String DEFAULT_ADMIN_PASS = "admin";
    private String atlasUrl;
    private String nifiUrl;
    private String[] basicAuth = {DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASS};
    
    private DataTypes.MapType STRING_MAP_TYPE = new DataTypes.MapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE);

    public String NAME = "name";
    public String FLOW = "flow";
    public String PROCESS_GROUP = "processGroup";
    public String SOURCE = "source";
    public String DESTINATION = "destination";
    public String PROPERTIES = "parameters";

    private Map<String, EnumTypeDefinition> enumTypeDefinitionMap = new HashMap<String, EnumTypeDefinition>();
	private Map<String, StructTypeDefinition> structTypeDefinitionMap = new HashMap<String, StructTypeDefinition>();
	private Map<String, HierarchicalTypeDefinition<ClassType>> classTypeDefinitions = new HashMap<String, HierarchicalTypeDefinition<ClassType>>();
	private List<Referenceable> inputs;
	private List<Referenceable> outputs;
	private int changesInFlow;
	
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATLAS_URL);
        properties.add(NIFI_URL);
        properties.add(ACTION_PAGE_SIZE);
        return properties;
    }
    
    public void initialize(ConfigurationContext reportingConfig){
    	
    }
    
    @Override
    public void onTrigger(ReportingContext reportingContext) {
    	// create the Atlas client if we don't have one
    	Properties props = System.getProperties();
        props.setProperty("atlas.conf", "/usr/hdp/current/atlas-client/conf");
        getLogger().info("***************** atlas.conf has been set to: " + props.getProperty("atlas.conf"));
    	
        inputs = new ArrayList<Referenceable>();
    	outputs = new ArrayList<Referenceable>();
        //EventAccess eventAccess = reportingContext.getEventAccess();
        int pageSize = reportingContext.getProperty(ACTION_PAGE_SIZE).asInteger();
        atlasUrl = reportingContext.getProperty(ATLAS_URL).getValue();
        nifiUrl = reportingContext.getProperty(NIFI_URL).getValue();
        String[] atlasURL = {atlasUrl};
		
    	if (atlasClient == null) {
            getLogger().info("Creating new Atlas client for {}", new Object[] {atlasUrl});
            atlasClient = new AtlasClient(atlasURL, basicAuth);
        }
    	
    	if(atlasVersion == 0.0){
        	atlasVersion = Double.valueOf(getAtlasVersion(atlasUrl + "/api/atlas/admin/version", basicAuth));
        	getLogger().info("********************* Atlas Version is: " + atlasVersion);
    	}
    	
    	getLogger().info("********************* Number of Reports Sent: " + timesTriggered);
        if(timesTriggered == 0){
        	getLogger().info("********************* Checking if data model has been created...");
			try {
				getLogger().info("********************* Created: " + atlasClient.createType(generateNifiFlowLineageDataModel()));
			} catch (AtlasServiceException e) {
				e.printStackTrace();
			} catch (AtlasException e) {
				e.printStackTrace();
			}
        }
        
       	Map<PropertyDescriptor,String> properties = reportingContext.getProperties();
       	getLogger().info("*********************LISTING ALL PROPERTIES");
       	for (Map.Entry<PropertyDescriptor, String> property: properties.entrySet()){
       		getLogger().info("********************* KEY: " + property.getKey());
       		getLogger().info("********************* VALUE: " + property.getValue());
       	}
       	
       	/*
       	List<Action> actions = reportingContext.getEventAccess().getFlowChanges(1, 10);
       	for (Action action: actions){
       		getLogger().info("********************* ID: " + action.getId());
       		getLogger().info("********************* SOURCEID: " + action.getSourceId());
       		getLogger().info("********************* NAME: " + action.getSourceName());
       		getLogger().info("********************* ACTIONDETAILS: " + action.getActionDetails());
       		getLogger().info("********************* DETAILS: " + action.getComponentDetails());
       		getLogger().info("********************* TYPE: " + action.getSourceType());
       		getLogger().info("********************* OPERATIONS: " + action.getOperation());       		
       	}*/
       	
        // load the reference to the flow controller, if it doesn't exist then create it
        try {
        	//Referenceable flowController = getFlowControllerReference(reportingContext);
        	Referenceable flowController = null;
            //if (flowController == null) {
            	//getLogger().info("flow controller didn't exist, creating it...");
            	flowController = createFlowController(reportingContext);
            	if(changesInFlow > 0){
            		getLogger().info(InstanceSerialization.toJson(flowController, true));
            		flowController = register(flowController);
        		}else{
        			getLogger().info("********************* Nochanges detected... nothing to do");
        		}
            //}
        } catch (Exception e) {
        	getLogger().error("Unable to get reference to flow controller from Atlas", e);
        	throw new ProcessException(e);
        }

        getLogger().info("Done processing actions");
        timesTriggered++;
        changesInFlow = 0;
    }

    private Referenceable getFlowControllerReference(ReportingContext context) throws Exception {
        String typeName = NiFiDataTypes.NIFI_FLOW_CONTROLLER.getName();
        String id = context.getEventAccess().getControllerStatus().getId();
        String name = context.getEventAccess().getControllerStatus().getName();

        String dslQuery = String.format("%s where %s = '%s'", typeName, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name+"-"+id);
        return ReferenceableUtil.getEntityReferenceFromDSL(atlasClient, typeName, dslQuery);
    }

    private Referenceable getProcessGroupReference(ProcessGroupStatus processGroup) throws Exception {
        String typeName = NiFiDataTypes.NIFI_PROCESS_GROUP.getName();
        String id = processGroup.getId();
        String name = processGroup.getName();
        
        String dslQuery = String.format("%s where %s = '%s'", typeName, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name+"-"+id);
        return ReferenceableUtil.getEntityReferenceFromDSL(atlasClient, typeName, dslQuery);
    }
    
    private Referenceable getProcessorReference(ProcessorStatus processor) throws Exception {
        String typeName = NiFiDataTypes.NIFI_PROCESSOR.getName();
        String id = processor.getId();
        String name = processor.getName();
        String type = processor.getType();
        
        String dslQuery = String.format("%s where %s = '%s'", typeName, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name+"-"+type+"-"+id);
        return ReferenceableUtil.getEntityReferenceFromDSL(atlasClient, typeName, dslQuery);
    }

    private Referenceable createFlowController(ReportingContext context) {
        String id = context.getEventAccess().getControllerStatus().getId();
        String name = context.getEventAccess().getControllerStatus().getName();
        Referenceable flowController = new Referenceable(NiFiDataTypes.NIFI_FLOW_CONTROLLER.getName());
        List<Referenceable> referenceableProcessGroups = new ArrayList<Referenceable>();
        
        flowController.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name+"-"+id);
        flowController.set(NAME, name+"-"+id);
        
        List<ProcessGroupStatus> processGroups = (List<ProcessGroupStatus>) context.getEventAccess().getControllerStatus().getProcessGroupStatus();
        getLogger().info("****************Flow Controller Process Groups : " + processGroups.size());
        
        Referenceable processGroupReferenceable = null;
        ProcessGroupStatus processGroup;
        if(processGroups.size() > 0){
        	Iterator<ProcessGroupStatus> processGroupIterator = processGroups.iterator();
        	while(processGroupIterator.hasNext()){
        		processGroup = processGroupIterator.next();
        		getLogger().info("****************Process Group Id: " + processGroup.getId());
        		getLogger().info("****************Process Group Name: " + processGroup.getName());        	
        		try {
        			processGroupReferenceable = getProcessGroupReference(processGroup);
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
        		if (processGroupReferenceable == null)
        			changesInFlow++;
        		else{
        			getLogger().info("****************Process Group Name: " + processGroup.getName() + " already exists");
        		}
        		referenceableProcessGroups.add(createProcessGroup(flowController, processGroup));
        	}
        }else{
        	processGroup = context.getEventAccess().getControllerStatus();
    		getLogger().info("****************Process Group Id: " + processGroup.getId());
    		getLogger().info("****************Process Group Name: " + processGroup.getName());        	
    		try {
    			processGroupReferenceable = getProcessGroupReference(processGroup);
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		if (processGroupReferenceable == null){
    			changesInFlow++;
    		}else{
    			getLogger().info("****************Process Group Name: " + processGroup.getName() + " already exists");
    		}
    		referenceableProcessGroups.add(createProcessGroup(flowController, processGroup));
        }
        flowController.set("process_groups", referenceableProcessGroups);
        flowController.set("inputs", inputs);
    	flowController.set("outputs", outputs);
        getLogger().info(InstanceSerialization.toJson(flowController, true));
        return flowController;
    }
    
    private Referenceable createProcessGroup(Referenceable flowController, ProcessGroupStatus processGroup) {
        String id = processGroup.getId();
        String name = processGroup.getName();
        List<Referenceable> referenceableProcessors = new ArrayList<Referenceable>();
        
        Referenceable referenceableProcessGroup = new Referenceable(NiFiDataTypes.NIFI_PROCESS_GROUP.getName());
        referenceableProcessGroup.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name+"-"+id);
        referenceableProcessGroup.set(NAME, name+"-"+id);
        referenceableProcessGroup.set(FLOW, flowController.getId());
        
        List<ProcessorStatus> processorCollection = (List<ProcessorStatus>) processGroup.getProcessorStatus();
        Iterator<ProcessorStatus> processorIterator = processorCollection.iterator();
        Referenceable referenceableProcessor = null;
        ProcessorStatus processor;
        while(processorIterator.hasNext()){
        	processor = processorIterator.next();
        	
            getLogger().info("****************Processor Id: " + processor.getId());
            getLogger().info("****************Processor Name: " + processor.getName());
            getLogger().info("****************Processor Type: " + processor.getType());
            getLogger().info("****************Processor GroupId: " + processor.getGroupId());
            
            try {
            	referenceableProcessor = getProcessorReference(processor);
        	} catch (Exception e) {
        		e.printStackTrace();
			}
            if(referenceableProcessor == null){
            	changesInFlow++;
            }else{
            	getLogger().info("****************Processor Name: " + processor.getName() + " already exists");
            }
            referenceableProcessors.add(createProcessor(referenceableProcessGroup, processor));
        }
        getLogger().info("****************Number of processors in Group: " + referenceableProcessors.size());
        referenceableProcessGroup.set("processors", referenceableProcessors);
        getLogger().info("****************Process Group: " + referenceableProcessGroup.toString());
        getLogger().info(InstanceSerialization.toJson(referenceableProcessGroup, true));
        return referenceableProcessGroup;
    }

    private Referenceable createProcessor(Referenceable processGroupReferenceable, ProcessorStatus processor) {
        String id = processor.getId();
        String name = processor.getName();
        String type = processor.getType();
        String configKey;
        String configValue;
        getLogger().info("****************Acquiring Processor Configs... ");
        Map<String,Object> processorConfigObject = getProcessorConfig(id, nifiUrl, basicAuth);
        getLogger().info("****************Configuration Map Size: " + processorConfigObject.size());
        Map<String,String> processorConfigMap = new HashMap<String,String>();
        if(processorConfigObject != null && processorConfigObject.size() > 0){
        	for(Entry<String,Object> configItem: processorConfigObject.entrySet()){
        		getLogger().info("****************Configuration Item: " + configItem);
        		configKey = configItem.getKey();
        		if(configItem.getValue() == null){
        			configValue = "";
        		}else{
        			configValue = configItem.getValue().toString();
        		}
        		processorConfigMap.put(configKey, configValue);
        	}
        }
        
        Referenceable processorReferenceable = new Referenceable(NiFiDataTypes.NIFI_PROCESSOR.getName());
        processorReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name+"-"+type+"-"+id);
        processorReferenceable.set(NAME, name);
        processorReferenceable.set(PROCESS_GROUP, processGroupReferenceable.getId());
        processorReferenceable.set(PROPERTIES, processorConfigMap);
        
        Referenceable externalReferenceable = null;
        getLogger().info("****************Determining if processor " + name + " of type " + type + "is an input or output to the flow... ");
        switch (processor.getType()) {
        case "PutKafka":
        	getLogger().info("****************Processor " + name +" of type " + type + "is an ouput, creating or acquiring external entity...");
        	try {
        		externalReferenceable = getEntityReferenceFromDSL("kafka_topic", "kafka_topic where topic='" + processorConfigMap.get("Topic Name") + "'");
				if(externalReferenceable == null){
					getLogger().info("****************External entity not found, creating... ");
					externalReferenceable = register(createKafkaTopic(processor, processorConfigMap));
					changesInFlow++;
				}
				outputs.add(externalReferenceable);
			} catch (Exception e) {
				e.printStackTrace();
			}
            break;
        case "PublishKafka":
        	getLogger().info("****************Processor " + name +" of type " + type + "is an ouput, creating or acquiring external entity...");
        	try {
        		externalReferenceable = getEntityReferenceFromDSL("kafka_topic", "kafka_topic where topic='" + processorConfigMap.get("Topic Name") + "'");
				if(externalReferenceable == null){
					getLogger().info("****************External entity not found, creating... ");
					externalReferenceable = register(createKafkaTopic(processor, processorConfigMap));
					changesInFlow++;
				}
				outputs.add(externalReferenceable);
			} catch (Exception e) {
				e.printStackTrace();
			}
            break;
        case "PublishKafka_0_10":
        	getLogger().info("****************Processor " + name +" of type " + type + "is an ouput, creating or acquiring external entity...");
        	try {
        		externalReferenceable = getEntityReferenceFromDSL("kafka_topic", "kafka_topic where topic='" + processorConfigMap.get("Topic Name") + "'");
				if(externalReferenceable == null){
					getLogger().info("****************External entity not found, creating... ");
					externalReferenceable = register(createKafkaTopic(processor, processorConfigMap));
					changesInFlow++;
				}
				outputs.add(externalReferenceable);
			} catch (Exception e) {
				e.printStackTrace();
			}
            break;
        case "ListenHTTP":
        	getLogger().info("****************Processor " + name +" of type " + type + "is an ouput, creating or acquiring external entity...");
        	try {
        		String basePath = processorConfigMap.get("Base Path").toString();
        		String listeningPort = processorConfigMap.get("Listening Port").toString();
        		String listenHttpServiceUrl = nifiUrl+listeningPort+basePath;
        		externalReferenceable = getEntityReferenceFromDSL("http_service", "http_service where name='" + listenHttpServiceUrl + "'");
				if(externalReferenceable == null){
					getLogger().info("****************External entity not found, creating... ");
					externalReferenceable = register(createHttpService(processor, processorReferenceable, processorConfigMap));
					changesInFlow++;
				}
				inputs.add(externalReferenceable);
			} catch (Exception e) {
				e.printStackTrace();
			}
        	break;
        default:
            break;
    	}
        
        getLogger().info(InstanceSerialization.toJson(processorReferenceable, true));
        return processorReferenceable;
    }
    
    private Referenceable createKafkaTopic(ProcessorStatus processor, Map<String, String> processorConfigMap){
    	Referenceable kafkaTopicReferenceable = new Referenceable("kafka_topic");
    	String topicName = processorConfigMap.get("Topic Name").toString();
        
    	kafkaTopicReferenceable.set("topic", topicName);
        kafkaTopicReferenceable.set("uri", "");
        kafkaTopicReferenceable.set(AtlasClient.OWNER, "");
        kafkaTopicReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, topicName);
        kafkaTopicReferenceable.set(AtlasClient.NAME, topicName);
        
        return kafkaTopicReferenceable;
    }
    
    private Referenceable createHttpService(ProcessorStatus processor, Referenceable referenceableProcessor, Map<String, String> processorConfigMap){
    	Referenceable HttpServiceReferenceable = new Referenceable("http_service");
    	String basePath = processorConfigMap.get("Base Path").toString();
		String listeningPort = processorConfigMap.get("Listening Port").toString();
		String listenHttpServiceUrl = nifiUrl+listeningPort+basePath;
		
    	//HttpServiceReferenceable.set("uri", listenHttpServiceUrl);
        HttpServiceReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, listenHttpServiceUrl);
        HttpServiceReferenceable.set(AtlasClient.NAME, listenHttpServiceUrl);
        HttpServiceReferenceable.set("implementation", referenceableProcessor.getId());
        
        return HttpServiceReferenceable;
    }

    public String generateNifiFlowLineageDataModel() throws AtlasException {
    	TypesDef typesDef;
		String nifiFlowLineageDataModelJSON;
		
    	try {
			atlasClient.getType(NiFiDataTypes.NIFI_FLOW_CONTROLLER.getName());
			getLogger().info("********************* Nifi Atlas Type: " + NiFiDataTypes.NIFI_FLOW_CONTROLLER.getName() + " is already present");
		} catch (AtlasServiceException e) {
			createFlowControllerClass();
		}
		
		try {
			atlasClient.getType(NiFiDataTypes.NIFI_PROCESS_GROUP.getName());
			getLogger().info("********************* Nifi Atlas Type: " + NiFiDataTypes.NIFI_PROCESS_GROUP.getName() + " is already present");
		} catch (AtlasServiceException e) {
			createProcessGroupClass();
		}
		
		try {
			atlasClient.getType(NiFiDataTypes.NIFI_PROCESSOR.getName());
			getLogger().info("********************* Nifi Atlas Type: " + NiFiDataTypes.NIFI_PROCESSOR.getName() + " is already present");
		} catch (AtlasServiceException e) {
			createProcessorClass();
		}
		
		try {
			atlasClient.getType(NiFiDataTypes.NIFI_CONNECTION.getName());
			getLogger().info("********************* Nifi Atlas Type: " + NiFiDataTypes.NIFI_CONNECTION.getName() + " is already present");
		} catch (AtlasServiceException e) {
			createConnectionClass();
		}
		
		try {
			atlasClient.getType(NiFiDataTypes.HTTP_SERVICE.getName());
			getLogger().info("********************* Nifi Atlas Type: " + NiFiDataTypes.HTTP_SERVICE.getName() + " is already present");
		} catch (AtlasServiceException e) {
			createHttpServiceClass();
		}
		
		typesDef = TypesUtil.getTypesDef(
				getEnumTypeDefinitions(), 	//Enums 
				getStructTypeDefinitions(), //Struct 
				getTraitTypeDefinitions(), 	//Traits 
				ImmutableList.copyOf(classTypeDefinitions.values()));
		
		nifiFlowLineageDataModelJSON = TypesSerialization.toJson(typesDef);
		getLogger().info("Submitting Types Definition: " + nifiFlowLineageDataModelJSON);
		getLogger().info("Generating the NiFi Data Model....");
		return nifiFlowLineageDataModelJSON;
    }

    private void createFlowControllerClass() throws AtlasException {
        final String typeName = NiFiDataTypes.NIFI_FLOW_CONTROLLER.getName();
        
        final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
                new AttributeDefinition("process_groups", DataTypes.arrayTypeName(NiFiDataTypes.NIFI_PROCESS_GROUP.getName()), Multiplicity.OPTIONAL, true, null)
        };
        
        addClassTypeDefinition(typeName, ImmutableSet.of(AtlasClient.PROCESS_SUPER_TYPE), attributeDefinitions);
        getLogger().info("Created definition for " + typeName);
    }

    private void createProcessGroupClass() throws  AtlasException {
        final String typeName = NiFiDataTypes.NIFI_PROCESS_GROUP.getName();

        final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
                new AttributeDefinition(FLOW, NiFiDataTypes.NIFI_FLOW_CONTROLLER.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("processors", DataTypes.arrayTypeName(NiFiDataTypes.NIFI_PROCESSOR.getName()), Multiplicity.OPTIONAL, true, null)
        };

        addClassTypeDefinition(typeName, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        getLogger().info("Created definition for " + typeName);
    }

    private void createProcessorClass() throws AtlasException {
        final String typeName = NiFiDataTypes.NIFI_PROCESSOR.getName();

        final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
                new AttributeDefinition(PROCESS_GROUP, NiFiDataTypes.NIFI_PROCESS_GROUP.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(PROPERTIES, STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null)
        };

        addClassTypeDefinition(typeName, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        getLogger().info("Created definition for " + typeName);
    }
    
    private void createHttpServiceClass() throws AtlasException {
        final String typeName = "http_service";

        final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
                new AttributeDefinition(PROPERTIES, STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("implementation", AtlasClient.REFERENCEABLE_SUPER_TYPE, Multiplicity.OPTIONAL, false, null)
        };

        addClassTypeDefinition(typeName, ImmutableSet.of(AtlasClient.DATA_SET_SUPER_TYPE), attributeDefinitions);
        getLogger().info("Created definition for " + typeName);
    }

    private void createConnectionClass() throws AtlasException {
        final String typeName = NiFiDataTypes.NIFI_CONNECTION.getName();

        final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
                new AttributeDefinition(SOURCE, NiFiDataTypes.NIFI_PROCESSOR.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(DESTINATION, NiFiDataTypes.NIFI_PROCESSOR.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(PROPERTIES, STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null)
        };

        addClassTypeDefinition(typeName, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        getLogger().info("Created definition for " + typeName);
    }

    private void addClassTypeDefinition(String typeName, ImmutableSet<String> superTypes, AttributeDefinition[] attributeDefinitions) {
        final HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, typeName, null, superTypes, attributeDefinitions);

        classTypeDefinitions.put(typeName, definition);
    }

    public TypesDef getTypesDef() {
        return TypesUtil.getTypesDef(getEnumTypeDefinitions(), getStructTypeDefinitions(), getTraitTypeDefinitions(), getClassTypeDefinitions());
    }

    public String getDataModelAsJSON() {
        return TypesSerialization.toJson(getTypesDef());
    }

    public ImmutableList<EnumTypeDefinition> getEnumTypeDefinitions() {
        return ImmutableList.copyOf(enumTypeDefinitionMap.values());
    }

    public ImmutableList<StructTypeDefinition> getStructTypeDefinitions() {
        return ImmutableList.copyOf(structTypeDefinitionMap.values());
    }

    public ImmutableList<HierarchicalTypeDefinition<ClassType>> getClassTypeDefinitions() {
        return ImmutableList.copyOf(classTypeDefinitions.values());
    }

    public ImmutableList<HierarchicalTypeDefinition<TraitType>> getTraitTypeDefinitions() {
        return ImmutableList.of();
    }
    
    public Referenceable getEntityReferenceFromDSL(String typeName,  String dslQuery)
            throws Exception {

        final JSONArray results = atlasClient.searchByDSL(dslQuery);
        if (results.length() == 0) {
            return null;
        } else {
            String guid;
            JSONObject row = results.getJSONObject(0);
            if (row.has("$id$")) {
                guid = row.getJSONObject("$id$").getString("id");
            } else {
                guid = row.getJSONObject("_col_0").getString("id");
            }
            return new Referenceable(guid, typeName, null);
        }
    }
    
    public Referenceable register(Referenceable referenceable) throws Exception {
        if (referenceable == null) {
            return null;
        }

        final String typeName = referenceable.getTypeName();
        getLogger().info("creating instance of type " + typeName);

        final String entityJSON = InstanceSerialization.toJson(referenceable, true);
        getLogger().info("Submitting new entity {} = {}", new Object[] { referenceable.getTypeName(), entityJSON });

        final List<String> guids = atlasClient.createEntity(entityJSON);
        getLogger().info("created instance for type " + typeName + ", guid: " + guids);

        return new Referenceable(guids.get(0), referenceable.getTypeName(), null);
    }
    
	private String getAtlasVersion(String urlString, String[] basicAuth){
		getLogger().info("************************ Getting Atlas Version from: " + urlString);
		JSONObject json = null;
		String versionValue = null;
        try{
        	json = readJSONFromUrlAuth(urlString, basicAuth);
        	getLogger().info("************************ Response from Atlas: " + json);
        	versionValue = json.getString("Version");
        } catch (Exception e) {
            e.printStackTrace();
        }
		return versionValue.substring(0,3);
	}
	
	private HashMap<String, Object> getProcessorConfig(String processorId, String urlString, String[] basicAuth){
		String processorResourceUri = "/nifi-api/processors/";
		String nifiProcessorUrl = urlString+processorResourceUri+processorId;
		System.out.println("************************ Getting Nifi Processor from: " + nifiProcessorUrl);
		JSONObject json = null;
		JSONObject nifiComponentJSON = null;
		HashMap<String,Object> result = null;
        try{
        	json = readJSONFromUrlAuth(nifiProcessorUrl, basicAuth);
        	System.out.println("************************ Response from Nifi: " + json);
        	nifiComponentJSON = json.getJSONObject("component").getJSONObject("config").getJSONObject("properties");
        	result = new ObjectMapper().readValue(nifiComponentJSON.toString(), HashMap.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
       
		return result;
	}
	
	private JSONObject readJSONFromUrlAuth(String urlString, String[] basicAuth) throws IOException, JSONException {
		String userPassString = basicAuth[0]+":"+basicAuth[1];
		JSONObject json = null;
		try {
            URL url = new URL (urlString);
            //Base64.encodeBase64String(userPassString.getBytes());

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", "Basic " + encoding);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	json = new JSONObject(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return json;
    }
	
	private String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	}
}
