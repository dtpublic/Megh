package com.datatorrent.lib.io.output;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.google.common.base.Splitter;

/**
 * <p>IngestionUtils class.</p>
 *
 * @since 1.0.0
 */
public class IngestionUtils {

	/**
	 * Converts Scheme part of the URI to lower case.
	 * Multiple URI can be comma separated.
	 * If no scheme is there, no change is made.
	 * @param 
	 * @return String with scheme part as lower case
	 */
	public static String convertSchemeToLowerCase(String uri) {
		if( uri == null )
			return null;
		StringBuilder inputMod = new StringBuilder();
		for(String f : Splitter.on(",").omitEmptyStrings().split(uri)) {
			String scheme =  URI.create(f).getScheme();
			if( scheme != null ){				
				inputMod.append(f.replaceFirst(scheme, scheme.toLowerCase()));
			}else{
				inputMod.append(f);
			}
			inputMod.append(",");
		}
		inputMod.setLength(inputMod.length() - 1);
		return inputMod.toString();
	}
	
  public static void createAppDataConnections(DAG dag, String topic, String schemaFile, OutputPort<List<Map<String, Object>>> data)
  {
    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    if (!StringUtils.isEmpty(gatewayAddress)) {
      String topicPrefix = topic + "_" + System.currentTimeMillis() + "_";
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");

      AppDataSnapshotServerMap snapshotServer = dag.addOperator("SnapshotServer_" + topic, new AppDataSnapshotServerMap());

      String snapshotServerJSON = SchemaUtils.jarResourceFileToString(schemaFile);
      snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);

      PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
      wsQuery.setUri(uri);
      wsQuery.setTopic(topicPrefix + "Query");
      snapshotServer.setEmbeddableQueryInfoProvider(wsQuery);

      PubSubWebSocketAppDataResult wsResult = dag.addOperator("QueryResult_" + topic, new PubSubWebSocketAppDataResult());
      wsResult.setUri(uri);
      wsResult.setTopic(topicPrefix + "QueryResult");
      wsResult.setNumRetries(2147483647);
      Operator.InputPort<String> queryResultPort = wsResult.input;

      dag.addStream("MapProvider_" + topic, data, snapshotServer.input);
      dag.addStream("Result_" + topic, snapshotServer.queryResult, queryResultPort).setLocality(Locality.CONTAINER_LOCAL);
    }
    else {
      ConsoleOutputOperator operator = dag.addOperator("Console_" + topic, new ConsoleOutputOperator());
      operator.setStringFormat(topic + ": %s");

      dag.addStream("MapProvider_" + topic, data, operator.input);
    }
  }
	
}
