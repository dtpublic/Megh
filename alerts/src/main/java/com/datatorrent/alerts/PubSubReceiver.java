package com.datatorrent.alerts;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.PubSubWebSocketInputOperator;
import com.datatorrent.netlet.util.DTThrowable;

public class PubSubReceiver extends PubSubWebSocketInputOperator<Message> implements ReceiverInterface
{
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  protected Message convertMessage(String message) throws IOException
  {
    JSONObject jsonObject;
    try {
      jsonObject = new JSONObject(message);
      JSONObject dataObject = jsonObject.getJSONObject("data");
      ObjectMapper mapper = new ObjectMapper();
      Message alert = mapper.readValue(dataObject.toString(), Message.class);
      System.out.println("Received alert : " + alert);
      return alert;

    } catch (JSONException e) {
      DTThrowable.rethrow(e);
    }

    return null;
  }

  @Override
  public DefaultOutputPort<Message> getMessageOutPort()
  {
    return outputPort;
  }
}
