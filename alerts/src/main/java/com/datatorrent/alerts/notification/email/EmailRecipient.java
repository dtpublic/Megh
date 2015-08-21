package com.datatorrent.alerts.notification.email;

import java.util.Collection;
import java.util.Collections;

public class EmailRecipient {
  protected final Collection<String> tos;
  protected final Collection<String> ccs;
  protected final Collection<String> bccs;
  
  public EmailRecipient( Collection<String> tos, Collection<String> ccs, Collection<String> bccs )
  {
    this.tos = Collections.unmodifiableCollection(tos);
    this.ccs = Collections.unmodifiableCollection(ccs);
    this.bccs = Collections.unmodifiableCollection(bccs);
  }
}
