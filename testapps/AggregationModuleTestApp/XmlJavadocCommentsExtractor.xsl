<?xml version="1.0" encoding="UTF-8"?>
<!--

  Copyright (c) 2015 DataTorrent, Inc.
  All rights reserved.

-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="xml" standalone="yes"/>

  <!-- copy xml by selecting only the following nodes, attributes and text -->
  <xsl:template match="node()|text()|@*">
    <xsl:copy>
      <xsl:apply-templates select="root|package|class|interface|method|field|type|comment|tag|text()|@name|@qualified|@text"/>
    </xsl:copy>
  </xsl:template>

  <!-- Strip off the following paths from the selected xml -->
  <xsl:template match="//root/package/interface/interface
                      |//root/package/interface/method/@qualified
                      |//root/package/class/interface
                      |//root/package/class/class
                      |//root/package/class/method/@qualified
                      |//root/package/class/field/@qualified" />

  <xsl:strip-space elements="*"/>
</xsl:stylesheet>
