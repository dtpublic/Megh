package com.datatorrent.demos.dimensions.telecom.generate;

public interface EntityGenerator< E > {
  E next();
}
