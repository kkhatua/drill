/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.sys;

//import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.registry.FunctionRegistryHolder;
import org.apache.drill.exec.ops.ExecutorFragmentContext;

//import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

/**
 * System table listing functions
 */
public class FunctionInfoIterator implements Iterator<Object> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionInfoIterator.class);
  private ExecutorFragmentContext context;
  private final Iterator<FunctionsInfo> itr;

  public FunctionInfoIterator(final ExecutorFragmentContext context) {
    this.context = context;
    this.itr = getFunctionsIterator();
  }

  @Override
  public boolean hasNext() {
    return itr.hasNext();
  }

  @Override
  public Object next() {
    return itr.next();
  }

  private Iterator<FunctionsInfo> getFunctionsIterator() {
    try (FunctionImplementationRegistry funcImplRegistry = new FunctionImplementationRegistry(context.getConfig())) {
      FunctionRegistryHolder functionRegistryHolder = funcImplRegistry.getLocalFunctionRegistry().getRegistryHolder();
      List<FunctionsInfo> functionsList = Lists.newArrayList();

      //Approach #1
      /*
      ListMultimap<String, DrillFuncHolder> functionMap = functionRegistryHolder.getAllFunctionsWithHolders();
      for (String functionName : functionMap.keySet()) {
        for (DrillFuncHolder functionHolder : functionMap.get(functionName)) {
          //functionHolder.sig
          functionsList.add(new FunctionsInfo(functionName, null, functionHolder));
        }
      }
      */

      //Appoach #2
      List<String> jarNames = functionRegistryHolder.getAllJarNames();
      for (String jarName : jarNames) {
        List<String> functionNames = functionRegistryHolder.getFunctionNamesByJar(jarName);
        for (String functionName : functionNames) {
          for (DrillFuncHolder functionHolder : functionRegistryHolder.getHoldersByFunctionName(functionName)) {
            //functionHolder.sig
            functionsList.add(new FunctionsInfo(functionName, null, functionHolder));
          }
        }

      }

      functionsList.sort(new Comparator<FunctionsInfo>() {
        @Override
        public int compare(FunctionsInfo fi1, FunctionsInfo fi2) {
          if (fi1.name.compareTo(fi2.name) != 0) {
            return fi1.name.compareTo(fi2.name);
          } else {
           return fi1.arguments.compareTo(fi2.arguments);
          }
        }
      });
      return functionsList.iterator();
    }
  }

  /**
   * Function Wrapper
   */
  public static class FunctionsInfo {

    public final String name;
    public final String signature;
    public final int paramCount;
    public final String registeredNames;
    public final String arguments;
    public final String returnType;
    //TODO: public final String description;

    public FunctionsInfo(String funcName, String funcSignature, DrillFuncHolder funcHolder) {
      this.name = funcName;
      this.signature = funcSignature;
      this.paramCount = funcHolder.getParamCount();
      this.registeredNames = funcHolder.getRegisteredNames().toString();
      this.returnType = funcHolder.getReturnType().getMinorType().toString();
      this.arguments = funcHolder.getInputParameters();
    }
  }
}


