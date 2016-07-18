/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.example.FileToJdbcApp;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import java.util.regex.Pattern;

// extracts words from input line
public class CustomParser extends BaseOperator
{
  // default pattern for word-separators
  private static final Pattern RegexDefault = Pattern.compile("[\\p{Punct}\\s]+");

  private String regexStr;    // configurable regex
  private transient Pattern regexPattern;      // compiled regex

  public final transient DefaultOutputPort<PojoEvent> output = new DefaultOutputPort<>();

  public final transient DefaultInputPort<String>
    input = new DefaultInputPort<String>() {

    @Override
    public void process(String line)
    {
      // use custom regex to split line into words and emit them
      final String[] words = regexPattern.split(line);

      PojoEvent pojo = new PojoEvent();
        try {
            int accnum = Integer.parseInt(words[0]);
            pojo.setAccountNumber(accnum);
        }catch (NumberFormatException e){
            pojo.setAccountNumber(0);
        }
        String name = words[1];
        pojo.setName(name);
        try {
            int amount = Integer.parseInt(words[2]);
            pojo.setAmount(amount);
        }catch (NumberFormatException e){
            pojo.setAmount(0);
        }
      output.emit(pojo);
    }
  };

  public String getRegexStr() {
    return this.regexStr;
  }

  public void setRegexStr(String regex) {
    this.regexStr = regex;
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (null == regexStr) {
      regexPattern = RegexDefault;
    } else {
      regexPattern = Pattern.compile(this.getRegexStr());
    }
  }

}
