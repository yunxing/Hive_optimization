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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;  
/**
 * A hierarchy physical optimizer, which contains a list of
 * PhysicalPlanResolver. Each resolver has its own set of optimization rule.
 */
public class PhysicalOptimizer {
  private PhysicalContext pctx;
  private List<PhysicalPlanResolver> resolvers;
  static final private Log LOG = LogFactory.getLog(PhysicalOptimizer.class.getName());
  public PhysicalOptimizer(PhysicalContext pctx, HiveConf hiveConf) {
    super();
	LOG.info("initilizing pysical optimizer");
    this.pctx = pctx;
    initialize(hiveConf);
  }

  /**
   * create the list of physical plan resolvers.
   *
   * @param hiveConf
   */
  private void initialize(HiveConf hiveConf) {
	
    resolvers = new ArrayList<PhysicalPlanResolver>();
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVESKEWJOIN)) {
      resolvers.add(new SkewJoinResolver());
    }
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN)) {
	  LOG.info("adding hive convert join to resolvers");
      resolvers.add(new CommonJoinResolver());
    }
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER)) {
      resolvers.add(new IndexWhereResolver());
    }
    resolvers.add(new MapJoinResolver());
  }

  /**
   * invoke all the resolvers one-by-one, and alter the physical plan.
   *
   * @return PhysicalContext
   * @throws HiveException
   */
  public PhysicalContext optimize() throws SemanticException {
	LOG.info("-------------in optimizer");	
    for (PhysicalPlanResolver r : resolvers) {
      pctx = r.resolve(pctx);
    }
    return pctx;
  }

}
