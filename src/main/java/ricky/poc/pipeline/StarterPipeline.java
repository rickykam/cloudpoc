/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package ricky.poc.pipeline;



import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;



/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=BlockingDataflowPipelineRunner
 */
public class StarterPipeline {
 

  public static void main(String[] args) {
    
	  
/**		Google Pipeline	    
  		// Create a DataflowPipelineOptions object. This object lets us set various execution
	    // options for our pipeline, such as the associated Cloud Platform project and the location
	    // in Google Cloud Storage to stage files.
	    DataflowPipelineOptions options = PipelineOptionsFactory.create()
	      .as(DataflowPipelineOptions.class);
	    options.setRunner(BlockingDataflowPipelineRunner.class);
	    // CHANGE 1/3: Your project ID is required in order to run your pipeline on the Google Cloud.
	    options.setProject("SET_YOUR_PROJECT_ID_HERE");
	    // CHANGE 2/3: Your Google Cloud Storage path is required for staging local files.
	    options.setStagingLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_STAGING_DIRECTORY");

	    // Create the Pipeline object with the options we defined above.
	    Pipeline p = Pipeline.create(options);

**/	  
	  
	  
	  
	  
	   Pipeline p = Pipeline.create(
		        PipelineOptionsFactory.fromArgs(args).withValidation().create());

/*	  define custom transform 
	  static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }
	
		PCollection<Integer> wordLengths = words.apply(
    	ParDo
    	.of(new ComputeWordLengthFn()));   	
	  
		   
*/
    
    p.apply(TextIO.Read.named("Read from Text").from("gs://ricky-poc-testing/input/test.csv"))
     .apply(ParDo.named("ExtractWords").of(new DoFn<String, String>() {
        @Override
        public void processElement(ProcessContext c) {
     //   	c.output(new String(c.element()).toUpperCase());
        	c.output(c.element());
          }
        
      }))
     
     
    .apply(TextIO.Write.named("Write").to("gs://ricky-poc-testing/output/output.csv"));
        
	
	
   
    

    p.run();
  }
}
