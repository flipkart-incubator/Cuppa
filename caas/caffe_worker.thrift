/*
 * Copyright 2012-2016, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

struct PredictionInput {
    1: string url;
    2: string model_id
}

struct BaseOutput {
    1: string status;
    2: string message;
}

struct PredictionOutput {
       1:BaseOutput bo;
       2:list<double> values;
}

struct CommissionInput {
    1: string modelId;
    2: string modelPathsJson;
}

service Worker {
     BaseOutput status()
     BaseOutput commission(1: CommissionInput ci)
     BaseOutput decommission()     
     PredictionOutput predict(1: PredictionInput pi)
}
