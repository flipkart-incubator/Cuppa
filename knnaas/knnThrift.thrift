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


struct ResultRecord{
    1: string id;
    2: double distance;
}

struct KNNLocalResult {
       1: list<ResultRecord> values;
       2: string message;
}


struct CommissionInput {
    1: string modelId;
    2: string modelPathsJson;
}

struct BaseOutput {
    1: string status;
    2: string message;
}

struct UpdateOutput {
    1: bool status;
    2: string message;
}

service KnnThriftService {
    BaseOutput status()
    KNNLocalResult predict(1: string model_id, 2: list<double> fv, 3: list<string> tv, 4: string data_point_id, 5: i32 by),
    UpdateOutput insert(1: string model_id, 2: string data_point_id, 3: list<double> fv, 4: list<string> tv),
    UpdateOutput remove(1: string model_id, 2: string data_point_id)
    UpdateOutput redis_insert(1: string model_id, 2: string data_point_id, 3: list<double> fv, 4: list<string> tv)
    UpdateOutput redis_delete(1: string model_id, 2: string data_point_id)
    BaseOutput commission(1: CommissionInput ci)
    BaseOutput decommission()
}

