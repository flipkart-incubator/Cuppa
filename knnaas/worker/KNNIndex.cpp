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

#include <iostream>
#include <string> 
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <queue>
#include <unordered_set>
#include <math.h>
#include <boost/python.hpp>
#include <boost/python/suite/indexing/map_indexing_suite.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <execinfo.h>

//typedef logger< gather::ostream_like::return_str<std::string>, write_to_cout> logger_type;
//BOOST_DECLAREstd::cout(g_single_log, logger_type)
//BOOST_DECLAREstd::cout_FILTER(g_filter, filter::no_ts)

//#define _LOG BOOST_LOG_USE_LOG_IF_FILTER(g_single_log, g_filter->is_enabled() ) 

// TODO Use doxygen style documentation. Code documentation is important. http://www.stack.nl/~dimitri/doxygen/
// TODO Use logging instead of std:cout
class ScopedGILRelease {
    public:
        inline ScopedGILRelease() { m_thread_state = PyEval_SaveThread(); }
        inline ~ScopedGILRelease() { PyEval_RestoreThread(m_thread_state); m_thread_state = NULL; }
    private:
        PyThreadState* m_thread_state;
};

namespace py = boost::python;

void handler(int sig) {
  // TODO Why the size if 10? Don't hard code
  void *array[10];
  size_t size;

  size = backtrace(array, 10);

  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

struct Record 
{
	long key;
    std::vector<float> vector;
    std::unordered_set<long> tags;
	bool deleted = false;

	long get_key() {
		return key;
	}

	void set_key(long key) {
		this->key = key;
	}

	std::vector<float> get_feature_vector() {
		return vector;
	}

	void set_feature_vector(std::vector<float>& vector) {
		this->vector = vector;
	}
	void set_tag_vector(std::vector<long>& tag_vector) {
		for (long i = 0; i < tag_vector.size(); i++) {
			tags.insert(tag_vector[i]);
		}
	}
	bool operator == (const Record& record) const { return key == record.key; }
};

typedef std::vector<Record> IndexType;

// TODO: Why is this global? This is only used in KNNIndex
std::mutex op_mutex;

class KNNIndex {

    private:

        IndexType index;
        std::map<long, long> index_ids_to_posn_map;
        std::queue<long> deleted_posns;
        // TODO: Use a better name?
        py::list g_retList;

    public:

        bool destruct(){
            std::vector<Record>().swap(index);
            index_ids_to_posn_map.clear();
            while(!deleted_posns.empty()){
                deleted_posns.pop();
            }
            return true;
        }
        void set_index(IndexType& index) {

		    this->index = index;
            long counter = 0;
		    for (auto it = index.begin(); it != index.end(); it++) {
			    index_ids_to_posn_map[it->key] = counter++;
	    	}  
	    }

	    IndexType& get_index() {

		    return this->index;

	    }   


        bool insert(Record& record) {

		    if (index_ids_to_posn_map.find(record.key) != index_ids_to_posn_map.end()) {
			    std::stringstream ss;
			    ss << "record with key " << record.key << " already exists";
			    throw std::logic_error(ss.str());
		    }
		
            if (!deleted_posns.empty()) {
                // TODO better name?
                long dp;
                std::lock_guard<std::mutex> guard(op_mutex);
                if(!deleted_posns.empty()){
                    dp = deleted_posns.front();
                    deleted_posns.pop();
                    index[dp].key = record.key;
                    index[dp].vector = record.vector;
                    index[dp].tags = record.tags;
                    index[dp].deleted = false;
                    index_ids_to_posn_map[record.key] = dp;
                    return true;
                }
            }
            std::lock_guard<std::mutex> guard(op_mutex);		
		    index.push_back(record);
		    index_ids_to_posn_map[record.key] = index.size() - 1;
            return true;
	    }

	    bool remove(Record& record) {
		    if (index_ids_to_posn_map.find(record.key) == index_ids_to_posn_map.end()) {
			    std::stringstream ss;
			    ss << "record with key " << record.key << " doesn't exist";
			    throw std::logic_error(ss.str());
            }
		    std::lock_guard<std::mutex> guard(op_mutex);		
		    index[index_ids_to_posn_map[record.key]].deleted = true;
            deleted_posns.push(index_ids_to_posn_map[record.key]);
		    index_ids_to_posn_map.erase(record.key);
		    return true;
	    }   

	    void print_index() {

		    for(IndexType::iterator it = index.begin(); it != index.end(); it++) {
			    std::cout << it->key << " -> VECTOR >> ";
			
			    for (std::vector<float>::iterator iter = it->vector.begin(); iter != it->vector.end(); iter++) {
				    std::cout << *iter << ", ";
			    }
			    
                std::cout << " -> TAGS >> ";
			
                for (auto iter = it->tags.begin(); iter != it->tags.end(); iter++) {
                    std::cout << *iter << ", ";
                }
			    
                std::cout << "\n";
		    }
	    }
	
	    const long print_stats() {
		    long count = 0;
		    for(IndexType::iterator it = index.begin(); it != index.end(); it++) {
			    count++;
		    }
		    return count;
	    }

	    struct ResultRecord {
		    long key;
		    float distance;

		    ResultRecord()
            {
            }

		    ResultRecord(long key, float distance) : key(key), distance(distance) 
            {
            }
		    
            bool operator < (const ResultRecord& record) const { return (distance < record.distance); }
            bool operator == (const ResultRecord& record) const { return key == record.key; }
		    bool operator() (const ResultRecord a, ResultRecord b) { return a.distance < b.distance; }
	    };

	    std::vector<ResultRecord> calculate_distance_multithreaded(
                                                                    std::vector<float>& input_vector, 
                                                                    std::vector<long>& input_tags, 
                                                                    int num_threads, 
                                                                    int top_n = 1000
                                                                  ) {
            long batch_size = (long)ceil(index.size() / num_threads);
		    int remainder = index.size() % num_threads;
		    long begin_index = 0;
		    long end_index;

		    std::vector<std::thread> v_threads;
		    std::vector<std::vector<ResultRecord>> v_results;

		    signal(SIGSEGV, handler);
		    ScopedGILRelease release_gil = ScopedGILRelease();
		    
            int total_threads = (remainder == 0) ? num_threads : num_threads + 1;

		    v_results.reserve(total_threads);
		    v_threads.reserve(total_threads);

            // TODO: Use threadpool instead. There is an overhead with thread creation and destruction.
		    for (int i = 0; i < total_threads; i++) {
			    v_results.push_back(std::vector<ResultRecord>());
			    std::vector<ResultRecord>& res = v_results[i];
			    end_index = (i < num_threads) ? begin_index + batch_size : begin_index + (index.size() - begin_index);
			    v_threads.push_back(
                                    std::thread(
                                        &KNNIndex::calculate_distance_return_list_using_iterator, 
                                            this, 
                                            i, 
                                            std::ref(input_vector), 
                                            std::ref(input_tags), 
                                            begin_index, 
                                            end_index,  
                                            std::ref(res), 
                                            top_n
                                            )
                                   );
			    begin_index += batch_size;
            }
            
            std::for_each(v_threads.begin(), v_threads.end(), std::mem_fn(&std::thread::join));
		    std::priority_queue<ResultRecord, std::vector<ResultRecord>, ResultRecord> top_k;
            
            for(int i = 0; i < v_results.size(); i++) {
                for (int j = 0; j < v_results[i].size(); j++) {
				    ResultRecord& res = v_results[i][j];
				    top_k.push(res);

				    if (top_k.size() > top_n) {
					    top_k.pop(); // TODO : how do i remove the element from the back? top_back?
				    }
			    }
		    }
            
		    std::vector<ResultRecord> res;
		    int result_count = (top_k.size() < top_n) ? top_k.size() : top_n;

		    while (res.size() < result_count) {
			    res.push_back(top_k.top());
			    top_k.pop();
		    }
		    
            return res;
	}

    void calculate_distance_return_list_using_iterator(
                                                        int thread_id, 
                                                        std::vector<float>& input_vector,
                                                        std::vector<long>& input_tags,
                                                        long begin_index,
                                                        long end_index,
                                                        std::vector<ResultRecord>& result,
                                                        int top_n
                                                       ) {
            float dist;
            long counter = 0;
		    IndexType::iterator index_iterator = index.begin();
            std::vector<float>::iterator index_feature_iterator, input_vector_iterator;
                //for (; (indexIterator != index.end()) && counter++ < (endIndex - beginIndex); indexIterator++) { 
		    int num_loops = end_index - begin_index;
            std::cout << "[DIST CAL STARTED]" << std::endl;  
            for (std::advance(index_iterator, begin_index); counter++ < num_loops; index_iterator++) { 
			    bool tags_matched = true;
                
			    if (index_iterator->deleted == true) {
                    std::cout << "[DELETED]" << std::endl;
				    continue;
			    }

                if (input_vector.size() != index_iterator->vector.size()) {
                    std::cout << "[Dim Mismatch]" << std::endl ;
                    std::cout << std::to_string(input_vector.size()); 
                    std::cout << "[Input Vector]" << std::endl;
                    std::cout << std::to_string(index_iterator->vector.size());
                    std::cout << "[Iterator Vector]" << std::endl;
                    continue;
                }
			
                for (auto it = input_tags.begin(); it != input_tags.end(); it++) {
				
                    if (index_iterator->tags.find(*it) == index_iterator->tags.end()) {
                        std::cout << "[TAG MISMATCHED]" << std::endl;
					    tags_matched = false;
					    break;
				    }
			    }
			
                if (tags_matched == false) {
				    continue;
			    }   
            
                for (
                    dist = 0, 
                    index_feature_iterator = index_iterator->vector.begin(), 
                    input_vector_iterator = input_vector.begin(); 
                  
                    input_vector_iterator != input_vector.end();

                    index_feature_iterator++, 
                    input_vector_iterator++
                    ) {
                
                    dist +=  (*index_feature_iterator - *input_vector_iterator) * (*index_feature_iterator - *input_vector_iterator);
                }
            
                float x = sqrt(dist);
			
                if (dist == 0) {
				    //std::cout << "Ignoring : " << indexIterator->key << " as distance is 0 \n";
				    continue;
		    	}
			    //std::cout << "iterator : " << indexIterator->key << " " <<  std::sqrt(dist) << "\n";

			    result.push_back(ResultRecord(index_iterator->key, std::sqrt(dist)));
            
             }   
		//std::cout << "Start Sorting \n";
		//std::cout<< "result count " << result.size()<<"\n";
		    if (result.size() > 0) {
			    int result_count = (result.size() < top_n) ? result.size() - 1 : top_n;
                std::partial_sort(result.begin(), result.begin() + result_count, result.end());
			    result.resize(result_count);
		    }
		//std::cout << "Exiting calculateDistanceReturnListUsingIterator \n";
        }

        std::vector<float> get_feature_vektor(long key) {
            if (index_ids_to_posn_map.find(key) == index_ids_to_posn_map.end()) {
			    std::stringstream ss;
			    ss << "record with key " << key << " doesn't exist";
			    throw std::logic_error(ss.str());
            }
            else {
                long posn = index_ids_to_posn_map[key];
                Record record = index[posn];
                return record.get_feature_vector();
            }

        }   
};


BOOST_PYTHON_MODULE(KNNINDEX)
{
  PyEval_InitThreads();
  using namespace boost::python;
  class_<Record>("Record")
	.def("set_key", &Record::set_key)
	.def("set_feature_vector", &Record::set_feature_vector)
	.def("set_tag_vector", &Record::set_tag_vector)
	.def("get_feature_vector", &Record::get_feature_vector)
	.def("get_key", &Record::get_key);
  class_<std::vector<Record>>("Index")
  	.def(vector_indexing_suite<std::vector<Record>>());
  class_<std::pair<long, std::vector<float>>>("IndexIDFeaturePair")
	.def_readwrite("first", &std::pair<long, std::vector<float>>::first)
	.def_readwrite("second", &std::pair<long, std::vector<float>>::second);
  class_<std::vector<float>>("FloatVector")
	.def(vector_indexing_suite<std::vector<float>>());
  class_<std::vector<long>>("LongVector")
	.def(vector_indexing_suite<std::vector<long>>());
  class_<KNNIndex>("KNNIndex")
	//.def("setIndex", &KNNIndex::setIndex)
	//.def("getIndex", &KNNIndex::getIndex)
	.def("insert", &KNNIndex::insert)
	.def("remove", &KNNIndex::remove)
	.def("printIndex", &KNNIndex::print_index)
	.def("calculate_distance_multithreaded", &KNNIndex::calculate_distance_multithreaded)
	.def("print_stats", &KNNIndex::print_stats)
	.def("get_feature_vektor", &KNNIndex::get_feature_vektor)
    .def("destruct", &KNNIndex::destruct);

  class_<KNNIndex::ResultRecord>("ResultRecord")
	.def_readwrite("key", &KNNIndex::ResultRecord::key)
	.def_readwrite("distance", &KNNIndex::ResultRecord::distance);
  class_<std::vector<KNNIndex::ResultRecord>>("KNNIndex::ResultRecordVector")
	.def(vector_indexing_suite<std::vector<KNNIndex::ResultRecord>>());
}
