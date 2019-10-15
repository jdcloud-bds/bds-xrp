# bds-xrp 
### src/ripple/app/ledger/LedgerHistory.h
//send data to kafka
int writeToKafka(const std::string& data, std::string& response_data);

//restructure the post request for kafka rest proxy
int post(const std::string& host, const std::string& port, const std::string& page, const std::string& data, std::string& response_data);

### src/ripple/app/ledger/LedgerHistory.cpp
The main implement methods for LedgerHistory.h file.

### src/ripple/core/Config.h
// kafka parameters
std::string KAFKA_IP;
std::string KAFKA_PORT;
std::string KAFKA_TOPIC;

### src/ripple/core/ConfigSections.h
```
#define SECTION_KAFKA_IP                "kafka_ip"
#define SECTION_KAFKA_PORT              "kafka_port"
#define SECTION_KAFKA_TOPIC             "kafka_topic"
```
### src/ripple/core/impl/Config.cpp

### src/ripple/net/impl/RPCCall.cpp
```
//rpc : parse params
Json::Value parseBatchLedger (Json::Value const& jvParams)
```

### src/ripple/rpc/handlers/Handlers.h
```
// kafka methods
{   "send_batch_ledger",    byRef(&doBatchLedgers),         Role::USER,  NO_CONDITION     },
```
