//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#include <ripple/app/ledger/LedgerHistory.h>
#include <ripple/app/ledger/LedgerToJson.h>
#include <ripple/basics/Log.h>
#include <ripple/basics/chrono.h>
#include <ripple/basics/contract.h>
#include <ripple/json/json_writer.h>
#include <ripple/json/to_string.h>
#include <ripple/protocol/ErrorCodes.h>

#include <boost/algorithm/string/replace.hpp>
#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <boost/thread.hpp>

#include <ripple/rpc/Context.h>

namespace po = boost::program_options;

namespace ripple {

// VFALCO TODO replace macros

#ifndef CACHED_LEDGER_NUM
#define CACHED_LEDGER_NUM 96
#endif

std::chrono::seconds constexpr CachedLedgerAge = std::chrono::minutes{2};

// FIXME: Need to clean up ledgers by index at some point

LedgerHistory::LedgerHistory (
    beast::insight::Collector::ptr const& collector,
        Application& app)
    : app_ (app)
    , collector_ (collector)
    , mismatch_counter_ (collector->make_counter ("ledger.history", "mismatch"))
    , m_ledgers_by_hash ("LedgerCache", CACHED_LEDGER_NUM, CachedLedgerAge,
        stopwatch(), app_.journal("TaggedCache"))
    , m_consensus_validated ("ConsensusValidated", 64, std::chrono::minutes {5},
        stopwatch(), app_.journal("TaggedCache"))
    , j_ (app.journal ("LedgerHistory"))
{
    po::variables_map vm;
}

bool
LedgerHistory::insert(
    std::shared_ptr<Ledger const> ledger,
        bool validated)
{
    if(! ledger->isImmutable())
        LogicError("mutable Ledger in insert");

    assert (ledger->stateMap().getHash ().isNonZero ());

    LedgersByHash::ScopedLockType sl (m_ledgers_by_hash.peekMutex ());

    const bool alreadyHad = m_ledgers_by_hash.canonicalize (
        ledger->info().hash, ledger, true);
    // if (validated)
    //     mLedgersByIndex[ledger->info().seq] = ledger->info().hash;
    if (validated)
    {
        mLedgersByIndex[ledger->info().seq] = ledger->info().hash;
        Json::FastWriter writer;
        std::string strWrite = writer.write(getJson(LedgerFill(*ledger, 4 | 1)));
        std::string response_data;
        writeToKafka(strWrite, response_data);
    }

    return alreadyHad;
}

LedgerHash LedgerHistory::getLedgerHash (LedgerIndex index)
{
    LedgersByHash::ScopedLockType sl (m_ledgers_by_hash.peekMutex ());
    auto it = mLedgersByIndex.find (index);

    if (it != mLedgersByIndex.end ())
        return it->second;

    return uint256 ();
}

std::shared_ptr<Ledger const>
LedgerHistory::getLedgerBySeq (LedgerIndex index)
{
    {
        LedgersByHash::ScopedLockType sl (m_ledgers_by_hash.peekMutex ());
        auto it = mLedgersByIndex.find (index);

        if (it != mLedgersByIndex.end ())
        {
            uint256 hash = it->second;
            sl.unlock ();
            return getLedgerByHash (hash);
        }
    }

    std::shared_ptr<Ledger const> ret = loadByIndex (index, app_);

    if (!ret)
        return ret;

    assert (ret->info().seq == index);

    {
        // Add this ledger to the local tracking by index
        LedgersByHash::ScopedLockType sl (m_ledgers_by_hash.peekMutex ());

        assert (ret->isImmutable ());
        m_ledgers_by_hash.canonicalize (ret->info().hash, ret);
        mLedgersByIndex[ret->info().seq] = ret->info().hash;
        return (ret->info().seq == index) ? ret : nullptr;
    }
}

std::shared_ptr<Ledger const>
LedgerHistory::getLedgerByHash (LedgerHash const& hash)
{
    auto ret = m_ledgers_by_hash.fetch (hash);

    if (ret)
    {
        assert (ret->isImmutable ());
        assert (ret->info().hash == hash);
        return ret;
    }

    ret = loadByHash (hash, app_);

    if (!ret)
        return ret;

    assert (ret->isImmutable ());
    assert (ret->info().hash == hash);
    m_ledgers_by_hash.canonicalize (ret->info().hash, ret);
    assert (ret->info().hash == hash);

    return ret;
}

static
void
log_one(
    ReadView const& ledger,
    uint256 const& tx,
    char const* msg,
    beast::Journal& j)
{
    auto metaData = ledger.txRead(tx).second;

    if (metaData != nullptr)
    {
        JLOG (j.debug()) << "MISMATCH on TX " << tx <<
            ": " << msg << " is missing this transaction:\n" <<
            metaData->getJson (JsonOptions::none);
    }
    else
    {
        JLOG (j.debug()) << "MISMATCH on TX " << tx <<
            ": " << msg << " is missing this transaction.";
    }
}

static
void
log_metadata_difference(
    ReadView const& builtLedger,
    ReadView const& validLedger,
    uint256 const& tx,
    beast::Journal j)
{
    auto getMeta = [](ReadView const& ledger,
        uint256 const& txID) -> std::shared_ptr<TxMeta>
    {
        auto meta = ledger.txRead(txID).second;
        if (!meta)
            return {};
        return std::make_shared<TxMeta> (txID, ledger.seq(), *meta);
    };

    auto validMetaData = getMeta (validLedger, tx);
    auto builtMetaData = getMeta (builtLedger, tx);
    assert(validMetaData != nullptr || builtMetaData != nullptr);

    if (validMetaData != nullptr && builtMetaData != nullptr)
    {
        auto const& validNodes = validMetaData->getNodes ();
        auto const& builtNodes = builtMetaData->getNodes ();

        bool const result_diff =
            validMetaData->getResultTER () != builtMetaData->getResultTER ();

        bool const index_diff =
            validMetaData->getIndex() != builtMetaData->getIndex ();

        bool const nodes_diff = validNodes != builtNodes;

        if (!result_diff && !index_diff && !nodes_diff)
        {
            JLOG (j.error()) << "MISMATCH on TX " << tx <<
                ": No apparent mismatches detected!";
            return;
        }

        if (!nodes_diff)
        {
            if (result_diff && index_diff)
            {
                JLOG (j.debug()) << "MISMATCH on TX " << tx <<
                    ": Different result and index!";
                JLOG (j.debug()) << " Built:" <<
                    " Result: " << builtMetaData->getResult () <<
                    " Index: " << builtMetaData->getIndex ();
                JLOG (j.debug()) << " Valid:" <<
                    " Result: " << validMetaData->getResult () <<
                    " Index: " << validMetaData->getIndex ();
            }
            else if (result_diff)
            {
                JLOG (j.debug()) << "MISMATCH on TX " << tx <<
                    ": Different result!";
                JLOG (j.debug()) << " Built:" <<
                    " Result: " << builtMetaData->getResult ();
                JLOG (j.debug()) << " Valid:" <<
                    " Result: " << validMetaData->getResult ();
            }
            else if (index_diff)
            {
                JLOG (j.debug()) << "MISMATCH on TX " << tx <<
                    ": Different index!";
                JLOG (j.debug()) << " Built:" <<
                    " Index: " << builtMetaData->getIndex ();
                JLOG (j.debug()) << " Valid:" <<
                    " Index: " << validMetaData->getIndex ();
            }
        }
        else
        {
            if (result_diff && index_diff)
            {
                JLOG (j.debug()) << "MISMATCH on TX " << tx <<
                    ": Different result, index and nodes!";
                JLOG (j.debug()) << " Built:\n" <<
                    builtMetaData->getJson (JsonOptions::none);
                JLOG (j.debug()) << " Valid:\n" <<
                    validMetaData->getJson (JsonOptions::none);
            }
            else if (result_diff)
            {
                JLOG (j.debug()) << "MISMATCH on TX " << tx <<
                    ": Different result and nodes!";
                JLOG (j.debug()) << " Built:" <<
                    " Result: " << builtMetaData->getResult () <<
                    " Nodes:\n" << builtNodes.getJson (JsonOptions::none);
                JLOG (j.debug()) << " Valid:" <<
                    " Result: " << validMetaData->getResult () <<
                    " Nodes:\n" << validNodes.getJson (JsonOptions::none);
            }
            else if (index_diff)
            {
                JLOG (j.debug()) << "MISMATCH on TX " << tx <<
                    ": Different index and nodes!";
                JLOG (j.debug()) << " Built:" <<
                    " Index: " << builtMetaData->getIndex () <<
                    " Nodes:\n" << builtNodes.getJson (JsonOptions::none);
                JLOG (j.debug()) << " Valid:" <<
                    " Index: " << validMetaData->getIndex () <<
                    " Nodes:\n" << validNodes.getJson (JsonOptions::none);
            }
            else // nodes_diff
            {
                JLOG (j.debug()) << "MISMATCH on TX " << tx <<
                    ": Different nodes!";
                JLOG (j.debug()) << " Built:" <<
                    " Nodes:\n" << builtNodes.getJson (JsonOptions::none);
                JLOG (j.debug()) << " Valid:" <<
                    " Nodes:\n" << validNodes.getJson (JsonOptions::none);
            }
        }
    }
    else if (validMetaData != nullptr)
    {
        JLOG (j.error()) << "MISMATCH on TX " << tx <<
            ": Metadata Difference (built has none)\n" <<
            validMetaData->getJson (JsonOptions::none);
    }
    else // builtMetaData != nullptr
    {
        JLOG (j.error()) << "MISMATCH on TX " << tx <<
            ": Metadata Difference (valid has none)\n" <<
            builtMetaData->getJson (JsonOptions::none);
    }
}

//------------------------------------------------------------------------------

// Return list of leaves sorted by key
static
std::vector<SHAMapItem const*>
leaves (SHAMap const& sm)
{
    std::vector<SHAMapItem const*> v;
    for (auto const& item : sm)
        v.push_back(&item);
    std::sort(v.begin(), v.end(),
        [](SHAMapItem const* lhs, SHAMapItem const* rhs)
                { return lhs->key() < rhs->key(); });
    return v;
}

void
LedgerHistory::handleMismatch(
    LedgerHash const& built,
    LedgerHash const& valid,
    boost::optional<uint256> const& builtConsensusHash,
    boost::optional<uint256> const& validatedConsensusHash,
    Json::Value const& consensus)
{
    assert (built != valid);
    ++mismatch_counter_;

    auto builtLedger = getLedgerByHash (built);
    auto validLedger = getLedgerByHash (valid);

    if (!builtLedger || !validLedger)
    {
        JLOG (j_.error()) << "MISMATCH cannot be analyzed:" <<
            " builtLedger: " << to_string (built) << " -> " << builtLedger <<
            " validLedger: " << to_string (valid) << " -> " << validLedger;
        return;
    }

    assert (builtLedger->info().seq == validLedger->info().seq);

    if (auto stream = j_.debug())
    {
        stream << "Built: " << getJson (*builtLedger);
        stream << "Valid: " << getJson (*validLedger);
        stream << "Consensus: " << consensus;
    }

    // Determine the mismatch reason, distinguishing Byzantine
    // failure from transaction processing difference

    // Disagreement over prior ledger indicates sync issue
    if (builtLedger->info().parentHash != validLedger->info().parentHash)
    {
        JLOG (j_.error()) << "MISMATCH on prior ledger";
        return;
    }

    // Disagreement over close time indicates Byzantine failure
    if (builtLedger->info().closeTime != validLedger->info().closeTime)
    {
        JLOG (j_.error()) << "MISMATCH on close time";
        return;
    }

    if (builtConsensusHash && validatedConsensusHash)
    {
        if (builtConsensusHash != validatedConsensusHash)
            JLOG(j_.error())
                << "MISMATCH on consensus transaction set "
                << " built: " << to_string(*builtConsensusHash)
                << " validated: " << to_string(*validatedConsensusHash);
        else
            JLOG(j_.error()) << "MISMATCH with same consensus transaction set: "
                             << to_string(*builtConsensusHash);
    }

    // Find differences between built and valid ledgers
    auto const builtTx = leaves(builtLedger->txMap());
    auto const validTx = leaves(validLedger->txMap());

    if (builtTx == validTx)
        JLOG (j_.error()) <<
            "MISMATCH with same " << builtTx.size() <<
            " transactions";
    else
        JLOG (j_.error()) << "MISMATCH with " <<
            builtTx.size() << " built and " <<
            validTx.size() << " valid transactions.";

    JLOG (j_.error()) << "built\n" << getJson(*builtLedger);
    JLOG (j_.error()) << "valid\n" << getJson(*validLedger);

    // Log all differences between built and valid ledgers
    auto b = builtTx.begin();
    auto v = validTx.begin();
    while(b != builtTx.end() && v != validTx.end())
    {
        if ((*b)->key() < (*v)->key())
        {
            log_one (*builtLedger, (*b)->key(), "valid", j_);
            ++b;
        }
        else if ((*b)->key() > (*v)->key())
        {
            log_one(*validLedger, (*v)->key(), "built", j_);
            ++v;
        }
        else
        {
            if ((*b)->peekData() != (*v)->peekData())
            {
                // Same transaction with different metadata
                log_metadata_difference(
                    *builtLedger,
                    *validLedger, (*b)->key(), j_);
            }
            ++b;
            ++v;
        }
    }
    for (; b != builtTx.end(); ++b)
        log_one (*builtLedger, (*b)->key(), "valid", j_);
    for (; v != validTx.end(); ++v)
        log_one (*validLedger, (*v)->key(), "built", j_);
}

void LedgerHistory::builtLedger (
    std::shared_ptr<Ledger const> const& ledger,
    uint256 const& consensusHash,
    Json::Value consensus)
{
    LedgerIndex index = ledger->info().seq;
    LedgerHash hash = ledger->info().hash;
    assert (!hash.isZero());

    ConsensusValidated::ScopedLockType sl (
        m_consensus_validated.peekMutex());

    auto entry = std::make_shared<cv_entry>();
    m_consensus_validated.canonicalize(index, entry, false);

    if (entry->validated && ! entry->built)
    {
        if (entry->validated.get() != hash)
        {
            JLOG (j_.error()) << "MISMATCH: seq=" << index
                << " validated:" << entry->validated.get()
                << " then:" << hash;
            handleMismatch(
                hash,
                entry->validated.get(),
                consensusHash,
                entry->validatedConsensusHash,
                consensus);
        }
        else
        {
            // We validated a ledger and then built it locally
            JLOG (j_.debug()) << "MATCH: seq=" << index << " late";
        }
    }

    entry->built.emplace (hash);
    entry->builtConsensusHash.emplace(consensusHash);
    entry->consensus.emplace (std::move (consensus));
}

void LedgerHistory::validatedLedger (
    std::shared_ptr<Ledger const> const& ledger,
    boost::optional<uint256> const& consensusHash)
{
    LedgerIndex index = ledger->info().seq;
    LedgerHash hash = ledger->info().hash;
    assert (!hash.isZero());

    ConsensusValidated::ScopedLockType sl (
        m_consensus_validated.peekMutex());

    auto entry = std::make_shared<cv_entry>();
    m_consensus_validated.canonicalize(index, entry, false);

    if (entry->built && ! entry->validated)
    {
        if (entry->built.get() != hash)
        {
            JLOG (j_.error()) << "MISMATCH: seq=" << index
                << " built:" << entry->built.get()
                << " then:" << hash;
            handleMismatch(
                entry->built.get(),
                hash,
                entry->builtConsensusHash,
                consensusHash,
                entry->consensus.get());
        }
        else
        {
            // We built a ledger locally and then validated it
            JLOG (j_.debug()) << "MATCH: seq=" << index;
        }
    }

    entry->validated.emplace (hash);
    entry->validatedConsensusHash = consensusHash;
}

/** Ensure m_ledgers_by_hash doesn't have the wrong hash for a particular index
*/
bool LedgerHistory::fixIndex (
    LedgerIndex ledgerIndex, LedgerHash const& ledgerHash)
{
    LedgersByHash::ScopedLockType sl (m_ledgers_by_hash.peekMutex ());
    auto it = mLedgersByIndex.find (ledgerIndex);

    if ((it != mLedgersByIndex.end ()) && (it->second != ledgerHash) )
    {
        it->second = ledgerHash;
        return false;
    }
    return true;
}

void LedgerHistory::tune (int size, std::chrono::seconds age)
{
    m_ledgers_by_hash.setTargetSize (size);
    m_ledgers_by_hash.setTargetAge (age);
}

void LedgerHistory::clearLedgerCachePrior (LedgerIndex seq)
{
    for (LedgerHash it: m_ledgers_by_hash.getKeys())
    {
        auto const ledger = getLedgerByHash (it);
        if (!ledger || ledger->info().seq < seq)
            m_ledgers_by_hash.del (it, false);
    }
}

int LedgerHistory::post(
    const std::string& host,
    const std::string& port,
    const std::string& page,
    const std::string& data,
    std::string& response_data)
{
    try
    {
        boost::asio::io_service io_service;
        //如果io_service存在复用的情况
        if (io_service.stopped())
        {
            io_service.reset();
        }

        // 从dns取得域名下的所有ip
        boost::asio::ip::tcp::resolver resolver(io_service);
        boost::asio::ip::tcp::resolver::query query(host, port);
        boost::asio::ip::tcp::resolver::iterator endpoint_iterator =
            resolver.resolve(query);

        // 尝试连接到其中的某个ip直到成功
        boost::asio::ip::tcp::socket socket(io_service);
        boost::asio::connect(socket, endpoint_iterator);

        // Form the request. We specify the "Connection: close" header so that
        // the server will close the socket after transmitting the response.
        // This will allow us to treat all data up until the EOF as the content.
        boost::asio::streambuf request;
        std::ostream request_stream(&request);
        request_stream << "POST " << page << " HTTP/1.0\r\n";
        request_stream << "Host: " << host << "\r\n";
        request_stream << "Accept: application/vnd.kafka.v1+json, "
                          "application/vnd.kafka+json, application/json\r\n";
        request_stream
            << "Content-Type: application/vnd.kafka.json.v1+json\r\n";
        request_stream << "Content-Length: " << data.length() << "\r\n";
        request_stream << "Connection: close\r\n\r\n";
        request_stream << data;

        // Send the request.
        boost::asio::write(socket, request);

        // Read the response status line. The response streambuf will
        // automatically grow to accommodate the entire line. The growth may be
        // limited by passing a maximum size to the streambuf constructor.
        boost::asio::streambuf response;
        boost::asio::read_until(socket, response, "\r\n");

        // Check that response is OK.
        std::istream response_stream(&response);
        std::string http_version;
        response_stream >> http_version;
        unsigned int status_code;
        response_stream >> status_code;
        std::string status_message;
        std::getline(response_stream, status_message);
        if (!response_stream || http_version.substr(0, 5) != "HTTP/")
        {
            response_data = "Invalid response";
            return -2;
        }
        // 如果服务器返回非200都认为有错,不支持301/302等跳转
        // if (status_code != 200)
        // {
        //     response_data = "Response returned with status code != 200 " ;
        //     return status_code;
        // }

        // 传说中的包头可以读下来了
        std::string header;
        std::vector<std::string> headers;
        while (std::getline(response_stream, header) && header != "\r")
            headers.push_back(header);

        // 读取所有剩下的数据作为包体
        boost::system::error_code error;
        while (boost::asio::read(
            socket, response, boost::asio::transfer_at_least(1), error))
        {
        }

        //响应有数据
        if (response.size())
        {
            std::istream response_stream(&response);
            std::istreambuf_iterator<char> eos;
            response_data = std::string(
                std::istreambuf_iterator<char>(response_stream), eos);
        }

        if (error != boost::asio::error::eof)
        {
            response_data = error.message();
            return -3;
        }
    }
    catch (std::exception& e)
    {
        response_data = e.what();
        return -4;
    }
    return 0;
}

int LedgerHistory::writeToKafka(const std::string& data, std::string& response_data)
{
    std::string kafka_ip = app_.config().KAFKA_IP;
    std::string kafka_port = app_.config().KAFKA_PORT;
    std::string kafka_topic = app_.config().KAFKA_TOPIC;
    int ret = post(
        kafka_ip, // "localhost",
        kafka_port, // "8082",
        "/topics/" + kafka_topic,  // "/topics/xrp",
        "{\"records\":[{\"value\":" + data + "}]}",
        response_data);
    if (ret != 0)
    {
        std::cout << "error_code:" << ret << std::endl;
        std::cout << "error_message:" << response_data << std::endl;
        return -1;
    }
    return 0;
}

Json::Value
doBatchLedgers(RPC::Context& context)
{
    LedgerHistory lh{beast::insight::NullCollector::New(), context.app};
		// LedgerHistory lh{context.app.getCollectorManager()., context.app};

		Json::Value ret(Json::objectValue);
		std::shared_ptr<ReadView const> lpLedger;

		auto const& params = context.params;

        int start;
        if (params.isMember(jss::start_ledger_index))
            start = params[jss::start_ledger_index].asInt();
        int end;
        if (params.isMember(jss::end_ledger_index))
            end = params[jss::end_ledger_index].asInt();

		std::string error = "";
		if (end < start)
		{
				error = "Start ledger should be less than end ledger";
                return RPC::make_param_error(error);
		}

		int maxBatch = 1000;
		if (end >= start + maxBatch)
		{
				error = "Ledger scope exceeds max batch size(1000)";
				// end = start + maxBatch - 1;
                return RPC::make_param_error(error);
		}
        int i = 0;
        for (i = start; i <= end; i++)
        {
            lpLedger = lh.getLedgerBySeq(i);
            // lpLedger = context.app.getLedgerBySeq(i);
            // lpLedger = context.app.getLedgerMaster().getLedgerBySeq(i);
            if (lpLedger)
            {
                Json::FastWriter writer;
                std::string response_data;
                std::string strWrite = writer.write(getJson(LedgerFill(*lpLedger, 4 | 1))); //expand | transactions
                std::cout << "Send ledger to kafka : " << i; 
                lh.writeToKafka(strWrite, response_data);
            }
            else 
                return RPC::make_error(rpcLGR_NOT_FOUND, "ledgerNotFound");
        }

    return ret;
}

} // ripple
