/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "rocks_snapshot_manager.h"
#include "rocks_recovery_unit.h"

#include <rocksdb/db.h>

#include "mongo/base/checked_cast.h"
#include "mongo/util/log.h"

namespace mongo {
    // This only checks invariants
    Status RocksSnapshotManager::prepareForCreateSnapshot(OperationContext* opCtx) {
        RocksRecoveryUnit::getRocksRecoveryUnit(opCtx)->prepareForCreateSnapshot(opCtx);
        return Status::OK();
    }

    void RocksSnapshotManager::setCommittedSnapshot(const Timestamp& timestamp) {
        stdx::lock_guard<stdx::mutex> lock(_mutex);

        uint64_t nameU64 = timestamp.asULL();
        _updatedCommittedSnapshot = !_committedSnapshot || *_committedSnapshot < nameU64;
        invariant(_updatedCommittedSnapshot || *_committedSnapshot == nameU64);
        if (!_updatedCommittedSnapshot)
            return;
        _committedSnapshot = nameU64;
        auto insResult = _snapshotMap.insert(SnapshotMap::value_type(nameU64, nullptr));
        if (insResult.second) {
            insResult.first->second = createSnapshot();
        }
        _committedSnapshotIter = insResult.first;
    }

    void RocksSnapshotManager::cleanupUnneededSnapshots() {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        if (!_updatedCommittedSnapshot) {
            return;
        }

        // erasing snapshots with timestamps less than *_committedSnapshot
        _snapshotMap.erase(_snapshotMap.begin(), _committedSnapshotIter);
        _updatedCommittedSnapshot = false;
    }

    void RocksSnapshotManager::dropAllSnapshots() {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _committedSnapshot = boost::none;
        _updatedCommittedSnapshot = false;
        _committedSnapshotIter = SnapshotMap::iterator{};
        _snapshotMap.clear();
    }

    bool RocksSnapshotManager::haveCommittedSnapshot() const {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        return bool(_committedSnapshot);
    }

    uint64_t RocksSnapshotManager::getCommittedSnapshotName() const {
        stdx::lock_guard<stdx::mutex> lock(_mutex);

        assertCommittedSnapshot_inlock();
        return *_committedSnapshot;
    }

    RocksSnapshotManager::SnapshotHolder RocksSnapshotManager::getCommittedSnapshot() const {
        stdx::lock_guard<stdx::mutex> lock(_mutex);

        assertCommittedSnapshot_inlock();
        return _committedSnapshotIter->second;
    }

    void RocksSnapshotManager::insertSnapshot(const Timestamp timestamp) {
        uint64_t nameU64 = timestamp.asULL();
        auto holder = createSnapshot();

        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _snapshotMap[nameU64] = holder;
    }

    void RocksSnapshotManager::assertCommittedSnapshot_inlock() const {
        uassert(ErrorCodes::ReadConcernMajorityNotAvailableYet,
                "Committed view disappeared while running operation", _committedSnapshot);
    }

    RocksSnapshotManager::SnapshotHolder RocksSnapshotManager::createSnapshot() {
        auto db = this->_db;
        return SnapshotHolder(db->GetSnapshot(), [db](const rocksdb::Snapshot* snapshot) {
            if (snapshot != nullptr) {
                invariant(db != nullptr);
                db->ReleaseSnapshot(snapshot);
            }
        });
    }

    void RocksSnapshotManager::opStarted(RocksRecoveryUnit* ru, const Timestamp& timestamp) {
        stdx::lock_guard<stdx::mutex> lock(_co_mutex);
        if (ru->_isTimestamped) {
            _commitOrder.erase(ru->_futureWritesTimestamp.asULL());
        }
        _commitOrder[timestamp.asULL()] = ru;
    }

    void RocksSnapshotManager::opAborted(RocksRecoveryUnit* ru){
        if (!ru->_isTimestamped)
            return;
        stdx::lock_guard<stdx::mutex> lock(_co_mutex);
        auto it = _commitOrder.find(ru->_futureWritesTimestamp.asULL());
        invariant(it != _commitOrder.end());
        auto next = _commitOrder.erase(it);
        if (next != _commitOrder.end() && next == _commitOrder.begin()){
            next->second->_allowCommit();
        }
    }

    void RocksSnapshotManager::opCommitted_inlock(){
        auto next = _commitOrder.erase(_commitOrder.begin());
        if (next != _commitOrder.end()) {
            next->second->_allowCommit();
        }
    }

    stdx::mutex& RocksSnapshotManager::getCOMutex() const {
        return _co_mutex;
    }

    bool RocksSnapshotManager::canCommit_inlock(RocksRecoveryUnit* ru) const {
        invariant(_commitOrder.size() > 0);
        return _commitOrder.begin()->second == ru;
    }

}  // namespace mongo
