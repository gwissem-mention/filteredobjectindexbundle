<?php
namespace Celltrak\FilteredObjectIndexBundle\Component\Index;

use Celltrak\FilteredObjectIndexBundle\Component\Query\GroupedFilterQuery;
use Celltrak\FilteredObjectIndexBundle\Component\Set\IntersectionSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\PersistedSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\UnionSet;
use Celltrak\FilteredObjectIndexBundle\Exception\LockedObjectException;
use Celltrak\RedisBundle\Component\Client\CelltrakRedis;
use Celltrak\RedisBundle\Component\Multi\Multi;


/**
 * Defines a group of filtered object indexes contextually related to each other.
 */
class IndexGroup
{
    /**
     * @var string
     * Prefix for all group Redis keys.
     */
    const KEY_ROOT = 'foi';

    /**
     * @var integer
     * Default number of seconds before an object write lock expires.
     */
    const DEFAULT_OBJECT_LOCK_TTL = 5;

    /**
     * @var integer
     * Number of seconds before giving up on acquiring an object's write lock.
     */
    const OBJECT_LOCK_WAIT_TIMEOUT = 3;


    /**
     * @var string
     * Name of index group.
     */
    protected $groupName;

    /**
     * @var CelltrakRedis
     * Redis client used by this group.
     */
    protected $redis;

    /**
     * @var integer
     * Number of seconds before an object write lock expires.
     */
    protected $objectLockTtl;

    /**
     * @var string
     * Namespace to prevent key collisions when used in a multi-tenant setup.
     */
    protected $tenantNamespace;


    /**
     * @param string $groupName
     * @param CelltrakRedis $redis
     * @param integer $objectLockTtl
     * @param string $tenantNamespace
     */
    public function __construct(
        $groupName,
        CelltrakRedis $redis,
        $objectLockTtl = self::DEFAULT_OBJECT_LOCK_TTL,
        $tenantNamespace = null
    ) {
        $this->groupName        = $groupName;
        $this->redis            = $redis;
        $this->objectLockTtl    = $objectLockTtl;
        $this->tenantNamespace  = $tenantNamespace;
    }

    /**
     * Adds object to index.
     *
     * @param mixed $objectId
     * @param string $index
     * @param array $filters    Set of filters assigned to object.
     * @param integer $lockWaitTimeout
     *
     * @return void
     * @throws LockedObjectException
     */
    public function addObjectToIndex(
        $objectId,
        $index,
        array $filters = [],
        $lockWaitTimeout = self::OBJECT_LOCK_WAIT_TIMEOUT
    ) {
        $this->waitForObjectLockOrFail($objectId, $lockWaitTimeout);

        try {
            list($isInIndex, $indexedFilters) = $this->inspectObjectInIndex(
                $objectId,
                $index
            );

            $transaction = $this->redis->createTransaction();

            if ($isInIndex) {
                $this->deIndexObject(
                    $objectId,
                    $index,
                    $indexedFilters,
                    $transaction
                );
            }

            $this->indexObject($objectId, $index, $filters, $transaction);
            $transaction->exec();

        } finally {
            $this->releaseObjectLock($objectId);
        }
    }

    /**
     * Removes object from index.
     *
     * @param mixed $objectId
     * @param string $index
     * @param integer $lockWaitTimeout
     *
     * @return boolean  Indicates whether object was removed.
     * @throws LockedObjectException
     */
    public function removeObjectFromIndex(
        $objectId,
        $index,
        $lockWaitTimeout = self::OBJECT_LOCK_WAIT_TIMEOUT
    ) {
        $this->waitForObjectLockOrFail($objectId, $lockWaitTimeout);

        try {
            list($isInIndex, $indexedFilters) = $this->inspectObjectInIndex(
                $objectId,
                $index
            );

            if ($isInIndex) {
                $transaction = $this->redis->createTransaction();
                $this->deIndexObject(
                    $objectId,
                    $index,
                    $indexedFilters,
                    $transaction
                );
                $transaction->exec();
            }
        } finally {
            $this->releaseObjectLock($objectId);
        }

        return $isInIndex;
    }

    /**
     * Removes object from all group indexes.
     *
     * @param mixed $objectId
     * @param integer $lockWaitTimeout
     *
     * @return array    Set of indexes where object was removed.
     * @throws LockedObjectException
     */
    public function removeObjectFromAllIndexes(
        $objectId,
        $lockWaitTimeout = self::OBJECT_LOCK_WAIT_TIMEOUT
    ) {
        $this->waitForObjectLockOrFail($objectId, $lockWaitTimeout);

        try {
            $indexesWithFilters = $this->inspectObject($objectId);

            if ($indexesWithFilters) {
                $transaction = $this->redis->createTransaction();

                $this->batchDeIndexObject(
                    $objectId,
                    $indexesWithFilters,
                    $transaction
                );

                $transaction->exec();
            }

        } finally {
            $this->releaseObjectLock($objectId);
        }

        // Just return indexes object removed from.
        return array_keys($indexesWithFilters);
    }

    /**
     * Moves object to index ensuring it only exists in target index.
     *
     * @param string $index
     * @param mixed $objectId
     * @param array $filters    Set of filters assigned to object.
     * @param integer $lockWaitTimeout
     *
     * @return array    Set of indexes where object was removed.
     * @throws LockedObjectException
     */
    public function moveObjectToIndex(
        $objectId,
        $index,
        array $filters = [],
        $lockWaitTimeout = self::OBJECT_LOCK_WAIT_TIMEOUT
    ) {
        $this->waitForObjectLockOrFail($objectId, $lockWaitTimeout);

        try {
            $transaction = $this->redis->createTransaction();

            $indexesWithFilters = $this->inspectObject($objectId);

            if ($indexesWithFilters) {
                $this->batchDeIndexObject(
                    $objectId,
                    $indexesWithFilters,
                    $transaction
                );
            }

            $this->indexObject($objectId, $index, $filters, $transaction);

            $transaction->exec();
        } finally {
            $this->releaseObjectLock($objectId);
        }

        // Return indexes object reemoved from.
        // Make sure not to include $index.
        $removedFromIndexes = array_keys($indexesWithFilters);
        return array_diff($removedFromIndexes, [$index]);
    }

    /**
     * Returns Set containing objects asssigned to index.
     *
     * @param string $index
     * @return PersistedSet
     */
    public function getIndexGlobalSet($index)
    {
        $setKey = $this->getIndexGlobalSetKey($index);
        return new PersistedSet($this->redis, $setKey);
    }

    /**
     * Returns Set containing objects assigned to index + filter.
     *
     * @param string $index
     * @param mixed $filter
     *
     * @return PersistedSet
     */
    public function getIndexFilterSet($index, $filter)
    {
        $setKey = $this->getIndexFilterSetKey($index, $filter);
        return new PersistedSet($this->redis, $setKey);
    }

    /**
     *
     */
    public function createUnionOfIndexFilters($index, array $filters)
    {
        $union = $this->createUnion();
        foreach ($filters as $filter) {
            $union->addSet($this->getIndexFilterSet($index, $filter));
        }
        return $union;
    }

    /**
     *
     */
    public function createIntersectionOfIndexFilters($index, array $filters)
    {
        $intersection = $this->createIntersection();
        foreach ($filters as $filter) {
            $intersection->addSet($this->getIndexFilterSet($index, $filter));
        }
        return $intersection;
    }

    /**
     * Creates UNION set using this group's index manager.
     *
     * @return UnionSet
     */
    public function createUnion()
    {
        return new UnionSet($this->redis);
    }

    /**
     * Creates INTERSECTION set using this group's index manager.
     *
     * @return IntersectionSet
     */
    public function createIntersection()
    {
        return new IntersectionSet($this->redis);
    }

    public function createSetForIndexGroupedFilterQuery(
        $index,
        GroupedFilterQuery $query
    ) {
        switch ($query->getGroupCount()) {
            case 0:
                $set = $this->getIndexGlobalSet($index);
                break;

            case 1:
                $filters = $query->current();
                $set = $this->createUnionOfIndexFilters($index, $filters);
                break;

            default:
                $set = $this->createIntersection();

                foreach ($query as $filters) {
                    $union = $this->createUnionOfIndexFilters($index, $filters);
                    $set->addSet($union);
                }
                break;
        }

        return $set;
    }

    /**
     * Indicates whether object is in index.
     *
     * @param mixed $objectId
     * @param string $index
     *
     * @return boolean
     */
    public function isObjectInIndex($objectId, $index)
    {
        $indexesKey = $this->getObjectIndexMapKey($objectId);
        return $this->redis->hExists($indexesKey, $index);
    }

    /**
     * Returns all indexes containing object.
     *
     * @param mixed $objectId
     *
     * @return array [$index, ...]
     */
    public function getIndexesWithObject($objectId)
    {
        $indexesKey = $this->getObjectIndexMapKey($objectId);
        return $this->redis->hKeys($indexesKey);
    }

    /**
     * Flush all objects out of index.
     *
     * @param string $index
     *
     * @return void
     */
    public function flushIndex($index)
    {
        $indexKeys = $this->getExistingIndexKeys($index);
        $this->redis->del($indexKeys);
    }

    /**
     * Flushes all objects from all group indexes.
     *
     * @return void
     */
    public function flushAllIndexes()
    {
        $groupKeys = $this->getExistingGroupKeys();
        $this->redis->del($groupKeys);
    }

    /**
     * Attempts to acquire object lock.
     *
     * @param mixed $objectId
     *
     * @return boolean Indicates whether lock acquired.
     */
    protected function acquireObjectLock($objectId)
    {
        $lockKey = $this->getObjectLockKey($objectId);
        $params = ['NX', 'EX' => $this->objectLockTtl];
        $result = $this->redis->set($lockKey, 1, $params);
        return $result;
    }

    /**
     * Continually tries to acquire object's lock until wait timeout.
     *
     * @param mixed $objectId
     * @param integer $lockWaitTimeout
     *
     * @return boolean
     * @throws InvalidArgumentException
     */
    protected function waitForObjectLock($objectId, $lockWaitTimeout)
    {
        if (is_int($lockWaitTimeout) === false) {
            throw new \InvalidArgumentException("lockWaitTimeout {$lockWaitTimeout} must be integer");
        }

        $timeout = time() + $lockWaitTimeout;
        $lockAcquired = false;

        do {
            $lockAcquired = $this->acquireObjectLock($objectId);
        } while ($lockAcquired === false && time() < $timeout);

        return $lockAcquired;
    }

    /**
     * Continually tries to acquire object's lock or fails if exceeds timeout.
     *
     * @param mixed $objectId
     * @param integer $lockWaitTimeout
     *
     * @return void
     * @throws LockedObjectException
     */
    protected function waitForObjectLockOrFail($objectId, $lockWaitTimeout)
    {
        $lockAcquired = $this->waitForObjectLock($objectId, $lockWaitTimeout);

        if ($lockAcquired === false) {
            throw new LockedObjectException("Exceeded lock wait timeout ({$lockWaitTimeout} seconds) for objectId {$objectId}");
        }
    }

    /**
     * Releases object lock.
     *
     * @param mixed $objectId
     *
     * @return void
     */
    protected function releaseObjectLock($objectId)
    {
        $lockKey = $this->getObjectLockKey($objectId);
        $this->redis->del($lockKey);
    }

    /**
     * Returns whether object is in index, and if so, which filters have been
     * assigned.
     *
     * @param mixed $objectId
     * @param string $index
     *
     * @return array [$isInIndex, [$filter1, ...]]
     */
    protected function inspectObjectInIndex($objectId, $index)
    {
        $indexesKey = $this->getObjectIndexMapKey($objectId);
        $result = $this->redis->hGet($indexesKey, $index);

        if ($result == false) {
            $isInIndex = false;
            $indexedFilters = [];
        } else {
            $isInIndex = true;
            $indexedFilters = $this->decodeIndexedFilters($result);
        }

        return [$isInIndex, $indexedFilters];
    }

    /**
     * Returns object's map of assigned indexes and filters.
     *
     * @param mixed $objectId
     *
     * @return array [$indexName => [$filter1, ...], ...]
     */
    protected function inspectObject($objectId)
    {
        $indexesKey = $this->getObjectIndexMapKey($objectId);
        $result = $this->redis->hGetAll($indexesKey);

        if ($result == false) {
            $indexesWithFilters = [];
        } else {
            $indexesWithFilters = array_map(
                [$this, 'decodeIndexedFilters'],
                $result
            );
        }

        return $indexesWithFilters;
    }

    /**
     * Encodes filters for storage in object's index map.
     *
     * @param array $filters
     *
     * @return string
     */
    protected function encodeIndexedFilters(array $filters)
    {
        return json_encode($filters);
    }

    /**
     * Decodes filters retrieved from object's index map.
     *
     * @param string $encodedFilters
     *
     * @return array
     * @throws RuntimeException
     */
    protected function decodeIndexedFilters($encodedFilters)
    {
        $filters = json_decode($encodedFilters);

        if (is_null($filters)) {
            $errorCode = json_last_error();
            throw new \RuntimeException("Failed to JSON decode indexed filters (err: {$errorCode}): {$encodedFilters}");
        }

        return $filters;
    }

    /**
     * Adds call to add object to index.
     *
     * @param mixed $objectId
     * @param string $index
     * @param array $filters
     * @param Multi $multi
     *
     * @return void
     */
    protected function indexObject(
        $objectId,
        $index,
        array $filters = [],
        Multi $multi
    ) {
        $this->addToObjectIndexMap($objectId, $index, $filters, $multi);
        $this->addObjectToIndexGlobalSet($objectId, $index, $multi);

        foreach ($filters as $filter) {
            $this->addObjectToIndexFilterSet($objectId, $index, $filter, $multi);
        }
    }

    /**
     * Adds calls to remove object from index.
     *
     * @param mixed $objectId
     * @param string $index
     * @param array $filters
     * @param Multi $multi
     *
     * @return void
     */
    protected function deIndexObject(
        $objectId,
        $index,
        array $filters = [],
        Multi $multi
    ) {
        $this->removeFromObjectIndexMap($objectId, $index, $multi);
        $this->removeObjectFromIndexGlobalSet($objectId, $index, $multi);

        foreach ($filters as $filter) {
            $this->removeObjectFromIndexFilterSet(
                $objectId,
                $index,
                $filter,
                $multi
            );
        }
    }

    /**
     * Add calls to remove object from multiple indexes.
     *
     * @param mixed $objectId
     * @param array $indexesWithFilters As returned from #inspectObject
     * @param Multi $multi
     *
     * @return void
     */
    protected function batchDeIndexObject(
        $objectId,
        array $indexesWithFilters,
        Multi $multi
    ) {
        foreach ($indexesWithFilters as $index => $indexedFilters) {
            $this->deIndexObject($objectId, $index, $indexedFilters, $multi);
        }
    }

    /**
     * Adds call to add index to object's index map.
     *
     * @param mixed $objectId
     * @param string $index
     * @param array $filters
     * @param Multi $multi
     *
     * @return void
     */
    protected function addToObjectIndexMap(
        $objectId,
        $index,
        array $filters = [],
        Multi $multi
    ) {
        $objectIndexesKey = $this->getObjectIndexMapKey($objectId);
        $encodedFilters = $this->encodeIndexedFilters($filters);
        $multi->hSet($objectIndexesKey, $index, $encodedFilters);
    }

    /**
     * Adds call to remove index from object index map.
     *
     * @param mixed $objectId
     * @param string $index
     * @param Multi $multi
     *
     * @return void
     */
    protected function removeFromObjectIndexMap($objectId, $index, Multi $multi)
    {
        $objectIndexesKey = $this->getObjectIndexMapKey($objectId);
        $multi->hDel($objectIndexesKey, $index);
    }

    /**
     * Adds call to add object into index's global set.
     *
     * @param mixed $objectId
     * @param string $index
     * @param Multi $multi
     *
     * @return void
     */
    protected function addObjectToIndexGlobalSet($objectId, $index, Multi $multi)
    {
        $indexGlobalKey = $this->getIndexGlobalSetKey($index);
        $multi->sAdd($indexGlobalKey, $objectId);
    }

    /**
     * Adds call to remove object from index's global set.
     *
     * @param mixed $objectId
     * @param string $index
     * @param Multi $multi
     *
     * @return void
     */
    protected function removeObjectFromIndexGlobalSet(
        $objectId,
        $index,
        Multi $multi
    ) {
        $indexGlobalKey = $this->getIndexGlobalSetKey($index);
        $multi->sRem($indexGlobalKey, $objectId);
    }

    /**
     * Adds call to add object into index filter set.
     *
     * @param mixed $objectId
     * @param string $index
     * @param mixed $filter
     * @param Multi $multi
     *
     * @return void
     */
    protected function addObjectToIndexFilterSet(
        $objectId,
        $index,
        $filter,
        Multi $multi
    ) {
        $indexFilterKey = $this->getIndexFilterSetKey($index, $filter);
        $multi->sAdd($indexFilterKey, $objectId);
    }

    /**
     * Adds call to remove object from index filter set.
     *
     * @param mixed $objectId
     * @param string $index
     * @param mixed $filter
     * @param Multi $multi
     *
     * @return void
     */
    protected function removeObjectFromIndexFilterSet(
        $objectId,
        $index,
        $filter,
        Multi $multi
    ) {
        $indexFilterKey = $this->getIndexFilterSetKey($index, $filter);
        $multi->sRem($indexFilterKey, $objectId);
    }

    /**
     * Returns Redis keys defined for group.
     * @return array
     */
    protected function getExistingGroupKeys()
    {
        $groupKeyPattern = $this->qualifyKey('*');
        return $this->redis->scanForKeys($groupKeyPattern);
    }

    /**
     * Returns Redis keys defined for index.
     * @param string $index
     * @return array
     */
    protected function getExistingIndexKeys($index)
    {
        $indexKeyPattern = $this->qualifyIndexKey($index, '*');
        return $this->redis->scanForKeys($indexKeyPattern);
    }

    /**
     * Returns fully qualified object lock key.
     * @param mixed $objectId
     * @return string
     */
    protected function getObjectLockKey($objectId)
    {
        return $this->qualifyObjectKey($objectId, "lock");
    }

    /**
     * Returns fully qualified object indexes key.
     * @param mixed $objectId
     * @return string
     */
    protected function getObjectIndexMapKey($objectId)
    {
        return $this->qualifyObjectKey($objectId, "map");
    }

    /**
     * Returns fully qualified index "global" filter key.
     * @param string $index
     * @return string
     */
    protected function getIndexGlobalSetKey($index)
    {
        return $this->qualifyIndexKey($index, "g");
    }

    /**
     * Returns fully qualified index filter key.
     * @param string $index
     * @param string $filter
     * @return string
     */
    protected function getIndexFilterSetKey($index, $filter)
    {
        return $this->qualifyIndexKey($index, "f:{$filter}");
    }

    /**
     * Returns fully qualified index filter keys.
     * @param string $index
     * @param array $filters
     * @return array
     */
    protected function getIndexFilterSetKeys($index, array $filters)
    {
        return array_map(
            function($filter) use ($index) {
                return $this->getIndexFilterSetKey($index, $filter);
            },
            $filters
        );
    }

    /**
     * Returns fully qualified object-specific key.
     * @param mixed $objectId
     * @param string $key
     * @return string
     */
    protected function qualifyObjectKey($objectId, $key)
    {
        return $this->qualifyKey("obj:{$objectId}:{$key}");
    }

    /**
     * Returns fully qualified index-specific key.
     * @param string $index
     * @param string $key
     * @return string
     */
    protected function qualifyIndexKey($index, $key)
    {
        return $this->qualifyKey("idx:{$index}:{$key}");
    }

    /**
     * Fully qualifies Redis key.
     * @param string $key
     * @return string
     */
    protected function qualifyKey($key)
    {
        $qualifiedKey = self::KEY_ROOT;

        if ($this->tenantNamespace) {
            $qualifiedKey .= ":{$this->tenantNamespace}";
        }

        $qualifiedKey .= ":{$this->groupName}:{$key}";
        return $qualifiedKey;
    }


}
