<?php
namespace Celltrak\FilteredObjectIndexBundle\Component\Set;

use Celltrak\RedisBundle\Component\Multi\Pipeline;


/**
 * Defines Set already persisted in Redis.
 */
class PersistedSet extends BaseSet
{

    /**
     * @var string
     * Set's Redis key.
     */
    protected $persistedSetKey;

    /**
     * @param FilteredObjectIndexManager $indexManager
     * @param string $persistedSetKey
     */
    public function __construct(
        FilteredObjectIndexManager $indexManager,
        $persistedSetKey
    ) {
        parent::__construct($indexManager);

        $this->persistedSetKey = $persistedSetKey;
    }

    /**
     * {@inheritDoc}
     */
    public function count()
    {
        if ($this->hasLoadedObjectIds()) {
            $objectCount = count($this->objectIds);
        } else {
            $objectCount = $this->indexManager
                ->getRedis()
                ->sCard($this->persistedSetKey);
        }

        return $objectCount;
    }

    /**
     * {@inheritDoc}
     */
    public function getObjectIds()
    {
        $this->loadObjectIdsOnce();
        return $this->objectIds;
    }

    /**
     * {@inheritDoc}
     */
    public function hasObject($objectId)
    {
        if ($this->hasLoadedObjectIds()) {
            $hasObject = in_array($objectId, $this->objectIds);
        } else {
            $hasObject = $this->indexManager
                ->getRedis()
                ->sIsMember($this->persistedSetKey, $objectId);
        }

        return $hasObject;
    }

    /**
     * {@inheritDoc}
     */
    public function initializeForCalculation(Pipeline $pipeline)
    {
        // Persisted Set is already to go. Just return its key.
        return $this->persistedSetKey;
    }

    /**
     * Returns $persistedSetKey.
     * @return string
     */
    public function getPersistedSetKey()
    {
        return $this->persistedSetKey;
    }

    /**
     * {@inheritDoc}
     */
    protected function loadObjectIds()
    {
        $this->objectIds = $this->indexManager
            ->getRedis()
            ->sMembers($this->persistedSetKey) ?: [];
    }

}
